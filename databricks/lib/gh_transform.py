"""Transformações PySpark do Guiding Hand (Bronze -> Prata -> Ouro).

Este módulo é usado pelos notebooks do Databricks E pelos testes locais
(tests/test_transformacoes.py). Ele não depende de Delta nem de Auto Loader:
recebe e devolve DataFrames. A escrita (Auto Loader, MERGE, CREATE TABLE)
fica nos notebooks.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# As UDFs deste módulo são executadas nos workers. Serializar o módulo "por valor"
# evita ModuleNotFoundError nos workers (local e Databricks serverless), já que o
# módulo vive numa pasta do workspace e não num pacote instalado.
try:
    import sys as _sys
    from pyspark import cloudpickle as _cp
    _cp.register_pickle_by_value(_sys.modules[__name__])
except Exception:  # pragma: no cover - versões antigas do PySpark
    pass

# ------------------------------------------------------------------ constantes
RECORTE = {"sul": -23.5750, "oeste": -46.7100, "norte": -23.5600, "leste": -46.6940}
LIMIAR_CONFIANCA = 0.5
FONTES = ["clima", "osm", "eventos_oculos", "sp156", "olhovivo"]

TIPOS_OBSTACULO = {
    # tipo: (descricao, altura, a bengala costuma detectar?)
    "placa": ("Placa ou letreiro", "acima_cintura", False),
    "galho_arvore": ("Galho de árvore baixo", "acima_cintura", False),
    "orelhao": ("Orelhão", "acima_cintura", False),
    "poste": ("Poste", "acima_cintura", True),
    "cacamba": ("Caçamba de entulho", "abaixo_cintura", True),
    "carro_calcada": ("Carro sobre a calçada", "abaixo_cintura", True),
    "lixeira": ("Lixeira", "abaixo_cintura", True),
    "entulho": ("Entulho ou material de obra", "abaixo_cintura", True),
    "buraco": ("Buraco", "chao", True),
    "calcada_irregular": ("Calçada irregular ou quebrada", "chao", True),
    "pessoa": ("Pessoa em movimento (não é barreira)", "movel", True),
}
TIPOS_BARREIRA = [t for t in TIPOS_OBSTACULO if t != "pessoa"]

# Palavras-chave do nome do serviço SP156 -> tipo de obstáculo do Guiding Hand.
MAPA_SERVICO_SP156 = [
    ("buraco", "buraco"), ("pavimenta", "buraco"), ("asfalto", "buraco"),
    ("calcada", "calcada_irregular"), ("calçada", "calcada_irregular"), ("passeio", "calcada_irregular"),
    ("poda", "galho_arvore"), ("arvore", "galho_arvore"), ("árvore", "galho_arvore"),
    ("placa", "placa"), ("sinaliza", "placa"),
    ("poste", "poste"), ("ilumina", "poste"),
    ("lixeira", "lixeira"), ("entulho", "entulho"), ("cacamba", "cacamba"), ("caçamba", "cacamba"),
    ("veiculo", "carro_calcada"), ("veículo", "carro_calcada"),
]

# Nomes candidatos dos campos SP156 no GeoSampa (confirmados na validação de fontes;
# a função escolhe o primeiro que existir, sem diferenciar maiúsculas).
CAMPOS_SP156 = {
    "protocolo": ["protocolo", "nr_protocolo", "numero_protocolo", "cd_protocolo", "id_solicitacao", "id"],
    "servico": ["servico", "nm_servico", "assunto", "tema", "tipo_servico", "ds_servico"],
    "status": ["status", "situacao", "status_solicitacao", "ds_status"],
    "data_abertura": ["data_abertura", "dt_abertura", "data_solicitacao", "dt_solicitacao", "data_registro"],
    "data_conclusao": ["data_conclusao", "dt_conclusao", "data_encerramento", "dt_encerramento",
                       "data_parecer", "dt_parecer"],
}

# ------------------------------------------------------------------ geohash
_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def geohash_encode(lat: float | None, lon: float | None, precisao: int = 8) -> str | None:
    if lat is None or lon is None:
        return None
    lat_int, lon_int = [-90.0, 90.0], [-180.0, 180.0]
    bits, bit, ch, par = [16, 8, 4, 2, 1], 0, 0, True
    resultado = []
    while len(resultado) < precisao:
        intervalo, valor = (lon_int, lon) if par else (lat_int, lat)
        meio = (intervalo[0] + intervalo[1]) / 2
        if valor > meio:
            ch |= bits[bit]
            intervalo[0] = meio
        else:
            intervalo[1] = meio
        par = not par
        if bit < 4:
            bit += 1
        else:
            resultado.append(_BASE32[ch])
            bit, ch = 0, 0
    return "".join(resultado)


def geohash_centro(gh: str | None) -> tuple[float, float] | None:
    if not gh:
        return None
    lat_int, lon_int, par = [-90.0, 90.0], [-180.0, 180.0], True
    for c in gh:
        cd = _BASE32.index(c)
        for mascara in (16, 8, 4, 2, 1):
            intervalo = lon_int if par else lat_int
            meio = (intervalo[0] + intervalo[1]) / 2
            if cd & mascara:
                intervalo[0] = meio
            else:
                intervalo[1] = meio
            par = not par
    return ((lat_int[0] + lat_int[1]) / 2, (lon_int[0] + lon_int[1]) / 2)


geohash_udf = F.udf(geohash_encode, T.StringType())
_centro_schema = T.StructType([T.StructField("lat", T.DoubleType()), T.StructField("lon", T.DoubleType())])
geohash_centro_udf = F.udf(lambda g: geohash_centro(g), _centro_schema)


def com_centro_celula(df: DataFrame, coluna_gh: str = "geohash8", prefixo: str = "") -> DataFrame:
    """Adiciona lat/lon do centro da célula geohash (calculado em DataFrame, sem UDF no SQL)."""
    return (df.withColumn("_c", geohash_centro_udf(coluna_gh))
              .withColumn(f"{prefixo}lat_celula", F.round("_c.lat", 5))
              .withColumn(f"{prefixo}lon_celula", F.round("_c.lon", 5))
              .drop("_c"))


def dentro_recorte(lat: Column, lon: Column, r: dict = RECORTE) -> Column:
    return lat.between(r["sul"], r["norte"]) & lon.between(r["oeste"], r["leste"])


# ------------------------------------------------------------------ BRONZE
PADRAO_ARQUIVO = r"([a-z0-9_]+)_(\d{4}-\d{2}-\d{2})\.json$"


def enriquecer_bronze(df_texto: DataFrame, caminho_col: str = "_metadata.file_path") -> DataFrame:
    """Recebe o DataFrame lido como texto inteiro (uma linha por arquivo) e adiciona
    metadados de origem e carga. O conteúdo bruto é preservado sem alteração."""
    caminho = F.col(caminho_col)
    return (df_texto
            .withColumnRenamed("value", "conteudo_bruto")
            .withColumn("arquivo_origem", caminho)
            .withColumn("nome_arquivo", F.regexp_extract(caminho, r"([^/]+)$", 1))
            .withColumn("fonte", F.regexp_extract(caminho, PADRAO_ARQUIVO, 1))
            .withColumn("data_coleta", F.to_date(F.regexp_extract(caminho, PADRAO_ARQUIVO, 2)))
            .withColumn("tamanho_bytes", F.length("conteudo_bruto"))
            .withColumn("hash_conteudo", F.sha2("conteudo_bruto", 256))
            .withColumn("carregado_em", F.current_timestamp()))


# ------------------------------------------------------------------ PRATA: utilidades
def _primeira_regra(*regras: tuple[str, Column]) -> Column:
    """Devolve o código da primeira regra violada (ou NULL se o registro é válido)."""
    expr = F.lit(None).cast("string")
    for codigo, violou in reversed(regras):
        expr = F.when(violou, F.lit(codigo)).otherwise(expr)
    return expr


def _mais_recente(df: DataFrame, chave: list[str], ordem: Column) -> DataFrame:
    from pyspark.sql.window import Window
    w = Window.partitionBy(*chave).orderBy(ordem.desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def separar(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    validos = df.filter(F.col("regra_violada").isNull()).drop("regra_violada")
    barrados = df.filter(F.col("regra_violada").isNotNull())
    return validos, barrados


def para_quarentena(df_barrados: DataFrame, fonte: str, chave_col: str) -> DataFrame:
    colunas = [c for c in df_barrados.columns if c not in ("regra_violada",)]
    return df_barrados.select(
        F.lit(fonte).alias("fonte"),
        F.coalesce(F.col(chave_col).cast("string"), F.sha2(F.to_json(F.struct(*colunas)), 256)).alias("chave_registro"),
        F.col("regra_violada").alias("regra"),
        F.col("data_coleta"),
        F.col("arquivo_origem"),
        F.to_json(F.struct(*colunas)).alias("registro_json"),
        F.current_timestamp().alias("avaliado_em"),
    )


# ------------------------------------------------------------------ PRATA: eventos dos óculos
SCHEMA_EVENTOS = T.StructType([
    T.StructField("gerador", T.StringType()),
    T.StructField("data_referencia", T.StringType()),
    T.StructField("eventos", T.ArrayType(T.StructType([
        T.StructField("event_id", T.StringType()),
        T.StructField("device_id", T.StringType()),
        T.StructField("ts_deteccao", T.StringType()),
        T.StructField("tipo_obstaculo", T.StringType()),
        T.StructField("confianca", T.DoubleType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("distancia_m", T.DoubleType()),
        T.StructField("altura", T.StringType()),
        T.StructField("modelo_versao", T.StringType()),
        T.StructField("origem", T.StringType()),
    ]))),
])


def eventos_da_bronze(bronze: DataFrame) -> DataFrame:
    return (bronze.filter(F.col("fonte") == "eventos_oculos")
            .withColumn("j", F.from_json("conteudo_bruto", SCHEMA_EVENTOS))
            .select("arquivo_origem", "data_coleta", "carregado_em", F.explode("j.eventos").alias("e"))
            .select(
                F.trim("e.event_id").alias("event_id"),
                F.trim("e.device_id").alias("device_id"),
                F.to_timestamp("e.ts_deteccao", "yyyy-MM-dd'T'HH:mm:ssXXX").alias("ts_deteccao"),
                F.lower(F.trim("e.tipo_obstaculo")).alias("tipo_obstaculo"),
                F.col("e.confianca").alias("confianca"),
                F.col("e.lat").alias("lat"), F.col("e.lon").alias("lon"),
                F.col("e.distancia_m").alias("distancia_m"),
                F.lower(F.trim("e.altura")).alias("altura"),
                F.col("e.modelo_versao").alias("modelo_versao"),
                F.coalesce(F.col("e.origem"), F.lit("desconhecida")).alias("origem"),
                "arquivo_origem", "data_coleta", "carregado_em"))


def validar_eventos(df: DataFrame, recorte: dict = RECORTE) -> DataFrame:
    tipos = list(TIPOS_OBSTACULO)
    return df.withColumn("regra_violada", _primeira_regra(
        ("RQ1_CAMPOS_OBRIGATORIOS",
         F.col("event_id").isNull() | F.col("ts_deteccao").isNull()
         | F.col("lat").isNull() | F.col("lon").isNull()),
        ("RQ2_FORA_DO_RECORTE", ~dentro_recorte(F.col("lat"), F.col("lon"), recorte)),
        ("RQ3_TIPO_FORA_DO_DOMINIO", F.col("tipo_obstaculo").isNull() | ~F.col("tipo_obstaculo").isin(tipos)),
        ("RQ4_CONFIANCA_INVALIDA", ~F.col("confianca").between(0.0, 1.0) | F.col("confianca").isNull()),
        ("RQ5_CONFIANCA_ABAIXO_DO_LIMIAR", F.col("confianca") < LIMIAR_CONFIANCA),
    ))


def prata_eventos(bronze: DataFrame, salt: str, recorte: dict = RECORTE) -> tuple[DataFrame, DataFrame]:
    """Retorna (eventos válidos deduplicados e protegidos, registros barrados)."""
    validos, barrados = separar(validar_eventos(eventos_da_bronze(bronze), recorte))
    validos = _mais_recente(validos, ["event_id"], F.col("carregado_em"))
    validos = (validos
               .withColumn("device_hash", F.sha2(F.concat(F.col("device_id"), F.lit(salt)), 256))
               .withColumn("geohash8", geohash_udf("lat", "lon", F.lit(8)))
               .withColumn("geohash7", F.substring("geohash8", 1, 7))
               .withColumn("eh_barreira", F.col("tipo_obstaculo").isin(TIPOS_BARREIRA))
               # chave de célula: tipo + geohash8 (~38 m x 19 m). A identidade final da
               # barreira (que junta células vizinhas) é resolvida na Ouro.
               .withColumn("celula_key", F.when(F.col("eh_barreira"),
                                                F.sha2(F.concat_ws("|", "tipo_obstaculo", "geohash8"), 256)))
               # minimização: coordenada exata fica só na Bronze; Prata guarda ~11 m
               .withColumn("lat_aprox", F.round("lat", 4))
               .withColumn("lon_aprox", F.round("lon", 4))
               .drop("device_id", "lat", "lon"))
    return com_centro_celula(validos), barrados


# ------------------------------------------------------------------ OURO: resolução de entidade
RAIO_MESMA_BARREIRA_M = 20.0


def _distancia_m(lat1, lon1, lat2, lon2) -> float:
    import math
    dy = (lat2 - lat1) * 111_320.0
    dx = (lon2 - lon1) * 111_320.0 * math.cos(math.radians((lat1 + lat2) / 2))
    return math.hypot(dx, dy)


def consolidar_barreiras(spark: SparkSession, eventos: DataFrame,
                         raio_m: float = RAIO_MESMA_BARREIRA_M) -> tuple[DataFrame, DataFrame]:
    """Junta células (tipo+geohash8) do mesmo tipo cujos centros distam <= raio_m.

    Motivo: o ruído do GPS faz detecções do MESMO obstáculo caírem em células vizinhas;
    contar cada célula como uma barreira inflaria o mapa. Executado no driver
    (união-busca), adequado ao volume do protótipo (centenas a poucos milhares de
    células). Em escala, substituir por clusterização espacial distribuída (ex.: H3).

    Retorna (dim_barreira_oculos, mapa_celula_barreira).
    """
    celulas = (eventos.filter("eh_barreira")
               .groupBy("celula_key", "tipo_obstaculo")
               .agg(F.avg("lat_aprox").alias("lat"), F.avg("lon_aprox").alias("lon"),
                    F.count("*").alias("n"))
               .collect())
    pai = {c["celula_key"]: c["celula_key"] for c in celulas}

    def raiz(k):
        while pai[k] != k:
            pai[k] = pai[pai[k]]
            k = pai[k]
        return k

    por_tipo: dict[str, list] = {}
    for c in celulas:
        por_tipo.setdefault(c["tipo_obstaculo"], []).append(c)
    for lista in por_tipo.values():
        for i, a in enumerate(lista):
            for b in lista[i + 1:]:
                if _distancia_m(a["lat"], a["lon"], b["lat"], b["lon"]) <= raio_m:
                    ra, rb = raiz(a["celula_key"]), raiz(b["celula_key"])
                    if ra != rb:
                        pai[max(ra, rb)] = min(ra, rb)

    grupos: dict[str, list] = {}
    for c in celulas:
        grupos.setdefault(raiz(c["celula_key"]), []).append(c)
    mapa, dim = [], []
    import hashlib
    for raiz_k, membros in grupos.items():
        tipo = membros[0]["tipo_obstaculo"]
        chave = hashlib.sha256(f"oculos|{tipo}|{raiz_k}".encode()).hexdigest()
        total = sum(m["n"] for m in membros)
        lat = sum(m["lat"] * m["n"] for m in membros) / total
        lon = sum(m["lon"] * m["n"] for m in membros) / total
        dim.append((chave, "oculos_sintetico", tipo, round(lat, 5), round(lon, 5),
                    geohash_encode(lat, lon, 7), len(membros)))
        mapa.extend((m["celula_key"], chave) for m in membros)
    esquema_dim = ("barreira_key string, origem string, tipo_obstaculo string, lat double, lon double, "
                   "geohash7 string, n_celulas int")
    return (spark.createDataFrame(dim, esquema_dim),
            spark.createDataFrame(mapa, "celula_key string, barreira_key string"))


# ------------------------------------------------------------------ PRATA: clima
SCHEMA_CLIMA = T.StructType([
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("timezone", T.StringType()),
    T.StructField("hourly", T.StructType([
        T.StructField("time", T.ArrayType(T.StringType())),
        T.StructField("temperature_2m", T.ArrayType(T.DoubleType())),
        T.StructField("precipitation", T.ArrayType(T.DoubleType())),
        T.StructField("rain", T.ArrayType(T.DoubleType())),
        T.StructField("wind_gusts_10m", T.ArrayType(T.DoubleType())),
        T.StructField("visibility", T.ArrayType(T.DoubleType())),
        T.StructField("weather_code", T.ArrayType(T.IntegerType())),
    ])),
])


def prata_clima(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    h = "j.hourly"
    df = (bronze.filter(F.col("fonte") == "clima")
          .withColumn("j", F.from_json("conteudo_bruto", SCHEMA_CLIMA))
          .select("arquivo_origem", "data_coleta", "carregado_em",
                  F.col("j.latitude").alias("lat_grade"), F.col("j.longitude").alias("lon_grade"),
                  F.explode(F.arrays_zip(f"{h}.time", f"{h}.temperature_2m", f"{h}.precipitation",
                                         f"{h}.rain", f"{h}.wind_gusts_10m", f"{h}.visibility",
                                         f"{h}.weather_code")).alias("z"))
          .select("arquivo_origem", "data_coleta", "carregado_em", "lat_grade", "lon_grade",
                  F.to_timestamp("z.time", "yyyy-MM-dd'T'HH:mm").alias("data_hora"),
                  F.col("z.temperature_2m").alias("temperatura_c"),
                  F.coalesce(F.col("z.precipitation"), F.lit(0.0)).alias("precipitacao_mm"),
                  F.col("z.rain").alias("chuva_mm"),
                  F.col("z.wind_gusts_10m").alias("rajada_kmh"),
                  F.col("z.visibility").alias("visibilidade_m"),
                  F.col("z.weather_code").alias("codigo_tempo")))
    df = df.withColumn("regra_violada", _primeira_regra(
        ("RQ1_CAMPOS_OBRIGATORIOS", F.col("data_hora").isNull()),
        ("RQ6_MEDIDA_FISICA_IMPOSSIVEL",
         (F.col("precipitacao_mm") < 0) | (F.col("visibilidade_m") < 0)
         | ~F.coalesce(F.col("temperatura_c"), F.lit(20.0)).between(-10, 50)),
    ))
    validos, barrados = separar(df)
    validos = _mais_recente(validos, ["data_hora"], F.col("carregado_em"))
    return validos.withColumn("choveu", F.col("precipitacao_mm") > 0), barrados


# ------------------------------------------------------------------ PRATA: OSM
SCHEMA_OSM = T.StructType([T.StructField("elements", T.ArrayType(T.StructType([
    T.StructField("type", T.StringType()),
    T.StructField("id", T.LongType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("lon", T.DoubleType()),
    T.StructField("center", T.StructType([T.StructField("lat", T.DoubleType()),
                                          T.StructField("lon", T.DoubleType())])),
    T.StructField("tags", T.MapType(T.StringType(), T.StringType())),
])))])


def _categoria_osm(tags: Column) -> Column:
    return (F.when(tags["tactile_paving"].isNotNull() | tags["kerb"].isNotNull(), "acessibilidade")
            .when(tags["highway"] == "bus_stop", "parada_onibus")
            .when(tags["highway"] == "crossing", "travessia")
            .when((tags["footway"] == "sidewalk") | (tags["highway"] == "footway"), "calcada")
            .when(tags["barrier"].isNotNull() | tags["power"].isNotNull() | tags["man_made"].isNotNull()
                  | (tags["highway"] == "street_lamp") | tags["amenity"].isNotNull()
                  | tags["natural"].isNotNull(), "obstaculo_potencial")
            .otherwise("outro"))


def _tipo_osm(tags: Column) -> Column:
    return (F.when((tags["power"] == "pole") | (tags["man_made"] == "utility_pole")
                   | (tags["highway"] == "street_lamp"), "poste")
            .when(tags["amenity"] == "waste_basket", "lixeira")
            .when(tags["natural"] == "tree", "galho_arvore"))


def prata_osm(bronze: DataFrame, recorte: dict = RECORTE) -> tuple[DataFrame, DataFrame]:
    df = (bronze.filter(F.col("fonte") == "osm")
          .withColumn("j", F.from_json("conteudo_bruto", SCHEMA_OSM))
          .select("arquivo_origem", "data_coleta", "carregado_em", F.explode("j.elements").alias("e"))
          .select("arquivo_origem", "data_coleta", "carregado_em",
                  F.concat_ws("/", "e.type", F.col("e.id").cast("string")).alias("osm_key"),
                  F.coalesce("e.lat", "e.center.lat").alias("lat"),
                  F.coalesce("e.lon", "e.center.lon").alias("lon"),
                  F.col("e.tags").alias("tags")))
    df = (df.withColumn("categoria", _categoria_osm(F.col("tags")))
          .withColumn("tipo_obstaculo", _tipo_osm(F.col("tags")))
          .withColumn("regra_violada", _primeira_regra(
              ("RQ1_CAMPOS_OBRIGATORIOS", F.col("osm_key").isNull() | F.col("lat").isNull()),
              ("RQ2_FORA_DO_RECORTE", ~dentro_recorte(F.col("lat"), F.col("lon"), recorte)))))
    validos, barrados = separar(df)
    validos = (_mais_recente(validos, ["osm_key"], F.col("carregado_em"))
               .withColumn("geohash8", geohash_udf("lat", "lon", F.lit(8)))
               .withColumn("geohash7", F.substring("geohash8", 1, 7))
               .withColumn("tags_json", F.to_json("tags")).drop("tags"))
    return com_centro_celula(validos), barrados


# ------------------------------------------------------------------ PRATA: SP156
SCHEMA_SP156 = T.StructType([T.StructField("camadas", T.ArrayType(T.StructType([
    T.StructField("camada", T.StringType()),
    T.StructField("resposta", T.StructType([T.StructField("features", T.ArrayType(T.StructType([
        T.StructField("id", T.StringType()),
        T.StructField("geometry", T.StructType([
            T.StructField("type", T.StringType()),
            T.StructField("coordinates", T.ArrayType(T.DoubleType()))])),
        T.StructField("properties", T.MapType(T.StringType(), T.StringType())),
    ])))])),
])))])


def _campo(props: Column, nomes: list[str]) -> Column:
    minusculas = F.transform_keys(props, lambda k, v: F.lower(k))
    return F.coalesce(*[minusculas[n] for n in nomes])


def _para_timestamp(c: Column) -> Column:
    formatos = ["yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss",
                "yyyy-MM-dd", "dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy"]
    return F.coalesce(*[F.try_to_timestamp(c, F.lit(f)) for f in formatos])


def _tipo_sp156(servico: Column) -> Column:
    s = F.lower(servico)
    expr = F.lit(None).cast("string")
    for chave, tipo in reversed(MAPA_SERVICO_SP156):
        expr = F.when(s.contains(chave), F.lit(tipo)).otherwise(expr)
    return expr


def _status_sp156(status: Column) -> Column:
    s = F.lower(status)
    return (F.when(s.rlike("finaliz|conclu|atendid|execut|encerrad"), "resolvida")
            .when(s.rlike("cancel|indefer|improced"), "cancelada")
            .when(s.isNull(), "desconhecido")
            .otherwise("aberta"))


def prata_sp156(bronze: DataFrame, recorte: dict = RECORTE) -> tuple[DataFrame, DataFrame]:
    df = (bronze.filter(F.col("fonte") == "sp156")
          .withColumn("j", F.from_json("conteudo_bruto", SCHEMA_SP156))
          .select("arquivo_origem", "data_coleta", "carregado_em", F.explode("j.camadas").alias("c"))
          .select("arquivo_origem", "data_coleta", "carregado_em", F.col("c.camada").alias("camada"),
                  F.explode("c.resposta.features").alias("f"))
          .withColumn("p", F.col("f.properties")))
    df = df.select(
        "arquivo_origem", "data_coleta", "carregado_em", "camada",
        F.coalesce(_campo(F.col("p"), CAMPOS_SP156["protocolo"]), F.col("f.id")).alias("protocolo"),
        _campo(F.col("p"), CAMPOS_SP156["servico"]).alias("servico"),
        _campo(F.col("p"), CAMPOS_SP156["status"]).alias("status_original"),
        _para_timestamp(_campo(F.col("p"), CAMPOS_SP156["data_abertura"])).alias("data_abertura"),
        _para_timestamp(_campo(F.col("p"), CAMPOS_SP156["data_conclusao"])).alias("data_conclusao"),
        F.col("f.geometry.coordinates")[1].alias("lat"),
        F.col("f.geometry.coordinates")[0].alias("lon"),
    )
    df = (df.withColumn("servico", F.coalesce("servico", "camada"))
          .withColumn("tipo_obstaculo", _tipo_sp156(F.col("servico")))
          .withColumn("status", _status_sp156(F.col("status_original")))
          .withColumn("regra_violada", _primeira_regra(
              ("RQ1_CAMPOS_OBRIGATORIOS", F.col("protocolo").isNull() | F.col("lat").isNull()),
              ("RQ2_FORA_DO_RECORTE", ~dentro_recorte(F.col("lat"), F.col("lon"), recorte)),
              ("RQ7_DATAS_INCOERENTES",
               F.col("data_conclusao").isNotNull() & F.col("data_abertura").isNotNull()
               & (F.col("data_conclusao") < F.col("data_abertura"))),
          )))
    validos, barrados = separar(df)
    validos = (_mais_recente(validos, ["protocolo"], F.col("carregado_em"))
               .withColumn("geohash8", geohash_udf("lat", "lon", F.lit(8)))
               .withColumn("geohash7", F.substring("geohash8", 1, 7))
               .withColumn("barreira_key", F.when(F.col("tipo_obstaculo").isNotNull(),
                                                  F.sha2(F.concat_ws("|", F.lit("sp156"), "protocolo"), 256))))
    return com_centro_celula(validos), barrados


# ------------------------------------------------------------------ PRATA: Olho Vivo
SCHEMA_OLHOVIVO = T.StructType([T.StructField("buscas", T.ArrayType(T.StructType([
    T.StructField("termo", T.StringType()),
    T.StructField("resposta", T.ArrayType(T.StructType([
        T.StructField("cp", T.LongType()), T.StructField("np", T.StringType()),
        T.StructField("ed", T.StringType()), T.StructField("py", T.DoubleType()),
        T.StructField("px", T.DoubleType())]))),
])))])


def prata_olhovivo(bronze: DataFrame, recorte: dict = RECORTE) -> tuple[DataFrame, DataFrame]:
    df = (bronze.filter(F.col("fonte") == "olhovivo")
          .withColumn("j", F.from_json("conteudo_bruto", SCHEMA_OLHOVIVO))
          .select("arquivo_origem", "data_coleta", "carregado_em", F.explode("j.buscas").alias("b"))
          .select("arquivo_origem", "data_coleta", "carregado_em", F.explode("b.resposta").alias("p"))
          .select("arquivo_origem", "data_coleta", "carregado_em",
                  F.col("p.cp").alias("codigo_parada"), F.initcap(F.trim("p.np")).alias("nome_parada"),
                  F.trim("p.ed").alias("endereco"), F.col("p.py").alias("lat"), F.col("p.px").alias("lon")))
    df = df.withColumn("regra_violada", _primeira_regra(
        ("RQ1_CAMPOS_OBRIGATORIOS", F.col("codigo_parada").isNull() | F.col("lat").isNull()),
        ("RQ2_FORA_DO_RECORTE", ~dentro_recorte(F.col("lat"), F.col("lon"), recorte))))
    validos, barrados = separar(df)
    return _mais_recente(validos, ["codigo_parada"], F.col("carregado_em")), barrados


# ------------------------------------------------------------------ OURO
def dim_tipo_obstaculo(spark: SparkSession) -> DataFrame:
    linhas = [(t, d, a, b, t != "pessoa") for t, (d, a, b) in TIPOS_OBSTACULO.items()]
    return spark.createDataFrame(linhas, "sk_tipo string, descricao string, altura string, "
                                         "bengala_costuma_detectar boolean, eh_barreira boolean")


def dim_origem(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([
        ("oculos_sintetico", "Detecções dos óculos (dados SINTÉTICOS do gerador sintetico_v1)", True),
        ("sp156", "Solicitações de zeladoria SP156 (Prefeitura de SP, via GeoSampa)", False),
        ("osm", "Infraestrutura mapeada no OpenStreetMap (ODbL)", False),
    ], "sk_origem string, descricao string, sintetico boolean")


def dim_local(prata_eventos: DataFrame, prata_sp156: DataFrame, prata_osm: DataFrame) -> DataFrame:
    celulas = (prata_eventos.select("geohash7").union(prata_sp156.select("geohash7"))
               .union(prata_osm.select("geohash7")).distinct().filter(F.col("geohash7").isNotNull()))
    return (com_centro_celula(celulas, "geohash7")
            .select(F.col("geohash7").alias("sk_local"),
                    F.col("lat_celula").alias("lat_centro"), F.col("lon_celula").alias("lon_centro"),
                    F.lit("Pinheiros - entorno da Estacao Pinheiros").alias("recorte"),
                    F.lit("celula geohash 7 (~153 m x 153 m)").alias("granularidade")))


def sql_merge(tabela: str, chaves: list[str], so_inserir: bool = False, origem: str = "_novo") -> str:
    """MERGE idempotente usado pelo notebook 02 (e validado pelos testes)."""
    cond = " AND ".join(f"alvo.{k} <=> novo.{k}" for k in chaves)
    atualiza = ("" if so_inserir else
                "WHEN MATCHED AND alvo.hash_registro <> novo.hash_registro THEN UPDATE SET * ")
    return (f"MERGE INTO {tabela} AS alvo USING {origem} AS novo ON {cond} "
            f"{atualiza}WHEN NOT MATCHED THEN INSERT *")
