"""Gera o relatório PDF da disciplina de Big Data NA ORDEM EXIGIDA PELA FIAP, a partir das
evidências reais do projeto. Nada é inventado: o que faltar aparece em VERMELHO como PENDENTE.

Entradas (todas opcionais; quanto mais, mais completo):
  docs/relatorio/integrantes.csv           nome,rm  (uma linha por integrante)
  docs/relatorio/config_relatorio.json     {"link_repositorio": "...", "link_mvp": "..."}
  docs/evidencias/evidencias_lote_<N>.json exportado pelo notebook 00_processar_lote (Databricks)
  docs/evidencias/airflow_*.json           gravado pelo menu 3 (estados e horários das tarefas)
  docs/validacao_fontes/relatorio_*.json   gravado pelo menu 1 (validação real das fontes)
  docs/lotes/lote_<N>.md                   gravado pelo menu 4
  docs/relatorio/prints/*.png              prints (nomes em PRINTS abaixo)

Uso:  python scripts/gerar_relatorio.py [--evidencias arquivo.json] [--saida arquivo.pdf]
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
from datetime import datetime
from pathlib import Path

from reportlab.graphics.shapes import Drawing, Line, Polygon, Rect, String
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (Image, KeepTogether, PageBreak, Paragraph, Preformatted, SimpleDocTemplate,
                                Spacer, Table, TableStyle)

RAIZ = Path(__file__).resolve().parents[1]
PRINTS = {
    "airflow_graph": ("01_airflow_graph.png", "Airflow — visão Graph com execução concluída"),
    "airflow_grid": ("02_airflow_grid.png", "Airflow — visão Grid com execuções de dias diferentes"),
    "airflow_variables": ("03_airflow_variables.png", "Airflow — Admin › Variables (chaves fora do código)"),
    "chegada": ("04_pasta_chegada.png", "Pasta chegada/ com um arquivo por fonte e por dia"),
    "upload_lote1": ("05_upload_lote1.png", "Upload do lote 1 no Volume chegada"),
    "upload_lote2": ("06_upload_lote2.png", "Volume chegada com os arquivos de 15/11 (lote 1) e 16/11 (lote 2)"),
    "bronze": ("07_bronze_contagens.png", "controle_execucoes_bronze — lote 1 (1ª execução e reexecuções) e lote 2"),
    "prata": ("08_prata_quarentena.png", "prata_quarentena — registros barrados por regra (execução do lote 1)"),
    "ouro": ("09_ouro_tabelas.png", "Notebook 03_ouro — tabelas da Ouro na execução do lote 1 (contagens finais na seção 7)"),
    "mvp": ("10_tela_mvp.png", "Tela do MVP com as 204 barreiras exportadas de tb_mvp_mapa_barreiras (19/11)"),
}

est = getSampleStyleSheet()
H1 = ParagraphStyle("h1", parent=est["Heading1"], fontSize=15, spaceBefore=10, spaceAfter=6,
                    textColor=colors.HexColor("#7a0c36"))
H2 = ParagraphStyle("h2", parent=est["Heading2"], fontSize=12, spaceBefore=8, spaceAfter=4)
TXT = ParagraphStyle("txt", parent=est["BodyText"], fontSize=9.5, leading=13)
PEQ = ParagraphStyle("peq", parent=TXT, fontSize=8, leading=10)
PEND = ParagraphStyle("pend", parent=TXT, textColor=colors.HexColor("#b00020"))
LEG = ParagraphStyle("leg", parent=PEQ, alignment=TA_CENTER, textColor=colors.HexColor("#444444"))
CODIGO = ParagraphStyle("cod", parent=PEQ, fontName="Courier", fontSize=7, leading=8.5)


def esc(t) -> str:
    return (str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def pendente(texto: str) -> Paragraph:
    return Paragraph(f"<b>PENDENTE:</b> {esc(texto)}", PEND)


def tabela(linhas: list[list], larguras=None, cab=True) -> Table:
    dados = [[Paragraph(esc("—" if c is None else c), PEQ) for c in l] for l in linhas]
    t = Table(dados, colWidths=larguras, repeatRows=1 if cab else 0)
    estilo = [("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#999999")),
              ("VALIGN", (0, 0), (-1, -1), "TOP")]
    if cab:
        estilo += [("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#ececec"))]
    t.setStyle(TableStyle(estilo))
    return t


def tabela_de_dicts(registros: list[dict], limite=12, larguras=None) -> Table | Paragraph:
    if not registros:
        return Paragraph("(consulta sem linhas)", PEQ)
    cols = list(registros[0])
    return tabela([cols] + [[r.get(c) for c in cols] for r in registros[:limite]], larguras)


def inventario_lotes():
    """Arquivos de chegada/ registrados por scripts/preparar_lote.py em docs/lotes/lote_N.md (dados reais)."""
    linhas = []
    for md in sorted((RAIZ / "docs" / "lotes").glob("lote_*.md")):
        for l in md.read_text(encoding="utf-8").splitlines():
            cel = [c.strip() for c in l.strip().strip("|").split("|")]
            if l.startswith("|") and len(cel) == 5 and cel[0].endswith(".json"):
                linhas.append([md.stem] + cel)
    return linhas


def figura(chave: str, largura=16 * cm, altura_max=18 * cm):
    nome, legenda = PRINTS[chave]
    caminho = RAIZ / "docs" / "relatorio" / "prints" / nome
    if not caminho.exists() and chave == "chegada" and inventario_lotes():
        return [Paragraph("Arquivos de <i>chegada/</i> registrados por scripts/preparar_lote.py (docs/lotes/): "
                          "um arquivo por fonte e por dia, com tamanho e SHA-256.", TXT),
                tabela([["Lote", "Arquivo", "Fonte", "Dia de coleta", "Bytes", "SHA-256 (12)"]] + inventario_lotes()),
                Spacer(1, 6)]
    if not caminho.exists():
        return [pendente(f"print '{legenda}' — salve como docs/relatorio/prints/{nome}")]
    img = Image(str(caminho))
    proporcao = img.imageHeight / img.imageWidth
    img.drawWidth, img.drawHeight = largura, largura * proporcao
    if img.drawHeight > altura_max:
        img.drawWidth, img.drawHeight = altura_max / proporcao, altura_max
    return [KeepTogether([img, Paragraph(f"Figura — {esc(legenda)}", LEG)]), Spacer(1, 6)]


def figura_par(chaves, largura=8 * cm):
    """Duas figuras lado a lado (mesma altura máxima), cada uma com a sua legenda."""
    celulas = []
    for chave in chaves:
        nome, legenda = PRINTS[chave]
        caminho = RAIZ / "docs" / "relatorio" / "prints" / nome
        if not caminho.exists():
            return [pendente(f"print '{legenda}' — salve como docs/relatorio/prints/{nome}")]
        img = Image(str(caminho))
        prop = img.imageHeight / img.imageWidth
        img.drawWidth, img.drawHeight = largura, largura * prop
        if img.drawHeight > 9.5 * cm:
            img.drawWidth, img.drawHeight = 9.5 * cm / prop, 9.5 * cm
        celulas.append([img, Paragraph(f"Figura — {esc(legenda)}", LEG)])
    t = Table([celulas], colWidths=[largura + 0.3 * cm] * len(chaves))
    t.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("ALIGN", (0, 0), (-1, -1), "CENTER")]))
    return [t, Spacer(1, 6)]


def diagrama() -> Drawing:
    d = Drawing(17 * cm, 4.2 * cm)
    caixas = ["Fontes\n(5)", "Airflow 3\n07:00", "chegada/", "Volume\n(upload)", "Bronze", "Prata", "Ouro", "MVP"]
    w, h, gap, y = 1.75 * cm, 1.3 * cm, 0.35 * cm, 1.6 * cm
    for i, nome in enumerate(caixas):
        x = i * (w + gap)
        cor = colors.HexColor("#7a0c36") if nome == "MVP" else colors.HexColor("#2b2b2b")
        d.add(Rect(x, y, w, h, fillColor=colors.white, strokeColor=cor, strokeWidth=1.2, rx=4, ry=4))
        for j, parte in enumerate(nome.split("\n")):
            d.add(String(x + w / 2, y + h - 0.5 * cm - j * 0.38 * cm, parte, fontSize=7.5, textAnchor="middle"))
        if i < len(caixas) - 1:
            xa = x + w
            d.add(Line(xa, y + h / 2, xa + gap, y + h / 2, strokeWidth=1))
            d.add(Polygon([xa + gap, y + h / 2, xa + gap - 4, y + h / 2 + 3, xa + gap - 4, y + h / 2 - 3],
                          fillColor=colors.black))
    d.add(String(0, 0.6 * cm, "Open-Meteo · OpenStreetMap · SP156/GeoSampa · Olho Vivo · eventos dos óculos (sintéticos)",
                 fontSize=7))
    d.add(String(0, 0.1 * cm, "Databricks Free Edition não se conecta ao Airflow: chegada/ -> Volume é manual, em 2 lotes",
                 fontSize=7))
    return d


# ---------------------------------------------------------------- interpretação factual
def interpretar(cid: str, r: list[dict]) -> str:
    if not r:
        return "A consulta não retornou linhas com os dados atuais."
    try:
        if cid == "C1":
            t = r[0]
            inv = sum(x["ocorrencias"] for x in r if x.get("invisivel_para_bengala"))
            tot = sum(x["ocorrencias"] for x in r)
            return (f"O tipo mais recorrente é '{t['tipo_obstaculo']}' ({t['ocorrencias']} ocorrências, origem "
                    f"{t['origem']}). {inv} de {tot} ocorrências ({100 * inv / max(tot, 1):.0f}%) são de obstáculos "
                    "que a bengala não costuma detectar — exatamente o público do alerta dos óculos.")
        if cid == "C2":
            t = r[0]
            return (f"A célula {t['celula_geohash7']} (centro {t['lat_centro']}, {t['lon_centro']}) concentra "
                    f"{t['barreiras_distintas']} barreiras distintas e {t['ocorrencias']} ocorrências: é a primeira "
                    "candidata para ação de zeladoria no recorte.")
        if cid == "C3":
            return (f"{len(r)} barreiras foram detectadas em mais de um dia de coleta. Recorrência entre coletas indica "
                    "barreira persistente, e não um falso alarme pontual. Os eventos dos óculos são sintéticos: o resultado "
                    "demonstra a lógica do pipeline, não o estado real das calçadas.")
        if cid == "C4":
            partes = [f"{x['condicao']}: {x['deteccoes']} detecções ({x['deteccoes_por_hora']} por hora)" for x in r]
            return ("; ".join(partes) + ". A comparação por hora evita confundir mais horas secas com mais barreiras. "
                    "O clima é real (Open-Meteo), mas as detecções são sintéticas e o gerador não usa o clima: a "
                    "diferença NÃO indica relação causal entre chuva e barreiras.")
        if cid == "C5":
            partes = []
            for x in r:
                partes.append(f"origem {x['origem']}: {x['barreiras_mapeadas']} barreiras mapeadas, "
                              f"{x['invisiveis_para_bengala']} invisíveis à bengala ({x['pct_invisiveis_para_bengala']}%), "
                              f"{x['elegiveis_confirmacao_rpa']} prontas para confirmação pelo RPA, "
                              f"{x['resolvidas_oficialmente']} resolvidas pela zeladoria")
            return ("Impacto: " + "; ".join(partes) + ". Cada barreira invisível à bengala que entra no mapa é um "
                    "risco de queda ou batida na cabeça que passa a ter alerta e encaminhamento. Ressalvas: os registros "
                    "do SP156 são históricos (2020), e as barreiras dos óculos vêm de eventos sintéticos.")
        if cid == "C6":
            ident = sum(x["identificadas"] for x in r)
            resol = sum(x["resolvidas"] for x in r)
            return (f"O arquivo de evidências guarda as {len(r)} primeiras datas do resultado (a tabela mostra as 10 "
                    f"primeiras): nelas há {ident} identificações e {resol} resoluções. O resultado completo, no notebook "
                    "04_consultas_mvp, cobre todos os dias com movimento. As datas de 2020 vêm do histórico do SP156; as "
                    "de novembro de 2026, dos eventos sintéticos.")
    except (KeyError, TypeError, IndexError):
        pass
    return "Ver tabela de resultados."


# ---------------------------------------------------------------- carregamento
def ultimo(padrao: str) -> Path | None:
    arquivos = sorted(glob.glob(str(RAIZ / padrao)))
    return Path(arquivos[-1]) if arquivos else None


def carregar_evidencias(caminho: str | None) -> dict | None:
    p = Path(caminho) if caminho else ultimo("docs/evidencias/evidencias_lote_*.json")
    return json.loads(p.read_text(encoding="utf-8-sig")) if p and p.exists() else None


def resumo_c5(ev: dict) -> str:
    """Distingue mapeadas, prontas para confirmação e resolvidas, com os números da consulta C5."""
    c5 = next((c for c in ev.get("consultas", []) if c["id"] == "C5"), None)
    if not c5 or not c5["resultado"]:
        return ""
    try:
        conf = sum(int(x["elegiveis_confirmacao_rpa"]) for x in c5["resultado"])
        resol = sum(int(x["resolvidas_oficialmente"]) for x in c5["resultado"])
    except (KeyError, TypeError, ValueError):
        return ""
    return (f" (C5: {conf} prontas para confirmação e {resol} resolvidas pela zeladoria, estas registros históricos do "
            "SP156 de 2020)")


def integrantes() -> list[list[str]]:
    p = RAIZ / "docs" / "relatorio" / "integrantes.csv"
    if not p.exists():
        return []
    with p.open(encoding="utf-8-sig") as fh:
        return [[l["nome"].strip(), l["rm"].strip()] for l in csv.DictReader(fh)
                if l.get("nome") and not l["nome"].startswith("(")]


# ---------------------------------------------------------------- documento
def gerar(ev: dict | None, saida: Path) -> Path:
    cfg_p = RAIZ / "docs" / "relatorio" / "config_relatorio.json"
    cfg = json.loads(cfg_p.read_text(encoding="utf-8-sig")) if cfg_p.exists() else {}
    teste = not ev or ev.get("ambiente") != "databricks"
    h: list = []

    h.append(Paragraph("Guiding Hand — Big Data Architecture &amp; Data Integration", H1))
    h.append(Paragraph("FIAP Global Solution 2026 · CitizenTech · Engenharia de Software 4º ano", TXT))
    h.append(Paragraph(f"Gerado em {datetime.now():%d/%m/%Y %H:%M} a partir das evidências do repositório.", PEQ))
    if teste:
        h.append(Paragraph("<b>PRÉVIA — as evidências deste PDF vêm de execução LOCAL DE TESTE (dados sintéticos e "
                           "fixtures). Não entregar esta versão.</b>", PEND))

    h.append(Paragraph("1. Integrantes", H1))
    ints = integrantes()
    h.append(tabela([["Nome completo", "RM"]] + ints, [12 * cm, 4 * cm]) if ints else
             pendente("preencha docs/relatorio/integrantes.csv (nome,rm) — a ausência tira 0,5 ponto"))

    h.append(Paragraph("2. O produto em uma frase e as perguntas que o MVP responde", H1))
    h.append(Paragraph("<b>Óculos que avisam pessoas cegas ou com baixa visão sobre obstáculos que a bengala não "
                       "alcança e transformam cada detecção num mapa de barreiras para a zeladoria de Pinheiros.</b>", TXT))
    for cid, pergunta, *_ in [(c["id"], c["pergunta"]) for c in (ev or {}).get("consultas", [])] or [
            ("C1–C6", "consultas ainda não executadas")]:
        h.append(Paragraph(f"• <b>{esc(cid)}</b> — {esc(pergunta)}", TXT))

    h.append(Paragraph("3. Diagrama do fluxo de dados (da fonte ao MVP)", H1))
    h.append(diagrama())

    h.append(Paragraph("4. Tabela de fontes", H1))
    val_p = ultimo("docs/validacao_fontes/relatorio_*.json")
    val = json.loads(val_p.read_text(encoding="utf-8-sig")) if val_p else {}
    def sv(chave):
        r = next((v for k, v in val.items() if chave in k), None)
        return r["status"] if r else "não validado"
    h.append(tabela([
        ["Fonte", "Origem", "Formato", "Frequência", "Validação real"],
        ["Clima", "Open-Meteo Forecast API (sem chave)", "JSON", "Diária (dia anterior)", sv("Open-Meteo")],
        ["Infraestrutura de calçada", "OpenStreetMap / Overpass (ODbL)", "JSON", "Diária", sv("Overpass")],
        ["Zeladoria SP156", "Prefeitura de SP / GeoSampa WFS", "GeoJSON", "Diária", sv("GeoSampa")],
        ["Paradas de ônibus", "SPTrans Olho Vivo (token)", "JSON", "Diária", sv("Olho Vivo")],
        ["Eventos dos óculos", "Gerador sintetico_v1 (documentado)", "JSON", "Diária", "sintético"],
    ], [3.2 * cm, 5 * cm, 1.8 * cm, 3 * cm, 3 * cm]))
    if not val_p:
        h.append(pendente("rode o menu 1 (validação das fontes) para preencher a coluna 'Validação real'"))

    h.append(PageBreak())
    h.append(Paragraph("5. Requisitos 01 a 09", H1))
    req = [
        ("01 · Fontes", "3 fontes externas reais coletadas (Open-Meteo, OpenStreetMap/Overpass e SP156/GeoSampa, este com "
                     "registros históricos de 2020); Olho Vivo implementado, mas não coletado por falta de token; eventos dos "
                     "óculos sintéticos, com geração documentada (docs/fontes.md).", None),
        ("02 · DAG agendado", "guiding_hand_coleta, diário 07:00 (America/Sao_Paulo); grava chegada/<fonte>/<fonte>_<AAAA-MM-DD>.json; reexecutar regrava o mesmo arquivo."
         + (f" Ambiente de execução do Airflow: {cfg['ambiente_airflow']}." if cfg.get("ambiente_airflow") else ""), "chegada"),
        ("03 · Paralelismo e segurança", "5 coletas em paralelo entre 'inicio' e 'conferir'; conferir valida existência, JSON e estrutura; chaves em Variables; 2 retries e callback de falha com notificação; credenciais mascaradas em mensagens de erro.", "airflow_variables"),
        ("04 · Dois lotes", "Lotes montados por scripts/preparar_lote.py (só arquivos ainda não enviados), registrados em docs/lotes/ "
         "e enviados manualmente ao Volume chegada: lote 1 com a coleta de 15/11 e lote 2 com a coleta de 16/11.", "lotes"),
        ("05 · Bronze", "Auto Loader (cloudFiles, text/wholetext) com checkpoint e trigger availableNow; uma linha por arquivo, bruto preservado, arquivo_origem, data_coleta, hash e carregado_em.", "bronze"),
        ("06 · Prata", "Schema explícito, nulos tratados, normalização, deduplicação, MERGE por chave com hash (idempotente) e 7 regras de qualidade com quarentena.", "prata"),
        ("07 · Ouro e SQL", "Fato de ocorrências + dim_tempo, dim_local, dim_tipo_obstaculo, dim_origem, dim_barreira; 6 consultas (C5 de impacto).", "ouro"),
        ("08 · LGPD e MVP", "device_hash SHA-256 + salt, coordenadas generalizadas, nenhuma imagem; o MVP consome tb_mvp_mapa_barreiras (sem dado pessoal).", "mvp"),
        ("09 · Repositório", "README com integrantes, arquitetura e reprodução; DAGs, notebooks, SQL, amostras; scanner de segredos.", None),
    ]
    lotes_md = sorted((RAIZ / "docs" / "lotes").glob("lote_*.md"))
    for titulo, texto, chave in req:
        h.append(Paragraph(titulo, H2))
        h.append(Paragraph(esc(texto), TXT))
        if chave == "lotes":
            h.append(Paragraph("Lotes registrados: " + (", ".join(p.stem for p in lotes_md) if lotes_md else "nenhum"),
                               TXT if len(lotes_md) >= 2 else PEND))
            h += figura_par(["upload_lote1", "upload_lote2"])
        elif chave == "mvp":
            h += figura(chave, 9.5 * cm, altura_max=15 * cm)
        elif chave:
            h += figura(chave, 13 * cm, altura_max=11 * cm)

    h.append(PageBreak())
    h.append(Paragraph("6. Prints do Airflow (Graph e Grid)", H1))
    h += figura("airflow_graph") + figura("airflow_grid")
    af_p = ultimo("docs/evidencias/airflow_*.json")
    if af_p:
        tarefas = json.loads(af_p.read_text(encoding="utf-8-sig"))
        h.append(Paragraph(f"Estados e horários registrados automaticamente ({af_p.name}):", TXT))
        h.append(tabela([["Tarefa", "Estado", "Início", "Fim"]] +
                        [[t["task_id"], t["state"], str(t.get("start_date"))[:23], str(t.get("end_date"))[:23]]
                         for t in sorted(tarefas, key=lambda x: str(x.get("start_date")))]))

    h.append(Paragraph("7. Contagens da Bronze (antes/depois do 2º lote e da 2ª execução)", H1))
    execs = (ev or {}).get("bronze_execucoes")
    if execs:
        h.append(tabela([["Execução", "Arquivos antes", "Arquivos depois", "Novos", "Quando"]] +
                        [[e["rotulo"], e["linhas_antes"], e["linhas_depois"], e["arquivos_novos"], e["executado_em"][:19]]
                         for e in execs]))
    else:
        h.append(pendente("evidências da Bronze no Databricks (notebook 00_processar_lote, lotes 1 e 2)"))
    if ev and ev.get("bronze_por_fonte_data"):
        h.append(Spacer(1, 4))
        h.append(tabela_de_dicts(ev["bronze_por_fonte_data"], 20))
    if ev and ev.get("quarentena_por_regra"):
        h.append(Paragraph("Registros barrados por regra de qualidade (Prata)", H2))
        h.append(tabela_de_dicts(ev["quarentena_por_regra"], 20))

    h.append(PageBreak())
    h.append(Paragraph("8. Consultas SQL, resultados e interpretação", H1))
    if not ev:
        h.append(pendente("resultados das consultas (evidencias_lote_2.json do Databricks)"))
    for c in (ev or {}).get("consultas", []):
        bloco = [Paragraph(f"{esc(c['id'])} — {esc(c['pergunta'])}", H2),
                 Preformatted(c["sql"], CODIGO, maxLineLength=120),
                 Spacer(1, 3), tabela_de_dicts(c["resultado"], limite := (10 if c["id"] == "C6" else 30)),
                 Paragraph(f"Mostrando {min(limite, len(c['resultado']))} de {len(c['resultado'])} linhas do resultado exportado."
                           if len(c["resultado"]) > limite else f"Resultado completo: {len(c['resultado'])} linhas.", PEQ),
                 Spacer(1, 3),
                 Paragraph("<b>Interpretação:</b> " + esc(interpretar(c["id"], c["resultado"])), TXT)]
        h += bloco

    inicio_conclusao = len(h)
    h.append(Paragraph("9. Conclusão técnica e link do repositório", H1))
    if ev:
        cont = ev.get("contagens", {})
        h.append(Paragraph(esc(
            f"O pipeline executa 5 coletas em paralelo no Airflow 3 (Olho Vivo pulado por falta de token), ingere "
            f"dois lotes de dias diferentes de forma incremental e idempotente no "
            f"Databricks e entrega ao MVP {cont.get('tb_mvp_mapa_barreiras', '?')} barreiras mapeadas a partir de "
            f"{cont.get('fato_ocorrencia_barreira', '?')} ocorrências{resumo_c5(ev)}. A resolução de entidade espacial evita contar o "
            "mesmo obstáculo várias vezes; regras de qualidade isolam registros inválidos sem perder rastreabilidade; "
            "a tabela do MVP não contém dados pessoais. Limitações: eventos dos óculos sintéticos até o protótipo gerar "
            "dados de campo; SP156 com registros históricos de 2020; clima real sem relação causal demonstrada; "
            "transferência para o Volume manual por restrição do Free Edition. É um piloto de demonstração, não um "
            "serviço de navegação validado em produção."), TXT))
    else:
        h.append(pendente("conclusão (gerada automaticamente quando houver evidências do Databricks)"))
    link = cfg.get("link_repositorio")
    h.append(Paragraph(f"Repositório: {esc(link)}", TXT) if link else
             pendente("link público do GitHub em docs/relatorio/config_relatorio.json"))
    h[inicio_conclusao:] = [KeepTogether(h[inicio_conclusao:])]   # título não fica sozinho no fim da página

    def rodape(canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 7)
        canvas.drawString(2 * cm, 1.2 * cm, "Guiding Hand · Big Data · FIAP GS 2026")
        canvas.drawRightString(19 * cm, 1.2 * cm, f"página {doc.page}")
        if teste:
            canvas.setFillColor(colors.HexColor("#b00020"))
            canvas.drawCentredString(10.5 * cm, 28.6 * cm, "PRÉVIA COM DADOS DE TESTE — NÃO ENTREGAR")
        canvas.restoreState()

    saida.parent.mkdir(parents=True, exist_ok=True)
    SimpleDocTemplate(str(saida), pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm, topMargin=1.8 * cm,
                      bottomMargin=1.8 * cm, title="Guiding Hand - Big Data",
                      author="Equipe Guiding Hand").build(h, onFirstPage=rodape, onLaterPages=rodape)
    return saida


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--evidencias")
    ap.add_argument("--saida", default=str(RAIZ / "docs" / "relatorio" / "Relatorio_Big_Data_Guiding_Hand.pdf"))
    a = ap.parse_args()
    ev = carregar_evidencias(a.evidencias)
    destino = gerar(ev, Path(a.saida))
    texto = destino.read_bytes()
    print(f"PDF gerado: {destino}")
    pendencias = []
    if not integrantes():
        pendencias.append("integrantes.csv")
    if not ev or ev.get("ambiente") != "databricks":
        pendencias.append("evidências do Databricks")
    faltando = [n for k, (n, _) in PRINTS.items() if not (RAIZ / "docs" / "relatorio" / "prints" / n).exists()
                and not (k == "chegada" and inventario_lotes())]
    if faltando:
        pendencias.append(f"{len(faltando)} prints ({', '.join(faltando)})")
    print("Pendências: " + (", ".join(pendencias) if pendencias else "nenhuma"))
