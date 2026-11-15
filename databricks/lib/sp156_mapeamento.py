"""Mapeamento dos campos do SP156 (GeoSampa) para os campos canônicos da Prata.

Módulo SEM dependência de Spark: usado pela Prata (gh_transform.py), pela verificação da
coleta (scripts/verificar_chegada.py) e pelo perfilador (scripts/perfil_sp156.py), para que
os três usem exatamente o mesmo mapeamento.

A escolha de qual campo representa o PROTOCOLO não é fixada no código: ela é feita a partir
dos dados reais pelo perfilador e gravada em ``mapeamento_sp156.json`` (ao lado deste arquivo),
junto com a referência da evidência. Os nomes listados lá têm prioridade sobre as listas abaixo.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

ARQUIVO_MAPEAMENTO = Path(__file__).with_name("mapeamento_sp156.json")

# Nomes candidatos por campo canônico (sem diferenciar maiúsculas). O primeiro que existir vence.
CAMPOS_SP156_BASE: dict[str, list[str]] = {
    "protocolo": ["protocolo", "nr_protocolo", "numero_protocolo", "cd_protocolo", "id_solicitacao", "id"],
    "servico": ["servico", "dc_servico", "nm_servico", "assunto", "tema", "tipo_servico", "ds_servico"],
    "status": ["status", "tx_situacao_solicitacao", "situacao", "status_solicitacao", "ds_status"],
    "data_abertura": ["data_abertura", "dt_abertura", "data_solicitacao", "dt_solicitacao", "data_registro"],
    "data_conclusao": ["data_conclusao", "dt_conclusao", "data_encerramento", "dt_encerramento",
                       "data_parecer", "dt_parecer"],
}

# Palavras-chave do serviço SP156 -> tipo de obstáculo do Guiding Hand.
MAPA_SERVICO_SP156: list[tuple[str, str]] = [
    ("buraco", "buraco"), ("pavimenta", "buraco"), ("asfalto", "buraco"),
    ("calcada", "calcada_irregular"), ("calçada", "calcada_irregular"), ("passeio", "calcada_irregular"),
    ("poda", "galho_arvore"), ("arvore", "galho_arvore"), ("árvore", "galho_arvore"),
    ("placa", "placa"), ("sinaliza", "placa"),
    ("poste", "poste"), ("ilumina", "poste"),
    ("lixeira", "lixeira"), ("entulho", "entulho"), ("cacamba", "cacamba"), ("caçamba", "cacamba"),
    ("veiculo", "carro_calcada"), ("veículo", "carro_calcada"),
]

# Classificação do status (texto em minúsculas). Usada pela Prata e pelo perfilador.
STATUS_RESOLVIDA_RE = "finaliz|conclu|atendid|execut|encerrad"
STATUS_CANCELADA_RE = "cancel|indefer|improced"


def carregar_mapeamento(caminho: Path | None = None) -> dict:
    caminho = caminho or ARQUIVO_MAPEAMENTO  # resolvido a cada leitura (sempre o arquivo atual)
    if not caminho.exists():
        return {"campos": {}, "protocolo_composto_com_camada": False}
    dados = json.loads(caminho.read_text(encoding="utf-8"))
    dados.setdefault("campos", {})
    dados.setdefault("protocolo_composto_com_camada", False)
    return dados


def campos_sp156(caminho: Path | None = None) -> dict[str, list[str]]:
    """Listas de nomes por campo canônico, com o campo escolhido nos dados reais em 1º lugar."""
    escolhidos = carregar_mapeamento(caminho)["campos"]
    resultado = {}
    for canon, nomes in CAMPOS_SP156_BASE.items():
        preferido = (escolhidos.get(canon) or "").lower()
        resultado[canon] = ([preferido] if preferido else []) + [n for n in nomes if n != preferido]
    return resultado


def classificar_status(texto) -> str:
    """Mesma regra da Prata (gh_transform._status_sp156), em Python puro."""
    if texto is None:
        return "desconhecido"
    s = str(texto).lower()
    if re.search(STATUS_RESOLVIDA_RE, s):
        return "resolvida"
    if re.search(STATUS_CANCELADA_RE, s):
        return "cancelada"
    return "aberta"


def tipo_obstaculo(servico) -> str | None:
    s = str(servico or "").lower()
    return next((tipo for chave, tipo in MAPA_SERVICO_SP156 if chave in s), None)
