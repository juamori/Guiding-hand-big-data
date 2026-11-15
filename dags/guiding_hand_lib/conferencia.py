"""Conferência dos arquivos da pasta chegada/ e notificações do pipeline."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests

from . import config
from .coletores import nome_arquivo

CHAVE_ESPERADA = {
    "clima": "hourly",
    "osm": "elements",
    "eventos_oculos": "eventos",
    "sp156": "camadas",
    "olhovivo": "buscas",
}


@dataclass
class ItemConferencia:
    fonte: str
    obrigatoria: bool
    arquivo: str
    existe: bool
    bytes: int
    json_valido: bool
    estrutura_ok: bool
    sha256: str | None
    situacao: str  # ok | ausente | invalido


def conferir_arquivo(pasta_chegada: Path, fonte: str, data_coleta: date) -> ItemConferencia:
    obrigatoria = config.FONTES[fonte]["obrigatoria"]
    caminho = pasta_chegada / fonte / nome_arquivo(fonte, data_coleta)
    if not caminho.exists():
        return ItemConferencia(fonte, obrigatoria, str(caminho), False, 0, False, False, None, "ausente")
    conteudo = caminho.read_bytes()
    json_valido, estrutura_ok = False, False
    try:
        dados = json.loads(conteudo)
        json_valido = True
        estrutura_ok = isinstance(dados, dict) and CHAVE_ESPERADA[fonte] in dados
    except ValueError:
        pass
    ok = len(conteudo) > 0 and json_valido and estrutura_ok
    return ItemConferencia(fonte, obrigatoria, str(caminho), True, len(conteudo), json_valido,
                           estrutura_ok, hashlib.sha256(conteudo).hexdigest(),
                           "ok" if ok else "invalido")


def conferir_chegada(pasta_chegada: Path, data_coleta: date) -> dict[str, Any]:
    itens = [conferir_arquivo(pasta_chegada, f, data_coleta) for f in config.FONTES]
    problemas_obrigatorios = [i.fonte for i in itens if i.obrigatoria and i.situacao != "ok"]
    manifesto = {
        "data_coleta": data_coleta.isoformat(),
        "conferido_em": datetime.now(timezone.utc).isoformat(),
        "aprovado": not problemas_obrigatorios,
        "fontes_obrigatorias_com_problema": problemas_obrigatorios,
        "itens": [asdict(i) for i in itens],
    }
    pasta_manifesto = pasta_chegada / "_manifestos"
    pasta_manifesto.mkdir(parents=True, exist_ok=True)
    (pasta_manifesto / f"manifesto_{data_coleta.isoformat()}.json").write_text(
        json.dumps(manifesto, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifesto


def registrar_notificacao(pasta_logs: Path, evento: dict[str, Any], webhook_url: str | None = None) -> None:
    """Registra a notificação em arquivo (sempre) e envia para um webhook (se configurado).

    O webhook pode ser um "Incoming Webhook" do Teams, Discord ou Slack; a URL fica
    na Variable ``gh_webhook_url`` e nunca no código.
    """
    pasta_logs.mkdir(parents=True, exist_ok=True)
    evento = {"registrado_em": datetime.now(timezone.utc).isoformat(), **evento}
    with (pasta_logs / "notificacoes.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(evento, ensure_ascii=False) + "\n")
    if webhook_url:
        texto = f"[Guiding Hand] {evento.get('tipo')}: {evento.get('mensagem')}"
        try:
            requests.post(webhook_url, json={"text": texto, "content": texto}, timeout=10)
        except requests.RequestException:
            pass  # notificação nunca derruba o pipeline; o arquivo já registrou
