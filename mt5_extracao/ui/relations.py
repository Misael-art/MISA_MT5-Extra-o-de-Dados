"""Relações do Laboratório.

Inventário lê o registry e a base. Matriz, alvo, linha do tempo, retorno por intenção,
convergência e candidatos leem o relatório congelado em Séries pelo mesmo serviço do CLI.
Sem análise, a tela fica vazia: a fixture abaixo só exercita layout e cores nos testes.
A UI não recalcula efeito, não estima PnL e não autoriza Expert.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import threading
import tkinter as tk
from tkinter import filedialog, ttk

import numpy as np

from mt5_extracao.laboratory.service import PREDICTIVE_LAGS
from mt5_extracao.ui import charts
from mt5_extracao.ui.components import ScrollableContent as _ScrollablePage
from mt5_extracao.ui.components import ContentPanel, DetailsPanel, FilterBar, Notice, PageHeader, TablePanel
from mt5_extracao.ui.visual_tokens import SPACING, TYPOGRAPHY


VIEW_NAMES = ("Inventário", "Matriz", "Linha do tempo", "Ganhos e perdas", "Convergências", "Candidatos")
_STATE_TEXT = {
    "FAVORABLE": "Favorável OOS",
    "DETERIORATING": "Deterioração",
    "ADVERSE": "Adverso",
    "INCONCLUSIVE": "Inconclusivo",
}
_STATE_COLORS = {
    "FAVORABLE": "accent",
    "DETERIORATING": "warn",
    "ADVERSE": "bad",
    "INCONCLUSIVE": "muted",
}
_STATE_MARKERS = {"FAVORABLE": "o", "DETERIORATING": "^", "ADVERSE": "X", "INCONCLUSIVE": "s"}


# Dados de apresentação combinados com Analytics para exercitar estados e casos de borda.
# Os textos/valores são exemplos sintéticos; classificações e motivos são dados de entrada,
# nunca inferidos pelas funções de desenho.
_SYNTHETIC_FIXTURE = {
    "scope": {
        "feed": "feed de demonstração",
        "asset": "EURUSD · contrato sintético",
        "expert": "Expert Alpha · v1 · preset Base",
        "timeframe": "H1",
        "direction": "Compra e venda",
        "target": "Retorno futuro · 3 barras",
        "horizon": "3 barras fechadas",
        "method": "Spearman · correlação",
        "period": "01/01/2025 a 31/12/2025",
        "window": "60 sessões",
        "step": "20 sessões",
    },
    "indicator_names": ("RSI(14)", "MACD(12,26,9)", "ATR(14)", "ADX(14)"),
    "target_names": ("Retorno +3 barras", "MFE +10 barras", "Retorno +20 barras"),
    "target_matrix": (
        (
            {"label": "+0,31", "strength": "positive_moderate", "method": "Spearman", "n": "n=408", "effect_ci": "IC 95%: +0,19 a +0,42", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "alvo maduro na fixture", "reason": "Correlação com movimento futuro; não é probabilidade de ganho nem recomendação de operação."},
            {"label": "−0,12", "strength": "negative_weak", "method": "Spearman", "n": "n=382", "effect_ci": "IC 95%: −0,23 a −0,01", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "alvo maduro na fixture", "reason": "Associação negativa com o MFE no exemplo; sinal negativo não significa perda do Expert."},
            {"label": "—", "strength": "missing", "method": "Spearman", "n": "n=34", "effect_ci": "rótulo imaturo", "utility_state": "INCONCLUSIVE", "evidence_state": "INSUFFICIENT", "freshness_state": "UNAVAILABLE", "presence_state": "UNKNOWN", "support_state": "horizonte ainda não maduro", "reason": "O horizonte de 20 barras ainda não maturou para estas observações; não preencher lacuna com zero."},
        ),
        (
            {"label": "+0,18", "strength": "positive_weak", "method": "Spearman", "n": "n=408", "effect_ci": "IC 95%: +0,06 a +0,29", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "alvo maduro na fixture", "reason": "Correlação positiva com o alvo; o estado continua exploratório."},
            {"label": "+0,09", "strength": "positive_weak", "method": "Spearman", "n": "n=382", "effect_ci": "IC 95%: −0,02 a +0,20", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "alvo maduro na fixture", "reason": "Intervalo sintético cruza zero; a tela não reavalia nem cria um estado."},
            {"label": "+0,02", "strength": "neutral", "method": "Spearman", "n": "n=360", "effect_ci": "IC 95%: −0,10 a +0,13", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "STALE", "presence_state": "UNKNOWN", "support_state": "origem vencida", "reason": "Dado STALE: o selo de frescor permanece separado da associação exibida."},
        ),
        (
            {"label": "−0,07", "strength": "negative_weak", "method": "Spearman", "n": "n=408", "effect_ci": "IC 95%: −0,18 a +0,05", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "alvo maduro na fixture", "reason": "Associação fraca e inconclusiva; sem relação detectada não equivale a incompatibilidade."},
            {"label": "+0,22", "strength": "positive_moderate", "method": "Spearman", "n": "n=382", "effect_ci": "IC 95%: +0,10 a +0,33", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "alvo maduro na fixture", "reason": "Associação direta no exemplo; estado sem autorização operacional."},
            {"label": "+0,16", "strength": "positive_weak", "method": "Spearman", "n": "n=360", "effect_ci": "IC 95%: +0,04 a +0,27", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "STALE", "presence_state": "UNKNOWN", "support_state": "origem vencida", "reason": "Dado STALE e relação exploratória; não atualizar ou promover na camada visual."},
        ),
        (
            {"label": "+0,39", "strength": "positive_moderate", "method": "Spearman", "n": "n=408", "effect_ci": "IC 95%: +0,27 a +0,49", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "alvo maduro na fixture", "reason": "Associação com alvo futuro; não mede aderência do Expert nem lucro."},
            {"label": "−0,28", "strength": "negative_moderate", "method": "Spearman", "n": "n=382", "effect_ci": "IC 95%: −0,39 a −0,15", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "alvo maduro na fixture", "reason": "Correlação inversa com alvo futuro; não equivale a efeito operacional adverso."},
            {"label": "+0,11", "strength": "positive_weak", "method": "Spearman", "n": "n=360", "effect_ci": "IC 95%: −0,01 a +0,23", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "STALE", "presence_state": "UNKNOWN", "support_state": "origem vencida", "reason": "Origem vencida; o frescor é exibido independentemente do sinal."},
        ),
    ),
    "timeline": {
        "series": (
            {
                "label": "RSI(14) × retorno futuro +3 barras",
                "metric": "Spearman ρ · escala fixa −1 a +1",
                "windows": (
                    {"start": "2025-01-01", "end": "2025-02-28", "center": "2025-01-30", "segment": "Aquecimento", "value": None, "value_text": "—", "ci": "indefinido", "n": "n=0", "utility_state": "INCONCLUSIVE", "evidence_state": "INSUFFICIENT", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "WARMUP", "label_maturity": "rótulo indisponível", "reason": "Aquecimento: indicador e alvo ainda não estão maduros. Sem valor nesta janela."},
                    {"start": "2025-03-01", "end": "2025-05-31", "center": "2025-04-15", "segment": "Treino", "value": 0.26, "value_text": "+0,26", "ci": "IC 95%: +0,08 a +0,42", "n": "n=110", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "amostra sintética", "label_maturity": "maduro", "reason": "Estimativa de treino usada apenas para definir a referência congelada da fixture."},
                    {"start": "2025-06-01", "end": "2025-08-31", "center": "2025-07-15", "segment": "Validação", "value": 0.03, "value_text": "+0,03", "ci": "IC 95%: −0,14 a +0,19", "n": "n=104", "utility_state": "DETERIORATING", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "ADEQUATE", "reference_scope": "FROZEN_TRAIN", "label_maturity": "maduro", "reason": "Deterioração comparada à referência de treino congelada (estado fornecido pela fixture). Não é efeito adverso validado."},
                    {"start": "2025-09-01", "end": "2025-10-31", "center": "2025-10-01", "segment": "OOS", "value": -0.04, "value_text": "−0,04", "ci": "IC 95%: −0,21 a +0,12", "n": "n=61", "utility_state": "INCONCLUSIVE", "evidence_state": "INSUFFICIENT", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "amostra OOS sintética baixa", "label_maturity": "maduro", "reason": "OOS com suporte insuficiente no exemplo; coeficiente negativo não vira estado adverso."},
                    {"start": "2025-11-01", "end": "2025-12-31", "center": "2025-12-01", "segment": "OOS", "value": 0.19, "value_text": "+0,19", "ci": "IC 95%: +0,02 a +0,34", "n": "n=82", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "STALE", "presence_state": "UNKNOWN", "support_state": "fixture sintética", "label_maturity": "maduro", "reason": "Associação positiva e origem STALE; a tela não promove o dado nem substitui a validação do produtor."},
                ),
            },
            {
                "label": "MACD(12,26,9) × retorno futuro +3 barras",
                "metric": "Spearman ρ · escala fixa −1 a +1",
                "windows": (
                    {"start": "2025-01-01", "end": "2025-02-28", "center": "2025-01-30", "segment": "Aquecimento", "value": None, "value_text": "—", "ci": "indefinido", "n": "n=0", "utility_state": "INCONCLUSIVE", "evidence_state": "INSUFFICIENT", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "WARMUP", "label_maturity": "rótulo indisponível", "reason": "Aquecimento: indicador e alvo ainda não estão maduros. Sem valor nesta janela."},
                    {"start": "2025-03-01", "end": "2025-05-31", "center": "2025-04-15", "segment": "Treino", "value": -0.21, "value_text": "−0,21", "ci": "IC 95%: −0,38 a −0,03", "n": "n=110", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "amostra sintética", "label_maturity": "maduro", "reason": "Associação inversa no treino; esse sinal não significa que o setup perde."},
                    {"start": "2025-06-01", "end": "2025-08-31", "center": "2025-07-15", "segment": "Validação", "value": -0.09, "value_text": "−0,09", "ci": "IC 95%: −0,26 a +0,08", "n": "n=104", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "amostra sintética", "label_maturity": "maduro", "reason": "Relação exploratória ainda inconclusiva; sem sinal de incompatibilidade."},
                    {"start": "2025-09-01", "end": "2025-10-31", "center": "2025-10-01", "segment": "OOS", "value": -0.02, "value_text": "−0,02", "ci": "IC 95%: −0,19 a +0,15", "n": "n=61", "utility_state": "INCONCLUSIVE", "evidence_state": "INSUFFICIENT", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "amostra OOS sintética baixa", "label_maturity": "maduro", "reason": "OOS com suporte insuficiente; presença da configuração não equivale a utilidade."},
                    {"start": "2025-11-01", "end": "2025-12-31", "center": "2025-12-01", "segment": "OOS", "value": None, "value_text": "—", "ci": "indefinido", "n": "n=0", "utility_state": "INCONCLUSIVE", "evidence_state": "INSUFFICIENT", "freshness_state": "UNAVAILABLE", "presence_state": "UNKNOWN", "support_state": "MISSING_SOURCE", "label_maturity": "sem fonte", "reason": "Fonte ausente nesta janela. A lacuna não é interpolada e não recebe valor zero."},
                ),
            },
        ),
        "reference": "Ranking congelado no treino · sem escolher o melhor por janela",
        "ranking_metric": "Spearman ρ",
    },
    "distribution": {
        "bins": (("−2 a −1 R", "LOSS"), ("−1 a 0 R", "LOSS"), ("Empate", "FLAT"),
                 ("0 a +1 R", "WIN"), ("+1 a +2 R", "WIN"), ("> +2 R", "WIN")),
        "directions": (
            {"label": "Compra", "wins": "38", "losses": "28", "flats": "4", "censored": "2", "n_total": "72", "n_mature": "70", "presence_state": "UNKNOWN", "freshness_state": "CURRENT", "counts": (6, 22, 4, 26, 10, 2), "reason": "Distribuição sintética em R por operação; custos incompletos impedem expectativa líquida."},
            {"label": "Venda", "wins": "34", "losses": "31", "flats": "3", "censored": "4", "n_total": "72", "n_mature": "68", "presence_state": "UNKNOWN", "freshness_state": "CURRENT", "counts": (8, 23, 3, 21, 11, 2), "reason": "Distribuição sintética em R por operação; denominador inclui rótulos censurados declarados."},
        ),
        "population": "operações maturadas do Expert Alpha · exemplo sintético",
        "unit": "R por operação · bins calculados pelo produtor",
        "cost_status": "INCOMPLETE_COSTS",
    },
    "convergence": {
        "pairs": (
            {
                "label": "ADX(14) + ATR(14)",
                "comparison": "População e janelas pareadas · OOS sintético",
                "plot_values": (0.06, 0.08, 0.04, 0.10),
                "rows": (
                    {"label": "Baseline Expert", "effect": "+0,06 R/op", "ci": "IC 95%: −0,03 a +0,15", "n": "n=86", "delta": "referência congelada", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "fixture sintética", "reason": "Baseline congelada no treino; comparações usam a mesma população e as mesmas janelas."},
                    {"label": "ADX(14) isolado", "effect": "+0,08 R/op", "ci": "IC 95%: −0,02 a +0,18", "n": "n=86", "delta": "+0,02 R/op", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "fixture sintética", "reason": "Delta fornecido pela fixture; a UI não o recalcula."},
                    {"label": "ATR(14) isolado", "effect": "+0,04 R/op", "ci": "IC 95%: −0,06 a +0,14", "n": "n=86", "delta": "−0,02 R/op", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "fixture sintética", "reason": "Delta fornecido pela fixture; a UI não o recalcula."},
                    {"label": "ADX(14) + ATR(14)", "effect": "+0,10 R/op", "ci": "IC 95%: +0,00 a +0,20", "n": "n=86", "delta": "+0,04 R/op", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "fixture sintética", "reason": "Exemplo de dupla cujo delta isolado é fornecido, mas não indica validação nem autorização."},
                ),
            },
            {
                "label": "RSI(14) + CCI(20)",
                "comparison": "População e janelas pareadas · OOS sintético",
                "plot_values": (0.06, 0.02, 0.01, -0.03),
                "rows": (
                    {"label": "Baseline Expert", "effect": "+0,06 R/op", "ci": "IC 95%: −0,03 a +0,15", "n": "n=86", "delta": "referência congelada", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "fixture sintética", "reason": "Baseline congelada no treino; mesma população e janela do comparativo."},
                    {"label": "RSI(14) isolado", "effect": "+0,02 R/op", "ci": "IC 95%: −0,08 a +0,12", "n": "n=86", "delta": "−0,04 R/op", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "fixture sintética", "reason": "Resultado isolado sintético; não é ranking de candidatos."},
                    {"label": "CCI(20) isolado", "effect": "+0,01 R/op", "ci": "IC 95%: −0,09 a +0,11", "n": "n=86", "delta": "−0,05 R/op", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "fixture sintética", "reason": "Resultado isolado sintético; não é ranking de candidatos."},
                    {"label": "RSI(14) + CCI(20)", "effect": "−0,03 R/op", "ci": "IC 95%: −0,13 a +0,07", "n": "n=86", "delta": "−0,09 R/op", "utility_state": "ADVERSE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "efeito adverso exploratório da fixture", "reason": "Valor adverso e estado exploratório fornecidos apenas para demonstrar cores e detalhes; não é conclusão do projeto."},
                ),
            },
        ),
    },
    "candidates": (
        {"id": "EXEMPLO-A", "proposal": "RSI(14) + alvo de 3 barras", "scope": "Expert Alpha · H1 · compra/venda", "window": "treino 2025-03–05 · OOS 2025-09–12", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "utility_state": "INCONCLUSIVE", "support_state": "fixture sintética", "validity": "sem validade operacional", "reason": "Exemplo visual de proposta de pesquisa. Sem comparação reproduzível do Analytics e sem botão para aplicar."},
        {"id": "EXEMPLO-B", "proposal": "ADX(14) + ATR(14)", "scope": "Expert Alpha · H1 · compra/venda", "window": "treino congelado · OOS sintético", "evidence_state": "INSUFFICIENT", "freshness_state": "STALE", "presence_state": "UNKNOWN", "utility_state": "INCONCLUSIVE", "support_state": "custos incompletos e origem vencida", "validity": "não elegível", "reason": "Sem custo completo e com origem STALE. A linha é visualmente cinza; não representa candidato existente."},
    ),
}


def _mix(background: str, foreground: str, amount: float) -> str:
    """Mistura duas cores para o fundo sutil da matriz; não classifica efeito nem estado."""
    first = tuple(int(background[index:index + 2], 16) for index in (1, 3, 5))
    second = tuple(int(foreground[index:index + 2], 16) for index in (1, 3, 5))
    channels = tuple(round(a * (1 - amount) + b * amount) for a, b in zip(first, second))
    return "#" + "".join(f"{value:02X}" for value in channels)


def _date(value: str) -> dt.datetime:
    """Aceita dia ou instante. Blocos no mesmo dia civil precisam da hora para não se empilhar."""
    text = str(value).strip().replace("T", " ")
    if len(text) >= 16 and text[10] == " ":
        return dt.datetime.strptime(text[:16], "%Y-%m-%d %H:%M")
    return dt.datetime.strptime(text[:10], "%Y-%m-%d")


def _fit_label_to_parent(label, parent, *, reserve=24, minimum=220):
    """Mantém o texto legível ao redimensionar o contêiner."""
    def resize(event):
        label.configure(wraplength=max(minimum, event.width - reserve))

    parent.bind("<Configure>", resize, add="+")
    return label


def _visual_state(row: dict) -> str:
    """Mapeia apenas estados explícitos do produtor; nunca estima evidência na UI."""
    utility = row.get("utility_state", "INCONCLUSIVE")
    evidence = row.get("evidence_state", "INSUFFICIENT")
    freshness = row.get("freshness_state", "UNAVAILABLE")
    adequate = row.get("support_state") == "ADEQUATE"
    if utility == "FAVORABLE":
        valid_oos = (evidence == "VALIDATED" and row.get("segment") == "OOS"
                     and row.get("validation_scope") == "FINAL_RESERVE")
        return utility if valid_oos and adequate and freshness == "CURRENT" else "INCONCLUSIVE"
    if utility == "ADVERSE":
        predeclared = row.get("criterion_state") == "PREDECLARED"
        return utility if evidence == "VALIDATED" and predeclared and adequate and freshness == "CURRENT" \
            else "INCONCLUSIVE"
    if utility == "DETERIORATING":
        frozen_reference = row.get("reference_scope") == "FROZEN_TRAIN"
        return utility if evidence != "INSUFFICIENT" and frozen_reference and adequate and freshness == "CURRENT" \
            else "INCONCLUSIVE"
    return utility if utility == "INCONCLUSIVE" else "INCONCLUSIVE"


def _display_state(row: dict) -> str:
    state = _visual_state(row)
    label = _STATE_TEXT.get(state, "Inconclusivo")
    evidence = row.get("evidence_state", "INSUFFICIENT")
    return f"{label} · {evidence}"


_EMPTY_SCOPE = {"feed": "análise congelada", "asset": "—", "expert": "sem Expert", "timeframe": "—",
                "direction": "não se aplica", "method": "Spearman", "period": "—"}


def _detail_text(row: dict, scope: dict | None = None) -> str:
    """Formata proveniência/estado fornecidos, sem deduzir campos a partir do efeito."""
    scope = scope or _EMPTY_SCOPE
    period = row.get("period") or row.get("window") or scope["period"]
    segment = row.get("segment", row.get("baseline", "segmento não informado"))
    effect = row.get("effect_text", row.get("label", row.get("effect", "—")))
    interval = row.get("effect_ci", row.get("ci", "IC não fornecido"))
    return "\n".join((
        f"Período/janela: {period} · segmento {segment} · rótulo {row.get('label_maturity', 'maturidade não informada')} · "
        f"disponibilidade {row.get('available_at', row.get('as_of', 'não informada'))}",
        f"População: {row.get('population', 'não informada')}",
        f"Escopo: {scope.get('feed', 'fonte não informada')} · {scope['asset']} · {scope['expert']} · "
        f"{scope['timeframe']} · {scope['direction']}",
        f"Método/escala: {row.get('method', scope['method'])} · {row.get('scale', 'valor informado pelo produtor')}",
        f"Efeito/IC e suporte: {effect} · {interval} · {row.get('n', 'n não fornecido')} · "
        f"{row.get('support_state', 'suporte não informado')}",
        f"Estados: presença {row.get('presence_state', 'UNKNOWN')} · utilidade {row.get('utility_state', 'INCONCLUSIVE')} · "
        f"evidência {row.get('evidence_state', 'INSUFFICIENT')} · frescor {row.get('freshness_state', 'UNAVAILABLE')}",
        f"Cor semântica: {_STATE_TEXT.get(_visual_state(row), 'Inconclusivo')} · estados coloridos só passam com evidência, "
        "suporte e frescor exigidos; os estados informados não são reavaliados pela UI.",
        f"Custos/ajustes: {row.get('cost_status', 'custos não aplicados')} · "
        f"{row.get('adjustment', 'sem ajuste otimizado')}",
        f"Motivo: {row.get('reason', row.get('reason_text', 'Não informado.'))}",
    ))


_LAB_EMPTY = {
    "label": "—", "strength": "missing", "n": "sem análise", "freshness_state": "UNAVAILABLE",
    "reason": "Congele uma análise em Séries e confluências; a matriz indicador × indicador usa o mesmo relatório.",
    "lab_scope": {"feed": "nenhuma análise congelada", "asset": "—", "expert": "sem Expert", "timeframe": "—",
              "direction": "não se aplica", "method": "Spearman dos valores", "period": "—"},
    "population": "nenhuma", "cost_status": "não se aplica", "adjustment": "sem ajustes",
}
_REASON_LABELS = {"UNCONFIRMED_EVENT": "provisório", "SAME_SERIES": "mesma série", "NO_OVERLAP": "sem sobreposição",
                  "INSUFFICIENT_PAIRS": "pares < 3", "CONSTANT_SERIES": "constante",
                  "WARMUP_EXCEEDS_WINDOW": "warmup", "INELIGIBLE_DATA": "inelegível", "NO_VALUES": "sem valores",
                  "NON_NUMERIC_STORED_FEATURE": "tipo não numérico", "UNVERIFIED_STORED_EVENT": "evento não qualificado",
                  "NO_FROZEN_VALUES": "sem fatos disponíveis", "NO_DIRECTION_RULE": "sem regra direcional"}
_VERDICT_TEXT = {"HOLDOUT_CONSISTENT": "consistente fora da amostra", "CI_INCLUDES_ZERO": "IC inclui zero",
                 "HOLDOUT_SIGN_FLIP": "inverte fora", "HOLDOUT_WEAK": "fraca fora", "FIT_WEAK": "fraca no ajuste",
                 "WITHIN_NULL": "dentro do acaso", "NO_GAIN_OVER_PERSISTENCE": "não supera persistência",
                 "HOLDOUT_INSUFFICIENT": "amostra fora curta"}


def _channel_label(channel):
    return f"{channel['timeframe']} · {channel['name']}"


def _source_feed(matrix) -> str:
    """Proveniência da célula. O alvo de preço não tem manifesto; o indicador × indicador tem."""
    parts = []
    for item in matrix.get("source") or []:
        if item.get("manifest_id") and item.get("timeframe"):
            parts.append(f"{item['timeframe']} {item['manifest_id'][:10]}")
        elif item.get("first_open_utc"):
            parts.append(f"{item['first_open_utc']} … {item.get('last_close_utc', '—')}")
    return ", ".join(parts) or "análise congelada"


def _lab_cell(cell, matrix):
    """Formata a célula do serviço; estados e motivos vêm prontos, a UI só escolhe tom pelo efeito."""
    from mt5_extracao.ui.laboratory_series import event_sample_text
    effect = cell["effect"]
    if effect is None:
        label, strength = _REASON_LABELS.get(cell["reason"], cell["reason"]), "missing"
    else:
        size = abs(effect)
        tone = "neutral" if size < .1 else "weak" if size < .4 else "strong"
        label = f"{effect:+.2f}".replace("-", "−").replace(".", ",")
        strength = tone if tone == "neutral" else ("positive_" if effect > 0 else "negative_") + tone
    verdict = _VERDICT_TEXT.get(cell["verdict"], "sem corte temporal (descritiva)" if cell["lag_bars"] == 0 else "—")
    ci = cell["holdout_ci_95"]
    direction = cell["direction_correlation"]
    stored = matrix.get("alignment") == "nominal_grid_with_frozen_declared_fact_availability"
    return {
        "label": label, "strength": strength, "n": f"n={cell['n_pairs']}",
        "subtitle": f"n={cell['n_pairs']}",
        "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "utility_state": "INCONCLUSIVE",
        "evidence_state": "EXPLORATORY" if effect is not None else "INSUFFICIENT",
        "method": "Spearman dos valores alinhados", "scale": "−1…+1; sinal é associação, não lucro",
        "effect_ci": (f"IC 95% fora da amostra (direção): {ci[0]:+.2f} a {ci[1]:+.2f}" if ci else "IC não calculado"),
        "support_state": (f"{cell['unique_source_pairs']} pares de origem distintos" if cell["unique_source_pairs"]
                          is not None else "pares de origem não informados") + event_sample_text(cell.get("event_sample")),
        "segment": "descritiva" if cell["lag_bars"] == 0 else f"candidata preditiva +{cell['lag_bars']} · {verdict}",
        "period": f"{matrix['source'][0]['first_open_utc']} a {matrix['source'][0]['last_close_utc']}",
        "available_at": f"corte {matrix['as_of']} · " + (
            "grade nominal; fatos congelados usam disponibilidade declarada em cada decisão; preços/fórmulas locais descritivos"
            if stored else "fechamento nominal, recebimento histórico não comprovado"),
        "label_maturity": "não se aplica",
        "population": f"fechamentos {matrix['grid_timeframe']} com valor nos dois canais",
        "cost_status": "não se aplica (relação entre indicadores)", "adjustment": "sem otimização; defasagens fixas",
        "reason": cell["reason_text"] or (f"Correlação de direção {direction:+.2f}." if direction is not None else
            "Regra de direção não declarada; nenhuma intenção inferida." if cell.get("direction_reason") == "NO_DIRECTION_RULE"
            else "Efeito exploratório; não é rentabilidade nem preset aprovado."),
        "lab_scope": {"feed": _source_feed(matrix),
                  "asset": matrix["symbol"], "expert": "sem Expert", "timeframe": matrix["grid_timeframe"],
                  "direction": "não se aplica", "method": "Spearman", "period": matrix["as_of"]},
    }


class _MatrixGrid:
    """Matriz rolável com botões navegáveis e detalhe persistente por célula."""

    def __init__(self, parent, theme, rows, columns, cells, on_select, key):
        self.theme = theme
        self.rows = tuple(rows)
        self.columns = tuple(columns)
        self.cells = cells
        self.on_select = on_select
        self.key = key
        self.row_index = 0
        self.column_index = 0
        self.row_width = 190
        self.column_width = 154
        self.row_height = 54
        self.header_height = 32
        self.frame = ttk.Frame(parent, style="Surface.TFrame")
        self.canvas = tk.Canvas(self.frame, height=250, highlightthickness=1)
        self.theme.classic(self.canvas)
        self.v_scroll = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.canvas.yview)
        self.h_scroll = ttk.Scrollbar(self.frame, orient=tk.HORIZONTAL, command=self.canvas.xview)
        self.canvas.configure(yscrollcommand=self.v_scroll.set, xscrollcommand=self.h_scroll.set)
        self.canvas.grid(row=0, column=0, sticky="nsew")
        self.v_scroll.grid(row=0, column=1, sticky="ns")
        self.h_scroll.grid(row=1, column=0, sticky="ew")
        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(0, weight=1)
        self.canvas.bind("<Button-1>", self._click)
        self.canvas.bind("<Key>", self._key)
        self.canvas.bind("<Configure>", lambda _event: self.redraw())
        self.redraw()

    def set_data(self, rows, columns, cells):
        self.rows, self.columns, self.cells = tuple(rows), tuple(columns), cells
        self.header_height = 60 if any(len(label) > 20 for label in self.columns) else 32
        self.row_index = min(self.row_index, len(self.rows) - 1)
        self.column_index = min(self.column_index, len(self.columns) - 1)
        self.redraw()

    def redraw(self):
        if not self.canvas.winfo_exists():
            return
        c = self.theme.colors
        self.canvas.configure(background=c["surface"])
        self.canvas.delete("all")
        total_width = self.row_width + len(self.columns) * self.column_width
        total_height = self.header_height + len(self.rows) * self.row_height
        self.canvas.configure(scrollregion=(0, 0, total_width, total_height))
        self.canvas.create_text(self.row_width / 2, self.header_height / 2, text="Indicador / fator",
                                fill=c["ink2"], font=self.theme.font_bold)
        for col, label in enumerate(self.columns):
            x = self.row_width + (col + 0.5) * self.column_width
            self.canvas.create_text(x, self.header_height / 2, text=label, width=self.column_width - 10,
                                    fill=c["ink"], font=self.theme.font_bold, justify=tk.CENTER)
        alpha = {"strong": 0.28, "moderate": 0.19, "weak": 0.11, "neutral": 0.04, "missing": 0.08}
        for row, row_name in enumerate(self.rows):
            y = self.header_height + row * self.row_height
            self.canvas.create_rectangle(0, y, self.row_width, y + self.row_height, fill=c["surface"],
                                         outline=c["border"])
            self.canvas.create_text(12, y + self.row_height / 2, text=row_name, anchor="w", width=self.row_width - 20,
                                    fill=c["ink"], font=self.theme.font_bold)
            for col in range(len(self.columns)):
                cell = self.cells[row][col]
                strength = cell.get("strength", "missing")
                if strength.startswith("positive"):
                    fill = _mix(c["surface"], c["series"][2], alpha.get(strength.removeprefix("positive_"), 0.18))
                elif strength.startswith("negative"):
                    fill = _mix(c["surface"], c["series"][1], alpha.get(strength.removeprefix("negative_"), 0.18))
                elif strength == "strong":
                    fill = _mix(c["surface"], c["series"][2], alpha["strong"])
                elif strength == "moderate":
                    fill = _mix(c["surface"], c["series"][2], alpha["moderate"])
                elif strength == "weak":
                    fill = _mix(c["surface"], c["series"][1], alpha["weak"])
                elif strength == "missing":
                    fill = _mix(c["surface"], c["muted"], alpha["missing"])
                else:
                    fill = _mix(c["surface"], c["axis"], alpha["neutral"])
                x0 = self.row_width + col * self.column_width
                x1 = x0 + self.column_width
                y1 = y + self.row_height
                outline = c["accent"] if (row, col) == (self.row_index, self.column_index) else c["border"]
                width = 2 if (row, col) == (self.row_index, self.column_index) else 1
                self.canvas.create_rectangle(x0, y, x1, y1, fill=fill, outline=outline, width=width,
                                             tags=(f"cell:{row}:{col}",))
                self.canvas.create_text((x0 + x1) / 2, y + 16, text=cell.get("label", "—"),
                                        fill=c["ink"], font=self.theme.font_bold, tags=(f"cell:{row}:{col}",))
                freshness = {"CURRENT": "atual", "STALE": "STALE", "UNAVAILABLE": "sem fonte"}.get(
                    cell.get("freshness_state", "UNAVAILABLE"), "não informado")
                presence = {"PRESENT": "sim", "ABSENT": "não", "UNKNOWN": "?"}.get(
                    cell.get("presence_state", "UNKNOWN"), "?")
                self.canvas.create_text((x0 + x1) / 2, y + 39,
                                        text=cell.get("subtitle") or
                                        f"{cell.get('n', 'n não informado')} · {freshness} · setup {presence}",
                                        width=self.column_width - 8, justify=tk.CENTER,
                                        fill=c["ink2"], font=self.theme.font_small, tags=(f"cell:{row}:{col}",))
        self._announce()

    def _announce(self):
        cell = self.cells[self.row_index][self.column_index]
        self.on_select(f"{self.rows[self.row_index]} × {self.columns[self.column_index]}", cell)

    def _click(self, event):
        self.canvas.focus_set()
        x, y = self.canvas.canvasx(event.x), self.canvas.canvasy(event.y)
        row = int((y - self.header_height) // self.row_height)
        col = int((x - self.row_width) // self.column_width)
        if 0 <= row < len(self.rows) and 0 <= col < len(self.columns):
            self.row_index, self.column_index = row, col
            self.redraw()
        return "break"

    def _key(self, event):
        movements = {"Left": (0, -1), "Right": (0, 1), "Up": (-1, 0), "Down": (1, 0)}
        if event.keysym not in movements:
            return None
        dr, dc = movements[event.keysym]
        self.row_index = min(max(0, self.row_index + dr), len(self.rows) - 1)
        self.column_index = min(max(0, self.column_index + dc), len(self.columns) - 1)
        self.redraw()
        return "break"


class RelationsPanel:
    """Subvisões de relações. Números só aparecem depois de uma análise congelada."""

    def __init__(self, parent, theme, db=None, run_bg=None, write_guard=None, write_guard_factory=None, open_series=None):
        self.theme, self.db = theme, db
        self.write_guard, self.open_series = write_guard, open_series
        self.write_guard_factory = write_guard_factory
        self._creator = None
        self._windows = None
        # Sem run_bg (testes/SDK), a leitura somente agregada roda na própria thread.
        self._run_bg = run_bg or (lambda work, done: done(*self._call(work)))
        self._inventory_request = 0
        self._inventory_cursors = [None]
        self.frame = ttk.Frame(parent, style="Surface.TFrame")
        self._details: dict[str, tk.Text] = {}
        self._detail_panels: dict[str, ContentPanel] = {}
        self._matrices: list[_MatrixGrid] = []
        self._charts: dict[str, charts.Chart] = {}
        self._tables: dict[str, TablePanel] = {}
        self._timeline_iids: list[str] = []
        self._timeline_pick_cid = None
        self._build()

    def _build(self):
        head = PageHeader(self.frame, "Relações e aderência",
                          "associações, estabilidade temporal e evidência do setup")
        head.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        self.synthetic_notice = Notice(
            self.frame,
            "Inventário: registry e base. Matriz, alvo, linha do tempo, retornos e candidatos saem da análise "
            "congelada em Séries, pelo mesmo serviço do CLI. Relações salvas recupera resultados persistidos da base. "
            "Sem análise, nenhum número de exemplo é exibido. "
            "Retorno do preço não é lucro, trade nem autorização.",
            kind="warning", theme=self.theme,
        )
        _fit_label_to_parent(self.synthetic_notice.message, self.synthetic_notice, reserve=128)
        self.synthetic_notice.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        self._build_state_legend()
        self._build_filters()

        self.notebook = ttk.Notebook(self.frame, style="Compact.TNotebook")
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=(SPACING["xs"], 0))
        self.pages: dict[str, ttk.Frame] = {}
        self._page_scrolls: dict[str, _ScrollablePage] = {}
        builders = {
            "Inventário": self._build_inventory,
            "Matriz": self._build_matrix,
            "Linha do tempo": self._build_timeline,
            "Ganhos e perdas": self._build_distribution,
            "Convergências": self._build_convergence,
            "Candidatos": self._build_candidates,
        }
        for name in VIEW_NAMES:
            page = _ScrollablePage(self.notebook, self.theme)
            self.notebook.add(page, text=name)
            self.pages[name] = page
            self._page_scrolls[name] = page
            builders[name](page.content)
        self._style_timeline_toolbar()
        self._saved_context_packs = [(widget, widget.pack_info(), anchor) for widget, anchor in (
            (self.state_legend, self.notebook), (self.scope_filter_bar, self.notebook),
            (self.scope_note, self.notebook), (self.matrix_intro, self.matrix_notebook),
            (self.correlation_legend, self.matrix_notebook))]
        self._analysis_notice = self.synthetic_notice.message.cget("text")
        self.notebook.bind("<<NotebookTabChanged>>", self._sync_saved_context, add="+")

    def _configure_styles(self):
        c = self.theme.colors
        style = ttk.Style(self.theme.root)
        for state, token in _STATE_COLORS.items():
            style.configure(f"Relations.{state}.TLabel", background=c["surface"], foreground=c[token],
                            font=self.theme.font_bold)
        style.configure("Relations.Fixture.TLabel", background=c["surface"], foreground=c["muted"],
                        font=self.theme.font_small)

    def _build_state_legend(self):
        self._configure_styles()
        legend = DetailsPanel(self.frame, "Estados: presença ≠ evidência ≠ autorização",
                              expanded=False, information=True, theme=self.theme, padding=SPACING["xs"])
        self.state_legend = legend
        legend.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        header = ttk.Label(legend.body, text="Estados condicionais do produtor · presença do setup ≠ evidência ≠ autorização · "
                           "azul não aparece na fixture sintética.", style="Muted.TLabel")
        _fit_label_to_parent(header, legend.body)
        header.pack(anchor="w", fill=tk.X, pady=(0, SPACING["2xs"]))
        labels_frame = ttk.Frame(legend.body, style="Surface.TFrame")
        labels_frame.pack(fill=tk.X)
        self._legend_labels_frame = labels_frame
        labels = (
            ("FAVORABLE", "Azul · VALIDATED no OOS final + suporte/frescor adequados"),
            ("DETERIORATING", "Amarelo · comparação explícita com treino congelado"),
            ("ADVERSE", "Vermelho · VALIDATED, critério pré-fixado, suporte/frescor adequados"),
            ("INCONCLUSIVE", "Cinza · inconclusivo/insuficiente/ausente/STALE"),
        )
        self._legend_labels = []
        for state, label_text in labels:
            label = ttk.Label(labels_frame, text=f"● {label_text}", style=f"Relations.{state}.TLabel",
                              justify=tk.LEFT)
            self._legend_labels.append((label, state, label_text))

        def resize_legend(event):
            columns = 2 if event.width >= 1000 else 1
            for column in range(2):
                labels_frame.columnconfigure(column, weight=1 if column < columns else 0,
                                             uniform="relations-state-legend" if column < columns else "")
            for index, (label, _state, _text) in enumerate(self._legend_labels):
                label.grid(row=index // columns, column=index % columns, sticky="ew",
                           padx=(SPACING["sm"], 0) if columns == 2 and index % columns else (0, 0),
                           pady=(0, SPACING["2xs"]))
                label.configure(wraplength=max(220, (event.width - 24) // columns))

        labels_frame.bind("<Configure>", resize_legend, add="+")

    def _build_filters(self):
        bar = FilterBar(self.frame, padding=SPACING["xs"])
        bar.pack(fill=tk.X)
        self.scope_filter_bar = bar
        self.scope_blocks = []
        self.scope_labels = {}
        for column, (key, label) in enumerate((("asset", "Feed/ativo/contrato"), ("expert", "Expert/versão/preset"),
                                               ("timeframe", "Timeframe"), ("direction", "Direção"))):
            block = ttk.Frame(bar, style="Surface.TFrame")
            block.grid(row=0, column=column, sticky="ew", padx=SPACING["xs"])
            scope_label = ttk.Label(block, text=f"{label}: —",
                                    style="Surface.TLabel", justify=tk.LEFT)
            self.scope_labels[key] = scope_label
            _fit_label_to_parent(scope_label, block, reserve=16, minimum=180)
            scope_label.pack(anchor="w", fill=tk.X)
            self.scope_blocks.append(block)
            bar.columnconfigure(column, weight=1, uniform="relations-filter")
        self.advanced = DetailsPanel(bar, "Recorte avançado", expanded=False, information=True,
                                     theme=self.theme, padding=SPACING["xs"])
        self.advanced.grid(row=1, column=0, columnspan=4, sticky="ew", padx=SPACING["xs"],
                           pady=(SPACING["xs"], 0))
        self.advanced_blocks = []
        self.advanced_labels = {}
        for column, (key, label) in enumerate((("target", "Alvo"), ("horizon", "Horizonte"), ("method", "Método/escala"),
                                               ("period", "Período"), ("window", "Janela"), ("step", "Passo"))):
            block = ttk.Frame(self.advanced.body, style="Surface.TFrame")
            block.grid(row=column // 3, column=column % 3, sticky="ew", padx=SPACING["xs"], pady=SPACING["2xs"])
            ttk.Label(block, text=label, style="Muted.TLabel").pack(anchor="w")
            detail_value = ttk.Label(block, text="—", style="Surface.TLabel")
            self.advanced_labels[key] = detail_value
            _fit_label_to_parent(detail_value, block, reserve=16, minimum=180)
            detail_value.pack(anchor="w", fill=tk.X)
            self.advanced_blocks.append(block)
            self.advanced.body.columnconfigure(column % 3, weight=1, uniform="advanced-filter")
        self.advanced.body.columnconfigure(0, weight=1)
        self.advanced.body.columnconfigure(1, weight=1)
        self.advanced.body.columnconfigure(2, weight=1)
        scope_note = ttk.Label(self.frame,
                               text="Recorte da análise em Séries; estes campos não filtram nem recalculam. Relações salvas exibe seu próprio recorte.",
                               style="Relations.Fixture.TLabel")
        self.scope_note = scope_note
        _fit_label_to_parent(scope_note, self.frame)
        scope_note.pack(anchor="w", fill=tk.X, pady=(SPACING["2xs"], 0))

        def resize_scope(event):
            columns = 1 if event.width < 620 else 2 if event.width < 1040 else 4
            if getattr(bar, "_relations_columns", None) != columns:
                bar._relations_columns = columns
                for index, block in enumerate(self.scope_blocks):
                    block.grid_configure(row=index // columns, column=index % columns)
                detail_row = (len(self.scope_blocks) + columns - 1) // columns
                self.advanced.grid_configure(row=detail_row, column=0, columnspan=columns)
                for index in range(4):
                    bar.columnconfigure(index, weight=1 if index < columns else 0,
                                        uniform="relations-filter" if index < columns else "")

            detail_columns = 1 if event.width < 620 else 2 if event.width < 1040 else 3
            if getattr(bar, "_relations_detail_columns", None) != detail_columns:
                bar._relations_detail_columns = detail_columns
                for index, block in enumerate(self.advanced_blocks):
                    block.grid_configure(row=index // detail_columns, column=index % detail_columns)
                for index in range(3):
                    self.advanced.body.columnconfigure(index, weight=1 if index < detail_columns else 0,
                                                       uniform="advanced-filter" if index < detail_columns else "")

        bar.bind("<Configure>", resize_scope, add="+")

    def _page_detail(self, parent, key, visible_lines=4):
        panel = DetailsPanel(parent, title="Detalhe da seleção", expanded=False, information=True,
                             on_toggle=getattr(parent, "_relations_reflow", None), theme=self.theme,
                             padding=SPACING["xs"])
        parent._relations_disclosure = panel
        area = ttk.Frame(panel.body, style="Surface.TFrame")
        area.pack(fill=tk.X)
        text = self.theme.classic(tk.Text(area, width=36, height=visible_lines, wrap=tk.WORD, undo=False,
                                          font=self.theme.font, padx=SPACING["2xs"], pady=SPACING["2xs"]))
        scroll = ttk.Scrollbar(area, orient=tk.VERTICAL, command=text.yview)
        text.configure(yscrollcommand=scroll.set, state=tk.DISABLED)
        text.grid(row=0, column=0, sticky="nsew")
        scroll.grid(row=0, column=1, sticky="ns")
        panel.title_label.configure(wraplength=340, justify=tk.LEFT)
        area.columnconfigure(0, weight=1)
        area.rowconfigure(0, weight=1)
        text.bind("<Configure>", lambda event, target=text: target.configure(wrap="word"))
        text.bind("<Escape>", panel.collapse)
        self._details[key] = text
        self._detail_panels[key] = panel
        return panel

    def _split_view(self, parent, primary_weight=3, detail_weight=1):
        """Gráfico/tabela e detalhe ficam lado a lado; em largura estreita as áreas são empilhadas."""
        split = ttk.Frame(parent, style="Surface.TFrame")
        split.pack(fill=tk.BOTH, expand=True)
        primary = ttk.Frame(split, style="Surface.TFrame")
        detail = ttk.Frame(split, style="Surface.TFrame")
        primary.grid(row=0, column=0, sticky="nsew")
        detail.grid(row=0, column=1, sticky="nsew", padx=(SPACING["xs"], 0))
        split._relations_orientation = None
        split._relations_layout = None

        def resize(_event=None):
            width = split.winfo_width()
            disclosure = getattr(detail, "_relations_disclosure", None)
            expanded = disclosure is not None and disclosure.expanded
            detail_min = max(220, self.theme.font.measure("0" * 28) + 2 * SPACING["md"])
            # A proporção também precisa reservar espaço ao texto, não apenas ao gráfico.
            detail_column_min = detail_min + SPACING["xs"]
            minimum = detail_column_min * (primary_weight + detail_weight) / detail_weight
            split._relations_detail_minwidth = detail_min
            orientation = tk.VERTICAL if not expanded or width < max(900, minimum) else tk.HORIZONTAL
            layout = (orientation, expanded)
            if split._relations_layout != layout:
                split._relations_layout = layout
                split._relations_orientation = orientation
                if orientation == tk.VERTICAL:
                    primary.grid_configure(row=0, column=0, columnspan=2, padx=0)
                    detail.grid_configure(row=1, column=0, columnspan=2, padx=0, pady=(SPACING["xs"], 0))
                    split.rowconfigure(0, weight=3 if expanded else 1)
                    split.rowconfigure(1, weight=2 if expanded else 0)
                    split.columnconfigure(0, weight=1, minsize=0, uniform="")
                    split.columnconfigure(1, weight=0, minsize=0, uniform="")
                else:
                    primary.grid_configure(row=0, column=0, columnspan=1, padx=0)
                    detail.grid_configure(row=0, column=1, columnspan=1, padx=(SPACING["xs"], 0), pady=0)
                    split.rowconfigure(0, weight=1)
                    split.rowconfigure(1, weight=0)
                    split.columnconfigure(0, weight=primary_weight, uniform="relations-split",
                                          minsize=round(detail_column_min * primary_weight / detail_weight))
                    split.columnconfigure(1, weight=detail_weight, uniform="relations-split",
                                          minsize=detail_column_min)

        split.bind("<Configure>", resize, add="+")
        detail._relations_reflow = resize
        return split, primary, detail

    def _set_detail(self, key, title, row):
        label = self._details.get(key)
        if label is None:
            return
        self._detail_panels[key].title_label.configure(text=f"Detalhe da seleção · {title}")
        label.configure(state=tk.NORMAL)
        label.delete("1.0", tk.END)
        label.insert("1.0", _detail_text(row, row.get("lab_scope")))
        label.configure(state=tk.DISABLED)

    @staticmethod
    def _bind_table(table: TablePanel, rows, key):
        tree = table.tree
        tree.delete(*tree.get_children())
        for index, row in enumerate(rows):
            tag = _visual_state(row)
            tree.insert("", tk.END, iid=f"{key}-{index}", values=row["values"], tags=(tag,))

    def _style_tree_states(self, tree):
        c = self.theme.colors
        for state, token in _STATE_COLORS.items():
            tree.tag_configure(state, foreground=c[token])

    @staticmethod
    def _set_headings(table, labels):
        for column, text in labels.items():
            table.tree.heading(column, text=text)

    def _build_matrix(self, parent):
        intro = ttk.Label(parent,
                          text="Coeficientes de −1 a +1, zero no centro. As matrizes da análise usam Spearman; relações salvas informam o método original. Associação não mostra lucro, utilidade ou aprovação.",
                          style="Muted.TLabel", justify=tk.LEFT)
        self.matrix_intro = intro
        _fit_label_to_parent(intro, parent)
        intro.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        self.correlation_legend = ttk.Frame(parent, style="Surface.TFrame")
        self.correlation_legend.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        self.correlation_swatches = []
        legend_items = (("negative_strong", "Inversa forte"), ("negative_weak", "Inversa fraca"),
                        ("neutral", "≈ 0 · associação fraca"), ("positive_weak", "Direta fraca"),
                        ("positive_strong", "Direta forte"))
        for column, (tone, label) in enumerate(legend_items):
            swatch = self.theme.classic(tk.Canvas(self.correlation_legend, width=28, height=16,
                                                  highlightthickness=0))
            swatch.grid(row=0, column=column * 2, padx=(SPACING["xs"], 0), pady=SPACING["2xs"])
            text = ttk.Label(self.correlation_legend, text=label, style="Muted.TLabel")
            text.grid(row=0, column=column * 2 + 1, sticky="w", padx=(SPACING["2xs"], SPACING["sm"]))
            self.correlation_swatches.append((swatch, tone))
        self._draw_correlation_legend()
        inner = ttk.Notebook(parent, style="Compact.TNotebook")
        inner.pack(fill=tk.BOTH, expand=True)
        self.matrix_notebook = inner
        indicator_frame = ttk.Frame(inner, padding=SPACING["xs"], style="Surface.TFrame")
        target_frame = ttk.Frame(inner, padding=SPACING["xs"], style="Surface.TFrame")
        inner.add(indicator_frame, text="Indicador × indicador")
        inner.add(target_frame, text="Indicador × alvo")
        self.indicator_split, indicator_primary, indicator_details = self._split_view(indicator_frame, 4, 1)
        self.target_split, target_primary, target_details = self._split_view(target_frame, 4, 1)
        self.indicator_detail = self._page_detail(indicator_details, "matrix-indicators", visible_lines=15)
        self.target_detail = self._page_detail(target_details, "matrix-targets", visible_lines=15)
        self.matrix_details = {"indicators": self.indicator_detail, "targets": self.target_detail}
        controls = ttk.Frame(indicator_primary, style="Surface.TFrame")
        controls.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        ttk.Label(controls, text="Defasagem do preditor (barras):", style="Muted.TLabel").pack(side=tk.LEFT)
        self.lab_lag = tk.StringVar(value="0")
        self.lab_lag_box = ttk.Combobox(controls, textvariable=self.lab_lag, width=4, state="readonly",
                                        values=[str(lag) for lag in (0, *PREDICTIVE_LAGS)])
        self.lab_lag_box.pack(side=tk.LEFT, padx=SPACING["xs"])
        self.lab_lag_box.bind("<<ComboboxSelected>>", lambda _event: self._refresh_lab_matrix())
        # Linha própria: quebrar o texto ao lado do combobox realimenta a largura do contêiner.
        self.lab_matrix_status = ttk.Label(indicator_primary, text=_LAB_EMPTY["reason"], style="Muted.TLabel",
                                           justify=tk.LEFT)
        self.lab_matrix_status.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        _fit_label_to_parent(self.lab_matrix_status, indicator_primary)
        self.lab_report = self.lab_matrix = None
        self._lab_request = 0
        self.indicator_grid = _MatrixGrid(indicator_primary, self.theme, ("Sem análise",), ("—",), [[_LAB_EMPTY]],
                                          lambda title, row: self._set_detail("matrix-indicators", title, row),
                                          "indicator-matrix")
        self.indicator_grid.frame.pack(fill=tk.BOTH, expand=True)
        self.indicator_detail.pack(fill=tk.BOTH, expand=True, padx=(SPACING["xs"], 0))
        self.target_grid = _MatrixGrid(target_primary, self.theme, ("Sem análise",), ("Retorno do preço",),
                                       [[_LAB_EMPTY]],
                                       lambda title, row: self._set_detail("matrix-targets", title, row),
                                       "target-matrix")
        self.target_grid.frame.pack(fill=tk.BOTH, expand=True)
        self.target_detail.pack(fill=tk.BOTH, expand=True, padx=(SPACING["xs"], 0))
        self._matrices.extend((self.indicator_grid, self.target_grid))
        self._set_detail("matrix-indicators", "Sem análise", _LAB_EMPTY)
        self._set_detail("matrix-targets", "Sem análise", _LAB_EMPTY)
        self.saved_frame = ttk.Frame(inner, padding=SPACING["xs"], style="Surface.TFrame")
        inner.add(self.saved_frame, text="Relações salvas")
        self._build_saved_relations(self.saved_frame)
        def saved_tab_changed(_event):
            self._sync_saved_context()
            if inner.select() == str(self.saved_frame) and not self._saved_loaded and not self._saved_loading:
                self.load_saved_relations()
        inner.bind("<<NotebookTabChanged>>", saved_tab_changed, add="+")

    def _sync_saved_context(self, _event=None):
        if not hasattr(self, "_saved_context_packs"):
            return
        active = (self.notebook.select() == str(self.pages["Matriz"])
                  and self.matrix_notebook.select() == str(self.saved_frame))
        if active == getattr(self, "_saved_scope_active", None):
            return
        self._saved_scope_active = active
        for widget, options, anchor in self._saved_context_packs:
            if active:
                widget.pack_forget()
            elif not widget.winfo_manager():
                widget.pack(before=anchor, **{k:v for k,v in options.items() if k not in ("in", "before", "after")})
        self.synthetic_notice.message.configure(text=(
            "Resultados exploratórios salvos na base. Selecione um par para conferir amostra e procedência; "
            "associação não comprova ganho." if active else self._analysis_notice))


    @staticmethod
    def _saved_number(value):
        return "Indisponível" if value is None else format(value, "+.4f").replace(".", ",")

    def _build_saved_relations(self, parent):
        self._saved_request = self._saved_selection_request = 0
        self._saved_loaded = self._saved_loading = False
        self._saved_reading = self._transfer_pending = False
        self._transfer_cancel = threading.Event()
        self._saved_filter_entries = []
        self._transfer_result = None
        self.frame.bind("<Destroy>", self._transfer_destroyed, add="+")
        self._saved_cursors, self._saved_rows = [None], {}
        self._saved_next_cursor = None
        controls = ttk.Frame(parent, style="Surface.TFrame")
        controls.pack(fill=tk.X)
        self.saved_symbol, self.saved_timeframe = tk.StringVar(), tk.StringVar()
        for title, variable, width in (("Ativo:", self.saved_symbol, 12), ("Timeframe:", self.saved_timeframe, 6)):
            ttk.Label(controls, text=title).pack(side=tk.LEFT)
            entry = ttk.Entry(controls, textvariable=variable, width=width)
            self._saved_filter_entries.append(entry)
            entry.pack(side=tk.LEFT, padx=SPACING["xs"])
            entry.bind("<Return>", lambda _event: self.load_saved_relations())
            variable.trace_add("write", self._saved_filters_changed)
        self.saved_refresh = ttk.Button(controls, text="Consultar", command=self.load_saved_relations)
        self.saved_refresh.pack(side=tk.LEFT)
        self.saved_new = ttk.Button(controls, text="Criar par…", command=self.open_relation_creator)
        self.saved_new.pack(side=tk.LEFT, padx=SPACING["xs"])
        self.saved_previous = ttk.Button(controls, text="Anterior", width=8, command=lambda: self._saved_page(-1))
        self.saved_next = ttk.Button(controls, text="Próxima", width=8, command=lambda: self._saved_page(1))
        self.saved_next.pack(side=tk.RIGHT);self.saved_previous.pack(side=tk.RIGHT)
        self.saved_status = ttk.Label(parent, text="Consulte os resultados salvos; filtros vazios mostram todos os recortes.",
                                      style="Muted.TLabel")
        self.saved_status.pack(fill=tk.X, pady=SPACING["xs"])
        _fit_label_to_parent(self.saved_status, parent)
        summary = ContentPanel(parent, title="Resultado selecionado", padding=SPACING["xs"])
        summary.pack(fill=tk.X)
        self.saved_effect = ttk.Label(summary.body, text="Selecione uma relação", font=self.theme.font_value, style="Surface.TLabel")
        self.saved_effect.pack(anchor="w")
        self.saved_scope = ttk.Label(summary.body, text="Resultado original; sem recalcular.", style="Muted.TLabel", justify=tk.LEFT)
        self.saved_scope.pack(fill=tk.X);_fit_label_to_parent(self.saved_scope, summary.body)
        self.saved_sample = ttk.Label(summary.body, text="", style="Surface.TLabel", justify=tk.LEFT)
        self.saved_sample.pack(fill=tk.X);_fit_label_to_parent(self.saved_sample, summary.body)
        self.saved_qualification = ttk.Label(summary.body, text="Exploratório: sem causalidade ou utilidade financeira comprovadas.",
                                              style="Muted.TLabel", justify=tk.LEFT)
        self.saved_qualification.pack(fill=tk.X);_fit_label_to_parent(self.saved_qualification, summary.body)
        transfers = ttk.Frame(parent, style="Surface.TFrame")
        transfers.pack(fill=tk.X, pady=SPACING["xs"])
        self.saved_import = ttk.Button(transfers, text="Importar par…", command=self.import_saved_relation)
        self.saved_import.pack(side=tk.LEFT)
        self.saved_export = ttk.Button(transfers, text="Exportar selecionado…", command=self.export_saved_relation)
        self.saved_export.pack(side=tk.LEFT, padx=SPACING["xs"])
        self.saved_windows = ttk.Button(transfers, text="Janelas móveis…", command=self.open_relation_windows)
        self.saved_windows.pack(side=tk.LEFT, padx=SPACING["xs"])
        self.saved_cancel = ttk.Button(transfers, text="Cancelar transferência", command=self.cancel_saved_transfer)
        self.saved_cancel.pack(side=tk.LEFT)
        self.saved_table = TablePanel(parent, "Resultados armazenados · origem conferida ao selecionar",
            columns=("scope", "left", "right", "method", "lag", "effect", "pairs"),
            widths=(100, 210, 210, 85, 65, 115, 65), anchors=("w","w","w","center","center","center","center"),
            height=5, xscroll=True, padding=SPACING["xs"])
        self.saved_table.pack(fill=tk.BOTH, expand=True, pady=SPACING["xs"])
        self._set_headings(self.saved_table, dict(scope="Ativo / TF", left="Série esquerda · versão",
            right="Série direita · versão", method="Método", lag="Defasagem", effect="Coeficiente", pairs="Pares"))
        self.saved_table.tree.bind("<<TreeviewSelect>>", self._saved_selected, add="+")
        self._saved_selectmode = str(self.saved_table.tree.cget("selectmode"))
        detail = self._page_detail(parent, "saved-relations", visible_lines=9)
        detail.pack(fill=tk.X)
        self._clear_saved_relation("Selecione um resultado para conferir suas origens e detalhes.")
        self.saved_previous.state(["disabled"]);self.saved_next.state(["disabled"])

    def _clear_saved_relation(self, message):
        self._saved_selection_request += 1
        self._saved_selected_key = None
        self._saved_dto = None
        self._saved_reading = False
        self.saved_effect.configure(text="Selecione uma relação")
        self.saved_scope.configure(text=message);self.saved_sample.configure(text="")
        self.saved_qualification.configure(text="Exploratório: sem causalidade ou utilidade financeira comprovadas.")
        self._write_saved_detail(message)
        self._sync_transfer_controls()

    def _write_saved_detail(self, message):
        text = self._details["saved-relations"]
        text.configure(state=tk.NORMAL);text.delete("1.0", tk.END);text.insert("1.0", message);text.configure(state=tk.DISABLED)

    def _saved_filters_changed(self, *_args):
        if self._transfer_pending:self.cancel_saved_transfer()
        self._saved_request += 1
        self._saved_rows, self._saved_cursors, self._saved_next_cursor = {}, [None], None
        self.saved_table.tree.delete(*self.saved_table.tree.get_children())
        self.saved_previous.state(["disabled"]);self.saved_next.state(["disabled"])
        self._saved_loading = False
        self.saved_refresh.state(["!disabled"])
        self.saved_status.configure(text="Filtros alterados. Clique Consultar para carregar o novo recorte.")
        self._clear_saved_relation("Nenhuma relação selecionada no novo recorte.")
        if self._transfer_pending:self.saved_status.configure(text="Filtros alterados; cancelando a transferência anterior — aguarde…")

    def load_saved_relations(self, cursor=None, *, reset=True, analysis_id=None, focused_message=None):
        if self._transfer_pending:return
        from mt5_extracao import sdk
        self._saved_request += 1
        request, db = self._saved_request, self.db
        self._saved_loading = True
        self.saved_refresh.state(["disabled"])
        symbol, timeframe = self.saved_symbol.get().strip() or None, self.saved_timeframe.get().strip() or None
        if reset:self._saved_cursors = [None]
        self._saved_rows = {};self._saved_next_cursor = None
        self.saved_table.tree.delete(*self.saved_table.tree.get_children())
        self.saved_previous.state(["disabled"]);self.saved_next.state(["disabled"])
        self.saved_status.configure(text="Carregando relações salvas — aguarde…")
        self._clear_saved_relation("Aguarde a consulta do recorte selecionado.")
        def work():
            return sdk.query_saved_relations(db, symbol=symbol, timeframe=timeframe, analysis_id=analysis_id, page_size=25, cursor=cursor)
        def done(result, error):
            if request != self._saved_request or not self.saved_table.tree.winfo_exists():return
            self._saved_loaded = True
            self._saved_loading = False
            self.saved_refresh.state(["!disabled"])
            self._sync_transfer_controls()
            if error or result["status"] != "ok":
                message = str(error) if error else "; ".join(item["message"] for item in result["errors"])
                self.saved_status.configure(text="Consulta indisponível: "+message)
                self._clear_saved_relation("Consulte novamente após corrigir a causa. "+message)
                self._saved_cursors = [None]
                return
            for index, row in enumerate(result["items"]):
                key = "saved-"+str(index);self._saved_rows[key] = row
                self.saved_table.tree.insert("", tk.END, iid=key, values=(row["symbol"]+" / "+row["timeframe"],
                    row["left"]["name"]+" · "+row["left"]["algorithm_version"],
                    row["right"]["name"]+" · "+row["right"]["algorithm_version"],row["relation"]["method"],
                    row["lag_bars"],self._saved_number(row["relation"]["effect"]),row["relation"]["n_pairs"]))
            self._saved_next_cursor = result["page"]["next_cursor"]
            self.saved_next.state(["!disabled"] if self._saved_next_cursor else ["disabled"])
            self.saved_previous.state(["!disabled"] if len(self._saved_cursors)>1 else ["disabled"])
            self.saved_status.configure(text="; ".join(result["reasons"]) if not result["items"] else
                (focused_message or "Resultado recém salvo selecionado. Consulte novamente para ver os demais pares.") if analysis_id else
                f"{result['matched']} relações salvas · página {len(self._saved_cursors)} · selecione uma linha para conferir as origens.")
            if result["items"]:
                self.saved_table.tree.selection_set("saved-0");self._saved_selected(None)
            else:self._clear_saved_relation("Nenhuma relação corresponde ao recorte atual.")
        self._run_bg(work, done)

    def _saved_page(self, step):
        if self._transfer_pending:return
        if step > 0 and self._saved_next_cursor:self._saved_cursors.append(self._saved_next_cursor)
        elif step < 0 and len(self._saved_cursors)>1:self._saved_cursors.pop()
        else:return
        self.load_saved_relations(self._saved_cursors[-1], reset=False)

    def _saved_selected(self, _event):
        if self._transfer_pending:return
        from mt5_extracao import sdk
        selection = self.saved_table.tree.selection()
        row = self._saved_rows.get(selection[0]) if selection else None
        if row is None:return
        selected_key = self._saved_request, row["analysis_id"]
        if getattr(self, "_saved_selected_key", None) == selected_key:return
        self._clear_saved_relation("Conferindo resultado e origens — aguarde…")
        self._saved_selected_key = selected_key
        self._saved_reading = True
        self._sync_transfer_controls()
        token, request, ident = self._saved_selection_request, self._saved_request, row["analysis_id"]
        self.saved_effect.configure(text="Aguarde…")
        def work():return sdk.load_feature_relation(self.db, ident)
        def done(dto, error):
            if token != self._saved_selection_request or request != self._saved_request or not self.saved_table.tree.winfo_exists():return
            self._saved_reading = False
            if error:
                self._saved_selected_key = None
                self.saved_effect.configure(text="Indisponível");self.saved_scope.configure(text=str(error))
                self._write_saved_detail("Resultado não exibido: "+str(error));self._sync_transfer_controls();return
            self._show_saved_relation(dto)
        self._run_bg(work, done)

    def _show_saved_relation(self, dto):
        self._saved_dto = dto
        self._sync_transfer_controls()
        ident = dto["provenance"]["analysis_id"]
        relation, population, scope, provenance = (dto[key] for key in ("relation","population","scope","provenance"))
        self.saved_effect.configure(text=self._saved_number(relation["effect"])+" · "+relation["method"].title())
        def label(side):
            item=dto[side]
            return f"{item['definition'].get('name') or item['feature_id']} · versão {item['algorithm_version']} [{item['unit'] or 'unidade não declarada'}]"
        window = scope.get("decision_window")
        interval = (f"Janela UTC {window['start_inclusive_utc']} → {window['end_inclusive_utc']} · "
                    f"defasagem {scope['lag_bars']} nominal." if window else
                    f"Corte UTC {provenance['as_of_utc']} · esquerda antecede a direita em {scope['lag_bars']} períodos nominais.")
        self.saved_scope.configure(text=f"{scope['symbol']} / {scope['timeframe']} · {label('left')} × {label('right')}\n"+interval)
        self.saved_sample.configure(text=f"Pares: {relation['n_pairs']} · fontes distintas: {population['n_distinct_source_pairs']} · "
            f"sessões: {relation['n_sessions']} · excluídos: {sum(population['exclusions'].values())} de {population['n_total']}.")
        self.saved_qualification.configure(text=("Referência sintética · " if dto['synthetic_fixture'] else "")+
            "Exploratório; sem causalidade ou utilidade financeira comprovadas."+
            (" Resultado insuficiente: consulte o motivo nos detalhes." if relation['effect'] is None else ""))
        def number(value):return "não estimado" if value is None else format(value, ".6g").replace(".", ",")
        lines=[f"ID: {ident}",f"Snapshot: {provenance['snapshot_manifest_id']}",f"Esquerda: {dto['left']['feature_id']}",
            f"Direita: {dto['right']['feature_id']}",f"Motivo: {relation['reason'] or 'amostra numérica disponível'}",
            f"Corte UTC: {provenance['as_of_utc']}",
            "Exclusões: "+json.dumps(population['exclusions'],ensure_ascii=False),
            f"Intervalo do efeito: {number(relation['effect_ci_low'])} a {number(relation['effect_ci_high'])}",
            f"p bruto: {number(relation['p_raw'])}; p ajustado: {number(relation['p_adjusted'])}",
            "Origens conferidas: "+", ".join(provenance['lineage_manifests']),
            "Gerado em UTC: "+dto['generated_at_utc'],
            "Contagem de fontes não é amostra independente certificada; resultado original, sem recalcular."]
        if window:lines.append("Janela: "+json.dumps(window,ensure_ascii=False))
        self._write_saved_detail("\n".join(lines))

    def open_relation_creator(self):
        if self._transfer_pending:return
        if self._windows is not None and self._windows.alive():self._windows.window.lift();return
        from mt5_extracao.ui.relation_creator import RelationCreator
        if self._creator is not None and self._creator.alive():
            self._creator.window.lift()
            return
        chosen = getattr(self, "_saved_dto", None)
        report = self.lab_report or {}
        populations = {item["manifest_id"]:item for item in report.get("populations", [])}
        captures = {ident:dict(manifest_id=ident, symbol=populations.get(ident,{}).get("symbol",report.get("symbol")),
            timeframe=populations.get(ident,{}).get("timeframe",report.get("grid_timeframe")))
            for ident in report.get("selection",{}).get("manifests",[])}
        settings = {}
        as_of = report.get("selection",{}).get("as_of")
        synthetic = False
        if chosen is not None:
            ident=chosen["provenance"]["snapshot_manifest_id"]
            captures={ident:dict(manifest_id=ident,symbol=chosen["scope"]["symbol"],timeframe=chosen["scope"]["timeframe"]),**captures}
            settings=dict(chosen["scope"].get("settings",{}))
            as_of=chosen["provenance"]["as_of_utc"]
            synthetic=chosen["synthetic_fixture"]
        else:
            pairs=report.get("selection",{}).get("stored_features",[])
            if pairs:settings["left"]=pairs[0]
            if len(pairs)>1:settings["right"]=pairs[1]
        self._creator=RelationCreator(self.frame,self.theme,db=self.db,run_bg=self._run_bg,captures=list(captures.values()),
            as_of=as_of,settings=settings,synthetic_fixture=synthetic,write_guard=self.write_guard,
            write_guard_factory=self.write_guard_factory,
            on_saved=self._relation_created,open_series=self.open_series)

    def _relation_created(self, receipt, *, imported=False):
        scope=receipt["relation"]["scope"]
        self.saved_symbol.set(scope["symbol"]);self.saved_timeframe.set(scope["timeframe"])
        message = None
        if imported:
            message = ("Relação importada selecionada." if receipt["status"]=="INSERTED" else
                       "Relação já existente selecionada; recibo original preservado.")
            message += " Consulte novamente para ver os demais pares."
        self.load_saved_relations(analysis_id=receipt["analysis_id"], focused_message=message)

    def _sync_transfer_controls(self):
        if not hasattr(self, "saved_import"):return
        pending = self._transfer_pending
        unavailable = pending or self._saved_loading or self._saved_reading or self.db is None
        self.saved_import.state(["disabled"] if unavailable else ["!disabled"])
        self.saved_export.state(["disabled"] if unavailable or self._saved_dto is None else ["!disabled"])
        self.saved_windows.state(["disabled"] if unavailable or self._saved_dto is None else ["!disabled"])
        self.saved_cancel.state(["!disabled"] if pending and not self._transfer_cancel.is_set() else ["disabled"])
        self.saved_new.state(["disabled"] if pending else ["!disabled"])
        self.saved_refresh.state(["disabled"] if pending or self._saved_loading else ["!disabled"])
        for entry in self._saved_filter_entries:entry.state(["disabled"] if pending else ["!disabled"])
        if hasattr(self, "_saved_selectmode"):
            self.saved_table.tree.configure(selectmode="none" if pending else self._saved_selectmode)
            self.saved_previous.state(["!disabled"] if not pending and not self._saved_loading and len(self._saved_cursors)>1 else ["disabled"])
            self.saved_next.state(["!disabled"] if not pending and not self._saved_loading and self._saved_next_cursor else ["disabled"])

    def _can_transfer(self):
        if self._transfer_pending or self._saved_loading or self._saved_reading:return False
        if self._creator is not None and self._creator.alive():
            self._creator.window.lift()
            return False
        if self._windows is not None and self._windows.alive():self._windows.window.lift();return False
        return self.db is not None

    def open_relation_windows(self):
        if self._transfer_pending or self._saved_loading or self._saved_reading or self._saved_dto is None:return
        if self._creator is not None and self._creator.alive():self._creator.window.lift();return
        if self._windows is not None and self._windows.alive():self._windows.window.lift();return
        from mt5_extracao.ui.relation_windows import RelationWindows
        db,request,original=self.db,self._saved_request,self._saved_dto
        def guard_factory():
            if self.db is not db:raise ValueError("Base alterada; feche e reabra o relatório antes de salvar a janela.")
            return self.write_guard_factory() if self.write_guard_factory is not None else self.write_guard
        def saved(receipt):
            if self.db is db and self._saved_request==request:self._relation_created(receipt)
            else:
                if self.db is not db and self._saved_dto is original:self._saved_filters_changed()
                self.saved_status.configure(text="Janela salva no contexto original; consulte-o para selecionar o resultado.")
        self._windows=RelationWindows(self.frame,self.theme,db=db,relation=self._saved_dto,run_bg=self._run_bg,
            write_guard_factory=guard_factory,on_saved=saved)

    def import_saved_relation(self, path=None):
        if not self._can_transfer():return
        db = self.db
        path = path or filedialog.askopenfilename(parent=self.frame, title="Importar relação e capturas congeladas",
            filetypes=[("Pacote de relação JSON", "*.json")])
        if path and self.db is db:self._start_saved_transfer("import", path)

    def export_saved_relation(self, path=None):
        if not self._can_transfer() or self._saved_dto is None:return
        db, ident = self.db, self._saved_dto["provenance"]["analysis_id"]
        path = path or filedialog.asksaveasfilename(parent=self.frame, title="Exportar relação — arquivo novo",
            initialfile="relacao-"+ident[:12]+".json", defaultextension=".json", confirmoverwrite=False,
            filetypes=[("Pacote de relação JSON", "*.json")])
        if path and self.db is db and self._saved_dto is not None and self._saved_dto["provenance"]["analysis_id"]==ident:
            self._start_saved_transfer("export", path, ident)

    def _start_saved_transfer(self, action, path, ident=None):
        from mt5_extracao import sdk
        if not self._can_transfer():return
        db, request = self.db, self._saved_request
        try:
            # make() só captura a política; medições/SQL ficam no worker. Obter
            # aqui fixa o guarda na base escolhida antes de outro evento de UI.
            guard = (self.write_guard_factory() if self.write_guard_factory is not None else self.write_guard) if action=="import" else None
        except Exception as exc:
            self.saved_status.configure(text="Importação: não foi possível obter a política de escrita. "+str(exc))
            return
        self._transfer_pending = True
        self._transfer_cancel.clear()
        self._transfer_result = None
        self._sync_transfer_controls()
        verb = "Importando" if action=="import" else "Exportando"
        self.saved_status.configure(text=verb+" relação e capturas — aguarde…")
        def work():
            if action=="export":
                return sdk.export_feature_relation(db, ident, path, cancel_check=self._transfer_cancel.is_set)
            return sdk.import_feature_relation(db, path, write_guard=guard, cancel_check=self._transfer_cancel.is_set)
        def done(result, error):
            try:
                if not self.frame.winfo_exists():return
            except tk.TclError:return
            self._transfer_pending = False
            self._sync_transfer_controls()
            unchanged = self.db is db and self._saved_request==request
            if error:
                context = " do contexto anterior" if not unchanged else ""
                self.saved_status.configure(text=("Importação" if action=="import" else "Exportação")+context+": "+str(error))
                return
            self._transfer_result = dict(action=action, path=str(Path(path)), result=result)
            if action=="import" and unchanged:
                self._relation_created(result, imported=True)
            else:
                self.saved_status.configure(text=("Importação concluída na base original; consulte-a para selecionar o resultado." if action=="import" else
                    f"Exportação concluída: recibo original e {result['snapshots']} captura(s) no arquivo escolhido."))
        self._run_bg(work, done)

    def cancel_saved_transfer(self):
        if self._transfer_pending:
            self._transfer_cancel.set()
            self.saved_cancel.state(["disabled"])
            self.saved_status.configure(text="Cancelando transferência — aguarde a confirmação.")

    def _transfer_destroyed(self, event):
        if event.widget is self.frame:self._transfer_cancel.set()

    def set_lab_report(self, report):
        """Recebe o relatório congelado de Séries; matrizes e cruzamentos saem do mesmo serviço do CLI."""
        if report is None or (self.lab_report or {}).get("analysis_id") == report["analysis_id"]:
            return
        self.lab_report = report
        self._refresh_lab_matrix()

    def _refresh_lab_matrix(self):
        if self.lab_report is None:
            return
        from mt5_extracao.laboratory import service
        report, lag = self.lab_report, int(self.lab_lag.get())
        horizon = lag if lag in service.PREDICTIVE_LAGS else 1
        self._lab_request += 1
        request = self._lab_request
        self.lab_matrix_status.configure(text=f"Calculando cruzamentos da análise {report['analysis_id'][:12]}…")

        def work():
            matrix = service.relation_matrix(report, lag=lag)
            target = service.target_matrix(report, horizon=horizon)
            pairs = [pair for pair in report["pairs"] if pair["lag_bars"] == lag and pair["left"] != pair["right"]][:8]
            timelines = [service.stability_windows(report, pair["left"], pair["right"], lag=lag) for pair in pairs]
            channel_id = next((c["id"] for c in report["channels"] if c.get("directions")), report["channels"][0]["id"])
            outcomes = service.direction_outcomes(report, channel_id, horizon=horizon)
            return (matrix, target, timelines, outcomes, service.research_candidates(report),
                    service.holdout_comparison(report))

        def done(bundle, error):
            if request != self._lab_request:
                return
            if error is not None:
                self.lab_matrix_status.configure(text=f"Cruzamento indisponível (erro de execução): {error}")
                return
            matrix, target, timelines, outcomes, candidates, holdout = bundle
            self.lab_matrix = matrix
            names = [_channel_label(c) for c in matrix["channels"]]
            cells = [[_lab_cell(cell, matrix) for cell in row] for row in matrix["cells"]]
            self.indicator_grid.set_data(names, names, cells)
            target_names = [_channel_label(c) for c in target["channels"]]
            target_cells = [[_lab_cell(cell, target)] for cell in target["cells"][0]]
            self.target_grid.set_data(target_names, (f"Retorno +{target['lag_bars']}",), target_cells)
            self._apply_scope(report, horizon)
            self._apply_timelines(timelines)
            self._apply_outcomes(outcomes)
            self._apply_candidates(candidates)
            self._apply_holdout(holdout)
            self.lab_matrix_status.configure(text=(
                f"Dados reais · análise {matrix['analysis_id'][:12]} · {matrix['symbol']} · grade {matrix['grid_timeframe']} · "
                f"corte {matrix['as_of']} · defasagem {matrix['lag_bars']} · Spearman no fechamento nominal. "
                f"Alvo: retorno bruto do preço em {horizon} barras. Não é lucro nem autorização."))

        self._run_bg(work, done)

    def _apply_scope(self, report, horizon):
        timeframes = ", ".join(dict.fromkeys(c["timeframe"] for c in report["channels"]))
        labels = {"asset": report["symbol"], "expert": "sem Expert nesta análise",
                  "timeframe": timeframes or report["grid_timeframe"], "direction": "intenção do canal, não ordem"}
        for key, value in labels.items():
            self.scope_labels[key].configure(text=f"{self.scope_labels[key].cget('text').split(':')[0]}: {value}")
        advanced = {"target": f"retorno do fechamento +{horizon}", "horizon": f"{horizon} barras da grade",
                    "method": "Spearman; blocos cronológicos iguais", "period": report["selection"]["as_of"],
                    "window": "corte fixo 70/30 no par", "step": "defasagens 0, 1, 3 e 6"}
        for key, value in advanced.items():
            self.advanced_labels[key].configure(text=value)

    def _draw_correlation_legend(self):
        c = self.theme.colors
        tones = {
            "negative_strong": _mix(c["surface"], c["series"][1], 0.28),
            "negative_weak": _mix(c["surface"], c["series"][1], 0.11),
            "neutral": _mix(c["surface"], c["axis"], 0.04),
            "positive_weak": _mix(c["surface"], c["series"][2], 0.11),
            "positive_strong": _mix(c["surface"], c["series"][2], 0.28),
        }
        for canvas, tone in self.correlation_swatches:
            canvas.configure(background=c["surface"])
            canvas.delete("all")
            canvas.create_rectangle(1, 1, 27, 15, fill=tones[tone], outline=c["border"])

    def _build_timeline(self, parent):
        controls = FilterBar(parent, padding=SPACING["xs"])
        controls.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        series_label = ttk.Label(controls, text="Série:", style="Surface.TLabel")
        series_label.grid(row=0, column=0, sticky="w", padx=(0, SPACING["xs"]))
        self._timelines = []
        self.timeline_var = tk.StringVar(value="Sem análise")
        self.timeline_select = ttk.Combobox(controls, textvariable=self.timeline_var, state="readonly", width=28,
                                            values=["Sem análise"])
        self.timeline_select.grid(row=0, column=1, sticky="w")
        self.timeline_select.bind("<<ComboboxSelected>>", lambda _event: self._draw_timeline())
        ranking_note = ttk.Label(controls, text="Blocos cronológicos iguais; lacuna não é zero nem é ligada.",
                                 style="Relations.Fixture.TLabel", justify=tk.RIGHT)
        self.timeline_ranking_note = ranking_note
        ranking_note.grid(row=0, column=2, sticky="e", padx=(SPACING["sm"], 0))
        controls.columnconfigure(2, weight=1)

        def resize_timeline_controls(_event):
            # A largura requerida do texto pode exceder a área visível no Windows/DPI alto.
            width = min(controls.winfo_width(), self.notebook.winfo_width())
            if width < 1000:
                ranking_note.grid_configure(row=1, column=0, columnspan=3, sticky="w", padx=0)
                ranking_note.configure(wraplength=max(220, width - 24), justify=tk.LEFT)
            else:
                ranking_note.grid_configure(row=0, column=2, columnspan=1, sticky="e",
                                            padx=(SPACING["sm"], 0))
                ranking_note.configure(wraplength=max(220, width - 360), justify=tk.RIGHT)

        controls.bind("<Configure>", resize_timeline_controls, add="+")
        self.notebook.bind("<Configure>", resize_timeline_controls, add="+")

        self.timeline_split, primary, detail = self._split_view(parent)
        primary.columnconfigure(0, weight=1)
        primary.rowconfigure(0, weight=1)
        primary.rowconfigure(1, weight=0)
        panel = ContentPanel(primary, title="Linha do tempo da associação", subtitle="intervalos medidos · lacunas não interpoladas",
                             padding=SPACING["xs"])
        panel.grid(row=0, column=0, sticky="nsew")
        toolbar_row = ttk.Frame(panel.body, style="Surface.TFrame")
        toolbar_row.pack(fill=tk.X)
        self.timeline_chart = charts.Chart(panel.body, self.theme, height=3.0)
        self._charts["timeline"] = self.timeline_chart
        try:
            from matplotlib.backends.backend_tkagg import NavigationToolbar2Tk
            self.timeline_toolbar = NavigationToolbar2Tk(self.timeline_chart.canvas, toolbar_row, pack_toolbar=False)
            self.timeline_toolbar.update()
            self.timeline_toolbar.pack(side=tk.LEFT)
        except (ImportError, tk.TclError):
            self.timeline_toolbar = None
        self.timeline_chart.widget.pack(fill=tk.BOTH, expand=True)
        self.timeline_explanation = DetailsPanel(panel.body, "Como ler o traçado", expanded=False,
                                                 information=True, theme=self.theme, padding=SPACING["2xs"])
        self.timeline_explanation.pack(fill=tk.X, pady=(0, SPACING["2xs"]))
        timeline_note = ttk.Label(self.timeline_explanation.body,
                                  text="Faixas temporais neutras: ajuste e fora da amostra, ou aquecimento, treino, validação e OOS "
                                       "quando o relatório os nomeia. Cores de estado aparecem apenas nos marcadores. "
                                       "Segmentos retos ligam os valores recebidos, sem suavização. Lacunas não são conectadas.",
                                  style="Muted.TLabel")
        self.timeline_method_note = timeline_note
        _fit_label_to_parent(timeline_note, self.timeline_explanation.body)
        timeline_note.pack(fill=tk.X, pady=(0, SPACING["2xs"]))
        self.timeline_table = TablePanel(primary, "Janelas observadas", columns=("period", "segment", "rho", "interval", "n", "utility", "freshness", "presence"),
                                         widths=(170, 94, 70, 180, 70, 145, 95, 90), anchors=("w", "center", "center", "w", "center", "w", "center", "center"),
                                         height=3, xscroll=True, padding=SPACING["xs"])
        self.timeline_table.grid(row=1, column=0, sticky="ew", pady=(SPACING["xs"], 0))
        self._set_headings(self.timeline_table, {"period": "Período", "segment": "Segmento", "rho": "ρ",
                                                 "interval": "IC 95%", "n": "n", "utility": "Utilidade / evidência",
                                                 "freshness": "Frescor", "presence": "Presença"})
        self._style_tree_states(self.timeline_table.tree)
        self.timeline_table.tree.bind("<<TreeviewSelect>>", self._timeline_selected, add="+")
        self.timeline_detail = self._page_detail(detail, "timeline", visible_lines=15)
        self.timeline_detail.pack(fill=tk.BOTH, expand=True)
        self._draw_timeline()

    def _current_timeline_series(self):
        if not self._timelines:
            return {"label": "Sem análise", "windows": []}
        return next((item for item in self._timelines if item["label"] == self.timeline_var.get()), self._timelines[0])

    def _apply_timelines(self, timelines):
        labeled = []
        for item in timelines:
            labeled.append({**item, "label": f"{item['left']} → {item['right']} +{item['lag_bars']}"})
        self._timelines = labeled
        labels = [item["label"] for item in labeled] or ["Sem pares"]
        self.timeline_select.configure(values=labels)
        self.timeline_var.set(labels[0])
        self._draw_timeline()

    def _draw_timeline(self):
        series = self._current_timeline_series()
        chart = self.timeline_chart
        chart.clear()
        ax = chart.figure.add_subplot(111)
        c = self.theme.colors
        charts.style_axis(ax, self.theme, "Spearman ρ · janelas na ordem cronológica", grid_axis="both")
        # Faixas temporais usam apenas tons neutros. Cores semânticas são reservadas
        # aos marcadores de estado para não sugerir evidência favorável no OOS.
        segments = {
            "Aquecimento": 0.035, "Treino": 0.065, "Validação": 0.095, "OOS": 0.125,
            "Ajuste": 0.06, "Fora da amostra": 0.12,
        }
        for window in series["windows"]:
            ax.axvspan(_date(window["start"]), _date(window["end"]), color=c["axis"],
                       alpha=segments.get(window["segment"], 0.05), linewidth=0)
        centers = [_date(window["center"]) for window in series["windows"]]
        values = [np.nan if window["value"] is None else window["value"] for window in series["windows"]]
        import matplotlib.dates as mdates
        if centers:
            ax.plot(mdates.date2num(centers), values, color=c["ink2"], linewidth=1.8,
                    solid_capstyle="round", solid_joinstyle="round", zorder=2)
        for center, window in zip(centers, series["windows"]):
            if window["value"] is None:
                continue
            state = _visual_state(window)
            token = _STATE_COLORS.get(state, "muted")
            color = c[token]
            marker = _STATE_MARKERS.get(state, "s")
            ax.scatter([center], [window["value"]], marker=marker, s=52,
                       facecolors=color if marker != "X" else "none", edgecolors=color,
                       linewidths=1.5, zorder=3)
        ax.axhline(0, color=c["axis"], linewidth=0.9)
        ax.set_ylim(-1, 1)
        ax.set_yticks((-1, -0.5, 0, 0.5, 1))
        ax.set_ylabel("Spearman ρ", color=c["muted"], fontsize=TYPOGRAPHY["chart_label"])
        if centers:
            ax.set_xlabel("Início → fim da base", color=c["muted"], fontsize=TYPOGRAPHY["chart_label"])
            locator = mdates.AutoDateLocator()
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
            ax.set_xlim(min(centers), max(centers))
        else:
            from matplotlib.ticker import NullLocator
            ax.xaxis.set_major_locator(NullLocator())
            ax.set_xlim(0, 1)
            ax.set_xlabel("")
            ax.text(0.5, 0.5, "Congele uma análise em Séries", transform=ax.transAxes,
                    ha="center", va="center", color=c["muted"])
        chart.draw()

        tree = self.timeline_table.tree
        tree.delete(*tree.get_children())
        self._timeline_iids = []
        for index, window in enumerate(series["windows"]):
            iid = f"timeline-{index}"
            self._timeline_iids.append(iid)
            tree.insert("", tk.END, iid=iid,
                        values=(f"{window['start']} — {window['end']}", window["segment"], window["value_text"],
                                window["ci"], window["n"], _display_state(window), window["freshness_state"],
                                window["presence_state"]),
                        tags=(_visual_state(window),))
        if self._timeline_iids:
            tree.selection_set(self._timeline_iids[0])
            tree.focus(self._timeline_iids[0])
            self._timeline_selected(None)

        def select_window(event):
            if event.inaxes is not ax or event.xdata is None:
                return
            from matplotlib.dates import date2num
            coords = [date2num(center) for center in centers]
            index = min(range(len(coords)), key=lambda i: abs(coords[i] - event.xdata))
            iid = self._timeline_iids[index]
            tree.selection_set(iid)
            tree.focus(iid)
            tree.see(iid)
            self._timeline_selected(None)
        if self._timeline_pick_cid is not None:
            chart.canvas.mpl_disconnect(self._timeline_pick_cid)
        self._timeline_pick_cid = chart.canvas.mpl_connect("button_press_event", select_window)

    def _timeline_selected(self, _event):
        selection = self.timeline_table.tree.selection()
        if not selection:
            return
        index = int(selection[0].rsplit("-", 1)[1])
        windows = self._current_timeline_series()["windows"]
        if index >= len(windows):
            return
        window = windows[index]
        self._set_detail("timeline", f"{window['segment']} · {window['start']} a {window['end']}", window)

    def _build_distribution(self, parent):
        note = Notice(parent,
                      "Retorno bruto do preço depois da intenção do canal. Custos não aplicados; expectativa líquida "
                      "de trades não é calculada. Ganho, perda e empate somam o denominador.",
                      kind="warning", theme=self.theme)
        _fit_label_to_parent(note.message, note, reserve=128)
        self.distribution_notice = note
        note.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        self.distribution_split, primary, detail = self._split_view(parent)
        primary.columnconfigure(0, weight=1)
        primary.rowconfigure(0, weight=1)
        primary.rowconfigure(1, weight=0)
        panel = ContentPanel(primary, title="Retorno do preço por intenção",
                             subtitle="ganho, empate e perda somam o mesmo denominador", padding=SPACING["xs"])
        panel.grid(row=0, column=0, sticky="nsew")
        self.distribution_population_label = ttk.Label(
            panel.body, text="População: nenhuma análise congelada", style="Muted.TLabel")
        _fit_label_to_parent(self.distribution_population_label, panel.body)
        self.distribution_population_label.pack(anchor="w", pady=(0, SPACING["2xs"]))
        distribution_legend = ttk.Label(panel.body, text="Faixas: perdas (laranja) · empates (cinza) · ganhos (azul). "
                                        "As duas direções usam a mesma escala e unidade.", style="Muted.TLabel")
        _fit_label_to_parent(distribution_legend, panel.body)
        distribution_legend.pack(anchor="w", fill=tk.X)
        self.distribution_chart = charts.Chart(panel.body, self.theme, height=2.2)
        self._charts["distribution"] = self.distribution_chart
        self.distribution_chart.widget.pack(fill=tk.BOTH, expand=True)
        self.distribution_table = TablePanel(primary, "Contagens e denominadores", columns=("direction", "wins", "losses", "flats", "censored", "n_total", "n_mature", "presence", "freshness"),
                                             widths=(95, 86, 86, 86, 100, 86, 100, 90, 90), anchors=("w", "center", "center", "center", "center", "center", "center", "center", "center"),
                                             height=2, padding=SPACING["xs"])
        self.distribution_table.grid(row=1, column=0, sticky="ew", pady=(SPACING["xs"], 0))
        self._set_headings(self.distribution_table, {"direction": "Direção", "wins": "Ganhos", "losses": "Perdas",
                                                     "flats": "Empates", "censored": "Censurados", "n_total": "Total n",
                                                     "n_mature": "Maduros n", "presence": "Presença", "freshness": "Frescor"})
        self._style_tree_states(self.distribution_table.tree)
        self.distribution_table.tree.bind("<<TreeviewSelect>>", self._distribution_selected, add="+")
        self.distribution_detail = self._page_detail(detail, "distribution", visible_lines=15)
        self.distribution_detail.pack(fill=tk.BOTH, expand=True)
        self._outcomes = None
        self._draw_distribution()

    def _apply_outcomes(self, outcomes):
        self._outcomes = outcomes
        self.distribution_notice.message.configure(text=outcomes.get("direction_diagnosis") or
            "Retorno bruto do preço depois da intenção do canal. Custos não aplicados; expectativa líquida "
            "de trades não é calculada. Ganho, perda e empate somam o denominador.")
        self.distribution_population_label.configure(text=f"População: {outcomes['population']}")
        self._draw_distribution()

    def _draw_distribution(self):
        data = self._outcomes or {"directions": [], "bins": (), "unit": "sem análise congelada"}
        chart = self.distribution_chart
        chart.clear()
        c = self.theme.colors
        directions = list(data["directions"])
        categories = (("Perdas", "losses", c["series"][1]), ("Empates", "flats", c["muted"]),
                      ("Ganhos", "wins", c["series"][0]))
        if not directions:
            ax = chart.figure.add_subplot(111)
            charts.style_axis(ax, self.theme, "Sem análise congelada", grid_axis="y")
            ax.text(0.5, 0.5, "Congele uma análise em Séries", transform=ax.transAxes,
                    ha="center", va="center", color=c["muted"])
            ax.set_xticks([])
            ax.set_yticks([])
        else:
            axes = chart.figure.subplots(1, len(directions), sharey=True)
            axes = np.atleast_1d(axes)
            peak = 1
            for axis, direction in zip(axes, directions):
                charts.style_axis(axis, self.theme, direction.get("label") or "Intenção", grid_axis="y")
                counts = [int(direction.get(key, 0)) for _name, key, _color in categories]
                peak = max(peak, max(counts) if counts else 1)
                for index, (_name, _key, color) in enumerate(categories):
                    axis.bar(index, counts[index], color=color, width=0.68, edgecolor=c["surface"], linewidth=0.8)
                    axis.text(index, counts[index], str(counts[index]), ha="center", va="bottom",
                              fontsize=TYPOGRAPHY["chart_tick"], color=c["ink"], rotation=0)
                axis.set_xticks(range(3))
                axis.set_xticklabels([name for name, _key, _color in categories], fontsize=TYPOGRAPHY["chart_tick"])
                axis.set_xlabel("Sinal do retorno do preço", color=c["muted"], fontsize=TYPOGRAPHY["chart_label"])
            for axis in axes:
                axis.set_ylim(0, peak * 1.25)
            axes[0].set_ylabel("Fechamentos em cada faixa", color=c["muted"], fontsize=TYPOGRAPHY["chart_label"])
        chart.draw()
        tree = self.distribution_table.tree
        tree.delete(*tree.get_children())
        for index, row in enumerate(data["directions"]):
            tree.insert("", tk.END, iid=f"distribution-{index}",
                        values=(row["label"], row["wins"], row["losses"], row["flats"], row["censored"],
                                row["n_total"], row["n_mature"], row["presence_state"], row["freshness_state"]),
                        tags=(_visual_state(row),))
        if tree.get_children():
            tree.selection_set(tree.get_children()[0])
            self._distribution_selected(None)

    def _distribution_selected(self, _event):
        selection = self.distribution_table.tree.selection()
        if not selection:
            return
        index = int(selection[0].rsplit("-", 1)[1])
        rows = (self._outcomes or {}).get("directions") or []
        if index >= len(rows):
            return
        row = rows[index]
        self._set_detail("distribution", row["label"], {
            "method": "sinal do retorno bruto do fechamento futuro",
            "effect_text": f"ganhos {row['wins']} · perdas {row['losses']} · empates {row['flats']}",
            "n": f"n={row['n_total']}",
            "population": (self._outcomes or {}).get("population", "sem análise"),
            "support_state": "mesma população e horizonte",
            "utility_state": "INCONCLUSIVE", "evidence_state": row["evidence_state"],
            "freshness_state": "CURRENT", "presence_state": row["presence_state"],
            "cost_status": (self._outcomes or {}).get("cost_status", "custos não aplicados"),
            "reason": row["reason"],
        })

    def _build_convergence(self, parent):
        controls = FilterBar(parent, padding=SPACING["xs"])
        controls.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        pair_label = ttk.Label(controls, text="Comparação:", style="Surface.TLabel")
        pair_label.grid(row=0, column=0, sticky="w", padx=(0, SPACING["xs"]))
        self._holdout = []
        self.convergence_var = tk.StringVar(value="Fora da amostra")
        self.convergence_select = ttk.Combobox(controls, textvariable=self.convergence_var, state="readonly", width=28,
                                               values=["Fora da amostra"])
        self.convergence_select.grid(row=0, column=1, sticky="w")
        self.convergence_select.bind("<<ComboboxSelected>>", lambda _event: self._draw_convergence())
        comparison_note = ttk.Label(controls, text="valores OOS recebidos · comparação pareada · sem ranking",
                                   style="Relations.Fixture.TLabel", justify=tk.RIGHT)
        self.convergence_comparison_note = comparison_note
        comparison_note.grid(row=0, column=2, sticky="e", padx=(SPACING["sm"], 0))
        controls.columnconfigure(2, weight=1)

        def resize_convergence_controls(_event):
            # A largura requerida do texto pode exceder a área visível no Windows/DPI alto.
            width = min(controls.winfo_width(), self.notebook.winfo_width())
            if width < 1000:
                comparison_note.grid_configure(row=1, column=0, columnspan=3, sticky="w", padx=0)
                comparison_note.configure(wraplength=max(220, width - 24), justify=tk.LEFT)
            else:
                comparison_note.grid_configure(row=0, column=2, columnspan=1, sticky="e",
                                               padx=(SPACING["sm"], 0))
                comparison_note.configure(wraplength=max(220, width - 310), justify=tk.RIGHT)

        controls.bind("<Configure>", resize_convergence_controls, add="+")
        self.notebook.bind("<Configure>", resize_convergence_controls, add="+")
        note = Notice(parent, "ρ fora da amostra menos a persistência do alvo. Não é R/op. Custos não entram; expectativa líquida de trades não é calculada.",
                      kind="warning", theme=self.theme)
        _fit_label_to_parent(note.message, note, reserve=128)
        self.convergence_notice = note
        note.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        self.convergence_split, primary, detail = self._split_view(parent)
        primary.columnconfigure(0, weight=1)
        primary.rowconfigure(0, weight=1)
        primary.rowconfigure(1, weight=0)
        panel = ContentPanel(primary, title="ρ fora da amostra",
                             subtitle="cada par menos a persistência do próprio alvo", padding=SPACING["xs"])
        panel.grid(row=0, column=0, sticky="nsew")
        self.convergence_chart = charts.Chart(panel.body, self.theme, height=2.2)
        self._charts["convergence"] = self.convergence_chart
        self.convergence_chart.widget.pack(fill=tk.BOTH, expand=True)
        self.convergence_table = TablePanel(primary, "Pares do relatório congelado",
                                            columns=("scenario", "effect", "ci", "n", "delta", "utility", "evidence", "freshness", "presence"),
                                            widths=(140, 100, 165, 55, 110, 145, 100, 82, 90),
                                            anchors=("w", "center", "w", "center", "w", "w", "center", "center", "center"),
                                            height=2, xscroll=True, padding=SPACING["xs"])
        self.convergence_table.grid(row=1, column=0, sticky="ew", pady=(SPACING["xs"], 0))
        self._set_headings(self.convergence_table, {"scenario": "Cenário", "effect": "Efeito OOS bruto", "ci": "IC 95%",
                                                    "n": "n", "delta": "Δ bruto vs. base", "utility": "Utilidade / estado",
                                                    "evidence": "Evidência", "freshness": "Frescor", "presence": "Presença"})
        self._style_tree_states(self.convergence_table.tree)
        self.convergence_table.tree.bind("<<TreeviewSelect>>", self._convergence_selected, add="+")
        self.convergence_detail = self._page_detail(detail, "convergence", visible_lines=15)
        self.convergence_detail.pack(fill=tk.BOTH, expand=True)
        self._draw_convergence()

    def _current_pair(self):
        return {"label": self.convergence_var.get() or "Fora da amostra", "rows": self._holdout,
                "comparison": "ρ fora da amostra contra a persistência do alvo"}

    def _apply_holdout(self, rows):
        self._holdout = rows
        self.convergence_var.set("Fora da amostra")
        self._draw_convergence()

    def _draw_convergence(self):
        pair = self._current_pair()
        chart = self.convergence_chart
        chart.clear()
        ax = chart.figure.add_subplot(111)
        c = self.theme.colors
        charts.style_axis(ax, self.theme, "ρ fora da amostra · não é resultado em R", grid_axis="y")
        rows = pair["rows"]
        positions = range(len(rows))
        heights = [0 if row["effect"] is None else float(row["effect"]) for row in rows]
        colors = [c["muted"] if row["effect"] is None else (c["series"][2] if row["effect"] >= 0 else c["series"][1])
                  for row in rows]
        bars = ax.bar(positions, heights, color=colors, width=0.62, edgecolor=c["surface"], linewidth=0.8)
        for rect, row, height in zip(bars, rows, heights):
            if row["effect"] is None:
                rect.set_alpha(0)
            y = 0.08 if row["effect"] is None else height
            ax.text(rect.get_x() + rect.get_width() / 2, y, row["effect_text"],
                    ha="center", va="bottom" if y >= 0 else "top", rotation=0,
                    fontsize=TYPOGRAPHY["chart_tick"], color=c["ink"])
        ax.axhline(0, color=c["axis"], linewidth=0.9)
        ax.set_xticks(list(positions))
        labels = [row["label"] if len(row["label"]) <= 32 else row["label"][:31] + "…" for row in rows]
        ax.set_xticklabels(labels, rotation=0 if len(rows) <= 3 else 24, ha="center" if len(rows) <= 3 else "right",
                           fontsize=TYPOGRAPHY["chart_tick"])
        ax.set_ylim(-1, 1)
        ax.set_ylabel("ρ fora da amostra", color=c["muted"],
                      fontsize=TYPOGRAPHY["chart_label"])
        if not rows:
            ax.text(0.5, 0.5, "Nenhum par preditivo nesta análise", transform=ax.transAxes,
                    ha="center", va="center", color=c["muted"])
        chart.draw()
        tree = self.convergence_table.tree
        tree.delete(*tree.get_children())
        for index, row in enumerate(rows):
            tree.insert("", tk.END, iid=f"convergence-{index}",
                        values=(row["label"], row["effect_text"], row["ci"], row["n"], row["delta_text"],
                                _display_state(row), row["evidence_state"], row["freshness_state"], row["presence_state"]),
                                tags=(_visual_state(row),))
        if tree.get_children():
            tree.selection_set(tree.get_children()[0])
            self._convergence_selected(None)

    def _convergence_selected(self, _event):
        selection = self.convergence_table.tree.selection()
        if not selection:
            return
        index = int(selection[0].rsplit("-", 1)[1])
        pair = self._current_pair()
        if index >= len(pair["rows"]):
            return
        row = pair["rows"][index]
        self._set_detail("convergence", row["label"], {
            **row,
            "method": "ρ fora da amostra menos a persistência do alvo",
            "effect_text": f"{row['effect_text']} · Δρ {row['delta_text']}",
            "scale": "correlação −1…+1 · não é R por operação",
            "segment": pair["comparison"],
            "cost_status": "custos não incluídos; expectativa líquida de trades não é calculada",
        })

    @staticmethod
    def _call(work):
        try:
            return work(), None
        except Exception as error:  # mesmo contrato de run_bg: erro chega ao callback
            return None, error

    def _build_inventory(self, parent):
        from mt5_extracao.laboratory import inventory
        controls = ttk.Frame(parent, style="Surface.TFrame")
        controls.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        ttk.Label(controls, text="Buscar:").pack(side=tk.LEFT)
        self.inventory_query = tk.StringVar()
        entry = ttk.Entry(controls, textvariable=self.inventory_query, width=28)
        entry.pack(side=tk.LEFT, padx=SPACING["xs"])
        entry.bind("<Return>", lambda _e: self.load_inventory())
        ttk.Label(controls, text="Estado:").pack(side=tk.LEFT)
        self.inventory_state = tk.StringVar(value="Todos")
        state_box = ttk.Combobox(controls, textvariable=self.inventory_state, state="readonly", width=13,
                                 values=("Todos", *inventory.STATES))
        state_box.pack(side=tk.LEFT, padx=SPACING["xs"])
        state_box.bind("<<ComboboxSelected>>", lambda _e: self.load_inventory())
        ttk.Button(controls, text="Filtrar", command=self.load_inventory).pack(side=tk.LEFT)
        self.inventory_prev = ttk.Button(controls, text="◀", width=3, command=lambda: self._inventory_page(-1))
        self.inventory_prev.pack(side=tk.RIGHT)
        self.inventory_next = ttk.Button(controls, text="▶", width=3, command=lambda: self._inventory_page(1))
        self.inventory_next.pack(side=tk.RIGHT)
        self.inventory_status = ttk.Label(parent, text="Carregando inventário…", style="Muted.TLabel")
        self.inventory_status.pack(fill=tk.X)
        _fit_label_to_parent(self.inventory_status, parent)
        self.inventory_table = TablePanel(parent, "Indicadores, variantes e saídas · somente leitura",
            columns=("feature", "name", "version", "family", "outputs", "params", "state", "series"),
            widths=(210, 170, 70, 100, 120, 240, 110, 200),
            anchors=("w", "w", "center", "w", "w", "w", "center", "w"), height=12, xscroll=True,
            padding=SPACING["xs"])
        self.inventory_table.pack(fill=tk.BOTH, expand=True)
        self._set_headings(self.inventory_table, {"feature": "ID", "name": "Nome", "version": "Versão",
            "family": "Família", "outputs": "Saídas", "params": "Parâmetros", "state": "Estado",
            "series": "Fatos na base"})
        self.inventory_table.tree.bind("<<TreeviewSelect>>", self._inventory_selected, add="+")
        self.inventory_detail = self._page_detail(parent, "inventory", visible_lines=5)
        self.inventory_detail.pack(fill=tk.X, pady=(SPACING["xs"], 0))
        self._inventory_rows = {}
        self.load_inventory()

    def load_inventory(self, cursor=None, *, reset=True):
        from mt5_extracao.laboratory import inventory
        if reset:
            self._inventory_cursors = [None]
        self._inventory_request += 1
        request, db = self._inventory_request, self.db
        query, state = self.inventory_query.get(), self.inventory_state.get()
        self.inventory_status.configure(text="Carregando inventário…")
        self.inventory_next.state(["disabled"])
        self.inventory_prev.state(["disabled"])
        self._inventory_rows = {}
        self.inventory_table.tree.delete(*self.inventory_table.tree.get_children())
        self._clear_inventory_detail("Aguarde a consulta ou selecione uma definição do resultado atual.")
        def work():
            return inventory.search(db, query=query, state=None if state == "Todos" else state,
                                    page_size=50, cursor=cursor)
        def done(result, error):
            if request != self._inventory_request or not self.inventory_table.tree.winfo_exists():
                return  # resposta obsoleta: outra busca já foi pedida
            tree = self.inventory_table.tree
            tree.delete(*tree.get_children())
            self._inventory_rows = {}
            if error:
                self.inventory_status.configure(text=f"Inventário indisponível: {error}. Recomece a busca.")
                self._clear_inventory_detail("Consulta indisponível. Recomece a busca; nenhum resultado anterior está selecionado.")
                self._inventory_next_cursor = None
                self._inventory_cursors = [None]
                return
            for index, row in enumerate(result["items"]):
                iid = f"inventory-{index}"
                self._inventory_rows[iid] = row
                name = ("Nome não verificável" if row["origin"] == "data_store" and
                        row.get("stored_name_status") == "UNVERIFIED_LEGACY_NAME" else row["name"])
                series = ", ".join(f"{item['symbol']} {item['timeframe']} ({item['facts']})"
                                   for item in row["series"][:3]) or "—"
                params = (", ".join(f"{k}={v}" for k, v in row["parameters"].items())
                          if isinstance(row["parameters"], dict) else json.dumps(row["parameters"], ensure_ascii=False))
                tree.insert("", tk.END, iid=iid, values=(row["feature_id"], name, row["algorithm_version"],
                    row["family"], ", ".join(item["name"].rsplit(".", 1)[-1] for item in row["outputs"]) or "—",
                    params, row["state"], series))
            self._inventory_next_cursor = result["next_cursor"]
            self.inventory_next.state(["!disabled"] if result["next_cursor"] else ["disabled"])
            self.inventory_prev.state(["!disabled"] if len(self._inventory_cursors) > 1 else ["disabled"])
            counts = " · ".join(f"{name} {count}" for name, count in result["counts"].items())
            page = len(self._inventory_cursors)
            self.inventory_status.configure(text=result["reason"] or
                f"{result['matched']} de {result['total']} definições · página {page} · {counts}"
                + ("" if db is not None else " · sem base conectada: fatos não consultados"))
            if result["items"]:
                tree.selection_set("inventory-0")
                self._inventory_selected(None)
            else:
                self._clear_inventory_detail("Nenhuma definição corresponde à seleção atual.")
        self._run_bg(work, done)

    def _clear_inventory_detail(self, message):
        detail = self._details["inventory"]
        detail.configure(state=tk.NORMAL)
        detail.delete("1.0", tk.END)
        detail.insert(tk.END, message)
        detail.configure(state=tk.DISABLED)

    def _inventory_page(self, step):
        if step > 0 and getattr(self, "_inventory_next_cursor", None):
            self._inventory_cursors.append(self._inventory_next_cursor)
        elif step < 0 and len(self._inventory_cursors) > 1:
            self._inventory_cursors.pop()
        else:
            return
        self.load_inventory(self._inventory_cursors[-1], reset=False)

    def _inventory_selected(self, _event):
        selection = self.inventory_table.tree.selection()
        row = self._inventory_rows.get(selection[0]) if selection else None
        if row is None:
            return
        text = self._details["inventory"]
        text.configure(state=tk.NORMAL)
        text.delete("1.0", tk.END)
        lines = [f"{row['name']} · {row['feature_id']} v{row['algorithm_version']} · origem {row['origin']}",
                 f"Estado {row['state']}: {row['reason']}" + (f" Ação: {row['action']}" if row["action"] else ""),
                 f"Unidade {row['unit'] or 'não declarada'} · aquecimento "
                 f"{row['warmup'] if row['warmup'] is not None else 'não declarado'} · saídas "
                 + ", ".join(f"{o['name']} ({o.get('kind')})" for o in row["outputs"])]
        lines += [f"{s['symbol']} {s['timeframe']}: {s['facts']} fatos de {s['first']} a {s['last']} · "
                  f"último disponível {s['last_available_at']}" for s in row["series"]]
        text.insert("1.0", "\n".join(lines))
        text.configure(state=tk.DISABLED)

    def _build_candidates(self, parent):
        notice = Notice(parent,
                        "Só entram pares que o serviço marcou consistentes fora da amostra. "
                        "Não há ação de aplicar, exportar preset ou alterar Expert.",
                        kind="info", theme=self.theme)
        _fit_label_to_parent(notice.message, notice, reserve=128)
        self.candidate_notice = notice
        notice.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        self.candidate_table = TablePanel(parent, "Propostas de pesquisa · somente leitura",
                                          columns=("id", "proposal", "scope", "window", "evidence", "freshness", "presence", "validity"),
                                          widths=(90, 200, 240, 230, 130, 95, 90, 150),
                                          anchors=("center", "w", "w", "w", "center", "center", "center", "w"),
                                          height=2, xscroll=True, padding=SPACING["xs"])
        self.candidate_table.pack(fill=tk.X)
        self._set_headings(self.candidate_table, {"id": "ID", "proposal": "Proposta de pesquisa", "scope": "Escopo",
                                                  "window": "Treino / OOS", "evidence": "Evidência", "freshness": "Frescor",
                                                  "presence": "Presença", "validity": "Validade"})
        self._style_tree_states(self.candidate_table.tree)
        self.candidate_table.tree.bind("<<TreeviewSelect>>", self._candidate_selected, add="+")
        self.candidate_detail = self._page_detail(parent, "candidates", visible_lines=6)
        self.candidate_detail.pack(fill=tk.X, pady=(SPACING["xs"], 0))
        self._candidate_rows = []

    def _apply_candidates(self, rows):
        self._candidate_rows = rows
        tree = self.candidate_table.tree
        tree.delete(*tree.get_children())
        for index, row in enumerate(rows):
            tree.insert("", tk.END, iid=f"candidate-{index}",
                        values=(row["id"], row["proposal"], row["scope"], row["window"],
                                row["evidence_state"], row["freshness_state"], row["presence_state"], row["validity"]),
                        tags=(_visual_state(row),))
        if rows:
            tree.selection_set("candidate-0")
            self._candidate_selected(None)

    def _candidate_selected(self, _event):
        selection = self.candidate_table.tree.selection()
        if not selection:
            return
        index = int(selection[0].rsplit("-", 1)[1])
        if index >= len(self._candidate_rows):
            return
        row = self._candidate_rows[index]
        self._set_detail("candidates", row["proposal"], {
            **row,
            "period": row["window"],
            "method": "veredito HOLDOUT_CONSISTENT do serviço; a tela não recalcula",
            "effect_text": row["validity"], "effect_ci": "ver o par no relatório",
            "n": "pares do relatório", "segment": row["window"],
            "cost_status": "não comprovado; nenhuma aplicação operacional",
        })

    def render(self):
        """Atualiza paleta e gráficos quando a aplicação troca o tema."""
        self._configure_styles()
        self._draw_correlation_legend()
        for label, state, _text in self._legend_labels:
            label.configure(style=f"Relations.{state}.TLabel")
        for matrix in self._matrices:
            matrix.redraw()
        self._style_timeline_toolbar()
        self._style_tree_states(self.timeline_table.tree)
        self._style_tree_states(self.distribution_table.tree)
        self._style_tree_states(self.convergence_table.tree)
        self._style_tree_states(self.candidate_table.tree)
        self._draw_timeline()
        self._draw_distribution()
        self._draw_convergence()

    def _style_timeline_toolbar(self):
        if self.timeline_toolbar is None:
            return
        c = self.theme.colors
        self.timeline_toolbar.configure(background=c["surface"])
        for widget in self.timeline_toolbar.winfo_children():
            try:
                widget.configure(background=c["surface"], foreground=c["ink"],
                                 activebackground=c["select"], activeforeground=c["ink"])
            except tk.TclError:
                continue
