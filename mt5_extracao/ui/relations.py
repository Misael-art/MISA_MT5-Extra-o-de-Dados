"""Apresentação provisória das relações F18 dentro do Laboratório.

O contrato T18.1/T18.6 ainda não existe nesta base. Esta tela recebe somente a fixture
local abaixo; não consulta banco, não calcula estatísticas e não produz decisões para Experts.
Quando o DTO for entregue, o adaptador deverá alimentar os mesmos componentes sem mover
regras analíticas para os widgets.
"""
from __future__ import annotations

import datetime as dt
import tkinter as tk
from tkinter import ttk

import numpy as np

from mt5_extracao.ui import charts
from mt5_extracao.ui.components import ScrollableContent as _ScrollablePage
from mt5_extracao.ui.components import ContentPanel, DetailsPanel, FilterBar, Notice, PageHeader, TablePanel
from mt5_extracao.ui.visual_tokens import SPACING, TYPOGRAPHY


VIEW_NAMES = ("Matriz", "Linha do tempo", "Ganhos e perdas", "Convergências", "Candidatos")
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
    "indicator_matrix": (
        (
            {"label": "1,00", "strength": "strong", "method": "Spearman", "n": "n=420", "effect_ci": "identidade", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "fixture sintética", "reason": "Diagonal de referência: o indicador é comparado consigo mesmo."},
            {"label": "+0,74", "strength": "positive_strong", "method": "Spearman", "n": "n=408", "effect_ci": "IC 95%: +0,61 a +0,82", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "amostra sintética", "reason": "Associação direta no exemplo; sinal de correlação não informa lucro ou aprovação."},
            {"label": "−0,08", "strength": "negative_weak", "method": "Spearman", "n": "n=396", "effect_ci": "IC 95%: −0,18 a +0,03", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "amostra sintética", "reason": "Efeito próximo de zero; a associação linear baixa não prova ausência de relação não linear."},
            {"label": "—", "strength": "missing", "method": "Spearman", "n": "n=0", "effect_ci": "indefinido", "utility_state": "INCONCLUSIVE", "evidence_state": "INSUFFICIENT", "freshness_state": "UNAVAILABLE", "presence_state": "UNKNOWN", "support_state": "sem observações maduras", "reason": "Sem pares maduros nesta célula. Ausência de dado não é correlação zero nem efeito adverso."},
        ),
        (
            {"label": "+0,74", "strength": "positive_strong", "method": "Spearman", "n": "n=408", "effect_ci": "IC 95%: +0,61 a +0,82", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "amostra sintética", "reason": "Matriz simétrica de exemplo; a direção do relacionamento e a escala aparecem na legenda."},
            {"label": "1,00", "strength": "strong", "method": "Spearman", "n": "n=420", "effect_ci": "identidade", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "fixture sintética", "reason": "Diagonal de referência: o indicador é comparado consigo mesmo."},
            {"label": "−0,68", "strength": "negative_strong", "method": "Spearman", "n": "n=402", "effect_ci": "IC 95%: −0,76 a −0,55", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "amostra sintética", "reason": "Associação inversa. O sinal não informa utilidade, aderência nem resultado financeiro."},
            {"label": "+0,04", "strength": "neutral", "method": "Spearman", "n": "n=391", "effect_ci": "IC 95%: −0,06 a +0,14", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "amostra sintética", "reason": "Correlação linear quase zero no exemplo; isso não exclui padrão não linear."},
        ),
        (
            {"label": "−0,08", "strength": "negative_weak", "method": "Spearman", "n": "n=396", "effect_ci": "IC 95%: −0,18 a +0,03", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "amostra sintética", "reason": "O intervalo inclui zero; estado inconclusivo fornecido pela fixture."},
            {"label": "−0,68", "strength": "negative_strong", "method": "Spearman", "n": "n=402", "effect_ci": "IC 95%: −0,76 a −0,55", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "amostra sintética", "reason": "Associação inversa; não deve ser colorida como perda ou incompatibilidade."},
            {"label": "1,00", "strength": "strong", "method": "Spearman", "n": "n=420", "effect_ci": "identidade", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "fixture sintética", "reason": "Diagonal de referência: o indicador é comparado consigo mesmo."},
            {"label": "—", "strength": "missing", "method": "Spearman", "n": "n=420", "effect_ci": "não definido (variável constante)", "utility_state": "INCONCLUSIVE", "evidence_state": "INSUFFICIENT", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "variável constante", "reason": "Série constante: coeficiente indefinido é mostrado como indisponível, nunca como zero."},
        ),
        (
            {"label": "—", "strength": "missing", "method": "Spearman", "n": "n=0", "effect_ci": "indefinido", "utility_state": "INCONCLUSIVE", "evidence_state": "INSUFFICIENT", "freshness_state": "UNAVAILABLE", "presence_state": "UNKNOWN", "support_state": "sem observações maduras", "reason": "Sem pares maduros nesta célula. Nenhuma relação foi inferida."},
            {"label": "+0,04", "strength": "neutral", "method": "Spearman", "n": "n=391", "effect_ci": "IC 95%: −0,07 a +0,15", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "amostra sintética", "reason": "Valor ilustrativo quase neutro, separado dos estados de utilidade."},
            {"label": "—", "strength": "missing", "method": "Spearman", "n": "n=420", "effect_ci": "não definido (variável constante)", "utility_state": "INCONCLUSIVE", "evidence_state": "INSUFFICIENT", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "variável constante", "reason": "Variável constante; coeficiente indisponível, sem substituir ausência por zero."},
            {"label": "1,00", "strength": "strong", "method": "Spearman", "n": "n=420", "effect_ci": "identidade", "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "support_state": "fixture sintética", "reason": "Diagonal de referência: o indicador é comparado consigo mesmo."},
        ),
    ),
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
    return dt.datetime.strptime(value, "%Y-%m-%d")


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


def _detail_text(row: dict, scope: dict | None = None) -> str:
    """Formata proveniência/estado fornecidos, sem deduzir campos a partir do efeito."""
    scope = scope or _SYNTHETIC_FIXTURE["scope"]
    period = row.get("period") or row.get("window") or scope["period"]
    segment = row.get("segment", row.get("baseline", "segmento não informado"))
    effect = row.get("effect_text", row.get("label", row.get("effect", "—")))
    interval = row.get("effect_ci", row.get("ci", "IC não fornecido"))
    return "\n".join((
        f"Período/janela: {period} · segmento {segment} · rótulo {row.get('label_maturity', 'maturidade não informada')} · "
        f"disponibilidade {row.get('available_at', row.get('as_of', 'não informada pela fixture'))}",
        f"População: {row.get('population', 'não informada pela fixture')}",
        f"Escopo: {scope.get('feed', 'fonte não informada')} · {scope['asset']} · {scope['expert']} · "
        f"{scope['timeframe']} · {scope['direction']}",
        f"Método/escala: {row.get('method', scope['method'])} · {row.get('scale', 'valor informado pelo produtor')}",
        f"Efeito/IC e suporte: {effect} · {interval} · {row.get('n', 'n não fornecido')} · "
        f"{row.get('support_state', 'suporte não informado')}",
        f"Estados: presença {row.get('presence_state', 'UNKNOWN')} · utilidade {row.get('utility_state', 'INCONCLUSIVE')} · "
        f"evidência {row.get('evidence_state', 'INSUFFICIENT')} · frescor {row.get('freshness_state', 'UNAVAILABLE')}",
        f"Cor semântica: {_STATE_TEXT.get(_visual_state(row), 'Inconclusivo')} · estados coloridos só passam com evidência, "
        "suporte e frescor exigidos; os estados informados não são reavaliados pela UI.",
        f"Custos/ajustes: {row.get('cost_status', _SYNTHETIC_FIXTURE['distribution']['cost_status'])} · "
        f"{row.get('adjustment', 'ajustes/tentativas não informados pela fixture')}",
        f"Motivo: {row.get('reason', 'Não informado pela fixture.')}",
    ))


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
                                        text=f"{cell.get('n', 'n não informado')} · {freshness} · setup {presence}",
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
    """Subvisões visuais de F18 com fixture privada e estados explicitamente provisórios."""

    def __init__(self, parent, theme):
        self.theme = theme
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
            "DADOS SINTÉTICOS · Fixture local sem vínculo à base do usuário. DTO T18.1 e serviço T18.6 pendentes; "
            "valores e estados não são resultados analíticos reais.",
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
                              expanded=False, information=True, padding=SPACING["xs"])
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
        for column, (key, label) in enumerate((("asset", "Feed/ativo/contrato"), ("expert", "Expert/versão/preset"),
                                               ("timeframe", "Timeframe"), ("direction", "Direção"))):
            block = ttk.Frame(bar, style="Surface.TFrame")
            block.grid(row=0, column=column, sticky="ew", padx=SPACING["xs"])
            scope_label = ttk.Label(block, text=f"{label}: {_SYNTHETIC_FIXTURE['scope'][key]}",
                                    style="Surface.TLabel", justify=tk.LEFT)
            _fit_label_to_parent(scope_label, block, reserve=16, minimum=180)
            scope_label.pack(anchor="w", fill=tk.X)
            self.scope_blocks.append(block)
            bar.columnconfigure(column, weight=1, uniform="relations-filter")
        self.advanced = DetailsPanel(bar, "Recorte avançado", expanded=False, information=True, padding=SPACING["xs"])
        self.advanced.grid(row=1, column=0, columnspan=4, sticky="ew", padx=SPACING["xs"],
                           pady=(SPACING["xs"], 0))
        details = _SYNTHETIC_FIXTURE["scope"]
        self.advanced_blocks = []
        for column, (key, label) in enumerate((("target", "Alvo"), ("horizon", "Horizonte"), ("method", "Método/escala"),
                                               ("period", "Período"), ("window", "Janela"), ("step", "Passo"))):
            block = ttk.Frame(self.advanced.body, style="Surface.TFrame")
            block.grid(row=column // 3, column=column % 3, sticky="ew", padx=SPACING["xs"], pady=SPACING["2xs"])
            ttk.Label(block, text=label, style="Muted.TLabel").pack(anchor="w")
            detail_value = ttk.Label(block, text=details[key], style="Surface.TLabel")
            _fit_label_to_parent(detail_value, block, reserve=16, minimum=180)
            detail_value.pack(anchor="w", fill=tk.X)
            self.advanced_blocks.append(block)
            self.advanced.body.columnconfigure(column % 3, weight=1, uniform="advanced-filter")
        self.advanced.body.columnconfigure(0, weight=1)
        self.advanced.body.columnconfigure(1, weight=1)
        self.advanced.body.columnconfigure(2, weight=1)
        scope_note = ttk.Label(self.frame,
                               text="Campos demonstrativos; filtros reais serão ligados ao contrato versionado T18.1/T18.6.",
                               style="Relations.Fixture.TLabel")
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
                             on_toggle=getattr(parent, "_relations_reflow", None), padding=SPACING["xs"])
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
        label.insert("1.0", _detail_text(row))
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
                          text="Correlação Spearman: escala divergente fixa −1…+1, zero no centro. Cor e sinal mostram associação; não mostram lucro, utilidade ou aprovação.",
                          style="Muted.TLabel", justify=tk.LEFT)
        _fit_label_to_parent(intro, parent)
        intro.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        self.correlation_legend = ttk.Frame(parent, style="Surface.TFrame")
        self.correlation_legend.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        self.correlation_swatches = []
        legend_items = (("negative_strong", "Inversa forte"), ("negative_weak", "Inversa fraca"),
                        ("neutral", "≈ 0 · sem associação linear"), ("positive_weak", "Direta fraca"),
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
        names = _SYNTHETIC_FIXTURE["indicator_names"]
        indicator_frame = ttk.Frame(inner, padding=SPACING["xs"], style="Surface.TFrame")
        target_frame = ttk.Frame(inner, padding=SPACING["xs"], style="Surface.TFrame")
        inner.add(indicator_frame, text="Indicador × indicador")
        inner.add(target_frame, text="Indicador × alvo")
        self.indicator_split, indicator_primary, indicator_details = self._split_view(indicator_frame, 4, 1)
        self.target_split, target_primary, target_details = self._split_view(target_frame, 4, 1)
        self.indicator_detail = self._page_detail(indicator_details, "matrix-indicators", visible_lines=15)
        self.target_detail = self._page_detail(target_details, "matrix-targets", visible_lines=15)
        self.matrix_details = {"indicators": self.indicator_detail, "targets": self.target_detail}
        self.indicator_grid = _MatrixGrid(indicator_primary, self.theme, names, names,
                                          _SYNTHETIC_FIXTURE["indicator_matrix"],
                                          lambda title, row: self._set_detail("matrix-indicators", title, row),
                                          "indicator-matrix")
        self.indicator_grid.frame.pack(fill=tk.BOTH, expand=True)
        self.indicator_detail.pack(fill=tk.BOTH, expand=True, padx=(SPACING["xs"], 0))
        self.target_grid = _MatrixGrid(target_primary, self.theme, names, _SYNTHETIC_FIXTURE["target_names"],
                                       _SYNTHETIC_FIXTURE["target_matrix"],
                                       lambda title, row: self._set_detail("matrix-targets", title, row),
                                       "target-matrix")
        self.target_grid.frame.pack(fill=tk.BOTH, expand=True)
        self.target_detail.pack(fill=tk.BOTH, expand=True, padx=(SPACING["xs"], 0))
        self._matrices.extend((self.indicator_grid, self.target_grid))
        self._set_detail("matrix-indicators", "RSI(14) × RSI(14)", _SYNTHETIC_FIXTURE["indicator_matrix"][0][0])
        self._set_detail("matrix-targets", "RSI(14) × Retorno +3 barras", _SYNTHETIC_FIXTURE["target_matrix"][0][0])

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
        self.timeline_var = tk.StringVar(value=_SYNTHETIC_FIXTURE["timeline"]["series"][0]["label"])
        timeline_choices = [series["label"] for series in _SYNTHETIC_FIXTURE["timeline"]["series"]]
        self.timeline_select = ttk.Combobox(controls, textvariable=self.timeline_var, state="readonly", width=28,
                                            values=timeline_choices)
        self.timeline_select.grid(row=0, column=1, sticky="w")
        self.timeline_select.bind("<<ComboboxSelected>>", lambda _event: self._draw_timeline())
        ranking_note = ttk.Label(controls, text=_SYNTHETIC_FIXTURE["timeline"]["reference"],
                                 style="Relations.Fixture.TLabel", justify=tk.RIGHT)
        self.timeline_ranking_note = ranking_note
        ranking_note.grid(row=0, column=2, sticky="e", padx=(SPACING["sm"], 0))
        controls.columnconfigure(2, weight=1)

        def resize_timeline_controls(event):
            if event.width < 1000:
                ranking_note.grid_configure(row=1, column=0, columnspan=3, sticky="w", padx=0)
                ranking_note.configure(wraplength=max(220, event.width - 24), justify=tk.LEFT)
            else:
                ranking_note.grid_configure(row=0, column=2, columnspan=1, sticky="e",
                                            padx=(SPACING["sm"], 0))
                ranking_note.configure(wraplength=max(220, event.width - 360), justify=tk.RIGHT)

        controls.bind("<Configure>", resize_timeline_controls, add="+")

        self.timeline_split, primary, detail = self._split_view(parent)
        primary.columnconfigure(0, weight=1)
        primary.rowconfigure(0, weight=1)
        primary.rowconfigure(1, weight=0)
        panel = ContentPanel(primary, title="Linha do tempo da associação", subtitle="intervalos medidos · lacunas não interpoladas",
                             padding=SPACING["xs"])
        panel.grid(row=0, column=0, sticky="nsew")
        toolbar_row = ttk.Frame(panel.body, style="Surface.TFrame")
        toolbar_row.pack(fill=tk.X)
        self.timeline_chart = charts.Chart(panel.body, self.theme, height=2.3)
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
                                                 information=True, padding=SPACING["2xs"])
        self.timeline_explanation.pack(fill=tk.X, pady=(0, SPACING["2xs"]))
        timeline_note = ttk.Label(self.timeline_explanation.body,
                                  text="Faixas temporais neutras: aquecimento, treino, validação e OOS; cores de estado aparecem "
                                       "apenas nos marcadores. Segmentos retos ligam os valores recebidos, sem suavização. "
                                       "Lacunas não são conectadas.",
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
        series = _SYNTHETIC_FIXTURE["timeline"]["series"]
        return next((item for item in series if item["label"] == self.timeline_var.get()), series[0])

    def _draw_timeline(self):
        series = self._current_timeline_series()
        chart = self.timeline_chart
        chart.clear()
        ax = chart.figure.add_subplot(111)
        c = self.theme.colors
        charts.style_axis(ax, self.theme, "Spearman ρ · janelas na ordem cronológica", grid_axis="both")
        # Faixas temporais usam apenas tons neutros. Cores semânticas são reservadas
        # aos marcadores de estado para não sugerir evidência favorável no OOS.
        segments = (
            ("Aquecimento", c["axis"], 0.035),
            ("Treino", c["axis"], 0.065),
            ("Validação", c["axis"], 0.095),
            ("OOS", c["axis"], 0.125),
        )
        for window in series["windows"]:
            _, color, alpha = next(item for item in segments if item[0] == window["segment"])
            ax.axvspan(_date(window["start"]), _date(window["end"]), color=color, alpha=alpha, linewidth=0)
        centers = [_date(window["center"]) for window in series["windows"]]
        values = [np.nan if window["value"] is None else window["value"] for window in series["windows"]]
        import matplotlib.dates as mdates
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
        ax.set_xlabel("Início → fim da base", color=c["muted"], fontsize=TYPOGRAPHY["chart_label"])
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b/%y"))
        ax.set_xlim(_date("2025-01-01"), _date("2025-12-31"))
        chart.figure.autofmt_xdate(rotation=0)
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
        window = self._current_timeline_series()["windows"][index]
        self._set_detail("timeline", f"{window['segment']} · {window['start']} a {window['end']}", window)

    def _build_distribution(self, parent):
        distribution = _SYNTHETIC_FIXTURE["distribution"]
        note = Notice(parent,
                      "CUSTOS INCOMPLETOS · distribuição bruta em R fornecida pela fixture; expectativa líquida não exibida. "
                      "Rótulos censurados permanecem no denominador informado.",
                      kind="warning", theme=self.theme)
        _fit_label_to_parent(note.message, note, reserve=128)
        self.distribution_notice = note
        note.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        self.distribution_split, primary, detail = self._split_view(parent)
        primary.columnconfigure(0, weight=1)
        primary.rowconfigure(0, weight=1)
        primary.rowconfigure(1, weight=0)
        panel = ContentPanel(primary, title="Distribuição por direção e contexto",
                             subtitle=distribution["unit"], padding=SPACING["xs"])
        panel.grid(row=0, column=0, sticky="nsew")
        self.distribution_population_label = ttk.Label(
            panel.body, text=f"População: {distribution['population']}", style="Muted.TLabel")
        _fit_label_to_parent(self.distribution_population_label, panel.body)
        self.distribution_population_label.pack(anchor="w", pady=(0, SPACING["2xs"]))
        distribution_legend = ttk.Label(panel.body, text="Faixas: perdas (vermelho) · empates (cinza) · ganhos (verde). "
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
        self._draw_distribution()

    def _draw_distribution(self):
        data = _SYNTHETIC_FIXTURE["distribution"]
        chart = self.distribution_chart
        chart.clear()
        axes = chart.figure.subplots(1, 2, sharey=True)
        c = self.theme.colors
        bins = data["bins"]
        categories = (("Perdas", c["bad"]), ("Empates", c["muted"]), ("Ganhos", c["good"]))
        for axis, direction in zip(axes, data["directions"]):
            charts.style_axis(axis, self.theme, f"{direction['label']} · n={direction['n_total']} total, "
                              f"{direction['n_mature']} maturadas", grid_axis="y")
            for index, (bin_label, bin_category) in enumerate(bins):
                count = direction["counts"][index]
                category = {"LOSS": "Perdas", "FLAT": "Empates", "WIN": "Ganhos"}[bin_category]
                color = next(color for name, color in categories if name == category)
                axis.bar(index, count, color=color, width=0.68, edgecolor=c["surface"], linewidth=0.8)
                axis.text(index, count + 0.5, str(count), ha="center", va="bottom", fontsize=TYPOGRAPHY["chart_tick"],
                          color=c["ink2"])
            axis.set_xticks(range(len(bins)))
            axis.set_xticklabels([label for label, _category in bins], rotation=18, ha="right",
                                 fontsize=TYPOGRAPHY["chart_tick"])
            axis.set_ylim(0, 32)
            axis.set_xlabel("Resultado por trade · R", color=c["muted"], fontsize=TYPOGRAPHY["chart_label"])
        axes[0].set_ylabel("Operações em cada faixa", color=c["muted"], fontsize=TYPOGRAPHY["chart_label"])
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
        row = _SYNTHETIC_FIXTURE["distribution"]["directions"][index]
        self._set_detail("distribution", row["label"], {
            "period": _SYNTHETIC_FIXTURE["scope"]["period"],
            "method": "bins de resultado em R · valores fornecidos, sem recálculo",
            "effect_text": f"ganhos {row['wins']} · perdas {row['losses']} · empates {row['flats']}",
            "n": f"n total={row['n_total']} · n maduro={row['n_mature']} · censurados={row['censored']}",
            "population": _SYNTHETIC_FIXTURE["distribution"]["population"],
            "support_state": "mesma população, direção e unidade da fixture",
            "utility_state": "INCONCLUSIVE", "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT",
            "presence_state": row["presence_state"], "cost_status": _SYNTHETIC_FIXTURE["distribution"]["cost_status"],
            "reason": row["reason"],
        })

    def _build_convergence(self, parent):
        controls = FilterBar(parent, padding=SPACING["xs"])
        controls.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        pair_label = ttk.Label(controls, text="Par de exemplo:", style="Surface.TLabel")
        pair_label.grid(row=0, column=0, sticky="w", padx=(0, SPACING["xs"]))
        pairs = _SYNTHETIC_FIXTURE["convergence"]["pairs"]
        self.convergence_var = tk.StringVar(value=pairs[0]["label"])
        self.convergence_select = ttk.Combobox(controls, textvariable=self.convergence_var, state="readonly", width=20,
                                               values=[pair["label"] for pair in pairs])
        self.convergence_select.grid(row=0, column=1, sticky="w")
        self.convergence_select.bind("<<ComboboxSelected>>", lambda _event: self._draw_convergence())
        comparison_note = ttk.Label(controls, text="valores OOS recebidos · comparação pareada · sem ranking",
                                   style="Relations.Fixture.TLabel", justify=tk.RIGHT)
        self.convergence_comparison_note = comparison_note
        comparison_note.grid(row=0, column=2, sticky="e", padx=(SPACING["sm"], 0))
        controls.columnconfigure(2, weight=1)

        def resize_convergence_controls(event):
            if event.width < 1000:
                comparison_note.grid_configure(row=1, column=0, columnspan=3, sticky="w", padx=0)
                comparison_note.configure(wraplength=max(220, event.width - 24), justify=tk.LEFT)
            else:
                comparison_note.grid_configure(row=0, column=2, columnspan=1, sticky="e",
                                               padx=(SPACING["sm"], 0))
                comparison_note.configure(wraplength=max(220, event.width - 310), justify=tk.RIGHT)

        controls.bind("<Configure>", resize_convergence_controls, add="+")
        note = Notice(parent, "BASE BRUTA · custos não incluídos nos exemplos; valores OOS em R/op bruto, sem expectativa líquida.",
                      kind="warning", theme=self.theme)
        _fit_label_to_parent(note.message, note, reserve=128)
        self.convergence_notice = note
        note.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        self.convergence_split, primary, detail = self._split_view(parent)
        primary.columnconfigure(0, weight=1)
        primary.rowconfigure(0, weight=1)
        primary.rowconfigure(1, weight=0)
        panel = ContentPanel(primary, title="Baseline, fatores isolados e dupla",
                             subtitle="OOS · mesma população e janelas declaradas", padding=SPACING["xs"])
        panel.grid(row=0, column=0, sticky="nsew")
        self.convergence_chart = charts.Chart(panel.body, self.theme, height=2.2)
        self._charts["convergence"] = self.convergence_chart
        self.convergence_chart.widget.pack(fill=tk.BOTH, expand=True)
        self.convergence_table = TablePanel(primary, "Comparação fornecida pelo produtor",
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
        pairs = _SYNTHETIC_FIXTURE["convergence"]["pairs"]
        return next((item for item in pairs if item["label"] == self.convergence_var.get()), pairs[0])

    def _draw_convergence(self):
        pair = self._current_pair()
        chart = self.convergence_chart
        chart.clear()
        ax = chart.figure.add_subplot(111)
        c = self.theme.colors
        charts.style_axis(ax, self.theme, f"{pair['label']} · efeito OOS bruto informado", grid_axis="y")
        rows = pair["rows"]
        positions = range(len(rows))
        heights = pair["plot_values"]
        colors = [c[_STATE_COLORS.get(_visual_state(row), "muted")] for row in rows]
        bars = ax.bar(positions, heights, color=colors, width=0.62, edgecolor=c["surface"], linewidth=0.8)
        for rect, row, height in zip(bars, rows, heights):
            ax.text(rect.get_x() + rect.get_width() / 2, height, row["effect"],
                    ha="center", va="bottom" if height >= 0 else "top",
                    fontsize=TYPOGRAPHY["chart_tick"], color=c["ink"])
        ax.axhline(0, color=c["axis"], linewidth=0.9)
        ax.set_xticks(list(positions))
        ax.set_xticklabels([row["label"] for row in rows], rotation=10, ha="right",
                           fontsize=TYPOGRAPHY["chart_tick"])
        ax.set_ylabel("OOS bruto · R/op", color=c["muted"],
                      fontsize=TYPOGRAPHY["chart_label"])
        chart.draw()
        tree = self.convergence_table.tree
        tree.delete(*tree.get_children())
        for index, row in enumerate(rows):
            tree.insert("", tk.END, iid=f"convergence-{index}",
                        values=(row["label"], row["effect"], row["ci"], row["n"], row["delta"],
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
        row = pair["rows"][index]
        self._set_detail("convergence", row["label"], {
            **row,
            "period": _SYNTHETIC_FIXTURE["scope"]["period"],
            "method": "efeito bruto informado pelo produtor · OOS · comparação pareada",
            "effect_text": f"{row['effect']} · delta {row['delta']}",
            "scale": "R por operação bruto · custos não incluídos",
            "segment": pair["comparison"],
            "cost_status": "custos não incluídos; resultado bruto, sem expectativa líquida",
        })

    def _build_candidates(self, parent):
        notice = Notice(parent,
                        "T18.7 ainda não fornece candidatos. As linhas abaixo são exemplos sintéticos de apresentação; "
                        "não há ação de aplicar, exportar preset ou alterar Expert.",
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
        for index, row in enumerate(_SYNTHETIC_FIXTURE["candidates"]):
            self.candidate_table.tree.insert("", tk.END, iid=f"candidate-{index}",
                                             values=(row["id"], row["proposal"], row["scope"], row["window"],
                                                     row["evidence_state"], row["freshness_state"], row["presence_state"], row["validity"]),
                                             tags=(_visual_state(row),))
        self.candidate_table.tree.selection_set("candidate-0")
        self._candidate_selected(None)

    def _candidate_selected(self, _event):
        selection = self.candidate_table.tree.selection()
        if not selection:
            return
        index = int(selection[0].rsplit("-", 1)[1])
        row = _SYNTHETIC_FIXTURE["candidates"][index]
        self._set_detail("candidates", row["proposal"], {
            **row,
            "period": _SYNTHETIC_FIXTURE["scope"]["period"],
            "method": "proposta de pesquisa sintética · sem métrica calculada pela tela",
            "effect_text": row["validity"], "effect_ci": "não fornecido",
            "n": "não fornecido pela fixture", "segment": row["window"],
            "cost_status": "não comprovado; nenhuma aplicação operacional",
        })

    def render(self):
        """Atualiza paleta e gráficos quando a aplicação troca o tema; preserva a fixture passiva."""
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
