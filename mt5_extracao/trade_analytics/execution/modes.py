"""
Modos de execução do Laboratório (T16.7 e T16.8): registro aberto, com nome, explicação, parâmetros e como cada um
vira uma política do motor (`engine.ExitPolicy`) e, nos de dimensionamento, uma transformação da sequência.

    for key, mode in MODES.items(): ...
    policy = MODES["trailing_atr"].policy(base, {"multiplo": 3})
    available(robot)        # os modos que fazem sentido para o robô (martingale só se ele suporta; seção 4.4)

Cada modo é comparado com o modo base ("ponto_fixo") sobre as MESMAS entradas.
"""
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional

from mt5_extracao.trade_analytics.execution.engine import ExitPolicy

EXIT, SIZING = "saida", "dimensionamento"


@dataclass
class Mode:
    key: str
    name: str
    kind: str                                  # EXIT, SIZING ou RECOVERY
    help: str
    params: Dict[str, float] = field(default_factory=dict)        # padrão de cada parâmetro
    build: Callable[[ExitPolicy, dict], ExitPolicy] = lambda base, p: base
    study_only: bool = False                    # nunca exportável como preset (bateria)

    def policy(self, base: ExitPolicy, params: Optional[dict] = None) -> ExitPolicy:
        values = dict(self.params, **(params or {}))
        return self.build(base, values)


MODES: Dict[str, Mode] = {}


def register(mode: Mode) -> Mode:
    MODES[mode.key] = mode
    return mode


register(Mode("ponto_fixo", "Ponto fixo (base)", EXIT,
              "As regras do próprio robô: stop e alvo iniciais e as saídas dele. É a referência de comparação."))
register(Mode("alvo_r", "Alvo fixo em R", EXIT,
              "Troca o alvo por N vezes o risco inicial (R). Mesmo stop.", {"r": 2.0},
              lambda base, p: base.with_(target_r=p["r"])))
register(Mode("break_even", "Break-even", EXIT,
              "Leva o stop para o preço de entrada quando a operação anda N × R a favor.", {"r": 1.0},
              lambda base, p: base.with_(breakeven_r=p["r"])))
register(Mode("trailing_atr", "Trailing por ATR (Chandelier)", EXIT,
              "Stop móvel a N × ATR do fechamento; só anda a favor. Sem alvo fixo.", {"multiplo": 3.0},
              lambda base, p: base.with_(trailing_atr=p["multiplo"], target_r=None, use_original_target=False)))
register(Mode("trailing_barras", "Trailing pela mínima/máxima de N barras", EXIT,
              "Stop móvel na mínima (compra) ou máxima (venda) das últimas N barras. Sem alvo fixo.", {"barras": 10},
              lambda base, p: base.with_(trailing_bars=int(p["barras"]), target_r=None, use_original_target=False)))


def exit_modes() -> Dict[str, Mode]:
    return {k: m for k, m in MODES.items() if m.kind == EXIT}


# --- dimensionamento (T16.8) ----------------------------------------------------------------
# Gradientes: os mesmos conceitos do Gradiente Linear do HADOUKEN (T12.4 fixo, T12.5 dinâmico). Exportáveis só como
# parâmetros dele, que roda dentro da camada de segurança.

register(Mode("gradiente_fixo", "Gradiente linear fixo (contra)", SIZING,
              "Abre novos níveis a cada N pontos contra a posição (preço médio), até o número de níveis e o teto de "
              "exposição. O stop do conjunto continua o inicial: o risco cresce a cada nível.",
              {"niveis": 3, "espacamento_pontos": 100, "fator_lote": 1.0, "teto_lotes": 0},
              lambda base, p: base.with_(grid_levels=int(p["niveis"]), grid_direction="contra",
                                         grid_spacing_points=p["espacamento_pontos"], grid_lot_factor=p["fator_lote"],
                                         grid_max_volume=p["teto_lotes"] or None)))
register(Mode("gradiente_dinamico", "Gradiente dinâmico por ATR (contra)", SIZING,
              "Como o gradiente fixo, com espaçamento de k × ATR medido antes da entrada (alarga com a volatilidade).",
              {"niveis": 3, "k_atr": 1.0, "fator_lote": 1.0, "teto_lotes": 0},
              lambda base, p: base.with_(grid_levels=int(p["niveis"]), grid_direction="contra",
                                         grid_spacing_atr=p["k_atr"], grid_lot_factor=p["fator_lote"],
                                         grid_max_volume=p["teto_lotes"] or None)))
register(Mode("gradiente_direcional", "Gradiente direcional (piramidação)", SIZING,
              "Aumenta a posição a cada N pontos A FAVOR e sobe o stop do conjunto para a entrada anterior.",
              {"niveis": 3, "espacamento_pontos": 100, "fator_lote": 1.0, "teto_lotes": 0},
              lambda base, p: base.with_(grid_levels=int(p["niveis"]), grid_direction="favor", grid_trail_stop=True,
                                         grid_spacing_points=p["espacamento_pontos"], grid_lot_factor=p["fator_lote"],
                                         grid_max_volume=p["teto_lotes"] or None)))
# Martingale: transformação da sequência (sizing.apply) sobre o modo base. Só para robô que o suporta (seção 4.4).
SEQUENCE = {"martingale": False, "anti_martingale": True}       # modo -> anti?
register(Mode("martingale", "Martingale (só robôs que o suportam)", SIZING,
              "Multiplica o lote depois de cada perda e volta ao inicial depois de um ganho, com as travas do próprio "
              "robô (perda diária, drawdown, risco por operação, lote máximo). A bateria reprova martingale para "
              "operação real: aqui é estudo do comportamento.", {"niveis": 4}, study_only=True))
register(Mode("anti_martingale", "Anti-martingale (só robôs que o suportam)", SIZING,
              "Multiplica o lote depois de cada ganho e volta ao inicial depois de uma perda, com as travas do robô.",
              {"niveis": 4}, study_only=True))


# --- recuperação condicional (T16.15) -----------------------------------------------------------
# Hipóteses H1–H4 e H6 de docs/propostas/ANALYTICS_OPERACOES.md, seção 11.11. Decidem no fechamento da barra com os
# sinais causais de `reversal.compute` e executam na abertura seguinte. Sempre estudo: a bateria reprova preço médio.
RECOVERY = "recuperacao"
_ZONE = {"zona_r": 0.6}


def _recovery(action, **fixed):
    def build(base, p):
        return base.with_(recovery=action, recovery_zone_r=p["zona_r"],
                          recovery_lot_factor=p.get("fator_lote", 1.0),
                          recovery_max_adds=int(p.get("max_adicoes", 1)),
                          recovery_max_volume=p.get("teto_lotes") or None, **fixed)
    return build


register(Mode("saida_antecipada", "Saída antecipada (H1)", RECOVERY,
              "Perto do stop, se o fechamento confirma a continuação contra a posição, sai na abertura seguinte. "
              "Não usa capital extra.", dict(_ZONE), _recovery("saida"), study_only=True))
register(Mode("recuperacao_unica", "Recuperação com adição única (H2)", RECOVERY,
              "Perto do stop, com sinal causal de reversão, adiciona uma vez o lote inicial e sai no equilíbrio "
              "líquido do conjunto. O stop continua o inicial: a perda máxima cresce com a adição.",
              dict(_ZONE, fator_lote=1.0, teto_lotes=0), _recovery("adicao", recovery_exit="equilibrio"),
              study_only=True))
register(Mode("recuperacao_proporcional", "Recuperação com adição proporcional (H3)", RECOVERY,
              "Como a adição única, com lote maior que o inicial e teto de exposição.",
              dict(_ZONE, fator_lote=2.0, teto_lotes=0), _recovery("adicao", recovery_exit="equilibrio"),
              study_only=True))
register(Mode("recuperacao_gerida", "Recuperação gerida (H4)", RECOVERY,
              "Adição única; ao tocar o equilíbrio o stop sobe para ele e o alvo passa a ser a entrada original; "
              "se o fechamento perde o extremo da barra do sinal, zera na abertura seguinte.",
              dict(_ZONE, fator_lote=1.0, teto_lotes=0),
              _recovery("adicao", recovery_exit="escalonada", recovery_invalidate=True), study_only=True))
register(Mode("reversao_independente", "Reentrada independente (H6)", RECOVERY,
              "No mesmo sinal, fecha a posição perdedora e abre uma operação nova com o mesmo stop e a mesma saída "
              "da adição única. Mede o mérito do sinal sem carregar o lote antigo.",
              dict(_ZONE, fator_lote=1.0, teto_lotes=0), _recovery("independente", recovery_exit="equilibrio"),
              study_only=True))


def recovery_modes() -> Dict[str, Mode]:
    return {k: m for k, m in MODES.items() if m.kind == RECOVERY}


def available(robot=None, preset: Optional[str] = None) -> Dict[str, Mode]:
    """Os modos que fazem sentido para o robô: martingale e anti-martingale só se ele os suporta."""
    from mt5_extracao.trade_analytics.execution import sizing
    supported = robot is not None and sizing.martingale_support(robot, preset) is not None
    return {k: m for k, m in MODES.items() if k not in SEQUENCE or supported}
