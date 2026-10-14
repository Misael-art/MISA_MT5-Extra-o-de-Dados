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
    kind: str                                  # EXIT ou SIZING
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
