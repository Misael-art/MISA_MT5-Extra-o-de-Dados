"""
"Abrir visão MT5": abre (ou reabre) o MetaTrader 5 com a organização da janela escolhida em [MT5] aplicada, a
qualquer momento, sem depender do início de um robô (T12.L3).

    result = open_view(config, close_terminal=True)

A organização é gravada no `terminal.ini` com o terminal fechado (veja `layout.py`); por isso, se ele estiver
aberto, é preciso fechá-lo e abri-lo de novo, o que só acontece com `close_terminal=True`. Nenhuma ordem é enviada
e nenhum robô é colocado ou tirado de gráfico: o terminal volta com os gráficos que tinha.
"""
import logging
from typing import Callable, Optional

from . import layout, mt5_tools

log = logging.getLogger(__name__)


class TerminalOpen(RuntimeError):
    """O terminal está aberto e quem chamou não autorizou fechá-lo."""


DEFAULT_TIMEFRAME = "M15"


def open_view(config, close_terminal: bool = False, spawn: Optional[Callable] = None, symbol: str = "",
              timeframe: str = "") -> dict:
    """
    Aplica a organização da janela e abre o terminal. `symbol` e `timeframe` (padrão M15) só são usados quando o
    perfil de gráficos próprio ainda não tem gráfico: aí ele é criado com um gráfico desse ativo, maximizado (sem
    `symbol`, com o ativo e o timeframe do primeiro gráfico do perfil em que o terminal estava).
    Devolve {"reopened" (estava aberto e foi fechado), "organized" (a organização está ligada), "hidden": [o que foi
    escondido agora], "restored" (o arranjo anterior foi devolvido, com a organização desligada), "profile" (perfil
    de gráficos usado ou ""), "chart" (gráfico criado no perfil, ex. "EURUSD M15", ou ""), "panel_layout",
    "panel_charts" (gráficos do perfil próprio que receberam a disposição do painel), "bridge" (a ponte foi religada)}.
    """
    running = mt5_tools.terminal_running(config)
    if running and not close_terminal:
        raise TerminalOpen("O MetaTrader 5 está aberto: a organização da janela só é gravada com ele fechado. "
                           "Autorize fechar e reabrir o terminal (na linha de comando: --close-terminal).")
    data = mt5_tools.data_dir(config)
    common = mt5_tools.common_files_dir(config)
    result = {"reopened": running, "organized": False, "hidden": [], "restored": False, "profile": "", "chart": "",
              "panel_layout": layout.panel_layout(config), "panel_charts": 0, "bridge": False}
    bridge_was_running = False
    try:
        if running:
            bridge_was_running = mt5_tools.release_terminal(config)
        ini = ""
        if layout.enabled(config, common):
            opts = layout.options(config)
            result["organized"] = True
            result["hidden"] = layout.apply(data, toolbars=opts.toolbars, navigation=opts.navigation,
                                            toolbox=opts.toolbox)
            # O perfil próprio que já tem gráfico é usado como está: refazê-lo apagaria o gráfico em que um robô
            # ficou. Sem gráfico nenhum, ele nasce aqui com um gráfico maximizado do ativo pedido (sem o aviso ao
            # painel: não há gráfico auxiliar a fechar). Sem ativo, o terminal abre no perfil em que estava.
            if opts.own_profile and not layout.has_profile(data):
                if not symbol:           # sem ativo pedido: o gráfico que o usuário já tinha no perfil dele
                    symbol, timeframe = layout.user_chart(data)
                if symbol:
                    layout.prepare_profile(data, symbol, timeframe or DEFAULT_TIMEFRAME)
                    result["chart"] = f"{symbol} {timeframe or DEFAULT_TIMEFRAME}"
            if opts.own_profile and layout.has_profile(data):
                result["profile"] = layout.PROFILE
                ini = layout.startup_lines(layout.PROFILE)
        else:
            result["restored"] = layout.restore(data)
        result["panel_charts"] = layout.set_panel_input(data, layout.panel_code(result["panel_layout"]))
        mt5_tools.start_terminal(config, data, ini, "mt5x_view", spawn, is_running=lambda: False)
    finally:
        if bridge_was_running:
            mt5_tools.restart_bridge()
            result["bridge"] = True
    log.info(f"Visão do MT5 aberta: {describe(result)}")
    return result


def describe(result: dict) -> str:
    """Uma frase, para o log e para a tela, sobre o que open_view fez."""
    parts = ["MetaTrader 5 fechado e aberto de novo" if result["reopened"] else "MetaTrader 5 aberto"]
    if result["organized"]:
        parts.append("escondido agora: " + ", ".join(result["hidden"]) if result["hidden"]
                     else "janela organizada como pedido")
        if result["profile"]:
            parts.append(f"perfil de gráficos {result['profile']}" +
                         (f", com o gráfico de {result['chart']} maximizado" if result["chart"] else ""))
    else:
        parts.append("organização desligada" + (", arranjo anterior devolvido" if result["restored"] else ""))
    if result["panel_charts"]:
        parts.append(f"disposição do painel ajustada em {result['panel_charts']} gráfico(s)")
    if result["bridge"]:
        parts.append("ponte religada")
    return "; ".join(parts) + "."
