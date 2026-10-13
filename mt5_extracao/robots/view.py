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


def _panel_plan(config, db, account, data, symbol: str, timeframe: str) -> dict:
    """
    O que é preciso para pôr o painel no gráfico: {"robot", "expert", "parameters", "live", "symbol", "timeframe"},
    ou {"note": motivo} quando ele não pode ser mostrado (a visão segue, só com o gráfico).
    """
    from mt5_extracao import timeframes
    from . import catalog, live
    slug = config.get("MT5", "panel_robot", fallback="hdk_hadouken_painel").strip()
    preset = config.get("MT5", "panel_preset", fallback="").strip() or None
    if db is None or not slug:
        return {"note": ""}
    try:
        robot = catalog.get_robot(catalog.load_catalog(config, db), slug)
    except (ValueError, KeyError, LookupError) as e:
        return {"note": f"o painel '{slug}' não está no catálogo de robôs ({e})"}
    if not account or not account.get("login"):
        return {"note": "a conta ainda não foi conferida: conecte uma vez (\"Verificar conexão\") e abra a visão de novo"}
    if not symbol:
        symbol, timeframe = layout.user_chart(data)
    tf = timeframes.parse(timeframe or DEFAULT_TIMEFRAME)
    if not symbol or tf is None:
        return {"note": "não há um ativo para o gráfico: clique em um ativo da lista e abra a visão de novo"}
    gates, trading = live.panel_gates(config, robot, account, preset)
    failed = [g for g in gates if not g.ok]
    if failed:
        return {"note": "; ".join(f"{g.name}: {g.detail}" + (f" ({g.fix})" if g.fix else "") for g in failed)}
    plan = mt5_tools.install_plan(robot, data)
    parameters = ""
    if preset:
        target = next((i.target for i in plan if i.target.suffix.lower() == ".set" and i.target.stem == preset), None)
        if target is None or not target.is_file():
            return {"note": f"o preset {preset} não está na pasta do MT5 (rode: mt5x robots install {slug})"}
        parameters = target.name
    return {"robot": robot, "expert": mt5_tools.expert_name(plan[0].target.with_suffix(".ex5"), data),
            "parameters": live.panel_parameters(config, robot, data, parameters), "live": trading,
            "symbol": symbol, "timeframe": tf.name}


def open_view(config, close_terminal: bool = False, spawn: Optional[Callable] = None, symbol: str = "",
              timeframe: str = "", db=None, account: Optional[dict] = None, confirm: Optional[Callable] = None) -> dict:
    """
    Aplica a organização da janela e abre o terminal. `symbol` e `timeframe` (padrão M15) dizem o gráfico; sem
    `symbol`, vale o primeiro gráfico do perfil em que o terminal estava.

    Com `[MT5] view_panel` ligado, `db` e `account` (MT5Connector.account_summary() da conta conferida), o painel
    do HADOUKEN entra no gráfico pelo arquivo de início, na conta conferida. Sem a bateria de segurança aprovada
    ele entra só para visualização, com o PARAR TUDO acionado (veja live.panel_gates). Se o painel não puder ser
    mostrado, a visão segue só com o gráfico e `panel_note` diz o motivo. `confirm` (mt5_tools.confirm_startup)
    confere no log do terminal que o painel foi carregado.

    Devolve {"reopened" (estava aberto e foi fechado), "organized" (a organização está ligada), "hidden": [o que foi
    escondido agora], "restored" (o arranjo anterior foi devolvido, com a organização desligada), "profile" (perfil
    de gráficos usado ou ""), "chart" (gráfico criado no perfil, ex. "EURUSD M15", ou ""), "panel" (nome do painel
    posto no gráfico ou ""), "panel_trading" (negociação ligada para ele), "panel_blocked" (o PARAR TUDO foi
    acionado para ele ficar só para visualização), "panel_note", "panel_layout",
    "panel_charts" (gráficos do perfil próprio que receberam a disposição do painel), "bridge" (a ponte foi religada)}.
    """
    from datetime import datetime
    running = mt5_tools.terminal_running(config)
    if running and not close_terminal:
        raise TerminalOpen("O MetaTrader 5 está aberto: a organização da janela só é gravada com ele fechado. "
                           "Autorize fechar e reabrir o terminal (na linha de comando: --close-terminal).")
    data = mt5_tools.data_dir(config)
    common = mt5_tools.common_files_dir(config)
    result = {"reopened": running, "organized": False, "hidden": [], "restored": False, "profile": "", "chart": "",
              "panel": "", "panel_trading": False, "panel_blocked": False, "panel_note": "",
              "panel_layout": layout.panel_layout(config), "panel_charts": 0, "bridge": False}
    bridge_was_running = False
    panel = {}
    try:
        if running:
            bridge_was_running = mt5_tools.release_terminal(config)
        ini = ""
        if layout.enabled(config, common):
            opts = layout.options(config)
            result["organized"] = True
            result["hidden"] = layout.apply(data, toolbars=opts.toolbars, navigation=opts.navigation,
                                            toolbox=opts.toolbox)
            if opts.own_profile and opts.panel:
                panel = _panel_plan(config, db, account, data, symbol, timeframe)
                result["panel_note"] = panel.get("note", "")
            if panel.get("robot") is not None:
                # O painel entra pelo arquivo de início, num perfil refeito: um gráfico só, o dele, maximizado.
                layout.prepare_profile(data, panel["symbol"], panel["timeframe"], common)
                result.update(profile=layout.PROFILE, chart=f"{panel['symbol']} {panel['timeframe']}",
                              panel=panel["robot"].name, panel_trading=panel["live"])
                if not panel["live"]:
                    # Sem a bateria aprovada o painel só pode ser VISTO: o PARAR TUDO garante que a camada de
                    # segurança não envia ordem. Ele fica acionado até a pessoa liberar (mt5x robots stop --release).
                    from . import live
                    live.stop_all(config, "Painel aberto só para visualização (sem a bateria de segurança aprovada).")
                    result["panel_blocked"] = True
                ini = mt5_tools.startup_ini(panel["expert"], panel["symbol"], panel["timeframe"], account["login"],
                                            account.get("server", ""), panel["parameters"],
                                            live_trading=panel["live"]) + layout.startup_lines(layout.PROFILE)
            else:
                # O perfil próprio que já tem gráfico é usado como está. Sem gráfico nenhum, ele nasce aqui com um
                # gráfico maximizado do ativo pedido ou, sem ativo, do que o usuário já tinha (sem o aviso ao
                # painel: não há gráfico auxiliar a fechar). Sem nada disso, o terminal abre no perfil em que estava.
                if opts.own_profile and not layout.has_profile(data):
                    if not symbol:
                        symbol, timeframe = layout.user_chart(data)
                    if symbol:
                        layout.prepare_profile(data, symbol, timeframe or DEFAULT_TIMEFRAME)
                        result["chart"] = f"{symbol} {timeframe or DEFAULT_TIMEFRAME}"
                if opts.own_profile and layout.has_profile(data):
                    result["profile"] = layout.PROFILE
                    ini = layout.startup_lines(layout.PROFILE)
                result["panel_charts"] = layout.set_panel_input(data, layout.panel_code(result["panel_layout"]))
        else:
            result["restored"] = layout.restore(data)
        opened_at = datetime.now()
        mt5_tools.start_terminal(config, data, ini, "mt5x_view", spawn, is_running=lambda: False)
        if result["panel"] and confirm is not None:
            try:
                confirm(data, panel["expert"], opened_at)
            except RuntimeError as e:
                result.update(panel="", panel_note=str(e))
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
    if result["panel"]:
        parts.append(f"painel {result['panel']} no gráfico, " + ("com a negociação ligada" if result["panel_trading"]
                     else "só para visualização: ele não tem a bateria de segurança aprovada, então o PARAR TUDO foi "
                          "acionado (o painel aparece BLOQUEADO e nenhum robô do HADOUKEN opera até você liberar com "
                          "'mt5x robots stop --release')"))
    elif result["panel_note"]:
        parts.append("painel não mostrado: " + result["panel_note"])
    if result["panel_charts"]:
        parts.append(f"disposição do painel ajustada em {result['panel_charts']} gráfico(s)")
    if result["bridge"]:
        parts.append("ponte religada")
    return "; ".join(parts) + "."
