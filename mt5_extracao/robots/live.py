"""
Tempo real em conta de demonstração: travas, início do robô no MT5, acompanhamento e PARAR TUDO.

    gates = demo_gates(config, robot, account, preset)      # o que falta para iniciar, item por item
    start_demo(db, config, robot, account, "EURUSD", "H1", preset)
    status(mt5, magic)                                      # posições e resultado do robô (somente leitura)
    stop_all(config)                                        # grava HDK_KILL.txt: nenhum robô HADOUKEN opera

O Python não envia ordens: quem opera é o robô MQL5 dentro do MT5. Este módulo só coloca o robô no gráfico,
lê posições e negócios e grava o arquivo de parada. Conta real é recusada aqui (fica para a T8.8).

O início pelo terminal real NÃO foi validado: só com processos simulados (veja docs/PLANO_DE_TRABALHO.md, T8.7).
"""
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, List, Optional

from . import battery, catalog, mt5_tools, store

log = logging.getLogger(__name__)

DEAL_ENTRY_IN = 0          # demais valores (out, inout, out by) fecham posição


@dataclass
class Gate:
    name: str
    ok: bool
    detail: str = ""
    fix: str = ""


class GateError(RuntimeError):
    """O robô não pode iniciar: `gates` traz cada exigência, cumprida ou não."""

    def __init__(self, gates: List[Gate]):
        self.gates = gates
        failed = [g for g in gates if not g.ok]
        super().__init__("O robô não pode iniciar: " + "; ".join(f"{g.name} ({g.detail})" for g in failed))


def demo_gates(config, robot, account: Optional[dict], preset: Optional[str] = None,
               symbol: Optional[str] = None, timeframe: Optional[str] = None) -> List[Gate]:
    """
    Exigências para iniciar em demo. `account` é MT5Connector.account_summary() (None sem conexão). Com `symbol`
    e `timeframe` (o gráfico em que o robô vai entrar), a aprovação da bateria tem de ser desse ativo e timeframe.
    """
    slug = robot.key.split(":", 1)[1]
    if robot.kind != catalog.MQL5:
        return [Gate("Robô MQL5", False, "estratégia Python não opera",
                     "Para operar ela precisa de uma versão em MQL5 sobre a camada de segurança.")]
    gates = [Gate("Robô MQL5", True, robot.name)]

    compiled = None
    try:
        data = mt5_tools.data_dir(config)
        compiled = mt5_tools.install_plan(robot, data)[0].target.with_suffix(".ex5")
    except (OSError, ValueError) as e:
        gates.append(Gate("Compilado na pasta do MT5", False, str(e), f"Rode: mt5x robots compile {slug}"))
    if compiled is not None:
        ok = compiled.is_file()
        gates.append(Gate("Compilado na pasta do MT5", ok, compiled.name if ok else "arquivo .ex5 ausente",
                          "" if ok else f"Rode: mt5x robots compile {slug}"))

    check = robot.checks[0] if robot.checks else None
    command = f"mt5x robots battery {slug}" + (f" --preset {preset}" if preset else "")
    if check is None:
        gates.append(Gate("Bateria de segurança aprovada", False, "a bateria ainda não foi rodada", f"Rode: {command}"))
    elif not check["approved"]:
        gates.append(Gate("Bateria de segurança aprovada", False, f"reprovada em {check['created_at'][:16]}",
                          f"Corrija os itens reprovados e rode de novo: {command}"))
    elif (check.get("preset") or "") != (preset or ""):
        gates.append(Gate("Bateria de segurança aprovada", False,
                          f"a aprovação é do {'preset ' + check['preset'] if check.get('preset') else 'padrão do código'}"
                          ", não do que você vai usar", f"Rode: {command}"))
    elif symbol and check.get("symbol") and (check["symbol"].lower(), check.get("timeframe", "")) != \
            (symbol.lower(), timeframe or check.get("timeframe", "")):
        gates.append(Gate("Bateria de segurança aprovada", False,
                          f"a aprovação é de {check['symbol']} {check.get('timeframe', '')}, não de {symbol} "
                          f"{timeframe or ''}".rstrip(),
                          f"Teste e aprove neste ativo: mt5x robots backtest {slug} --symbol {symbol} --tf "
                          f"{timeframe or '<timeframe>'} e depois {command} --symbol {symbol}"))
    elif check.get("fingerprint") != catalog.fingerprint(robot):
        gates.append(Gate("Bateria de segurança aprovada", False,
                          "o código mudou depois da aprovação (ou ela é de uma versão anterior do programa)",
                          f"Compile, teste e rode a bateria de novo: {command}"))
    else:
        gates.append(Gate("Bateria de segurança aprovada", True, f"aprovada em {check['created_at'][:16]}"))

    if not account or not account.get("login"):
        gates.append(Gate("Conta de demonstração conectada", False, "nenhuma conta conectada no MetaTrader 5",
                          "Abra o MT5, entre na conta demo e tente de novo (mt5x doctor ajuda a diagnosticar)."))
    elif not account.get("demo"):
        gates.append(Gate("Conta de demonstração conectada", False,
                          f"a conta {account['login']} ({account.get('server', '')}) é REAL",
                          "Entre em uma conta de demonstração. Conta real não está disponível nesta versão."))
    else:
        gates.append(Gate("Conta de demonstração conectada", True, f"{account['login']} em {account.get('server', '')}"))

    try:
        killed = mt5_tools.kill_file(config).is_file()
    except OSError:
        killed = False
    gates.append(Gate("PARAR TUDO desligado", not killed, "o arquivo de parada está ativo" if killed else "",
                      "Libere com: mt5x robots stop --release" if killed else ""))
    return gates


BATTERY_GATE, KILL_GATE = "Bateria de segurança aprovada", "PARAR TUDO desligado"
TESTER_SOURCE = "strategy tester"      # o mesmo de backtest.TESTER_SOURCE (o backtest comum de um robô MQL5)
STATIC_GATE = "Leitura do código aprovada"


def is_manual_panel(robot) -> Optional[bool]:
    """
    O robô é um painel de operação manual? A evidência é o Strategy Tester: no teste mais recente do código que
    está no disco ele não fez nenhuma entrada sozinho. None se não há teste do código atual (não dá para saber).
    """
    for run in robot.runs:
        metrics = run.get("metrics", {})
        if run.get("source") == TESTER_SOURCE and metrics.get("source_fingerprint") == catalog.fingerprint(robot):
            return int(metrics.get("entries", 0) or 0) == 0 and int(metrics.get("trades", 0) or 0) == 0
    return None


def panel_gates(config, robot, account: Optional[dict], preset: Optional[str] = None) -> tuple:
    """
    Exigências para a visão do MT5 colocar um painel no gráfico. Devolve (travas, negociação ligada).

    Um painel de operação manual não tem como passar na parte estatística da bateria (ela mede operações
    automáticas). Decisão do dono do projeto (2026-10-13): **em conta de demonstração o painel manual opera sem
    bloqueio**, a menos que a pessoa religue o bloqueio (`[MT5] panel_demo_blocked`). Três casos:

    1. Bateria inteira aprovada: entra como qualquer robô, com a negociação ligada.
    2. Painel manual (veja is_manual_panel) em conta demo, com o robô compilado e a leitura do código aprovada para
       o código que está no disco (stop obrigatório, toda ordem pela camada de segurança, sem DLL): negociação
       ligada, salvo se `panel_demo_blocked` estiver ligado.
    3. Qualquer outro caso que cumpra as exigências do item 2 menos a de ser painel manual (um robô que opera
       sozinho, ou sem teste do código atual): só para visualização.

    "Só para visualização" quer dizer negociação desligada E o PARAR TUDO acionado pela visão: a chave
    `AllowLiveTrading` do arquivo de início não serve de garantia (medido no host, a negociação algorítmica do
    terminal estava ligada depois de o programa conectar). Conta real nunca entra por aqui. A trava da bateria de
    `mt5x robots start` para robôs automáticos não muda.
    """
    from . import layout
    gates = demo_gates(config, robot, account, preset)
    if all(g.ok for g in gates):
        return gates, True
    gates = [g for g in gates if g.name not in (BATTERY_GATE, KILL_GATE)]
    check = robot.checks[0] if robot.checks else None
    slug = robot.key.split(":", 1)[1]
    if check is None:
        gates.append(Gate(STATIC_GATE, False, "a bateria ainda não foi rodada", f"Rode: mt5x robots battery {slug}"))
    elif check.get("static_status") != battery.APPROVED:
        gates.append(Gate(STATIC_GATE, False, "a leitura do código reprovou",
                          f"Corrija os itens reprovados e rode: mt5x robots battery {slug}"))
    elif check.get("fingerprint") != catalog.fingerprint(robot):
        gates.append(Gate(STATIC_GATE, False, "o código mudou depois da leitura",
                          f"Compile e rode de novo: mt5x robots battery {slug}"))
    else:
        gates.append(Gate(STATIC_GATE, True, f"em {check['created_at'][:16]}"))
    manual = is_manual_panel(robot)
    return gates, bool(manual) and all(g.ok for g in gates) and not layout.panel_blocked(config)


def organize_window(config, data, symbol: str, timeframe: str) -> str:
    """
    Organização da janela do MT5 (padrão: ligada): janelas laterais e barras de ferramentas fechadas e o gráfico do
    robô sozinho e maximizado, num perfil de gráficos próprio. Cada parte pode ser desligada em [MT5] (veja
    layout.OPTION_KEYS). Devolve o trecho a acrescentar ao .ini de início ('' sem o perfil próprio, ou se a
    organização está desligada em [MT5] manage_layout ou pelo botão TELA do painel; aí o arranjo do usuário é
    devolvido). Falha aqui nunca impede o início do robô.
    """
    from . import layout
    try:
        common = mt5_tools.common_files_dir(config)
        if not layout.enabled(config, common):
            if layout.restore(data):
                log.info("Organização da janela desligada: o arranjo anterior do MT5 foi devolvido.")
            return ""
        opts = layout.options(config)
        layout.apply(data, toolbars=opts.toolbars, navigation=opts.navigation, toolbox=opts.toolbox)
        if not opts.own_profile:
            return ""
        return layout.startup_lines(layout.prepare_profile(data, symbol, timeframe, common))
    except (OSError, ValueError) as e:
        log.warning(f"Não foi possível organizar a janela do MT5 ({e}). O robô inicia com o arranjo atual.")
        return ""


def panel_parameters(config, robot, data, parameters: str) -> str:
    """
    Arquivo de parâmetros do início com a disposição do painel escolhida em [MT5] panel_layout. Só muda algo se a
    escolha não for 'auto' e o robô tiver o parâmetro `InpPanelLayout`: aí grava em MQL5/Presets uma cópia do preset
    (ou um arquivo só com esse parâmetro) e devolve o nome dela. O preset do usuário não é alterado.
    """
    from . import layout, mql
    choice = layout.panel_layout(config)
    if choice == "auto" or not any(i.name == layout.PANEL_INPUT for i in robot.inputs):
        return parameters
    folder = Path(data) / "MQL5" / "Presets"
    lines = mql.read_text(folder / parameters).splitlines() if parameters else []
    lines = [line for line in lines if not line.strip().startswith(layout.PANEL_INPUT + "=")]
    lines.append(f"{layout.PANEL_INPUT}={layout.panel_code(choice)}")
    folder.mkdir(parents=True, exist_ok=True)
    name = "mt5x_painel.set"
    (folder / name).write_bytes(b"\xff\xfe" + "".join(line + "\r\n" for line in lines).encode("utf-16-le"))
    return name


def start_demo(db, config, robot, account: Optional[dict], symbol: str, timeframe: str,
               preset: Optional[str] = None, spawn: Optional[Callable] = None,
               is_running: Optional[Callable] = None, confirm: Optional[Callable] = None) -> dict:
    """
    Confere as travas e abre o terminal com o robô no gráfico. Lança GateError se alguma exigência falhar.
    O terminal precisa estar fechado (o chamador fecha, se o usuário pediu). Depois de abrir, confere no log do
    terminal que o robô foi carregado, se quem chama passar `confirm` (o comando `mt5x robots start` passa
    mt5_tools.confirm_startup); se não foi, levanta RuntimeError e NÃO grava o robô como iniciado.
    """
    from mt5_extracao import timeframes
    tf = timeframes.parse(timeframe)
    if tf is None:
        raise ValueError(f"Timeframe inválido: {timeframe}")
    gates = demo_gates(config, robot, account, preset, symbol, tf.name)
    if not all(g.ok for g in gates):
        raise GateError(gates)
    data = mt5_tools.data_dir(config)
    plan = mt5_tools.install_plan(robot, data)
    compiled = plan[0].target.with_suffix(".ex5")
    parameters = ""
    if preset:
        target = next((i.target for i in plan if i.target.suffix.lower() == ".set" and i.target.stem == preset), None)
        if target is None or not target.is_file():
            raise ValueError(f"O preset {preset} não está na pasta do MT5. Rode: mt5x robots install "
                             + robot.key.split(":", 1)[1])
        parameters = target.name
    values = robot.presets.get(preset, {}) if preset else {}
    parameters = panel_parameters(config, robot, data, parameters)
    ini = mt5_tools.startup_ini(mt5_tools.expert_name(compiled, data), symbol, tf.name, account["login"],
                                account.get("server", ""), parameters)
    ini += organize_window(config, data, symbol, tf.name)
    opened_at = datetime.now()
    expert = mt5_tools.expert_name(compiled, data)
    mt5_tools.start_terminal(config, data, ini, "mt5x_startup", spawn, is_running)
    if confirm is not None:
        confirm(data, expert, opened_at)
    state = {"account": account["login"], "server": account.get("server", ""), "symbol": symbol,
             "timeframe": tf.name, "preset": preset or "", "magic": battery.magic_number(robot, values),
             "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "stopped_at": ""}
    _save_state(db, robot, catalog.DEMO, state)
    log.info(f"Robô {robot.name} iniciado em demo: conta {state['account']}, {symbol} {tf.name}")
    return state


def _save_state(db, robot, stage: str, state: dict) -> None:
    meta = dict(store.load_records(db).get(robot.key, {}).get("meta", {}), live=state)
    store.save_record(db, robot.key, robot.kind, robot.name, robot.path, stage, meta)


def live_state(db, robot) -> Optional[dict]:
    """O que foi gravado no último início deste robô (None se nunca iniciou)."""
    return store.load_records(db).get(robot.key, {}).get("meta", {}).get("live")


def mark_stopped(db, robot) -> None:
    """Registra que o robô saiu do ar: o card volta ao estágio que o disco e a bateria mostram."""
    state = live_state(db, robot)
    if state and not state.get("stopped_at"):
        state["stopped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _save_state(db, robot, catalog.COMPILED, state)


def status(mt5, magic, since: Optional[datetime] = None) -> dict:
    """
    Posições abertas e resultado fechado do robô (pelo magic number), somente leitura. `since` é o início do
    acompanhamento (padrão: últimas 24 horas).
    """
    magic = int(magic)
    now = datetime.now()
    since = since or now - timedelta(days=1)
    positions = [p for p in (mt5.positions_get() or ()) if int(getattr(p, "magic", 0)) == magic]
    deals = [d for d in (mt5.history_deals_get(since, now + timedelta(days=1)) or ())
             if int(getattr(d, "magic", 0)) == magic and int(getattr(d, "entry", DEAL_ENTRY_IN)) != DEAL_ENTRY_IN]
    results = [float(getattr(d, "profit", 0.0)) + float(getattr(d, "swap", 0.0)) + float(getattr(d, "commission", 0.0))
               for d in deals]
    return {"magic": magic, "since": since.strftime("%Y-%m-%d %H:%M:%S"),
            "open_positions": len(positions),
            "open_volume": sum(float(getattr(p, "volume", 0.0)) for p in positions),
            "open_profit": sum(float(getattr(p, "profit", 0.0)) + float(getattr(p, "swap", 0.0)) for p in positions),
            "positions_without_stop": sum(1 for p in positions if not float(getattr(p, "sl", 0.0))),
            "closed_trades": len(results), "closed_result": sum(results),
            "wins": sum(1 for r in results if r > 0), "losses": sum(1 for r in results if r < 0)}


def stop_all(config, reason: str = "") -> str:
    """PARAR TUDO: grava o arquivo que faz todo robô com a camada HADOUKEN parar de operar. Devolve o caminho."""
    path = mt5_tools.kill_file(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    path.write_text(f"PARAR TUDO acionado pelo mt5x em {stamp}. {reason}".strip() + "\r\n", encoding="cp1252",
                    errors="replace")
    log.warning(f"PARAR TUDO acionado: {path}")
    return str(path)


def release_stop(config) -> bool:
    """Remove o arquivo de parada (decisão de uma pessoa). True se havia um arquivo."""
    path = mt5_tools.kill_file(config)
    existed = path.is_file()
    path.unlink(missing_ok=True)
    return existed


def stop_active(config) -> bool:
    try:
        return mt5_tools.kill_file(config).is_file()
    except OSError:
        return False


# --- acompanhamento contínuo: um aviso por ação do robô ---------------------------------

DEAL_BUY, DEAL_SELL = 0, 1


def _money(value: float) -> str:
    return f"{value:+.2f}".replace(".", ",")


def describe_deal(deal) -> tuple:
    """(título, texto) de um negócio do robô, para o aviso. Entrada: lado, volume, ativo e preço; saída: resultado."""
    symbol, volume = getattr(deal, "symbol", ""), float(getattr(deal, "volume", 0.0))
    price, kind = float(getattr(deal, "price", 0.0)), int(getattr(deal, "type", -1))
    if int(getattr(deal, "entry", DEAL_ENTRY_IN)) == DEAL_ENTRY_IN:
        side = "COMPRA" if kind == DEAL_BUY else "VENDA" if kind == DEAL_SELL else "operação"
        return f"Robô abriu {side} em {symbol}", f"{volume:g} lote(s) a {price:g}"
    result = float(getattr(deal, "profit", 0.0)) + float(getattr(deal, "swap", 0.0)) + \
        float(getattr(deal, "commission", 0.0))
    return f"Robô fechou posição em {symbol}", f"{volume:g} lote(s) a {price:g}; resultado {_money(result)}"


def watch(mt5, magic, emit: Callable, since: Optional[datetime] = None, interval: float = 15.0,
          cycles: Optional[int] = None, sleep: Optional[Callable] = None, kill_active: Optional[Callable] = None) -> int:
    """
    Consulta o MT5 de tempos em tempos (somente leitura) e chama `emit(título, texto, nível)` uma vez para cada
    ação nova do robô: negócio de abertura ou de fechamento, posição sem stop, PARAR TUDO ativo, e perda ou volta
    da conexão. O que já existia quando o acompanhamento começou não gera aviso. Roda `cycles` consultas (None =
    até ser interrompido) e devolve quantos avisos emitiu.
    """
    import time
    from mt5_extracao import notify
    magic = int(magic)
    since = since or datetime.now() - timedelta(days=1)
    sleep = sleep or time.sleep
    seen_deals, warned_positions = set(), set()
    first, connected, kill_warned, emitted, done = True, True, False, 0, 0

    def out(title, text, level=notify.INFO):
        nonlocal emitted
        emitted += 1
        emit(title, text, level)

    while cycles is None or done < cycles:
        done += 1
        try:
            deals = [d for d in (mt5.history_deals_get(since, datetime.now() + timedelta(days=1)) or ())
                     if int(getattr(d, "magic", 0)) == magic]
            positions = [p for p in (mt5.positions_get() or ()) if int(getattr(p, "magic", 0)) == magic]
            if not connected:
                connected = True
                out("Acompanhamento do robô retomado", "A leitura do MetaTrader 5 voltou.")
        except Exception as e:
            if connected:
                connected = False
                out("Acompanhamento do robô interrompido",
                    f"Não foi possível ler o MetaTrader 5 ({type(e).__name__}). O robô pode continuar operando: "
                    "confira com 'mt5x mt5 status'.", notify.CRITICAL)
            if cycles is None or done < cycles:
                sleep(interval)
            continue
        for deal in sorted(deals, key=lambda d: (getattr(d, "time", 0), getattr(d, "ticket", 0))):
            ticket = getattr(deal, "ticket", None)
            if ticket in seen_deals:
                continue
            seen_deals.add(ticket)
            if not first:
                out(*describe_deal(deal))
        for position in positions:
            ticket = getattr(position, "ticket", None)
            if not float(getattr(position, "sl", 0.0)) and ticket not in warned_positions:
                warned_positions.add(ticket)
                out("Posição do robô SEM STOP", f"{getattr(position, 'symbol', '')}: "
                    f"{float(getattr(position, 'volume', 0.0)):g} lote(s) sem stop. Confira no MetaTrader 5.",
                    notify.CRITICAL)
        killed = bool(kill_active()) if kill_active else False
        if killed and not kill_warned:
            out("PARAR TUDO está ativo", "Os robôs com a camada de segurança não abrem novas operações.", notify.WARNING)
        kill_warned = killed
        first = False
        if cycles is None or done < cycles:
            sleep(interval)
    return emitted
