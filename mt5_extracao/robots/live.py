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


def demo_gates(config, robot, account: Optional[dict], preset: Optional[str] = None) -> List[Gate]:
    """Exigências para iniciar em demo. `account` é MT5Connector.account_summary() (None sem conexão)."""
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


def start_demo(db, config, robot, account: Optional[dict], symbol: str, timeframe: str,
               preset: Optional[str] = None, spawn: Optional[Callable] = None,
               is_running: Optional[Callable] = None) -> dict:
    """
    Confere as travas e abre o terminal com o robô no gráfico. Lança GateError se alguma exigência falhar.
    O terminal precisa estar fechado (o chamador fecha, se o usuário pediu).
    """
    from mt5_extracao import timeframes
    tf = timeframes.parse(timeframe)
    if tf is None:
        raise ValueError(f"Timeframe inválido: {timeframe}")
    gates = demo_gates(config, robot, account, preset)
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
    ini = mt5_tools.startup_ini(mt5_tools.expert_name(compiled, data), symbol, tf.name, account["login"],
                                account.get("server", ""), parameters)
    mt5_tools.start_terminal(config, data, ini, "mt5x_startup", spawn, is_running)
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
