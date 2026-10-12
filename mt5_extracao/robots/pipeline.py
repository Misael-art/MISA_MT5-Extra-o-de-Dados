"""
Ciclo de desenvolvimento em um comando: contrato -> instalar e compilar -> Strategy Tester -> bateria.

    result = check(db, config, robot, robots, "EURUSD", "H1", preset="Conservador")
    result.ok                      # todas as etapas passaram
    write_junit(result, "relatorio.xml")

Só chama o que já existe (contract, mt5_tools, backtest, battery). Para na primeira etapa que falhar; as seguintes
ficam como "não executada". Nada aqui envia ordens: o Strategy Tester simula.
"""
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from . import backtest, battery, catalog, contract, mt5_tools, store

PASSED, WARNED, FAILED, SKIPPED = "passou", "aviso", "falhou", "não executada"
STEPS = (("contract", "Contrato do robô"), ("compile", "Instalar e compilar"),
         ("backtest", "Backtest no Strategy Tester"), ("battery", "Bateria de segurança"))


@dataclass
class Step:
    id: str
    name: str
    status: str = SKIPPED
    seconds: float = 0.0
    detail: str = ""
    data: dict = field(default_factory=dict)


@dataclass
class CheckResult:
    robot_key: str
    steps: List[Step] = field(default_factory=list)
    report: Optional[battery.Report] = None

    @property
    def ok(self) -> bool:
        return all(s.status in (PASSED, WARNED) for s in self.steps)

    def to_dict(self) -> dict:
        return {"robot": self.robot_key, "ok": self.ok, "steps": [vars(s) for s in self.steps],
                "battery": self.report.to_dict() if self.report else None}


def record_compiled(db, robot, result) -> None:
    """Grava o estágio "Compilado" sem apagar o que outras etapas guardaram (ex.: o estado do tempo real)."""
    record = store.load_records(db).get(robot.key, {})
    stage = record.get("stage") if record.get("stage") in (catalog.DEMO, catalog.LIVE) else catalog.COMPILED
    store.save_record(db, robot.key, robot.kind, robot.name, robot.path, stage,
                      dict(record.get("meta", {}), compiled=result.compiled, warnings=result.warnings))


def check(db, config, robot, robots, symbol: str, timeframe: str, start=None, end=None,
          preset: Optional[str] = None, run: Optional[Callable] = None,
          is_running: Optional[Callable] = None) -> CheckResult:
    """Roda as quatro etapas para um robô MQL5. `run` e `is_running` são injetados nos testes (sem MT5)."""
    if robot.kind != catalog.MQL5:
        raise ValueError(f"{robot.name} é uma estratégia Python: use 'mt5x validate' e 'mt5x robots battery'.")
    result = CheckResult(robot.key, [Step(step_id, name) for step_id, name in STEPS])
    steps = {s.id: s for s in result.steps}

    def execute(step: Step, action) -> bool:
        started = time.monotonic()
        try:
            step.status, step.detail = action(step)
        except (OSError, RuntimeError, ValueError) as e:
            step.status, step.detail = FAILED, str(e)
        step.seconds = round(time.monotonic() - started, 2)
        return step.status in (PASSED, WARNED)

    def do_contract(step):
        if not contract.declared(robot):
            return WARNED, "o robô não declara contrato (veja docs/sdk/contrato.md)"
        problems = contract.validate(robot)
        step.data["problems"] = [vars(p) for p in problems]
        return (FAILED, "; ".join(str(p) for p in problems)) if problems else (PASSED, f"contrato {contract.CONTRACT_VERSION}")

    def do_compile(step):
        _, compiled = mt5_tools.compile_robot(config, robot, run=run)
        step.data["compile"] = compiled.to_dict()
        if not compiled.ok:
            messages = "; ".join(str(m) for m in compiled.messages[:5])
            return FAILED, compiled.summary + (f" {messages}" if messages else "")
        record_compiled(db, robot, compiled)
        if robot.stage == catalog.IMPORTED:
            robot.stage = catalog.COMPILED
        return (WARNED if compiled.warnings else PASSED), compiled.summary

    def do_backtest(step):
        record = backtest.run_mql5(db, config, robot, symbol, timeframe, start, end, preset, run=run,
                                   is_running=is_running)
        m = record["metrics"]
        step.data["run_id"] = record["run_id"]
        step.data["metrics"] = {k: v for k, v in m.items() if not isinstance(v, (dict, list))}
        detail = f"{m['trades']} operações, profit factor {m['profit_factor']:.2f}".replace(".", ",")
        return (PASSED, detail) if m["trades"] else (FAILED, "nenhuma operação no período: confira o ativo, o período "
                                                             "e se o robô tem sinal")

    def do_battery(step):
        result.report = battery.run_battery(db, config, robot, robots, preset)
        failed = [i.name for i in result.report.items if i.status == battery.FAIL]
        warned = sum(1 for i in result.report.items if i.status == battery.WARN)
        if failed:
            return FAILED, "reprovou em: " + "; ".join(failed)
        return (WARNED if warned else PASSED), f"aprovada ({warned} aviso(s))"

    for step_id, action in (("contract", do_contract), ("compile", do_compile), ("backtest", do_backtest),
                            ("battery", do_battery)):
        if not execute(steps[step_id], action):
            break
    return result


def write_junit(result: CheckResult, path) -> None:
    """Relatório JUnit XML: um caso por etapa e um por item da bateria (para o CI mostrar o que falhou)."""
    suite = ET.Element("testsuite", name="mt5x.robots.check", timestamp=time.strftime("%Y-%m-%dT%H:%M:%S"))
    cases = []
    for step in result.steps:
        case = ET.SubElement(suite, "testcase", classname=f"{result.robot_key}.etapas", name=step.name,
                             time=f"{step.seconds:.2f}")
        cases.append((case, step.status, step.detail))
    for item in (result.report.items if result.report else []):
        case = ET.SubElement(suite, "testcase", classname=f"{result.robot_key}.bateria.{item.group}", name=item.name,
                             time="0.00")
        status = {battery.FAIL: FAILED, battery.NA: SKIPPED}.get(item.status, PASSED)
        cases.append((case, status, item.detail + (f" | {item.fix}" if item.fix else "")))
    for case, status, detail in cases:
        if status == FAILED:
            ET.SubElement(case, "failure", message=detail[:300]).text = detail
        elif status == SKIPPED:
            ET.SubElement(case, "skipped", message=detail or SKIPPED)
        elif detail:
            ET.SubElement(case, "system-out").text = detail
    suite.set("tests", str(len(cases)))
    suite.set("failures", str(sum(1 for _, status, _ in cases if status == FAILED)))
    suite.set("skipped", str(sum(1 for _, status, _ in cases if status == SKIPPED)))
    suite.set("errors", "0")
    ET.ElementTree(suite).write(str(path), encoding="utf-8", xml_declaration=True)
