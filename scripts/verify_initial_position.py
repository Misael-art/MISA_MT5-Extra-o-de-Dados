#!/usr/bin/env python3
"""QA E-13 no Strategy Tester DEMO pelo executor existente; nunca PLAY.

Exemplo: .venv/bin/python scripts/verify_initial_position.py --framework ../hadouken
  --out ../../state/qa-tmp/operational-pillar/e13-native-s09
  --from-date 2026-10-12 --to-date 2026-10-16

Usa perfil/DB novos de QA e os caches nativos existentes. Não aprova um perfil
de composição, não altera fingerprints permitidos e não certifica conta ao vivo.
"""
import argparse
from dataclasses import replace
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def qa_public_fields(robot):
    """Reutiliza a lista pública revisada; inputs futuros não entram no recibo."""
    from mt5_extracao.composition import native
    from mt5_extracao.robots import native_inputs
    if robot.key != native.ROBOT_KEY or robot.version != native.PANEL_ROBOT_VERSION or {
        item.name: item.type for item in robot.inputs
    } != native.PANEL_INPUT_TYPES:
        raise ValueError("QA E-13 requer a interface pública S09 1.90 revisada; nenhum input novo é coletado.")
    fields = {name: "int" if kind in ("int", "long") else "number" if kind == "double"
              else "bool" if kind == "bool" else "token" for name, kind in native.PANEL_INPUT_TYPES.items()}
    return native_inputs.fields_for(robot, fields)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--framework", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    args = parser.parse_args(argv)
    begin, end = (datetime.strptime(x, "%Y-%m-%d") for x in (args.from_date, args.to_date))
    if not 0 < (end - begin).days <= 7:
        raise ValueError("QA requer janela histórica explícita de 1 a 7 dias, sem ampliar a coleta.")
    if args.out.exists():
        raise FileExistsError("Destino QA já existe; evidências anteriores preservadas.")
    from mt5_extracao.bootstrap import paths
    from mt5_extracao.bootstrap.config_builder import load_config
    from mt5_extracao.database_manager import DatabaseManager
    from mt5_extracao.robots import budget, catalog, controls, sessions
    from mt5_extracao.presets import execution, partial_events
    config = load_config(args.config or paths.config_path())
    config["STRATEGY"]["initial_equity"] = "100000"   # capital virtual do Tester
    robot = catalog.robot_from_file(args.framework / "Experts/HADOUKEN/Estrategias/HDK_S09_Referencia.mq5")
    fields = qa_public_fields(robot)
    fingerprint = catalog.fingerprint(robot)
    values = controls.effective_values(robot)
    values.update(InpLotMode="0", InpFixedLot="0.08", InpRiskPct="0.25", InpMagic="909202613",
                  InpPresetId="E13_QA_INITIAL", InpPartialStages="2", InpPartialTrigger1="0.5",
                  InpPartialQuantity1="0.25", InpPartialMode1="0", InpPartialBase1="0",
                  InpPartialTrigger2="1.0", InpPartialQuantity2="0.5", InpPartialMode2="0",
                  InpPartialBase2="1", InpPartialBreakeven="true")
    values["InpConfigHash"] = hashlib.sha256(json.dumps(
        dict(fingerprint=fingerprint, parameters={k:v for k,v in values.items() if k!="InpConfigHash"}),
        sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    robot = replace(robot, presets={"E13_QA_INITIAL": values})
    args.out.mkdir(parents=True, mode=0o700)
    db = DatabaseManager(db_path=str(args.out / "qa.sqlite"))
    prior = budget.Limits.from_config(config)
    limits = replace(prior, max_jobs=1, max_seconds=min(prior.max_seconds or 600, 600),
                     hard_ram_pct=min(prior.hard_ram_pct or 85, 85), min_free_mb=max(prior.min_free_mb, 1024))
    controller = budget.Budget(db, config, limits=limits)
    report = dict(scope="E-13; native Strategy Tester DEMO; no PLAY", source_fingerprint=fingerprint,
                  parameters=values, initial_capital=100000, model="0: generated ticks from M1, not real ticks",
                  window=[args.from_date,args.to_date], operational_acceptance=False, live_broker_exercised=False)
    failed = True
    try:
        jobs = controller.prepare([(robot, "EURUSD", "M5")], start=begin, end=end, preset="E13_QA_INITIAL",
                                  execution=dict(scope="E13_INITIAL_POSITION", parameters=values, tester_model="0"))
        control = controller.admit(jobs[0]["key"])
        options = dict(db=db, robot=robot, symbol="EURUSD", preset="E13_QA_INITIAL", params=values,
                       model="0", visual=False, control=control)
        session = sessions.Session(db, config, robot, "EURUSD", "M5", preset="E13_QA_INITIAL", mode="backtest",
                                   start=begin, end=end, native_options=options, input_receipt_fields=fields,
                                   expected_native_inputs=values, visible=False)
        report.update(session_id=session.id, directory=str(session.directory))
        session.run()
        if session.exception is not None:
            raise session.exception
        if session.state != "completed" or session.record is None or session.native_input_receipt is None:
            raise RuntimeError(session.error or "Tester/inputs sem confirmação; ensaio não qualificado.")
        if catalog.fingerprint(robot) != fingerprint:
            raise ValueError("Fonte mudou durante o ensaio; não atribuir resultado ao fingerprint anterior.")
        record = session.record
        report.update(run_id=record["run_id"], input_receipt=session.native_input_receipt,
                      cash_reconciled=record["metrics"].get("cash_reconciled"))
        if report["cash_reconciled"] is not True:
            raise RuntimeError("Livro e resultado financeiro não reconciliados; ensaio não qualificado.")
        signals = execution.native_signals(session.directory)
        sizing = execution.native_sizing(session.directory, signals, values)
        rows = execution._ledger_rows(db, record, signals)
        plan = dict(mode="staged_fraction", params=dict(count=2,trigger1_r=.5,quantity1=.25,base1=0,
                    trigger2_r=1.,quantity2=.5,base2=1,breakeven_after_confirmation=True))
        partials, deals = partial_events.read_events(session.directory, plan, signals, sizing, rows)
        counts = {str(stage): sum(p["state"]=="confirmed" and p["stage"]==stage for p in partials) for stage in (1,2)}
        report.update(state="observed" if all(counts.values()) else "not_exercised_in_full",
                      run_id=record["run_id"], closed_positions=len(rows), deals=len(deals), confirmed_stages=counts,
                      input_receipt=session.native_input_receipt, cash_reconciled=record["metrics"].get("cash_reconciled"),
                      events=partials, entries=sum(s["opened"] for s in signals),
                      limitation="Generated tick replay; no live partial fill, disconnection, multi-terminal or physical Windows acceptance")
        failed = False
    except Exception as error:
        report.update(state="failed", error=str(error))
        raise
    finally:
        controller.finish(failed=failed)
        (args.out / "receipt.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str)+"\n")
        db.close()
        print(json.dumps({k:v for k,v in report.items() if k not in ("events","parameters","input_receipt")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
