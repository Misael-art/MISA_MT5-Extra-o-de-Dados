"""T19.52a: política condicional de estudo sobre o livro (seleção por contexto, posição única, piso de PF)."""
import numpy as np
import pandas as pd
import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.presets import conditional_policy as cp
from mt5_extracao.trade_analytics import ledger


@pytest.fixture
def db(tmp_path):
    database = DatabaseManager(db_path=str(tmp_path / "policy.db"))
    yield database
    database.close()


def _trades(days, hour, pnl_of, minute=0):
    """Uma operação de 30 min por dia, na hora dada; pnl_of(i) decide o resultado."""
    rows = []
    for i, day in enumerate(pd.date_range("2026-01-05", periods=days, freq="D")):
        start = day + pd.Timedelta(hours=hour, minutes=minute)
        rows.append({"direction": 1, "entry_time": start, "exit_time": start + pd.Timedelta(minutes=30),
                     "entry_price": 100.0, "exit_price": 101.0, "stop_price": 99.0, "target_price": None,
                     "volume": 1.0, "pnl": float(pnl_of(i)), "r_multiple": 1.0, "exit_reason": "alvo"})
    return rows


def _save(db, run_id, robot, rows):
    ledger.save_trades(db, run_id, rows, robot_key=robot, symbol="X", timeframe="M15", source="python")


def _context_edge(db, days=100):
    """A ganha de manhã e perde à tarde; B o contrário. Cada um, sozinho, fica perto de zero."""
    win, lose = (lambda i: 3.0 if i % 4 else -1.0), (lambda i: -2.0 if i % 4 else 1.0)
    _save(db, "a", "python:a", _trades(days, 9, win) + _trades(days, 14, lose))
    _save(db, "b", "python:b", _trades(days, 9, lose, minute=45) + _trades(days, 14, win, minute=45))


def test_policy_routes_each_context_to_its_setup_and_passes_the_floor(db):
    _context_edge(db)
    report = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20)
    rules = report["policy"]["rules"]
    assert [item["setup"] for item in rules["08h"]] == ["python:a"]
    assert [item["setup"] for item in rules["12h"]] == ["python:b"]
    for name in ("validation", "holdout"):
        part = report["segments"][name]
        assert part["policy"]["profit_factor"] > 2 and part["blocked_no_rule"] > 0
        assert part["policy"]["net"] > part["static_best"]["net"]
        assert part["policy"]["net"] > part["accept_all"]["net"]
    assert report["status"] == "elegível" and report["reasons"] == []
    assert report["policy"]["activation_authorized"] is False and report["study_only"]
    again = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20)
    assert again["policy"]["policy_id"] == report["policy"]["policy_id"]             # determinística


def test_noise_is_rejected_and_the_floor_is_not_a_promise(db):
    rng = np.random.default_rng(3)
    for name in ("a", "b", "c"):
        draws = rng.normal(0, 1, 400)
        _save(db, name, f"python:{name}", _trades(100, 9, lambda i: draws[i]) + _trades(100, 14, lambda i: draws[100 + i]))
    report = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20)
    assert report["status"] in ("reprovada", "sem candidato elegível") and report["reasons"]
    # o oráculo vê o próprio trecho e parece ótimo: fica rotulado e fora do parecer
    assert "oracle_not_executable" in report["segments"]["holdout"]


def test_rules_are_frozen_in_the_search_segment(db):
    """Um contexto que só fica bom DEPOIS da procura não vira regra."""
    late_only = lambda i: (3.0 if i % 5 else -1.0) if i >= 70 else -1.0
    _save(db, "a", "python:a", _trades(100, 9, late_only))
    _save(db, "b", "python:b", _trades(100, 14, lambda i: -1.0 if i % 3 else 0.5))
    report = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20)
    assert report["policy"]["rules"] == {} and report["status"] == "sem candidato elegível"
    assert report["segments"]["holdout"]["policy"]["n"] == 0                          # R00: bloqueia tudo
    assert report["segments"]["holdout"]["oracle_not_executable"]["net"] > 0


def test_single_position_and_one_run_per_setup(db):
    _context_edge(db)
    _save(db, "a2", "python:a", _trades(100, 9, lambda i: 3.0))                      # repetição do mesmo robô
    report = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20)
    assert report["policy"]["runs"] == ["a", "b"]                                    # o run repetido não conta
    with pytest.raises(ValueError, match="um run por robô"):
        cp.study(db, None, "X", "M15", runs=["a", "a2", "b"], dims=("period",))
    overlap = _trades(100, 9, lambda i: 3.0 if i % 4 else -1.0, minute=10)           # entra com a posição de A aberta
    _save(db, "c", "python:c", overlap)
    report = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20, top=3)
    assert report["segments"]["holdout"]["blocked_position_open"] > 0


def test_invalid_requests_are_refused(db):
    _save(db, "a", "python:a", _trades(50, 9, lambda i: 1.0))
    with pytest.raises(ValueError, match="dois setups"):
        cp.study(db, None, "X", "M15")
    _save(db, "b", "python:b", _trades(50, 14, lambda i: 1.0))
    with pytest.raises(ValueError, match="piso"):
        cp.study(db, None, "X", "M15", min_profit_factor=0.5)
    with pytest.raises(ValueError, match="frações"):
        cp.study(db, None, "X", "M15", fractions=(0.5, 0.5, 0.5))
    with pytest.raises(ValueError, match="Dimensão"):
        cp.study(db, None, "X", "M15", dims=("lua",))


def test_cli_router(db, tmp_path, capsys):
    import json
    from mt5_extracao import cli
    _context_edge(db)
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {db.db_path}\n[TRADE_ANALYTICS]\nmin_trades_cell = 15\n",
                   encoding="utf-8")
    code = cli.main(["--config", str(cfg), "--json", "trades", "router", "--symbol", "X", "--timeframe", "M15"])
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert code == cli.EXIT_OK and out["data"]["router"]["status"] in ("elegível", "reprovada", "sem candidato elegível")
    assert out["data"]["router"]["min_profit_factor"] == 2.0


def _record(db, run_id, robot, currency="BRL", manifest="m" * 64, digest="d" * 64, context=None):
    """Só os metadados declarados que o estudo lê (o produtor real também vincula o manifesto)."""
    import json
    from sqlalchemy import text
    from mt5_extracao.robots import store
    metrics = {"currency": currency, "execution_identity": {"digest": digest, "code_sha256": "c" * 64}}
    if manifest:
        metrics["data_manifest_id"] = manifest
    if context is not None:
        metrics["comparison_context"] = context
    with db.engine.begin() as conn:
        conn.execute(text(store._CREATE_RUNS))
        conn.execute(text("DELETE FROM _robot_runs WHERE run_id = :r"), {"r": run_id})
        conn.execute(text("INSERT INTO _robot_runs (run_id, robot_key, source, symbol, timeframe, metrics_json, "
                          "created_at) VALUES (:r, :k, 'backtest', 'X', 'M15', :m, '2026-11-18T00:00:00+00:00')"),
                     {"r": run_id, "k": robot, "m": json.dumps(metrics)})


def test_evaluation_identity_is_separate_from_policy_and_freezes_cuts(db):
    """Risco T19.52: hash da política não incluía frações, piso, datas de corte nem a escolha de run."""
    _context_edge(db)
    base = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20)
    evaluation = base["evaluation"]
    assert evaluation["policy_id"] == base["policy"]["policy_id"]
    assert len(evaluation["segmentation"]["cuts"]) == 2 and not evaluation["segmentation"]["cuts_frozen_by_caller"]
    assert evaluation["run_selection"] == {"mode": "lowest_run_id", "chosen": {"python:a": "a", "python:b": "b"},
                                           "ignored": {}}
    # Mesma política, piso diferente: avaliação diferente, política idêntica.
    stricter = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20, min_profit_factor=2.5)
    assert stricter["policy"]["policy_id"] == base["policy"]["policy_id"]
    assert stricter["evaluation"]["evaluation_id"] != evaluation["evaluation_id"]
    # Cortes congelados reproduzem a mesma avaliação, mesmo com operações novas depois.
    frozen = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20,
                      cuts=evaluation["segmentation"]["cuts"])
    assert frozen["evaluation"]["segmentation"]["cuts"] == evaluation["segmentation"]["cuts"]
    assert frozen["segments"]["holdout"]["policy"] == base["segments"]["holdout"]["policy"]
    assert frozen["evaluation"]["evaluation_id"] != evaluation["evaluation_id"]   # origem dos cortes é declarada
    with pytest.raises(ValueError, match="ordem"):
        cp.study(db, None, "X", "M15", dims=("period",), cuts=list(reversed(evaluation["segmentation"]["cuts"])))


def test_implicit_choice_among_repeated_runs_blocks_promotion(db):
    _context_edge(db)
    _save(db, "a2", "python:a", _trades(100, 9, lambda i: 3.0))
    report = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20)
    assert report["evaluation"]["run_selection"]["ignored"] == {"python:a": ["a2"]}
    assert any("menor identificador" in b for b in report["promotion_blockers"])
    explicit = cp.study(db, None, "X", "M15", runs=["a", "b"], dims=("period",), min_rule_trades=20)
    assert explicit["evaluation"]["run_selection"]["mode"] == "explicit"
    assert not any("menor identificador" in b for b in explicit["promotion_blockers"])


def test_undeclared_provenance_never_promotes_an_eligible_study(db):
    _context_edge(db)
    report = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20)
    assert report["status"] == "elegível"                       # parecer estatístico preservado
    assert report["comparability"] == "não comprovada" and report["promotion_blockers"]
    _record(db, "a", "python:a", manifest=None)
    _record(db, "b", "python:b", manifest=None)
    declared = cp.study(db, None, "X", "M15", dims=("period",), min_rule_trades=20)
    assert declared["evaluation"]["runs"]["a"]["currency"] == "BRL"
    assert any("Manifesto de dados ausente" in b for b in declared["promotion_blockers"])
    assert not any("Moeda" in b for b in declared["promotion_blockers"])
    assert declared["evaluation"]["evaluation_id"] != report["evaluation"]["evaluation_id"]


def _lab(windows):
    rows = []
    for run_id, (start, end) in windows.items():
        for moment in pd.date_range(start, end, periods=3):
            rows.append({"run_id": run_id, "entry_time": moment, "exit_time": moment + pd.Timedelta(minutes=30)})
    return pd.DataFrame(rows)


def _declared(currency="BRL", manifest="m" * 64, digest="d" * 64):
    return {"currency": currency, "data_manifest_id": manifest, "execution_digest": digest}


def test_comparability_requires_same_currency_manifest_execution_and_window():
    lab = _lab({"a": ("2026-01-01", "2026-04-01"), "b": ("2026-01-01", "2026-04-01")})
    cuts = (pd.Timestamp("2026-02-15"), pd.Timestamp("2026-03-15"))
    proven = cp._comparability(lab, {"a": _economic(), "b": _economic()}, cuts)
    assert proven["status"] == "comprovada" and proven["currency"] == "BRL" and proven["blockers"] == []
    # Sem o recibo econômico (moeda, manifesto e execução declarados apenas) já não é comprovada.
    assert cp._comparability(lab, {"a": _declared(), "b": _declared()}, cuts)["status"] == "não comprovada"
    with pytest.raises(ValueError, match="moedas diferentes"):
        cp._comparability(lab, {"a": _declared(), "b": _declared(currency="USD")}, cuts)
    other = cp._comparability(lab, {"a": _declared(), "b": _declared(manifest="n" * 64)}, cuts)
    assert any("manifestos de dados diferentes" in b for b in other["blockers"])
    unknown = cp._comparability(lab, {"a": _declared(), "b": _declared(currency=None, digest=None)}, cuts)
    assert {"Moeda", "Identidade"} <= {b.split()[0] for b in unknown["blockers"]}
    short = cp._comparability(_lab({"a": ("2026-01-01", "2026-04-01"), "b": ("2026-01-01", "2026-03-01")}),
                              {"a": _declared(), "b": _declared()}, cuts)
    assert short["status"] == "não comprovada"
    assert any("depois do início da reserva: b" in b for b in short["blockers"])


def _receipt(**changes):
    """Recibo comparison_context completo, como o produtor do backtest Python grava (T19.52)."""
    from dataclasses import asdict
    from mt5_extracao.strategies.backtester import BacktestConfig
    settings = asdict(BacktestConfig())
    settings.update(changes.pop("settings", {}))
    context = {"schema_version": 1, "engine": "python_ohlcv_stop_first", "settings": settings, "trade_from": None,
               "source": "mt5", "clock": "broker_time", "instrument_spec": {"known": True, "point": 0.01},
               "currency": "BRL", "currency_source": "user_declared_account_currency",
               "external_cashflows": False, "code_sha256": "c" * 64}
    context.update(changes)
    return context


def _economic(currency="BRL", **changes):
    return {**_declared(currency=currency), "comparison_context": _receipt(currency=currency, **changes)}


def test_same_manifest_does_not_prove_same_costs_or_risk():
    """Defeito da revisão de 19/11: custos/risco diferentes saíam como comparabilidade comprovada."""
    lab = _lab({"a": ("2026-01-01", "2026-04-01"), "b": ("2026-01-01", "2026-04-01")})
    cuts = (pd.Timestamp("2026-02-15"), pd.Timestamp("2026-03-15"))
    cheap = _economic(settings={"commission_per_lot": 0.0, "risk_per_trade": 0.001})
    costly = _economic(settings={"commission_per_lot": 7.0, "risk_per_trade": 0.1})
    result = cp._comparability(lab, {"a": cheap, "b": costly}, cuts)
    assert result["status"] == "não comprovada"
    assert any("capital/risco/custos" in b for b in result["blockers"])
    assert cp._comparability(lab, {"a": _economic(), "b": _economic()}, cuts)["status"] == "comprovada"


@pytest.mark.parametrize("change, expected", [
    ({"engine": "outro"}, "Run b: configuração de execução incompleta/incompatível"),
    ({"code_sha256": "e" * 64}, "Código do motor"),
    ({"settings": {"slippage_points": 3.0}}, "capital/risco/custos"),
    ({"settings": {"initial_equity": 5000.0}}, "capital/risco/custos"),
    ({"settings": {"compound": False}}, "capital/risco/custos"),
    ({"trade_from": "2026-02-01 00:00:00"}, "Início efetivo"), ({"source": "csv"}, "Origem"),
    ({"clock": "utc"}, "Relógio"), ({"instrument_spec": {"known": True, "point": 0.1}}, "Especificação")])
def test_every_material_dimension_blocks_a_single_account(change, expected):
    lab = _lab({"a": ("2026-01-01", "2026-04-01"), "b": ("2026-01-01", "2026-04-01")})
    cuts = (pd.Timestamp("2026-02-15"), pd.Timestamp("2026-03-15"))
    result = cp._comparability(lab, {"a": _economic(), "b": _economic(**change)}, cuts)
    assert result["status"] == "não comprovada" and any(expected in b for b in result["blockers"])


@pytest.mark.parametrize("receipt", [None, {}, {"schema_version": 1}, "legado",
                                     "sem_custos", "fluxo_externo", "spec_desconhecida", "moeda_divergente"])
def test_missing_or_legacy_receipt_never_yields_proven(receipt):
    lab = _lab({"a": ("2026-01-01", "2026-04-01"), "b": ("2026-01-01", "2026-04-01")})
    cuts = (pd.Timestamp("2026-02-15"), pd.Timestamp("2026-03-15"))
    broken = _declared()
    if receipt == "legado":
        pass                                                # run antigo: sem comparison_context
    elif receipt == "sem_custos":
        context = _receipt()
        context["settings"].pop("commission_per_lot")
        broken["comparison_context"] = context
    elif receipt == "fluxo_externo":
        broken["comparison_context"] = _receipt(external_cashflows=None)
    elif receipt == "spec_desconhecida":
        broken["comparison_context"] = _receipt(instrument_spec={"known": False})
    elif receipt == "moeda_divergente":
        broken["comparison_context"] = _receipt(currency="USD")
    else:
        broken["comparison_context"] = receipt
    result = cp._comparability(lab, {"a": _economic(), "b": broken}, cuts)
    assert result["status"] == "não comprovada" and any(b.startswith("Run b:") for b in result["blockers"])


def test_strategy_differences_are_intentional_not_blockers():
    lab = _lab({"a": ("2026-01-01", "2026-04-01"), "b": ("2026-01-01", "2026-04-01")})
    cuts = (pd.Timestamp("2026-02-15"), pd.Timestamp("2026-03-15"))
    a, b = _economic(), _economic()
    b["execution_digest"] = "f" * 64                    # parâmetros/regras do setup entram só no digest
    result = cp._comparability(lab, {"a": a, "b": b}, cuts)
    assert result["status"] == "comprovada"
    assert "Configuração de capital/risco/custos" in result["checked"]
    assert result["intentional"] == ["setup (robô)", "parâmetros e regras da estratégia"]


def test_study_propagates_economic_blockers_and_reopens_the_evaluation(db):
    _context_edge(db)
    # Sem snapshot real o manifesto fica não declarado (bloqueio próprio); o foco aqui é o contexto econômico.
    _record(db, "a", "python:a", manifest=None, context=_receipt(settings={"commission_per_lot": 0.0}))
    _record(db, "b", "python:b", manifest=None, context=_receipt(settings={"commission_per_lot": 7.0}))
    split = cp.study(db, None, "X", "M15", runs=["a", "b"], dims=("period",), min_rule_trades=20)
    assert split["status"] == "elegível" and split["comparability"] == "não comprovada"
    assert any("capital/risco/custos" in b for b in split["promotion_blockers"])
    detail = split["comparability_detail"]["blockers"]
    assert any("capital/risco/custos" in b for b in detail) and set(detail) <= set(split["promotion_blockers"])
    cuts = split["evaluation"]["segmentation"]["cuts"]
    _record(db, "b", "python:b", manifest=None, context=_receipt(settings={"commission_per_lot": 0.0}))
    same = cp.study(db, None, "X", "M15", runs=["a", "b"], dims=("period",), min_rule_trades=20, cuts=cuts)
    assert not any("capital/risco/custos" in b or b.startswith("Run ") for b in same["promotion_blockers"])
    assert same["comparability_detail"]["blockers"] == [
        "Manifesto de dados ausente em algum run; fonte e revisão das barras não comprovadas."]
    assert same["policy"]["policy_id"] == split["policy"]["policy_id"]          # política não muda
    assert same["evaluation"]["evaluation_id"] != split["evaluation"]["evaluation_id"]   # avaliação reaberta
