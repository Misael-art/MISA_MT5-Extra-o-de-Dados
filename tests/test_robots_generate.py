"""T10.10: estratégia declarada em Python e geração do sinal MQL5 correspondente, sem MT5."""
import configparser
import json
from pathlib import Path

import pytest

from mt5_extracao import cli
from mt5_extracao.robots import battery, catalog, contract, generate, mql, parity
from mt5_extracao.strategies import indicators as ind
from mt5_extracao.strategies.backtester import run_backtest
from mt5_extracao.strategies.declared import DeclaredStrategy, SignalSpec
from tests.market_data import trend

SPEC = {"key": "cruzamento", "name": "Cruzamento de médias",
        "indicators": {"rapida": {"kind": "sma", "period": 10}, "media_longa": {"kind": "ema", "period": 40},
                       "forca": {"kind": "rsi", "period": 14}},
        "long": [["cross_above", "rapida", "media_longa"], ["above", "forca", 50]],
        "short": [["cross_below", "rapida", "media_longa"], ["below", "close", "media_longa"]],
        "atr_period": 10, "stop_atr": 2.5, "target_r": 3.0}


def test_declared_strategy_gives_the_signals_the_declaration_says():
    df = trend(3000, step=0.0, noise=1.0)
    spec = SignalSpec.from_dict(SPEC)
    strategy = DeclaredStrategy(spec)
    signals = strategy.gerar_sinais(df)
    fast, slow, rsi = ind.sma(df["close"], 10), ind.ema(df["close"], 40), ind.rsi(df["close"], 14)
    expected_long = strategy.crossed_above(fast, slow) & (rsi > 50)
    expected_short = strategy.crossed_below(fast, slow) & (df["close"] < slow)
    assert signals["entry_long"].sum() > 5 and signals["entry_short"].sum() > 5
    assert (signals["entry_long"] == (expected_long & ~expected_short)).all()
    distance = 2.5 * ind.sma(ind.true_range(df), 10)             # ATR como o iATR do MT5: média simples
    assert ((df["close"] - signals["stop_long"] - distance).abs().dropna() < 1e-9).all()
    assert strategy.exit_rules().target_r == 3.0 and strategy.key == "cruzamento"
    assert strategy.default_params == {"rapida_period": 10, "media_longa_period": 40, "forca_period": 14,
                                       "atr_period": 10, "stop_atr": 2.5, "target_r": 3.0}
    assert run_backtest(df, strategy).metrics["trades"] >= 0     # roda no backtester do projeto
    slower = DeclaredStrategy(spec, rapida_period=20).gerar_sinais(df)
    assert not (slower["entry_long"] == signals["entry_long"]).all()
    assert SignalSpec.from_dict(spec.to_dict()).to_dict() == spec.to_dict()


@pytest.mark.parametrize("change, text", [
    ({"key": "Com Espaço"}, "\"key\" inválida"),
    ({"indicators": {}}, "pelo menos um indicador"),
    ({"indicators": {"x": {"kind": "macd", "period": 9}}}, "não suportado"),
    ({"indicators": {"x": {"kind": "sma", "period": 1}}}, "inteiro a partir de 2"),
    ({"indicators": {"close": {"kind": "sma", "period": 9}}}, "reservado"),
    ({"long": [["cross_above", "rapida", "sumiu"]]}, "não é um indicador declarado"),
    ({"long": [["maior_que", "rapida", "forca"]]}, "Condição inválida"),
    ({"long": [["above", 1, 2]]}, "dois números"),
    ({"long": [], "short": []}, "ao menos uma condição"),
    ({"stop_atr": 0}, "toda entrada precisa de stop"),
    ({"extra": 1}, "desconhecido"),
])
def test_declaration_errors_say_what_is_not_supported(change, text):
    with pytest.raises(ValueError, match=text):
        SignalSpec.from_dict({**SPEC, **change})


def test_generated_sections_use_closed_bars_and_never_send_orders():
    parts = generate.sections(SignalSpec.from_dict(SPEC))
    assert "input int    InpRapidaPeriod = 10;" in parts["PARAMETROS"] and "InpMediaLongaPeriod = 40" in parts["PARAMETROS"]
    assert "g_rapida = iMA(_Symbol, PERIOD_CURRENT, InpRapidaPeriod, 0, MODE_SMA, PRICE_CLOSE);" in parts["INIT"]
    assert "MODE_EMA" in parts["INIT"] and "g_forca = iRSI(_Symbol, PERIOD_CURRENT, InpForcaPeriod, PRICE_CLOSE);" in parts["INIT"]
    signal = parts["SINAL"]
    assert "CopyBuffer(g_rapida, 0, 1, 2, v_rapida)" in signal and "CopyClose(_Symbol, PERIOD_CURRENT, 1, 2, v_close)" in signal
    assert ("bool buy = (v_rapida[1] > v_media_longa[1] && v_rapida[0] <= v_media_longa[0]) && (v_forca[1] > 50.0);"
            in signal)
    assert "bool sell = (v_rapida[1] < v_media_longa[1] && v_rapida[0] >= v_media_longa[0]) && " \
           "(v_close[1] < v_media_longa[1]);" in signal
    assert ", 0, 0, " not in signal                                  # nunca lê a barra em formação (deslocamento 0)
    assert not mql.ORDER_CALL.search("\n".join(parts.values()))
    only_long = generate.sections(SignalSpec.from_dict({**SPEC, "short": []}))
    assert "bool sell = false;" in only_long["SINAL"]


def test_create_builds_a_valid_robot_from_the_declaration(tmp_path):
    spec = SignalSpec.from_dict(SPEC)
    files = generate.create(spec, "Gerado", tmp_path, magic=660001)
    robot = catalog.robot_from_file(files[0])
    assert contract.validate(robot) == [] and robot.manifest["parity_with"] == generate.DECLARED
    assert robot.manifest["tunable"] == ["InpRapidaPeriod", "InpMediaLongaPeriod", "InpForcaPeriod", "InpStopAtrMult",
                                         "InpTakeProfitR"]
    defaults = {i.name: i.default for i in robot.inputs}
    assert (defaults["InpAtrPeriod"], defaults["InpStopAtrMult"], defaults["InpTakeProfitR"]) == ("10", "2.5", "3")
    assert defaults["InpRapidaPeriod"] == "10" and robot.presets["Gerado_Conservador"]["InpStopAtrMult"] == "2.5"
    source = mql.read_text(files[0])
    assert "//--- MT5X:SINAL:INICIO" in source and "bool buy = " in source and "{{" not in source
    assert generate.load_spec(robot).to_dict() == spec.to_dict() and files[-1].name == "Gerado.signal.json"
    items = {i.id: i for i in battery.static_items(robot, None, [robot])}
    assert items["orders"].status == battery.OK and items["contract"].status == battery.OK
    plain = catalog.robot_from_file(Path(__file__).parent / "robots_library" / "Experts" / "Demo" / "DemoGuard.mq5")
    with pytest.raises(FileNotFoundError, match="não foi gerado"):
        generate.load_spec(plain)


def test_parity_ignores_trades_closed_during_the_warm_up():
    python = [["2025-01-03 10:00:00", 5.0, 1.0], ["2025-01-06 10:00:00", -5.0, 1.0]]
    mql5 = [["2025-01-01 09:10:00", 9.0, 1.0], ["2025-01-03 10:20:00", 5.0, 1.0], ["2025-01-06 10:40:00", -5.0, 1.0]]
    assert parity.compare(python, mql5).matched_share == pytest.approx(2 / 3)
    report = parity.compare(python, mql5, warmup_until="2025-01-02 00:00:00")
    assert report.matched_share == 1.0 and report.excluded_warmup == 1 and report.mql5_trades == 2 and report.ok


def test_cli_generate(tmp_path, capsys):
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {tmp_path / 'db.sqlite'}\n", encoding="utf-8")
    spec_file = tmp_path / "decl.json"
    spec_file.write_text(json.dumps(SPEC), encoding="utf-8")
    args = ["--config", str(cfg)]
    argv = args + ["robots", "generate", str(spec_file), "--name", "Gerado", "--dir", str(tmp_path / "robos")]
    assert cli.main(argv) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "Robô Gerado gerado" in out and "mt5x robots parity gerado" in out
    assert cli.main(args + ["robots", "validate", "gerado"]) == cli.EXIT_OK
    capsys.readouterr()
    assert cli.main(args + ["robots", "generate", str(spec_file), "--dir", str(tmp_path)]) == cli.EXIT_USAGE
    spec_file.write_text(json.dumps({**SPEC, "indicators": {"x": {"kind": "macd", "period": 9}}}), encoding="utf-8")
    assert cli.main(args + ["robots", "generate", str(spec_file), "--name", "Outro", "--dir", str(tmp_path / "robos")]) \
        == cli.EXIT_USAGE
    assert "não suportado" in capsys.readouterr().err and not (tmp_path / "robos" / "Experts" / "Outro").exists()
