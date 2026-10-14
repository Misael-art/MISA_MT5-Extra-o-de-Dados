"""T12.2: carteira da conta, somente leitura (posições e ordens de todos os robôs e manuais, com o risco até o stop)."""
import configparser
import inspect
import json
from types import SimpleNamespace as NS

import pytest

from mt5_extracao import cli, mt5_connector
from mt5_extracao.bootstrap import paths
from mt5_extracao.robots import mt5_tools, portfolio

EUR = NS(trade_tick_size=0.00001, trade_tick_value=1.0, point=0.00001,
         currency_base="EUR", currency_profit="USD")
WIN = NS(trade_tick_size=5.0, trade_tick_value=1.0, point=1.0,
         currency_base="BRL", currency_profit="BRL")


class FakeMT5:
    def __init__(self, positions=(), orders=(), margin_mode=2, balance=100_000.0):
        self.positions, self.orders = list(positions), list(orders)
        self.account = NS(login=42, server="Fake-Demo", currency="USD", trade_mode=0, margin_mode=margin_mode,
                          balance=balance, equity=balance, margin=0.0, margin_free=balance)
        self.sent = []

    def account_info(self):
        return self.account

    def symbol_info(self, symbol):
        return {"EURUSD": EUR, "WIN$N": WIN}.get(symbol)

    def positions_get(self):
        return tuple(self.positions)

    def orders_get(self):
        return tuple(self.orders)

    def order_send(self, *args, **kwargs):                       # se alguém chamar, o teste falha
        self.sent.append(args)


def position(ticket, symbol, magic, kind, volume, price, sl, profit=0.0, tp=0.0):
    return NS(ticket=ticket, symbol=symbol, magic=magic, type=kind, volume=volume, price_open=price, sl=sl, tp=tp,
              profit=profit, swap=0.0)


def order(ticket, symbol, magic, kind, volume, price, sl):
    return NS(ticket=ticket, symbol=symbol, magic=magic, type=kind, volume_current=volume, price_open=price, sl=sl)


ROBOTS = [NS(kind="mql5", has_source=True, name="HDK_EmaAtr", manifest={}, presets={},
             inputs=[NS(name="InpMagicNumber", default="20251212")])]


def test_snapshot_names_owners_and_adds_the_risk_to_the_stop():
    mt5 = FakeMT5(
        positions=[position(1, "EURUSD", 20251212, 0, 2.0, 1.10000, 1.09900, profit=-50.0),      # 100 pontos x 2 lotes
                   position(2, "EURUSD", 0, 1, 1.0, 1.10000, 1.10250, profit=30.0),             # manual, vendido
                   position(3, "EURUSD", 777, 0, 1.0, 1.10000, 1.10100)],                       # stop já no lucro
        orders=[order(9, "EURUSD", 20251212, 2, 1.0, 1.09000, 1.08900)])                        # buy limit
    data = portfolio.snapshot(mt5, ROBOTS, max_risk_pct=1.0)
    risks = {p["ticket"]: p["risk_money"] for p in data["positions"]}
    assert risks[1] == pytest.approx(200.0) and risks[2] == pytest.approx(250.0) and risks[3] == 0.0
    owners = {p["ticket"]: p["owner"] for p in data["positions"]}
    assert owners == {1: "HDK_EmaAtr", 2: portfolio.MANUAL, 3: "robô não catalogado #777"}
    t = data["totals"]
    assert t["risk_open"] == pytest.approx(450.0) and t["risk_pending"] == pytest.approx(100.0)
    assert t["risk_pct"] == pytest.approx(0.55) and t["risk_complete"] and t["floating"] == pytest.approx(-20.0)
    assert t["positions"] == 3 and t["orders"] == 1 and t["manual"] == 1 and t["without_stop"] == 0
    assert data["state"] == portfolio.NORMAL and data["hedging"] and data["demo"]
    ema = next(g for g in data["by_owner"] if g["owner"] == "HDK_EmaAtr")
    assert ema["positions"] == 1 and ema["orders"] == 1 and ema["risk_money"] == pytest.approx(300.0)
    assert data["shared_symbols"] == {"EURUSD": sorted(["HDK_EmaAtr", portfolio.MANUAL, "robô não catalogado #777"])}
    eur = data["exposures"][0]
    assert eur["symbol"] == "EURUSD" and eur["currency"] == "USD"
    assert eur["buy_positions"] == 2 and eur["sell_positions"] == 1 and eur["buy_orders"] == 1
    assert eur["buy_volume"] == pytest.approx(4.0) and eur["sell_volume"] == pytest.approx(1.0)
    assert eur["buy_risk_money"] + eur["sell_risk_money"] == pytest.approx(
        t["risk_open"] + t["risk_pending"])
    currencies = {group["group"]: group for group in data["groups"]}
    assert currencies["EUR"]["buy_risk_money"] + currencies["EUR"]["sell_risk_money"] == pytest.approx(550.0)
    assert currencies["USD"]["currency"] == "USD"
    assert currencies["USD"]["buy_risk_money"] + currencies["USD"]["sell_risk_money"] == pytest.approx(550.0)
    assert portfolio.snapshot(mt5, ROBOTS, max_risk_pct=0.5)["state"] == portfolio.ALERT


def test_fixed_exposure_groups_override_currency_groups_and_can_overlap():
    mt5 = FakeMT5(positions=[position(1, "EURUSD", 0, 0, 1.0, 1.1, 1.099),
                             position(2, "WIN$N", 0, 1, 2.0, 130000.0, 130100.0)])
    data = portfolio.snapshot(mt5, exposure_groups="EUR*;EURUSD,WIN*")
    groups = {item["group"]: item for item in data["groups"]}
    assert groups["Grupo 1"]["buy_risk_money"] == pytest.approx(100.0)
    assert groups["Grupo 2"]["buy_risk_money"] == pytest.approx(100.0)
    assert groups["Grupo 2"]["sell_risk_money"] == pytest.approx(40.0)
    assert all(item["currency"] == "USD" for item in groups.values())
    # Um símbolo em dois grupos aparece em ambos; os grupos não devem ser somados entre si.
    assert sum(item["buy_risk_money"] + item["sell_risk_money"] for item in groups.values()) > (
        data["totals"]["risk_open"] + data["totals"]["risk_pending"])


def test_position_without_stop_makes_the_account_risk_incomplete():
    mt5 = FakeMT5(positions=[position(1, "EURUSD", 20251212, 0, 1.0, 1.1, 1.099), position(2, "EURUSD", 0, 0, 1.0, 1.1, 0.0)])
    data = portfolio.snapshot(mt5, ROBOTS)
    assert data["state"] == portfolio.ALERT and "sem stop" in data["reason"]
    assert not data["totals"]["risk_complete"] and data["totals"]["without_stop"] == 1
    assert data["positions"][1]["risk_money"] is None and data["positions"][1]["has_stop"] is False
    unknown = portfolio.snapshot(FakeMT5(positions=[position(1, "XPTO", 5, 0, 1.0, 10.0, 9.0)]))   # ativo sem dados
    assert unknown["positions"][0]["risk_money"] is None and unknown["state"] == portfolio.ALERT


def test_netting_account_and_empty_account():
    mt5 = FakeMT5(positions=[position(1, "WIN$N", 20251212, 0, 3.0, 130000.0, 129900.0)], margin_mode=0)
    data = portfolio.snapshot(mt5, ROBOTS)
    assert not data["hedging"] and data["positions"][0]["risk_money"] == pytest.approx(60.0)   # 100 pts / 5 x 1 x 3
    empty = portfolio.snapshot(FakeMT5())
    assert empty["state"] == portfolio.NORMAL and empty["totals"]["risk_pct"] == 0.0 and empty["by_owner"] == []
    no_account = FakeMT5()
    no_account.account = None
    with pytest.raises(RuntimeError, match="mt5x doctor"):
        portfolio.snapshot(no_account)


def test_portfolio_never_sends_orders():
    mt5 = FakeMT5(positions=[position(1, "EURUSD", 0, 0, 1.0, 1.1, 0.0)])
    portfolio.snapshot(mt5, ROBOTS, max_risk_pct=0.01)
    assert mt5.sent == []
    assert "order_send" not in inspect.getsource(portfolio) and "order_check" not in inspect.getsource(portfolio)


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "is_windows", lambda: False)
    prefix = tmp_path / "wine"
    mt5 = prefix / "drive_c" / "Program Files" / "MetaTrader 5"
    mt5.mkdir(parents=True)
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"path": str(mt5), "wine_prefix": str(prefix)},
                      "ROBOTS": {"account_exposure_groups": "EURUSD"},
                      "DATABASE": {"type": "sqlite", "path": str(tmp_path / "db.sqlite")}})
    cfg = tmp_path / "config.ini"
    with open(cfg, "w", encoding="utf-8") as f:
        config.write(f)
    return NS(config=config, prefix=prefix, args=["--config", str(cfg)])


def test_guardian_snapshot_is_read_from_common_files(home, capsys):
    assert portfolio.read_guardian(home.config) is None                       # MT5 nunca aberto: sem Common\Files
    assert cli.main(home.args + ["portfolio", "--guardian"]) == cli.EXIT_FAIL
    assert "HDK_AccountGuardian" in capsys.readouterr().out
    folder = (home.prefix / "drive_c" / "users" / "u" / "AppData" / "Roaming" / "MetaQuotes" / "Terminal" / "Common"
              / "Files")
    folder.mkdir(parents=True)
    assert mt5_tools.kill_file(home.config).parent == folder and portfolio.read_guardian(home.config) is None
    (folder / "HDK_portfolio_42.json").write_text(json.dumps({
        "schema": 1, "login": 42, "updated": "2026.10.13 06:00:00", "state": "ALERTA", "reason": "1 posição sem stop",
        "balance": 1000.0, "equity": 990.0, "floating": -10.0, "risk_open": 20.0, "risk_pending": 5.0,
        "risk_complete": False, "positions": [{"ticket": 1}], "orders": []}), encoding="utf-8")
    assert portfolio.read_guardian(home.config, 42)["login"] == 42 and portfolio.read_guardian(home.config, 7) is None
    assert cli.main(home.args + ["portfolio", "--guardian"]) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "ALERTA (1 posição sem stop)" in out and "25,00 (incompleto" in out and "1 posição(ões)" in out


def test_cli_portfolio_reads_through_the_backend(home, monkeypatch, capsys):
    fake = FakeMT5(positions=[position(1, "EURUSD", 0, 0, 1.0, 1.10000, 1.09900, profit=12.5)])
    monkeypatch.setattr(cli.Context, "connector", lambda self: object())
    monkeypatch.setattr(mt5_connector, "mt5", fake)
    assert cli.main(home.args + ["portfolio"]) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "DEMO, hedging" in out and "risco 100,00" in out and portfolio.MANUAL in out and "Somente leitura" in out
    assert "Exposição por ativo" in out and "Exposição por grupo" in out and "Grupo 1" in out
    assert fake.sent == []
