"""T11.4: avisos em pop-up das ações do programa e das operações do robô, sem mostrar pop-up nenhum nos testes."""
import configparser
import datetime as dt
from types import SimpleNamespace

import pytest

from mt5_extracao import cli, notify
from mt5_extracao.robots import live


def config(on="true"):
    c = configparser.ConfigParser()
    c.read_dict({"APP": {"notifications": on}})
    return c


def test_send_builds_the_notify_send_command_and_respects_the_switch(monkeypatch):
    monkeypatch.setattr(notify, "_is_windows", lambda: False)
    monkeypatch.setattr(notify.shutil, "which", lambda name: "/usr/bin/notify-send")
    calls = []
    runner = lambda command, **kw: calls.append(command) or SimpleNamespace(returncode=0)
    assert notify.send(config(), "Extração concluída", "EURUSD M1: 2 ok", runner=runner) is True
    command = calls[0]
    assert command[:5] == ["notify-send", "-a", "MT5 Extração", "-u", "normal"] and command[-3:] == [
        "--", "Extração concluída", "EURUSD M1: 2 ok"]
    notify.send(config(), "PARAR TUDO", "x", notify.CRITICAL, runner=runner)
    assert calls[1][4] == "critical" and calls[1][6] == "0"                 # fica na tela até a pessoa fechar
    assert notify.send(config("false"), "a", "b", runner=runner) is False and len(calls) == 2
    assert notify.send(None, "a", "b", runner=runner) is False              # sem configuração: desligado
    assert notify.send(configparser.ConfigParser(), "a", "b", runner=runner) is False     # chave ausente: desligado
    notify.send(config(), "t" * 200, "x" * 1000, runner=runner)
    assert len(calls[2][-2]) == 80 and len(calls[2][-1]) == notify.MAX_TEXT


def test_send_never_raises(monkeypatch):
    monkeypatch.setattr(notify, "_is_windows", lambda: False)
    monkeypatch.setattr(notify.shutil, "which", lambda name: "/usr/bin/notify-send")

    def broken(command, **kw):
        raise OSError("sem D-Bus")
    assert notify.send(config(), "a", "b", runner=broken) is False
    assert notify.send(config(), "a", "b", runner=lambda c, **k: SimpleNamespace(returncode=1)) is False
    monkeypatch.setattr(notify.shutil, "which", lambda name: None)
    assert notify.send(config(), "a", "b") is False                         # sem ferramenta: só log
    assert notify.send(object(), "a", "b") is False                         # configuração estranha: não derruba


def test_windows_balloon_takes_the_text_from_the_environment_not_the_command_line(monkeypatch):
    monkeypatch.setattr(notify, "_is_windows", lambda: True)
    monkeypatch.setattr(notify.shutil, "which", lambda name: "powershell.exe")
    seen = {}
    assert notify.send(config(), "Título; rm -rf", "texto \"com aspas\"",
                       runner=lambda command, **kw: seen.update(command=command, env=kw["env"])) is True
    assert seen["command"][0] == "powershell" and "rm -rf" not in " ".join(seen["command"])
    assert seen["env"]["MT5X_NOTIFY_TITLE"] == "Título; rm -rf" and "aspas" in seen["env"]["MT5X_NOTIFY_TEXT"]


def test_account_number_is_masked():
    assert notify.mask_account(5057256000) == "•••6000" and notify.mask_account("123") == "123"
    assert notify.mask_account(None) == ""


# --- operações do robô ------------------------------------------------------------------

def deal(ticket, entry, kind=0, profit=0.0, magic=9001, symbol="EURUSD"):
    return SimpleNamespace(ticket=ticket, time=ticket, magic=magic, entry=entry, type=kind, volume=0.1, price=1.1,
                           symbol=symbol, profit=profit, swap=0.0, commission=-0.5)


def position(ticket, sl, magic=9001):
    return SimpleNamespace(ticket=ticket, magic=magic, volume=0.1, sl=sl, symbol="EURUSD")


class FakeMt5:
    """Devolve, a cada consulta, o próximo estado da lista (o último se repete)."""

    def __init__(self, states):
        self.states, self.i = states, -1

    def history_deals_get(self, start, end):
        self.i += 1
        state = self.states[min(self.i, len(self.states) - 1)]
        if isinstance(state, Exception):
            raise state
        return state["deals"]

    def positions_get(self):
        return self.states[min(self.i, len(self.states) - 1)]["positions"]


def run_watch(states, cycles, kill=lambda: False):
    notices = []
    count = live.watch(FakeMt5(states), "9001", lambda *a: notices.append(a), dt.datetime(2026, 1, 1), 0, cycles,
                       sleep=lambda s: None, kill_active=kill)
    assert count == len(notices)
    return notices


def test_each_new_deal_gives_exactly_one_notice():
    old = deal(1, 0)
    states = [{"deals": [old], "positions": []},                                       # já existia: sem aviso
              {"deals": [old, deal(2, 0, kind=1)], "positions": [position(2, 1.2)]},  # abriu venda
              {"deals": [old, deal(2, 0, kind=1)], "positions": [position(2, 1.2)]},  # nada novo
              {"deals": [old, deal(2, 0, kind=1), deal(3, 1, profit=12.8), deal(4, 0, magic=7)], "positions": []}]
    notices = run_watch(states, 6)
    assert [n[0] for n in notices] == ["Robô abriu VENDA em EURUSD", "Robô fechou posição em EURUSD"]
    assert notices[0][1] == "0.1 lote(s) a 1.1" and notices[1][1] == "0.1 lote(s) a 1.1; resultado +12,30"
    assert run_watch([{"deals": [old], "positions": []}], 3) == []


def test_position_without_stop_and_kill_switch_warn_once():
    state = {"deals": [], "positions": [position(5, 0.0), position(6, 1.2), position(7, 0.0, magic=1)]}
    notices = run_watch([state], 4, kill=lambda: True)
    assert [n[0] for n in notices] == ["Posição do robô SEM STOP", "PARAR TUDO está ativo"]
    assert notices[0][2] == notify.CRITICAL and notices[1][2] == notify.WARNING


def test_lost_and_recovered_connection_are_reported_once_each():
    ok = {"deals": [], "positions": []}
    notices = run_watch([ok, ConnectionError("ponte caiu"), ConnectionError("ainda"), ok, ok], 5)
    assert [n[0] for n in notices] == ["Acompanhamento do robô interrompido", "Acompanhamento do robô retomado"]
    assert "pode continuar operando" in notices[0][1] and notices[0][2] == notify.CRITICAL


# --- ações do programa ------------------------------------------------------------------

def test_cli_actions_notify_when_enabled_and_stay_quiet_otherwise(tmp_path, monkeypatch, capsys):
    sent = []
    monkeypatch.setattr(notify, "send", lambda cfg, title, text="", level=notify.INFO, runner=None:
                        sent.append((title, text, level, notify.enabled(cfg))))
    from mt5_extracao.bootstrap import paths
    monkeypatch.setattr(paths, "is_windows", lambda: False)
    library, prefix = tmp_path / "robos", tmp_path / "wine"
    (prefix / "drive_c" / "users" / "u" / "AppData" / "Roaming").mkdir(parents=True)     # onde fica o Common\Files
    for flag in ("true", "false"):
        cfg = tmp_path / f"{flag}.ini"
        cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {tmp_path / (flag + '.sqlite')}\n[APP]\nnotifications = {flag}\n"
                       f"[ROBOTS]\nlibrary_dirs = {library}\n[MT5]\nwine_prefix = {prefix}\n", encoding="utf-8")
    assert cli.main(["--config", str(tmp_path / "true.ini"), "robots", "new", "Avisa", "--dir", str(library)]) == cli.EXIT_OK
    assert cli.main(["--config", str(tmp_path / "true.ini"), "robots", "battery", "avisa"]) == cli.EXIT_FAIL
    title, text, level, on = sent[-1]
    assert title == "Avisa: bateria REPROVADA" and text.startswith("Reprovou em: ") and level == "aviso" and on
    assert cli.main(["--config", str(tmp_path / "false.ini"), "robots", "battery", "avisa"]) == cli.EXIT_FAIL
    assert sent[-1][3] is False                                    # desligado: o aviso não vai para o sistema
    assert cli.main(["--config", str(tmp_path / "true.ini"), "robots", "stop"]) == cli.EXIT_OK
    assert sent[-1][0] == "PARAR TUDO acionado" and sent[-1][2] == "crítico" and "NÃO foram fechadas" in sent[-1][1]
    capsys.readouterr()

    def explode(*a, **k):
        raise RuntimeError("notificador quebrado")
    monkeypatch.setattr(notify, "send", explode)
    assert cli.main(["--config", str(tmp_path / "true.ini"), "robots", "stop", "--release"]) == cli.EXIT_OK   # não derruba
