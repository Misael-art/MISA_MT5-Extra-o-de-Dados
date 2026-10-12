"""Conta de demonstração: detecção no terminal, comando do bootstrap e roteiro do assistente (sem MT5 real)."""
import argparse
import importlib.util
from pathlib import Path

from mt5_extracao.bootstrap import config_builder, demo_account, doctor, paths
from mt5_extracao.bootstrap.__main__ import main

REPO = Path(__file__).resolve().parents[1]


def _mt5_dir(tmp_path, with_account=False, config_name="Config"):
    mt5 = tmp_path / "MetaTrader 5"
    (mt5 / config_name).mkdir(parents=True)
    (mt5 / paths.MT5_TERMINAL_EXE).write_bytes(b"")
    if with_account:
        (mt5 / config_name / "accounts.dat").write_bytes(b"x")
    return mt5


def _write_config(root, mt5):
    cfg = config_builder.build_config(None, {"MT5": {"path": str(mt5)}})
    config_builder.write_config(paths.config_path(root), cfg)


def test_has_account_install_dir(tmp_path):
    assert not demo_account.has_account(_mt5_dir(tmp_path), env={})
    assert demo_account.has_account(_mt5_dir(tmp_path / "b", with_account=True, config_name="config"), env={})


def test_has_account_roaming_data_dir(tmp_path):
    mt5 = _mt5_dir(tmp_path)
    data = tmp_path / "AppData" / "MetaQuotes" / "Terminal" / "ABC123"
    (data / "config").mkdir(parents=True)
    (data / "origin.txt").write_bytes("C:\\Program Files\\MetaTrader 5".encode("utf-16"))
    env = {"APPDATA": str(tmp_path / "AppData")}
    assert not demo_account.has_account(mt5, env=env)
    (data / "config" / "accounts.dat").write_bytes(b"x")
    assert demo_account.has_account(mt5, env=env)


def test_doctor_warns_without_account(tmp_path):
    mt5 = _mt5_dir(tmp_path)
    _write_config(tmp_path, mt5)
    check = doctor.check_mt5_account(tmp_path)[0]
    assert check.status == doctor.WARN and "demo-account" in check.hint
    (mt5 / "Config" / "accounts.dat").write_bytes(b"x")
    assert doctor.check_mt5_account(tmp_path)[0].status == doctor.OK


def test_doctor_skips_account_check_without_mt5(tmp_path):
    assert doctor.check_mt5_account(tmp_path) == []


def test_demo_account_command_nothing_to_do(tmp_path, capsys):
    _write_config(tmp_path, _mt5_dir(tmp_path, with_account=True))
    assert main(["--root", str(tmp_path), "demo-account"]) == 0
    assert "já tem uma conta" in capsys.readouterr().out


def test_demo_account_command_needs_permission(tmp_path, capsys, monkeypatch):
    """Sem terminal interativo e sem --yes, nada é aberto: só explica e pede a autorização."""
    _write_config(tmp_path, _mt5_dir(tmp_path))
    called = []
    monkeypatch.setattr(demo_account, "open_demo_account", lambda *a, **k: called.append(1) or 0)
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    assert main(["--root", str(tmp_path), "demo-account"]) == 2
    out = capsys.readouterr().out
    assert "--yes" in out and "MetaQuotes" in out and not called
    assert main(["--root", str(tmp_path), "demo-account", "--yes"]) == 0 and called


# --- Roteiro do assistente (scripts/mt5_demo_account.py) com um assistente falso ---
def _load_script():
    spec = importlib.util.spec_from_file_location("mt5_demo_account", REPO / "scripts" / "mt5_demo_account.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeUser32:
    def __init__(self, wizard):
        self.wizard = wizard

    def SendMessageW(self, hwnd, msg, wparam, lparam):
        return 1 if hwnd in self.wizard.checked else 0


class _FakeWizard:
    """Páginas: empresa -> tipo de conta -> formulário -> resultado -> fechado."""

    def __init__(self, script, accept_form=True):
        s = script
        self.pages = [{s.ID_COMPANY_LIST, s.ID_NEXT}, {s.ID_RADIO_DEMO, s.ID_NEXT},
                      {s.ID_FIRST, s.ID_AGREE, s.ID_NEXT}, {s.ID_STATUS, s.ID_FINISH}]
        self.s, self.page, self.accept_form = script, 0, accept_form
        self.texts, self.birth, self.checked = {}, None, {script.ID_RADIO_DEMO}
        self.u = _FakeUser32(self)

    def find(self):
        return "dlg" if self.page < len(self.pages) else None

    def wait(self, timeout):
        return self.find()

    def item(self, dialog, ctrl_id):
        return ctrl_id if ctrl_id in self.pages[self.page] else None

    def enabled(self, dialog, ctrl_id):
        if self.page == 2 and ctrl_id == self.s.ID_NEXT:
            return self.accept_form and self.s.ID_AGREE in self.checked and self.birth is not None
        return self.item(dialog, ctrl_id) is not None

    def click(self, dialog, ctrl_id):
        if ctrl_id in (self.s.ID_NEXT, self.s.ID_FINISH):
            self.page += 1
        else:
            self.checked.add(ctrl_id)

    def set_text(self, dialog, ctrl_id, value):
        self.texts[ctrl_id] = value

    def set_birth(self, dialog, day, month, year):
        self.birth = (day, month, year)


def _args(**kw):
    base = dict(demo_account.DEMO_DEFAULTS, wait=1, timeout=30)
    base.update(kw)
    return argparse.Namespace(**base)


def test_wizard_script_fills_and_finishes(monkeypatch):
    script = _load_script()
    monkeypatch.setattr(script.time, "sleep", lambda s: None)
    wizard = _FakeWizard(script)
    assert script.run(_args(), wizard=wizard, log=lambda m: None) == script.EXIT_OK
    assert wizard.texts[script.ID_EMAIL] == demo_account.DEMO_DEFAULTS["email"]
    assert wizard.birth == (1, 1, 1990) and script.ID_AGREE in wizard.checked


def test_wizard_script_reports_rejected_form(monkeypatch):
    script = _load_script()
    monkeypatch.setattr(script.time, "sleep", lambda s: None)
    monkeypatch.setattr(script, "_wait_until", lambda cond, timeout: cond())
    wizard = _FakeWizard(script, accept_form=False)
    assert script.run(_args(), wizard=wizard, log=lambda m: None) == script.EXIT_REJECTED
    assert script.EXIT_REJECTED == demo_account.EXIT_REJECTED


def test_wizard_script_without_wizard():
    script = _load_script()
    wizard = _FakeWizard(script)
    wizard.page = len(wizard.pages)
    assert script.run(_args(), wizard=wizard, log=lambda m: None) == script.EXIT_NO_WIZARD


def test_auto_correct_never_maps_dollar_series_to_another_asset(connector, fake_mt5):
    """
    Regressão (MT5 real, MetaQuotes-Demo): WIN$N não existe lá, mas WINN (ação dos EUA) existe.
    A "correção" que removia o '$' gravava os preços de WINN como se fossem WIN$N.
    """
    fake_mt5.symbols = list(fake_mt5.symbols) + ["DOLN"]
    assert connector.auto_correct_symbol("DOL$N") == "DOL$N"
    assert connector.missing_symbols(["DOL$N", "DOLN"]) == ["DOL$N"]
    assert connector.server_name() == "Fake-Demo"


# --- Login automático com as credenciais do .env -------------------------------------------
def _clear_credentials(monkeypatch):
    for name in ("MT5_LOGIN", "MT5_PASSWORD", "MT5_SERVER"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("dotenv.load_dotenv", lambda *a, **k: False)


def test_no_credentials_keeps_terminal_account(connector, fake_mt5, monkeypatch):
    _clear_credentials(monkeypatch)
    assert connector._login_from_env() is True
    assert not fake_mt5.calls_named("login")


def test_login_from_env_switches_account(connector, fake_mt5, monkeypatch):
    _clear_credentials(monkeypatch)
    monkeypatch.setenv("MT5_LOGIN", "555")
    monkeypatch.setenv("MT5_PASSWORD", "segredo")
    monkeypatch.setenv("MT5_SERVER", "Corretora-DEMO")
    assert connector._login_from_env() is True
    assert fake_mt5.account_info().login == 555 and connector.server_name() == "Corretora-DEMO"
    # já está na conta certa: não entra de novo
    assert connector._login_from_env() is True
    assert len(fake_mt5.calls_named("login")) == 1


def test_login_from_env_failure_is_reported_without_password(connector, fake_mt5, monkeypatch, caplog):
    _clear_credentials(monkeypatch)
    fake_mt5.valid_password = "outra"
    monkeypatch.setenv("MT5_LOGIN", "555")
    monkeypatch.setenv("MT5_PASSWORD", "segredo-errado")
    assert connector._login_from_env() is False
    assert "MT5_SERVER" in caplog.text and "segredo-errado" not in caplog.text
