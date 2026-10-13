"""T12.L3: "Abrir visão MT5" (organização aplicada quando o usuário pedir) e a disposição do painel nas opções."""
import configparser
import types

import pytest

from mt5_extracao import cli
from mt5_extracao.bootstrap import paths
from mt5_extracao.robots import layout, live, mt5_tools, view

INI = ("[Pane-32841]\r\nID=32841\r\nIsVisible=1\r\n[Pane-32843]\r\nID=32843\r\nIsVisible=1\r\n"
       "[Pane-32900]\r\nID=32900\r\nIsVisible=1\r\n[Settings]\r\nSound=1\r\n")
CHART = ("<chart>\r\nid=1\r\nsymbol=EURUSD\r\n<expert>\r\nname=HDK_HADOUKEN_Painel\r\n<inputs>\r\nInpRiskPct=0.5\r\n"
         "InpPanelLayout=0\r\nInpManageLayout=true\r\n</inputs>\r\n</expert>\r\n</chart>\r\n")


@pytest.fixture
def mt5(tmp_path, monkeypatch):
    """Pasta de dados simulada e os passos que mexem no terminal trocados por registros (sem MT5 real)."""
    data, common = tmp_path / "mt5", tmp_path / "Common"
    (data / "Config").mkdir(parents=True)
    common.mkdir()
    (data / "Config" / "terminal.ini").write_bytes(INI.encode("utf-16"))
    env = types.SimpleNamespace(data=data, common=common, running=False, bridge=False, calls=[],
                                config=configparser.ConfigParser())
    monkeypatch.setattr(mt5_tools, "data_dir", lambda config, mt5=None: data)
    monkeypatch.setattr(mt5_tools, "common_files_dir", lambda config: common)
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda config=None: env.running)

    def release(config=None):
        assert layout.visibility(env.shown())["32841"] == "1" or env.calls      # nada é gravado antes de fechar
        env.calls.append("fechar")
        env.running = False
        return env.bridge

    def start(config, data_dir, ini_text, name, spawn=None, is_running=None):
        assert not env.running and name == "mt5x_view"
        env.calls.append(("abrir", ini_text))
    monkeypatch.setattr(mt5_tools, "release_terminal", release)
    monkeypatch.setattr(mt5_tools, "restart_bridge", lambda: env.calls.append("ponte"))
    monkeypatch.setattr(mt5_tools, "start_terminal", start)
    env.shown = lambda: (data / "Config" / "terminal.ini").read_bytes().decode("utf-16")
    return env


def profile_chart(env, text=CHART):
    folder = layout.profile_dir(env.data)
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "chart01.chr").write_bytes(b"\xff\xfe" + text.encode("utf-16-le"))
    return folder / "chart01.chr"


def test_view_with_the_terminal_closed_applies_the_options_and_opens_it(mt5):
    mt5.config.read_dict({"MT5": {"layout_hide_toolbars": "false"}})
    result = view.open_view(mt5.config)
    assert mt5.calls == [("abrir", "")]                                # sem perfil próprio ainda: abre sem /config
    assert result == {"reopened": False, "organized": True, "restored": False, "profile": "", "chart": "",
                      "panel_layout": "auto",
                      "panel_charts": 0, "bridge": False,
                      "hidden": ["Caixa de Ferramentas / Testador", "Observação do Mercado / Navegador"]}
    seen = layout.visibility(mt5.shown())
    assert seen == {"32841": "0", "32843": "0", "32900": "1"} and "Sound=1" in mt5.shown()
    assert "escondido agora: Caixa de Ferramentas" in view.describe(result)


def test_view_refuses_to_close_an_open_terminal_without_permission(mt5):
    mt5.running = True
    with pytest.raises(view.TerminalOpen, match="--close-terminal"):
        view.open_view(mt5.config)
    assert mt5.calls == [] and mt5.shown() == INI                      # nada foi tocado


def test_view_closes_reopens_and_puts_the_bridge_back(mt5):
    mt5.running, mt5.bridge = True, True
    result = view.open_view(mt5.config, close_terminal=True)
    assert mt5.calls == ["fechar", ("abrir", ""), "ponte"]             # fecha, grava, abre e religa a ponte
    assert result["reopened"] and result["bridge"] and len(result["hidden"]) == 3
    assert view.describe(result).startswith("MetaTrader 5 fechado e aberto de novo") and "ponte religada" in \
        view.describe(result)


def test_view_puts_the_bridge_back_even_when_opening_fails(mt5, monkeypatch):
    mt5.running, mt5.bridge = True, True

    def broken(*args, **kwargs):
        raise FileNotFoundError("Terminal não encontrado")
    monkeypatch.setattr(mt5_tools, "start_terminal", broken)
    with pytest.raises(FileNotFoundError):
        view.open_view(mt5.config, close_terminal=True)
    assert mt5.calls == ["fechar", "ponte"]


def test_view_uses_the_own_profile_as_it_is_and_updates_the_panel_layout(mt5):
    chart = profile_chart(mt5)
    mt5.config.read_dict({"MT5": {"panel_layout": "wide"}})
    result = view.open_view(mt5.config)
    assert mt5.calls == [("abrir", "[Charts]\r\nProfileLast=HADOUKEN\r\n")]
    assert result["profile"] == "HADOUKEN" and result["panel_layout"] == "wide" and result["panel_charts"] == 1
    text = chart.read_bytes().decode("utf-16")
    assert text == CHART.replace("InpPanelLayout=0", "InpPanelLayout=3")   # só essa linha; o robô segue no gráfico
    assert view.open_view(mt5.config)["panel_charts"] == 0                 # já estava: nada a regravar
    mt5.config["MT5"]["layout_own_profile"] = "false"
    mt5.calls.clear()
    assert view.open_view(mt5.config)["profile"] == "" and mt5.calls == [("abrir", "")]


def test_view_with_the_organization_off_gives_the_arrangement_back(mt5):
    view.open_view(mt5.config)
    assert layout.has_arrangement(mt5.data)
    mt5.config.read_dict({"MT5": {"manage_layout": "false"}})
    result = view.open_view(mt5.config)
    assert not result["organized"] and result["restored"] and result["hidden"] == [] and mt5.shown() == INI
    assert "organização desligada, arranjo anterior devolvido" in view.describe(result)
    (mt5.common / layout.OFF_FILE).write_text("x")                     # botão TELA do painel também desliga
    mt5.config["MT5"]["manage_layout"] = "true"
    assert not view.open_view(mt5.config)["organized"] and mt5.shown() == INI


def test_panel_layout_option_and_the_charts_it_may_touch(mt5):
    config = configparser.ConfigParser()
    assert layout.panel_layout(None) == layout.panel_layout(config) == "auto"
    config.read_dict({"MT5": {"panel_layout": " Compact "}})
    assert layout.panel_layout(config) == "compact" and layout.panel_code("compact") == 1
    config["MT5"]["panel_layout"] = "gigante"
    assert layout.panel_layout(config) == "auto"                       # valor desconhecido vale o padrão
    assert [layout.panel_code(n) for n in ("auto", "compact", "normal", "wide")] == [0, 1, 2, 3]
    assert layout.set_panel_input(mt5.data, 2) == 0                    # sem perfil próprio: nada
    profile_chart(mt5)
    other = mt5.data / "MQL5" / "Profiles" / "Charts" / "Default"
    other.mkdir(parents=True)
    (other / "chart01.chr").write_bytes(b"\xff\xfe" + CHART.encode("utf-16-le"))
    assert layout.set_panel_input(mt5.data, 2) == 1
    assert "InpPanelLayout=0" in (other / "chart01.chr").read_bytes().decode("utf-16")   # perfil do usuário: intocado
    profile_chart(mt5, "<chart>\r\nsymbol=EURUSD\r\n</chart>\r\n")       # gráfico sem o painel
    assert layout.set_panel_input(mt5.data, 3) == 0
    state = layout.status(config, mt5.data, mt5.common)
    assert state["panel_layout"] == "auto" and state["has_profile"] is True


def test_robot_start_passes_the_chosen_panel_layout_without_changing_the_preset(tmp_path):
    presets = tmp_path / "MQL5" / "Presets"
    presets.mkdir(parents=True)
    (presets / "Conservador.set").write_bytes(b"\xff\xfe" + "InpRiskPct=0.25\r\nInpPanelLayout=0\r\n".encode("utf-16-le"))
    panel = types.SimpleNamespace(inputs=[types.SimpleNamespace(name="InpRiskPct"),
                                          types.SimpleNamespace(name="InpPanelLayout")])
    strategy = types.SimpleNamespace(inputs=[types.SimpleNamespace(name="InpRiskPct")])
    config = configparser.ConfigParser()
    assert live.panel_parameters(config, panel, tmp_path, "Conservador.set") == "Conservador.set"   # auto: nada muda
    config.read_dict({"MT5": {"panel_layout": "normal"}})
    assert live.panel_parameters(config, strategy, tmp_path, "Conservador.set") == "Conservador.set"  # robô sem painel
    assert live.panel_parameters(config, panel, tmp_path, "Conservador.set") == "mt5x_painel.set"
    assert (presets / "mt5x_painel.set").read_bytes().decode("utf-16") == "InpRiskPct=0.25\r\nInpPanelLayout=2\r\n"
    assert (presets / "Conservador.set").read_bytes().decode("utf-16") == "InpRiskPct=0.25\r\nInpPanelLayout=0\r\n"
    assert live.panel_parameters(config, panel, tmp_path, "") == "mt5x_painel.set"                    # sem preset
    assert (presets / "mt5x_painel.set").read_bytes().decode("utf-16") == "InpPanelLayout=2\r\n"


def test_start_terminal_without_ini_opens_like_the_shortcut(tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "is_windows", lambda: False)
    prefix = tmp_path / "wine"
    folder = prefix / "drive_c" / "Program Files" / "MetaTrader 5"
    folder.mkdir(parents=True)
    (folder / "terminal64.exe").write_bytes(b"")
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"path": str(folder), "wine_prefix": str(prefix)}})
    lines = []
    mt5_tools.start_terminal(config, folder, "", "mt5x_view", lambda line, cwd, pre: lines.append(line), lambda: False)
    mt5_tools.start_terminal(config, folder, "[Charts]\r\nProfileLast=HADOUKEN\r\n", "mt5x_view",
                             lambda line, cwd, pre: lines.append(line), lambda: False)
    assert lines[0].endswith('terminal64.exe" /portable') and "/config" not in lines[0]
    assert lines[1].endswith('/portable /config:"C:\\Program Files\\MetaTrader 5\\mt5x_view.ini"')


def test_cli_view_needs_permission_to_reopen(mt5, tmp_path, capsys):
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[MT5]\npanel_layout = compact\n[DATABASE]\ntype = sqlite\npath = {tmp_path / 'db.sqlite'}\n",
                   encoding="utf-8")
    mt5.running = True
    assert cli.main(["--config", str(cfg), "mt5", "view"]) == cli.EXIT_USAGE
    assert "--close-terminal" in capsys.readouterr().err and mt5.calls == []
    assert cli.main(["--config", str(cfg), "mt5", "view", "--close-terminal"]) == cli.EXIT_OK
    assert "fechado e aberto de novo" in capsys.readouterr().out and mt5.calls[0] == "fechar"
    assert cli.main(["--config", str(cfg), "mt5", "layout"]) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "Compacta" in out and "panel_layout = compact" in out and "mt5x mt5 view --close-terminal" in out


def test_view_creates_the_own_profile_with_one_maximized_chart_when_there_is_none(mt5):
    result = view.open_view(mt5.config, symbol="PETR4")
    assert result["profile"] == "HADOUKEN" and result["chart"] == "PETR4 M15"
    assert mt5.calls == [("abrir", "[Charts]\r\nProfileLast=HADOUKEN\r\n")]
    folder = layout.profile_dir(mt5.data)
    chart = (folder / "chart01.chr").read_bytes().decode("utf-16")
    assert "symbol=PETR4\r\n" in chart and "period_size=15\r\n" in chart and "window_type=3\r\n" in chart
    assert not (mt5.common / layout.SEED_FILE).exists()                 # não há gráfico auxiliar para o painel fechar
    assert "com o gráfico de PETR4 M15 maximizado" in view.describe(result)
    (folder / "chart01.chr").write_bytes(b"\xff\xfe" + CHART.encode("utf-16-le"))    # um robô passou a morar ali
    again = view.open_view(mt5.config, symbol="VALE3", timeframe="H1")
    assert again["chart"] == "" and again["profile"] == "HADOUKEN"       # perfil que já tem gráfico: usado como está
    assert "HDK_HADOUKEN_Painel" in (folder / "chart01.chr").read_bytes().decode("utf-16")
    mt5.config.read_dict({"MT5": {"layout_own_profile": "false"}})
    for old in folder.glob("chart*.chr"):
        old.unlink()
    assert view.open_view(mt5.config, symbol="VALE3")["profile"] == ""   # opção desligada: nenhum perfil é criado
    assert not layout.has_profile(mt5.data)


def test_view_without_a_symbol_starts_from_the_chart_the_user_already_had(mt5):
    """Visto no host: o perfil do usuário tinha um gráfico minimizado. A visão o repete, maximizado, no perfil próprio."""
    default = mt5.data / "MQL5" / "Profiles" / "Charts" / "Default"
    default.mkdir(parents=True)
    user_chart = "<chart>\r\nsymbol=GBPUSD\r\nperiod_type=1\r\nperiod_size=4\r\nwindow_type=2\r\n</chart>\r\n"
    (default / "chart01.chr").write_bytes(b"\xff\xfe" + user_chart.encode("utf-16-le"))
    assert layout.user_chart(mt5.data) == ("GBPUSD", "H4")
    result = view.open_view(mt5.config)
    assert result["chart"] == "GBPUSD H4" and result["profile"] == "HADOUKEN"
    own = (layout.profile_dir(mt5.data) / "chart01.chr").read_bytes().decode("utf-16")
    assert "symbol=GBPUSD\r\n" in own and "window_type=3\r\n" in own
    assert (default / "chart01.chr").read_bytes().decode("utf-16") == user_chart      # o perfil do usuário: intocado
    # o perfil em que o terminal estava vem de common.ini; o próprio perfil "HADOUKEN" não serve de origem
    (mt5.data / "Config" / "common.ini").write_bytes("[Charts]\r\nProfileLast=Meu\r\n".encode("utf-16"))
    assert layout.user_chart(mt5.data) == ("", "")
    (mt5.data / "Config" / "common.ini").write_bytes("[Charts]\r\nProfileLast=HADOUKEN\r\n".encode("utf-16"))
    assert layout.user_chart(mt5.data) == ("GBPUSD", "H4")
    assert layout.user_chart(mt5.data / "nada") == ("", "")
