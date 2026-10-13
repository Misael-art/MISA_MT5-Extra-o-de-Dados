"""T12: organização da janela do MT5 ao iniciar um robô (janelas laterais fechadas, perfil próprio, gráfico maximizado)."""
import configparser

import pytest

from mt5_extracao.robots import layout, live

INI = ("[Window]\r\nFullscreen=0\r\n[Pane-59393]\r\nID=59393\r\nIsVisible=1\r\n[Pane-32841]\r\nID=32841\r\nIsVisible=1\r\n"
       "[Pane-32843]\r\nID=32843\r\nIsVisible=1\r\n[Pane-32844]\r\nID=32844\r\nIsVisible=0\r\n[Pane-32900]\r\nID=32900\r\n"
       "IsVisible=1\r\n[Docking]\r\nIsVisible=1\r\n")


def terminal(tmp_path, text=INI):
    config_dir = tmp_path / "Config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "terminal.ini").write_bytes(text.encode("utf-16"))
    return tmp_path


def read(data):
    return layout.ini_path(data).read_bytes().decode("utf-16")


def test_apply_hides_only_the_known_panes_and_keeps_the_rest(tmp_path):
    data = terminal(tmp_path)
    hidden = layout.apply(data)
    assert hidden == ["Caixa de Ferramentas / Testador", "Observação do Mercado / Navegador", "barra de ferramentas"]
    text = read(data)
    assert "[Pane-32841]\r\nID=32841\r\nIsVisible=0" in text and "[Pane-32843]\r\nID=32843\r\nIsVisible=0" in text
    assert "[Pane-32900]\r\nID=32900\r\nIsVisible=0" in text
    assert "[Pane-59393]\r\nID=59393\r\nIsVisible=1" in text          # janela que não é das nossas: intocada
    assert "[Docking]\r\nIsVisible=1" in text and "[Window]\r\nFullscreen=0" in text
    assert layout.apply(data) == []                                     # segunda vez: nada a fazer


def test_toolbars_can_stay_and_the_first_arrangement_is_kept_for_restore(tmp_path):
    data = terminal(tmp_path)
    assert "barra de ferramentas" not in layout.apply(data, toolbars=False)
    assert "[Pane-32900]\r\nID=32900\r\nIsVisible=1" in read(data)
    backup = layout.ini_path(data).with_name("terminal.ini" + layout.BACKUP_SUFFIX)
    assert backup.read_bytes().decode("utf-16") == INI
    layout.apply(data)                                                  # esconde também as barras
    assert backup.read_bytes().decode("utf-16") == INI                  # a cópia é sempre a do arranjo do usuário
    assert layout.restore(data) and read(data) == INI and not backup.exists()
    assert not layout.restore(data)                                     # sem cópia, nada a devolver


def test_missing_file_and_unknown_build_change_nothing(tmp_path):
    assert layout.apply(tmp_path) == []                                 # terminal nunca aberto
    data = terminal(tmp_path, "[Window]\r\nFullscreen=0\r\n[Pane-1]\r\nID=1\r\nIsVisible=1\r\n")
    assert layout.apply(data) == [] and "IsVisible=1" in read(data)


def test_profile_has_one_maximized_chart_of_the_robot_and_warns_the_panel(tmp_path):
    common = tmp_path / "Common"
    common.mkdir()
    folder = tmp_path / "MQL5" / "Profiles" / "Charts" / layout.PROFILE
    folder.mkdir(parents=True)
    (folder / "chart07.chr").write_bytes(b"velho")                      # sobra de outra abertura
    assert layout.prepare_profile(tmp_path, "WIN$N", "M5", common) == "HADOUKEN"
    assert sorted(p.name for p in folder.iterdir()) == ["chart01.chr", "order.wnd"]
    chart = (folder / "chart01.chr").read_bytes().decode("utf-16")
    assert "symbol=WIN$N\r\n" in chart and "period_type=0\r\nperiod_size=5\r\n" in chart and "window_type=3\r\n" in chart
    assert "<expert>" not in chart                                      # o robô entra pelo .ini de início, não por aqui
    assert (folder / "order.wnd").read_bytes().decode("utf-16") == "chart01.chr\r\n"
    assert (common / layout.SEED_FILE).is_file()
    hourly = layout.prepare_profile(tmp_path, "EURUSD", "H4")
    assert "period_type=1\r\nperiod_size=4\r\n" in (folder / "chart01.chr").read_bytes().decode("utf-16") and hourly
    assert layout.startup_lines("HADOUKEN") == "[Charts]\r\nProfileLast=HADOUKEN\r\n"
    with pytest.raises(ValueError, match="Timeframe"):
        layout.prepare_profile(tmp_path, "EURUSD", "S30")


def test_it_is_on_by_default_and_easy_to_turn_off(tmp_path):
    on, off = configparser.ConfigParser(), configparser.ConfigParser()
    off.read_dict({"MT5": {"manage_layout": "false"}})
    assert layout.enabled(None) and layout.enabled(on) and layout.enabled(on, tmp_path) and not layout.enabled(off)
    (tmp_path / layout.OFF_FILE).write_text("desligado pelo painel")    # botão TELA do painel
    assert not layout.enabled(on, tmp_path) and not layout.enabled(None, tmp_path)


def test_start_adds_the_profile_and_turning_off_gives_the_arrangement_back(tmp_path, monkeypatch):
    data = terminal(tmp_path)
    common = tmp_path / "Common"
    common.mkdir()
    monkeypatch.setattr(live.mt5_tools, "common_files_dir", lambda config: common)
    config = configparser.ConfigParser()
    assert live.organize_window(config, data, "EURUSD", "M15") == "[Charts]\r\nProfileLast=HADOUKEN\r\n"
    assert "[Pane-32841]\r\nID=32841\r\nIsVisible=0" in read(data)
    (common / layout.OFF_FILE).write_text("x")
    assert live.organize_window(config, data, "EURUSD", "M15") == "" and read(data) == INI
    (common / layout.OFF_FILE).unlink()
    assert live.organize_window(config, data, "EURUSD", "S30") == ""    # falha na organização não impede o início
