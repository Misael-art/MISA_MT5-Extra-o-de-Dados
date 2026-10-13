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


# --- T12.L1: organização por partes, devolução seletiva e estado -----------------------------------------------

FULL = ("[Window]\r\nFullscreen=0\r\n[Pane-32841]\r\nID=32841\r\nIsVisible=1\r\n[Pane-32846]\r\nID=32846\r\nIsVisible=1\r\n"
        "[Pane-32843]\r\nID=32843\r\nIsVisible=1\r\n[Pane-32845]\r\nID=32845\r\nIsVisible=0\r\n[Pane-32900]\r\nID=32900\r\n"
        "IsVisible=1\r\n[Pane-32931]\r\nID=32931\r\nIsVisible=1\r\n[Settings]\r\nSound=1\r\n[Common]\r\nLogin=1\r\n")


def shown(data):
    return layout.visibility(read(data))


def test_each_part_can_be_left_alone(tmp_path):
    data = terminal(tmp_path, FULL)
    assert layout.apply(data, toolbars=False, toolbox=False) == ["Observação do Mercado / Navegador"]
    assert shown(data) == {"32841": "1", "32846": "1", "32843": "0", "32845": "0", "32900": "1", "32931": "1"}
    data = terminal(tmp_path / "b", FULL)
    assert layout.apply(data, navigation=False, toolbars=False) == ["Caixa de Ferramentas / Testador"]
    assert shown(data)["32841"] == shown(data)["32846"] == "0" and shown(data)["32843"] == "1"
    data = terminal(tmp_path / "c", FULL)
    assert layout.apply(data, navigation=False, toolbox=False, toolbars=False) == []
    assert read(data) == FULL and not layout.has_arrangement(data)          # nada pedido: nada mexido, nada guardado


def test_a_part_turned_off_later_comes_back_as_the_user_had_it(tmp_path):
    data = terminal(tmp_path, FULL)
    layout.apply(data)                                                       # tudo escondido
    assert set(shown(data).values()) == {"0"}
    assert layout.apply(data, toolbars=False) == []                          # barras desligadas depois: voltam
    assert shown(data)["32900"] == shown(data)["32931"] == "1" and shown(data)["32841"] == "0"
    assert layout.apply(data, toolbars=False, navigation=False) == []
    assert shown(data)["32843"] == "1" and shown(data)["32845"] == "0"       # a que o usuário já escondia segue escondida
    assert layout.has_arrangement(data)


def test_restore_gives_back_only_the_windows_and_keeps_later_changes(tmp_path):
    """O terminal.ini também guarda opções do terminal: devolver o arranjo não pode desfazer o que mudou depois."""
    data = terminal(tmp_path, FULL)
    layout.apply(data)
    path = layout.ini_path(data)
    changed = read(data).replace("Sound=1", "Sound=0").replace("Login=1", "Login=777") + "[Novo]\r\nChave=1\r\n"
    path.write_bytes(changed.encode("utf-16"))                               # o terminal gravou outras coisas depois
    assert layout.restore(data)
    text = read(data)
    assert "Sound=0" in text and "Login=777" in text and "[Novo]\r\nChave=1" in text
    assert layout.visibility(text) == layout.visibility(FULL)                # as janelas, como o usuário tinha
    assert not layout.has_arrangement(data) and not layout.restore(data)


def test_restore_recreates_a_missing_terminal_ini_from_the_copy(tmp_path):
    data = terminal(tmp_path, FULL)
    layout.apply(data)
    layout.ini_path(data).unlink()
    assert layout.restore(data) and read(data) == FULL


def test_options_come_from_config_with_fallback_to_everything_on():
    assert layout.options(None) == layout.Options()
    config = configparser.ConfigParser()
    assert layout.options(config) == layout.Options(True, True, True, True, True, True)
    config.read_dict({"MT5": {"manage_layout": "no", "layout_hide_toolbars": "false", "layout_own_profile": "0",
                              "layout_hide_toolbox": "talvez"}})                # valor inválido vale o padrão
    assert layout.options(config) == layout.Options(enabled=False, navigation=True, toolbox=True, toolbars=False,
                                                    own_profile=False)
    from mt5_extracao.bootstrap import config_builder
    assert {key for key, *_ in layout.OPTION_KEYS} | {layout.MASTER_KEY} <= set(config_builder.DEFAULTS["MT5"])
    example = configparser.ConfigParser()
    example.read("config/config.ini.example", encoding="utf-8")
    assert all(example.getboolean("MT5", key) for key, *_ in layout.OPTION_KEYS)


def test_status_tells_who_turned_it_off_and_what_is_hidden(tmp_path):
    data, common = terminal(tmp_path, FULL), tmp_path / "Common"
    common.mkdir()
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"layout_hide_toolbars": "false"}})
    state = layout.status(config, data, common)
    assert state["active"] and not state["panel_off"] and not state["has_arrangement"]
    assert state["options"] == {"layout_hide_navigation": True, "layout_hide_toolbox": True,
                                "layout_hide_toolbars": False, "layout_own_profile": True, "view_panel": True}
    assert state["will_hide"] == ["Caixa de Ferramentas / Testador", "Observação do Mercado / Navegador"]
    assert state["hidden_now"] == ["Observação do Mercado / Navegador"]      # a que o usuário já tinha escondido
    layout.apply(data, toolbars=False)
    layout.set_panel_off(common, True)                                       # o mesmo arquivo do botão TELA
    state = layout.status(config, data, common)
    assert state["enabled"] and state["panel_off"] and not state["active"] and state["has_arrangement"]
    assert state["hidden_now"] == ["Caixa de Ferramentas / Testador", "Observação do Mercado / Navegador"]
    layout.set_panel_off(common, False)
    layout.set_panel_off(common, False)                                      # religar duas vezes não dá erro
    assert layout.status(config, data, common)["active"]
    assert layout.status(None)["active"] and layout.status(config, tmp_path / "nada")["hidden_now"] == []


def test_start_obeys_each_option(tmp_path, monkeypatch):
    common = tmp_path / "Common"
    common.mkdir()
    monkeypatch.setattr(live.mt5_tools, "common_files_dir", lambda config: common)
    config = configparser.ConfigParser()
    config.read_dict({"MT5": {"layout_own_profile": "false", "layout_hide_toolbox": "false"}})
    data = terminal(tmp_path / "t", FULL)
    assert live.organize_window(config, data, "EURUSD", "M15") == ""         # sem o perfil próprio
    assert not (data / "MQL5").exists() and not (common / layout.SEED_FILE).exists()
    assert shown(data)["32841"] == "1" and shown(data)["32843"] == "0" and shown(data)["32900"] == "0"
    config["MT5"]["layout_own_profile"] = "true"
    assert live.organize_window(config, data, "EURUSD", "M15") == "[Charts]\r\nProfileLast=HADOUKEN\r\n"
    config["MT5"]["manage_layout"] = "false"                                 # desligar devolve só as janelas
    assert live.organize_window(config, data, "EURUSD", "M15") == "" and read(data) == FULL


def test_profile_does_not_pile_up_charts_between_starts(tmp_path):
    """Achado 12 da bateria do painel: cada início deixa um gráfico só no perfil próprio."""
    folder = tmp_path / "MQL5" / "Profiles" / "Charts" / layout.PROFILE
    for symbol in ("EURUSD", "GBPUSD", "WIN$N"):
        layout.prepare_profile(tmp_path, symbol, "M15")
        (folder / "chart02.chr").write_bytes(b"grafico que o terminal gravou ao fechar")
    layout.prepare_profile(tmp_path, "PETR4", "H1")
    assert sorted(p.name for p in folder.iterdir()) == ["chart01.chr", "order.wnd"]
    assert "symbol=PETR4" in (folder / "chart01.chr").read_bytes().decode("utf-16")


def test_cli_shows_the_layout_state_and_restores_only_with_the_terminal_closed(tmp_path, monkeypatch, capsys):
    import json

    from mt5_extracao import cli
    from mt5_extracao.robots import mt5_tools
    data, common = terminal(tmp_path / "t", FULL), tmp_path / "Common"
    common.mkdir()
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[MT5]\nlayout_hide_toolbars = false\n[DATABASE]\ntype = sqlite\npath = {tmp_path / 'db.sqlite'}\n",
                   encoding="utf-8")
    monkeypatch.setattr(mt5_tools, "data_dir", lambda config, mt5=None: data)
    monkeypatch.setattr(mt5_tools, "common_files_dir", lambda config: common)
    running = [True]
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda config=None: running[0])

    assert cli.main(["--config", str(cfg), "mt5", "layout"]) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "Organização da janela: LIGADA" in out and "[ ] Esconder as barras de ferramentas" in out
    assert "[x] Fechar a Observação do Mercado e o Navegador" in out and "Arranjo anterior guardado: não" in out

    layout.apply(data, toolbars=False)
    assert cli.main(["--config", str(cfg), "mt5", "restore-layout"]) == cli.EXIT_USAGE       # terminal aberto
    assert "Feche o terminal" in capsys.readouterr().err and layout.has_arrangement(data)
    running[0] = False
    assert cli.main(["--config", str(cfg), "--json", "mt5", "layout"]) == cli.EXIT_OK
    state = json.loads(capsys.readouterr().out)["data"]
    assert state["has_arrangement"] is True and state["options"]["layout_hide_toolbars"] is False
    assert cli.main(["--config", str(cfg), "mt5", "restore-layout"]) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "devolvido" in out and "continua ligada" in out and read(data) == FULL
    assert cli.main(["--config", str(cfg), "mt5", "restore-layout"]) == cli.EXIT_OK
    assert "Não há arranjo anterior guardado" in capsys.readouterr().out

    (common / layout.OFF_FILE).write_text("x")
    assert cli.main(["--config", str(cfg), "mt5", "layout"]) == cli.EXIT_OK
    assert "DESLIGADA pelo botão TELA" in capsys.readouterr().out
