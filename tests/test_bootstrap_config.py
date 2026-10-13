import json
import os

import pytest

from mt5_extracao.bootstrap import config_builder, paths
from mt5_extracao.bootstrap.__main__ import main


def test_build_config_defaults():
    cfg = config_builder.build_config()
    assert cfg.get("DATABASE", "type") == "sqlite"
    assert cfg.get("EXTRACTION", "chunk_days_m1") == "30"
    assert cfg.get("BRIDGE", "port") == str(paths.DEFAULT_BRIDGE_PORT)


def test_build_config_preserves_existing_and_applies_overrides(tmp_path):
    ini = tmp_path / "c.ini"
    ini.write_text("[DATABASE]\npath = meu/banco.db\n[CUSTOM]\nfoo = bar\n", encoding="utf-8")
    existing = config_builder.load_config(ini)
    cfg = config_builder.build_config(existing, {"MT5": {"path": "/x"}, "DATABASE": {"type": None}})
    assert cfg.get("DATABASE", "path") == "meu/banco.db"      # preservado
    assert cfg.get("DATABASE", "type") == "sqlite"            # padrão (override None ignorado)
    assert cfg.get("CUSTOM", "foo") == "bar"                  # seção desconhecida mantida
    assert cfg.get("MT5", "path") == "/x"                     # override aplicado


def test_write_config_is_readable_by_default_configparser(tmp_path):
    import configparser
    path = tmp_path / "config" / "config.ini"
    config_builder.write_config(path, config_builder.build_config(None, {"MT5": {"path": r"C:\MT5"}}))
    cfg = configparser.ConfigParser()  # mesmo parser (com interpolação) usado pelo app.py
    cfg.read(path, encoding="utf-8")
    assert cfg.get("MT5", "path") == r"C:\MT5"
    assert cfg.getboolean("FALLBACK", "external_source_m1_fallback_enabled") is False


@pytest.mark.parametrize("password", ["simples", "com espaço", "aspas'e\"", r"barra\invertida", "#hash=igual$"])
def test_env_roundtrip(tmp_path, password):
    env = tmp_path / ".env"
    env.write_text("OUTRA=1\n", encoding="utf-8")
    assert config_builder.write_env_credentials(env, "123", password, "Srv-Demo")
    values = config_builder.read_env_file(env)
    assert values["MT5_PASSWORD"] == password
    assert values["MT5_LOGIN"] == "123"
    assert values["OUTRA"] == "1"
    if os.name != "nt":
        assert (env.stat().st_mode & 0o777) == 0o600


@pytest.mark.parametrize("password", ["com espaço", "aspas'e\"", r"barra\invertida", "#hash=igual$"])
def test_env_compatible_with_python_dotenv(tmp_path, password):
    dotenv = pytest.importorskip("dotenv")
    env = tmp_path / ".env"
    config_builder.write_env_credentials(env, "1", password, None)
    assert dotenv.dotenv_values(env)["MT5_PASSWORD"] == password


def test_env_none_values_do_not_overwrite(tmp_path):
    env = tmp_path / ".env"
    config_builder.write_env_credentials(env, "1", "senha", "srv")
    assert config_builder.write_env_credentials(env, None, None, None) is False
    config_builder.write_env_credentials(env, "2", None, None)
    assert config_builder.read_env_file(env)["MT5_PASSWORD"] == "senha"
    assert config_builder.read_env_file(env)["MT5_LOGIN"] == "2"


def test_ensure_symbols_file_does_not_overwrite(tmp_path):
    path = tmp_path / "s.json"
    assert config_builder.ensure_symbols_file(path) is True
    data = json.loads(path.read_text(encoding="utf-8"))
    assert "WIN$N" in data["indices_futuros_b3"]
    path.write_text("{}", encoding="utf-8")
    assert config_builder.ensure_symbols_file(path) is False
    assert path.read_text(encoding="utf-8") == "{}"


def _make_fake_mt5(tmp_path):
    if paths.is_windows():
        d = tmp_path / "MetaTrader 5"
        prefix = None
    else:
        prefix = tmp_path / "wine"
        d = prefix / "drive_c" / "Program Files" / "MetaTrader 5"
    d.mkdir(parents=True)
    (d / paths.MT5_TERMINAL_EXE).write_bytes(b"")
    return d, prefix


def test_cli_configure_non_interactive(tmp_path, monkeypatch):
    root = tmp_path / "proj"
    root.mkdir()
    mt5_dir, prefix = _make_fake_mt5(tmp_path)
    monkeypatch.setenv("SENHA_TESTE", "s3nh@ x")
    argv = ["--root", str(root), "configure", "--non-interactive", "--no-doctor",
            "--mt5-path", str(mt5_dir), "--login", "999", "--password-env", "SENHA_TESTE"]
    if prefix:
        argv += ["--wine-prefix", str(prefix)]
    assert main(argv) == 0
    cfg = config_builder.load_config(paths.config_path(root))
    assert cfg.get("MT5", "path") == str(mt5_dir)
    for name in paths.WORK_DIRS:
        assert (root / name).is_dir()
    assert paths.symbols_path(root).exists()
    env = config_builder.read_env_file(paths.env_path(root))
    assert env["MT5_PASSWORD"] == "s3nh@ x" and env["MT5_LOGIN"] == "999"
    if not paths.is_windows():
        assert cfg.get("MT5", "windows_path") == r"C:\Program Files\MetaTrader 5"
        assert cfg.get("BRIDGE", "enabled") == "true"

    # Reexecutar preserva edições manuais do usuário
    cfg.set("DATABASE", "path", "outro.db")
    config_builder.write_config(paths.config_path(root), cfg)
    assert main(["--root", str(root), "configure", "--non-interactive", "--no-doctor", "--skip-credentials",
                 "--mt5-path", str(mt5_dir)] + (["--wine-prefix", str(prefix)] if prefix else [])) == 0
    assert config_builder.load_config(paths.config_path(root)).get("DATABASE", "path") == "outro.db"


def test_cli_configure_invalid_mt5_path(tmp_path):
    root = tmp_path / "proj"
    root.mkdir()
    assert main(["--root", str(root), "configure", "--non-interactive", "--no-doctor",
                 "--mt5-path", str(tmp_path / "nada")]) == 1


COMMENTED = """# Arquivo do usuário, com comentários
[MT5]
# caminho do terminal
path = C:\\MT5
manage_layout = true   

[DATABASE]
; banco
type = sqlite
"""


def test_set_config_value_changes_one_line_and_keeps_comments(tmp_path):
    """T12.L2: a interface grava uma opção sem regravar o arquivo (comentários, ordem e o resto ficam)."""
    path = tmp_path / "config.ini"
    path.write_text(COMMENTED, encoding="utf-8")
    config_builder.set_config_value(path, "MT5", "manage_layout", "false")
    assert path.read_text(encoding="utf-8") == COMMENTED.replace("manage_layout = true   ", "manage_layout = false")
    config_builder.set_config_value(path, "MT5", "layout_hide_toolbars", "false")       # chave nova: fim da seção
    text = path.read_text(encoding="utf-8")
    assert "manage_layout = false\nlayout_hide_toolbars = false\n\n[DATABASE]" in text
    assert "# caminho do terminal" in text and "; banco" in text
    config_builder.set_config_value(path, "ROBOTS", "tester_model", "0")                # seção nova: fim do arquivo
    text = path.read_text(encoding="utf-8")
    assert text.endswith("type = sqlite\n\n[ROBOTS]\ntester_model = 0\n")
    config_builder.set_config_value(path, "DATABASE", "TYPE", "postgresql")             # chave sem diferença de caixa
    parsed = config_builder.load_config(path)
    assert parsed.get("DATABASE", "type") == "postgresql" and parsed.get("MT5", "path") == "C:\\MT5"
    assert parsed.getboolean("MT5", "manage_layout") is False and parsed.get("ROBOTS", "tester_model") == "0"
    assert not [p for p in tmp_path.iterdir() if p.name != "config.ini"]                # nenhum temporário sobrando


def test_set_config_value_creates_the_file_and_refuses_line_breaks(tmp_path):
    path = tmp_path / "novo" / "config.ini"
    config_builder.set_config_value(path, "MT5", "manage_layout", "false")
    assert path.read_text(encoding="utf-8") == "[MT5]\nmanage_layout = false\n"
    with pytest.raises(ValueError, match="quebra de linha"):
        config_builder.set_config_value(path, "MT5", "path", "a\n[X]\nb = 1")
    assert path.read_text(encoding="utf-8") == "[MT5]\nmanage_layout = false\n"


def test_set_config_value_does_not_touch_a_same_named_key_of_another_section(tmp_path):
    path = tmp_path / "config.ini"
    path.write_text("[A]\npath = 1\n[B]\npath = 2\n[C]\nx = 3\n", encoding="utf-8")
    config_builder.set_config_value(path, "B", "path", "9")
    config_builder.set_config_value(path, "B", "novo", "s")
    assert path.read_text(encoding="utf-8") == "[A]\npath = 1\n[B]\npath = 9\nnovo = s\n[C]\nx = 3\n"
