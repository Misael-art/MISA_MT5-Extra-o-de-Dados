"""T5.2: configuração do PostgreSQL (sem servidor)."""
import configparser

import pytest

from mt5_extracao import services


def cfg(text):
    c = configparser.ConfigParser(interpolation=None)
    c.read_string(text)
    return c


def test_url_required():
    with pytest.raises(ValueError, match=r"\[DATABASE\] url"):
        services.database_url(cfg("[DATABASE]\ntype = postgresql\n"))


def test_password_from_env_and_default_driver(monkeypatch, tmp_path):
    monkeypatch.setattr(services, "project_root", lambda: str(tmp_path))
    monkeypatch.setenv("DB_PASSWORD", "s3gr3d@")
    url = services.database_url(cfg("[DATABASE]\nurl = postgresql://mt5@localhost:5432/mt5\n"))
    assert url.startswith("postgresql+psycopg://mt5:")
    from sqlalchemy.engine import make_url
    assert make_url(url).password == "s3gr3d@"


def test_password_from_project_env_file(monkeypatch, tmp_path):
    monkeypatch.setattr(services, "project_root", lambda: str(tmp_path))
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    (tmp_path / ".env").write_text("DB_PASSWORD='de arquivo'\n", encoding="utf-8")
    from sqlalchemy.engine import make_url
    url = services.database_url(cfg("[DATABASE]\nurl = postgresql+psycopg://mt5@db/mt5\n"))
    assert make_url(url).password == "de arquivo"


def test_password_in_config_is_warned(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(services, "project_root", lambda: str(tmp_path))
    services.database_url(cfg("[DATABASE]\nurl = postgresql://mt5:segredo@db/mt5\n"))
    assert "DB_PASSWORD" in caplog.text and "segredo" not in caplog.text


def test_unreachable_server_is_not_connected_and_hides_password(caplog):
    from mt5_extracao.database_manager import DatabaseManager
    pytest.importorskip("psycopg")
    db = DatabaseManager(db_type="postgresql",
                         db_path="postgresql+psycopg://u:segredo@127.0.0.1:1/x?connect_timeout=2")
    assert not db.is_connected()
    assert "segredo" not in caplog.text
