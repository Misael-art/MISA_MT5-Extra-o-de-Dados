from sqlalchemy.engine import make_url

from tests.postgres_helpers import _schema_scoped_url


def test_schema_scoped_url_preserves_options_and_uses_only_test_schema():
    raw = ("postgresql+psycopg://user:password@localhost/mt5x_test"
           "?application_name=mt5x-tests&options=-cstatement_timeout%3D5000")
    scoped = make_url(_schema_scoped_url(raw, "mt5x_test_abcd"))

    assert scoped.password == "password"
    assert scoped.query["application_name"] == "mt5x-tests"
    assert scoped.query["options"] == "-cstatement_timeout=5000 -csearch_path=mt5x_test_abcd"
