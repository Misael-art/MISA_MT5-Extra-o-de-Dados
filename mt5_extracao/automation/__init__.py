"""Automação local por contratos explícitos, sem dependência da interface gráfica."""

PROTOCOL_VERSION = 1


def open_workflow(db, config, *, read_account=None, config_path=None, db_override=None):
    from .service import Workflow
    return Workflow(db, config, read_account=read_account, config_path=config_path, db_override=db_override)


__all__ = ["PROTOCOL_VERSION", "open_workflow"]
