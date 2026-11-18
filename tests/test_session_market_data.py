"""Cópia do histórico do terminal principal para a sessão nativa."""


def test_locked_market_data_stops_with_close_terminal_action():
    """Windows físico: com o terminal principal aberto, a cópia do .hcc falha por permissão."""
    import shutil
    import pytest
    from mt5_extracao.robots import sessions
    locked = shutil.Error([("C:/MT5/bases/MetaQuotes-Demo/history/EURUSD/2026.hcc", "E:/s/2026.hcc",
                            "[Errno 13] Permission denied")])
    for error in (locked, PermissionError(13, "Permission denied")):
        def copy(error=error):
            raise error
        with pytest.raises(RuntimeError, match="--close-terminal"):
            sessions._copy_market_data(copy)
    copied = []
    sessions._copy_market_data(lambda: copied.append(True))
    assert copied == [True]
