# Scripts manuais

Scripts de diagnóstico antigos que exigem o **MetaTrader 5 real** (Windows) e por isso não fazem parte dos
testes automatizados (`tests/`). Execute a partir da raiz do projeto:

```
set PYTHONPATH=.            &  .venv\Scripts\python scripts\manual\test_mt5_connection.py     (Windows)
PYTHONPATH=. .venv/bin/python scripts/manual/test_mt5_connection.py                            (Linux)
```

Para o diagnóstico do dia a dia use `python -m mt5_extracao.bootstrap doctor`.
