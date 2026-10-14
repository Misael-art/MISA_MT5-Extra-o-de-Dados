"""
Calendário econômico do próprio MetaTrader 5 (T16.3a). O MQL5 é o único caminho: o pacote Python do MT5 não tem
funções de calendário. O script `MT5X_CalendarExport.mq5` (pasta `trade_analytics/mql5/`) só lê e grava
`Common\\Files\\MT5X_calendar_<servidor>.csv`.

    install(config)                      # copia para MQL5\\Scripts\\MT5X e compila (MetaEditor)
    write_request(config, 1095, 30, "BR,US", 0)
    start(config)                        # abre o terminal executando o script (o MT5 precisa estar fechado)
    path = find_export(config); rows = read_export(path)

Com o MT5 já aberto: no Navegador, Scripts > MT5X > MT5X_CalendarExport (arrastar para um gráfico). O script lê o
mesmo pedido.
"""
import logging
import shutil
from pathlib import Path
from typing import List, Optional

import pandas as pd

log = logging.getLogger(__name__)

SCRIPT = Path(__file__).resolve().parents[1] / "mql5" / "MT5X_CalendarExport.mq5"
SCRIPT_FOLDER = "MT5X"
REQUEST_FILE = "MT5X_calendar_request.txt"
EXPORT_GLOB = "MT5X_calendar_*.csv"


def install(config, mt5=None, run=None) -> dict:
    """Copia o script para a pasta de dados do MT5 e compila. Devolve {"source", "compiled", "ok", "errors"}."""
    from mt5_extracao.robots import mt5_tools
    data = mt5_tools.data_dir(config, mt5)
    target_dir = Path(data) / "MQL5" / "Scripts" / SCRIPT_FOLDER
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / SCRIPT.name
    shutil.copy2(SCRIPT, target)
    result = mt5_tools.compile_source(target, mt5_tools.find_metaeditor(config), mt5_tools.wine_prefix(config), run)
    return {"source": str(target), "compiled": result.compiled or str(target.with_suffix(".ex5")), "ok": bool(result.ok),
            "errors": [str(m) for m in result.messages if m.kind == "error"], "summary": result.summary}


def write_request(config, days_back: int, days_forward: int, countries: str = "", min_importance: int = 0) -> Path:
    from mt5_extracao.robots import mt5_tools
    folder = mt5_tools.common_files_dir(config)
    if folder is None:
        raise FileNotFoundError("Pasta Common\\Files do MetaTrader não encontrada. Abra o MT5 uma vez.")
    path = folder / REQUEST_FILE
    path.write_text(f"days_back={int(days_back)}\r\ndays_forward={int(days_forward)}\r\n"
                    f"countries={countries.strip()}\r\nmin_importance={int(min_importance)}\r\n", encoding="ascii")
    return path


def startup_ini() -> str:
    """[StartUp] com Script= (documentação do MT5): o terminal abre, roda o script uma vez e segue aberto."""
    return "\r\n".join(["[StartUp]", f"Script={SCRIPT_FOLDER}\\{SCRIPT.stem}"]) + "\r\n"


def start(config, mt5=None, spawn=None, is_running=None) -> Path:
    from mt5_extracao.robots import mt5_tools
    return mt5_tools.start_terminal(config, mt5_tools.data_dir(config, mt5), startup_ini(), "mt5x_calendar",
                                    spawn=spawn, is_running=is_running)


def find_export(config) -> Optional[Path]:
    """A exportação mais recente em Common\\Files (None se não houver)."""
    from mt5_extracao.robots import mt5_tools
    folder = mt5_tools.common_files_dir(config)
    if folder is None:
        return None
    files = sorted(folder.glob(EXPORT_GLOB), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def read_export(path) -> List[dict]:
    """Linhas do CSV do script para `calendar.save`. A primeira linha (comentário) traz a diferença servidor × UTC."""
    from mt5_extracao.robots import mql
    lines = [line for line in mql.read_text(path).splitlines() if line.strip()]
    offset = None
    if lines and lines[0].startswith("#"):
        if "server_utc_offset_sec=" in lines[0]:
            try:
                offset = int(lines[0].split("=", 1)[1].strip())
            except ValueError:
                offset = None
        lines = lines[1:]
    if not lines:
        return []
    header = [h.strip() for h in lines[0].split(";")]
    rows = []
    for line in lines[1:]:
        cells = dict(zip(header, (c.strip() for c in line.split(";"))))
        if not cells.get("time"):
            continue
        number = lambda name: float(cells[name]) if cells.get(name) not in (None, "") else None  # noqa: E731
        rows.append({"event_time": pd.Timestamp(cells["time"].replace(".", "-")), "event_id": cells.get("event_id"),
                     "name": cells.get("name") or None, "country": cells.get("country") or None,
                     "currency": (cells.get("currency") or "").upper() or None, "symbol": None,
                     "impact": int(cells.get("importance") or 0), "actual": number("actual"),
                     "forecast": number("forecast"), "previous": number("previous")})
    if offset is not None:
        log.info(f"Calendário do MT5: horário do servidor (UTC{offset / 3600:+.1f}).")
    return rows
