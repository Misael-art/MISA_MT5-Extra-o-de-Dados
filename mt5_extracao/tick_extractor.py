"""
Extração de ticks (bid/ask/last) por blocos, com retomada.

    extractor = TickExtractor(connector, db_manager)
    resultado = extractor.extract("WIN$N", datetime(2024, 1, 2), datetime(2024, 1, 31))

- Blocos de chunk_hours (padrão 24 h) gravados assim que chegam, na tabela <símbolo>_ticks
  (chave time_msc + seq; ver DatabaseManager.save_ticks).
- Cada bloco é registrado em _extraction_log ('ok', 'empty' ou 'failed'); numa nova execução os
  blocos concluídos são pulados e só os que falharam são buscados de novo.
- Blocos consecutivos compartilham a borda (o MT5 recebe segundos inteiros pela ponte); a chave
  primária evita duplicatas.
"""
import logging
import time
from datetime import datetime, timedelta
from typing import Callable, Optional

log = logging.getLogger(__name__)

TICKS_TIMEFRAME = "ticks"   # compõe o nome da tabela: win_n_ticks


class TickExtractor:
    def __init__(self, connector, db_manager, chunk_hours: int = 24, max_retries: int = 3,
                 retry_delay: float = 1.0):
        self.connector = connector
        self.db = db_manager
        self.chunk = timedelta(hours=max(1, int(chunk_hours)))
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def table_name(self, symbol: str) -> str:
        return self.db.get_table_name_for_symbol(symbol, TICKS_TIMEFRAME)

    def _blocks(self, start: datetime, end: datetime):
        cur = start
        while cur < end:
            block_end = min(cur + self.chunk, end)
            yield cur, block_end
            cur = block_end

    def _fetch(self, symbol, a, b):
        delay = self.retry_delay
        for attempt in range(1, self.max_retries + 1):
            df = self.connector.get_ticks_range(symbol, a, b)
            if df is not None:
                return df
            log.warning(f"[{symbol}] ticks {a:%Y-%m-%d %H:%M}–{b:%H:%M}: tentativa {attempt}/{self.max_retries} falhou")
            if attempt < self.max_retries:
                time.sleep(delay)
                delay *= 2
        return None

    def extract(self, symbol: str, start: datetime, end: datetime, overwrite: bool = False,
                progress: Optional[Callable[[int, str], None]] = None) -> dict:
        """Retorna {'ok', 'empty', 'failed', 'skipped', 'rows'} (contagem de blocos e de ticks gravados)."""
        if end <= start:
            raise ValueError("a data final deve ser posterior à inicial")
        table = self.table_name(symbol)
        done = set() if overwrite else self.db.completed_blocks(table)
        blocks = list(self._blocks(start, end))
        result = {"ok": 0, "empty": 0, "failed": 0, "skipped": 0, "rows": 0}
        for i, (a, b) in enumerate(blocks, 1):
            pct = int(100 * i / len(blocks))
            if (a, b) in done:
                result["skipped"] += 1
                continue
            df = self._fetch(symbol, a, b)
            if df is None:
                self.db.record_block(table, a, b, 0, "failed", "mt5")
                result["failed"] += 1
                msg = f"{symbol}: falha nos ticks de {a:%Y-%m-%d %H:%M} (será tentado de novo na próxima execução)"
            elif df.empty:
                self.db.record_block(table, a, b, 0, "empty", "mt5")
                result["empty"] += 1
                msg = f"{symbol}: sem ticks em {a:%Y-%m-%d %H:%M}"
            else:
                rows = self.db.save_ticks(table, df)
                self.db.record_block(table, a, b, rows, "ok", "mt5")
                result["ok"] += 1
                result["rows"] += rows
                msg = f"{symbol}: {rows} ticks em {a:%Y-%m-%d %H:%M}"
            log.info(msg)
            if progress:
                progress(pct, msg)
        return result
