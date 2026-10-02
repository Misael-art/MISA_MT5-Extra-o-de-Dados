"""
Coleta contínua de snapshots do book de ofertas (DOM).

    collector = BookCollector(connector, db_manager, interval=1.0)
    resumo = collector.run(["WIN$N"], duration=3600)     # 1 hora; Ctrl+C também encerra

- Assina o book uma vez (market_book_add), lê a cada `interval` segundos e grava em
  <símbolo>_book (time_utc, time_msc, type, price, volume). type: 1 = venda, 2 = compra
  (constantes BOOK_TYPE_* do MT5).
- O MT5 não informa o horário do book: usa-se o relógio do computador, em UTC.
- Grava em lotes a cada `flush_every` snapshots: a memória não cresce com o tempo de coleta.
- Encerra liberando a assinatura (market_book_release), inclusive após erro ou Ctrl+C.
"""
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

log = logging.getLogger(__name__)

BOOK_TIMEFRAME = "book"   # compõe o nome da tabela: win_n_book


class BookCollector:
    def __init__(self, connector, db_manager, interval: float = 1.0, flush_every: int = 30,
                 clock: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep):
        if interval <= 0:
            raise ValueError("o intervalo deve ser maior que zero")
        self.connector = connector
        self.db = db_manager
        self.interval = interval
        self.flush_every = max(1, int(flush_every))
        self.clock = clock
        self.sleep = sleep
        self.stop_event = threading.Event()
        self._buffers: Dict[str, List[dict]] = {}
        self.max_buffered = 0   # maior número de linhas em memória (diagnóstico)

    def table_name(self, symbol: str) -> str:
        return self.db.get_table_name_for_symbol(symbol, BOOK_TIMEFRAME)

    def stop(self):
        self.stop_event.set()

    def _flush(self, summary):
        for symbol, rows in self._buffers.items():
            if rows:
                summary[symbol]["rows"] += self.db.save_book(self.table_name(symbol), rows)
                rows.clear()

    def run(self, symbols: List[str], duration: Optional[float] = None, max_snapshots: Optional[int] = None,
            progress: Optional[Callable[[str], None]] = None) -> Dict[str, dict]:
        """Coleta até `duration` segundos, `max_snapshots` leituras, stop() ou Ctrl+C.
        Retorna por símbolo {'snapshots', 'rows', 'failures', 'subscribed'}."""
        summary = {s: {"snapshots": 0, "rows": 0, "failures": 0, "subscribed": False} for s in symbols}
        active = []
        for symbol in symbols:
            if self.connector.book_subscribe(symbol):
                summary[symbol]["subscribed"] = True
                active.append(symbol)
                self._buffers[symbol] = []
        if not active:
            return summary
        start = self.clock()
        count = 0
        try:
            while not self.stop_event.is_set():
                tick = self.clock()
                now_ms = int(tick * 1000)
                now_text = datetime.fromtimestamp(tick, timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
                for symbol in active:
                    levels = self.connector.book_snapshot(symbol)
                    if levels is None:
                        summary[symbol]["failures"] += 1
                        continue
                    summary[symbol]["snapshots"] += 1
                    self._buffers[symbol].extend(
                        {"time_utc": now_text, "time_msc": now_ms, **level} for level in levels)
                self.max_buffered = max(self.max_buffered, sum(len(b) for b in self._buffers.values()))
                count += 1
                if count % self.flush_every == 0:
                    self._flush(summary)
                    if progress:
                        progress(", ".join(f"{s}: {summary[s]['snapshots']} snapshots" for s in active))
                if max_snapshots is not None and count >= max_snapshots:
                    break
                # Para quando o próximo snapshot cairia no limite ou depois dele
                if duration is not None and self.clock() - start + self.interval >= duration:
                    break
                self.sleep(max(0.0, self.interval - (self.clock() - tick)))
        except KeyboardInterrupt:
            log.info("Coleta do book interrompida pelo usuário.")
        finally:
            self._flush(summary)
            for symbol in active:
                self.connector.book_release(symbol)
        return summary
