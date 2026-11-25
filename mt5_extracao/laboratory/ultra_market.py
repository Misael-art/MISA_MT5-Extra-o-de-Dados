"""Fita bruta do broker para qualificação do Ultra, preservada em blocos verificáveis."""
import csv
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import file_digest
from itertools import zip_longest
import json
import math
from pathlib import Path
import re
from string import Template
from struct import Struct
from tempfile import TemporaryDirectory

from ..experiments.schema import digest


SCHEMA = "mt5x-ultra-market/1"
TICK_FIELDS = ("time_msc", "bid", "ask", "last", "volume", "flags", "volume_real")
SYMBOL_FIELDS = (
    "digits", "point", "trade_tick_size", "trade_tick_value", "trade_tick_value_profit",
    "trade_tick_value_loss", "trade_contract_size", "volume_min", "volume_max", "volume_step",
    "volume_limit", "currency_base", "currency_profit", "currency_margin", "spread_float",
    "trade_calc_mode", "trade_exemode", "filling_mode", "order_mode", "trade_mode", "trade_stops_level",
    "trade_freeze_level", "swap_mode", "swap_long", "swap_short", "swap_rollover3days",
    "margin_initial", "margin_maintenance", "margin_hedged", "margin_hedged_use_leg",
)
ACCOUNT_FIELDS = ("currency", "currency_digits", "leverage", "margin_mode")
PROBE_RECORD = Struct("<qdddqId")
PROBE_MARKER = "MT5X_ULTRA_MARKET|"
FIELD_TYPES = (int, float, float, float, int, int, float)


def _axis(value):
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is not None or dt.microsecond:
        raise ValueError("Use segundos inteiros no relógio do MT5, sem converter o horário em UTC.")
    return int(dt.replace(tzinfo=timezone.utc).timestamp())


def _sha(path):
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def _fields(obj, names):
    return {name: getattr(obj, name, None) for name in names}


def _demo(backend, expected_server):
    info, terminal = backend.account_info(), backend.terminal_info()
    if (info is None or terminal is None or not terminal.connected
            or info.trade_mode != backend.ACCOUNT_TRADE_MODE_DEMO
            or info.server != expected_server):
        raise ValueError("Abra a conta DEMO no servidor selecionado e confirme a conexão no MT5.")
    return info, terminal


def _price_atoms(price, tick_size):
    nearest = round(Decimal(str(price)) / tick_size)
    grid = float(nearest * tick_size)
    # A DLL expõe double, inclusive valores a um ULP do decimal da mesma cotação.
    return nearest if abs(grid - price) <= max(math.ulp(grid), math.ulp(price)) else None


class Coverage:
    def __init__(self, tick_size):
        self.tick_size = tick_size
        self.last_msc = None
        self.first_msc = None
        self.last_minute = None
        self.sequence = 0
        self.rows_at_msc = set()
        self.counts = dict(rows=0, executable=0, crossed=0, missing_quote=0, duplicate=0, same_ms=0,
                           positive_last=0, positive_reported_volume=0, trade_flag=0,
                           observed_quote_minutes=0, off_grid=0)
        self.longest_gap = None
        self.spread_min = self.spread_max = None
        self.spread_sum = 0
        self.spread_count = 0

    def add(self, row, trade_flags):
        msc, bid, ask, last, volume, flags, volume_real = row
        counts = self.counts
        if msc == self.last_msc:
            self.sequence += 1
            counts["same_ms"] += 1
        else:
            if self.last_msc is not None:
                if msc < self.last_msc:
                    raise ValueError("O MT5 retornou ticks fora da ordem original.")
                gap = msc - self.last_msc
                if self.longest_gap is None or gap > self.longest_gap["duration_msc"]:
                    self.longest_gap = dict(from_msc=self.last_msc, to_msc=msc, duration_msc=gap)
            self.rows_at_msc.clear()
            self.sequence = 0
        if row in self.rows_at_msc:
            counts["duplicate"] += 1
        self.rows_at_msc.add(row)
        if self.first_msc is None:
            self.first_msc = msc
        self.last_msc = msc
        counts["rows"] += 1
        counts["trade_flag"] += bool(trade_flags & flags)
        counts["positive_last"] += last > 0
        counts["positive_reported_volume"] += volume > 0 or volume_real > 0
        if bid <= 0 or ask <= 0:
            counts["missing_quote"] += 1
        elif bid > ask:
            counts["crossed"] += 1
        else:
            counts["executable"] += 1
            minute = msc // 60000
            if self.last_minute != minute:
                counts["observed_quote_minutes"] += 1
                self.last_minute = minute
            if self.tick_size is not None:
                b, a = (_price_atoms(price, self.tick_size) for price in (bid, ask))
                if b is None or a is None:
                    counts["off_grid"] += 1
                else:
                    spread = a - b
                    self.spread_min = spread if self.spread_min is None else min(spread, self.spread_min)
                    self.spread_max = spread if self.spread_max is None else max(spread, self.spread_max)
                    self.spread_sum += spread
                    self.spread_count += 1
        return self.sequence

    def summary(self):
        return dict(self.counts, first_msc=self.first_msc, last_msc=self.last_msc,
                    longest_gap=self.longest_gap, spread_ticks=dict(min=self.spread_min, max=self.spread_max,
                        sum=self.spread_sum, count=self.spread_count))


def capture(backend, symbol, start, end, destination, *, expected_server, chunk_minutes=15):
    """Lê uma sessão DEMO já conectada, sem alterar conta, iniciar Expert ou enviar pedidos."""
    first, last = _axis(start), _axis(end)
    if not 0 < first < last or type(chunk_minutes) is not int or not 1 <= chunk_minutes <= 60:
        raise ValueError("Informe um intervalo crescente e blocos de 1 a 60 minutos.")
    target = Path(destination)
    if target.exists() or target.is_symlink():
        raise FileExistsError("Escolha um destino novo para manter o snapshot anterior.")
    account, terminal = _demo(backend, expected_server)
    instrument = backend.symbol_info(symbol)
    if (instrument is None or instrument.name != symbol or instrument.custom
            or not backend.symbol_select(symbol, True)):
        raise ValueError("Selecione no MT5 o símbolo exato oferecido pelo servidor DEMO.")
    info = _fields(instrument, SYMBOL_FIELDS)
    tick_size = Decimal(str(info["trade_tick_size"])) if info["trade_tick_size"] else None
    current_tick = backend.symbol_info_tick(symbol)
    clock_observed = datetime.now(timezone.utc)
    current_msc = getattr(current_tick, "time_msc", None)
    trade_flags = backend.TICK_FLAG_LAST | backend.TICK_FLAG_VOLUME | backend.TICK_FLAG_BUY | backend.TICK_FLAG_SELL
    manifest = dict(schema=SCHEMA, symbol=symbol, source=dict(provider="mt5", server=expected_server,
        origin="observed_broker_ticks", mode="demo", terminal_build=terminal.build, copy_mode="COPY_TICKS_ALL"),
        time=dict(start_inclusive=start, end_exclusive=end, basis="mt5_raw_clock", utc_offset="unproven",
                  utc_clock_sample=clock_observed.isoformat(), last_tick_msc_at_sample=current_msc,
                  start_msc=first * 1000, end_msc=last * 1000),
        account=_fields(account, ACCOUNT_FIELDS),
        contract=dict(observed_at_utc=clock_observed.isoformat(), current=info,
            historical=dict(status="unproven", commission=None, swap=None, margin=None, conversion=None,
                            session_schedule=None)), blocks=[])
    coverage = Coverage(tick_size if tick_size and tick_size.is_finite() and tick_size > 0 else None)
    with TemporaryDirectory(prefix=".ultra-market-", dir=target.parent) as temporary:
        stage = Path(temporary) / "snapshot"
        stage.mkdir(mode=0o700)
        for a in range(first, last, chunk_minutes * 60):
            b = min(a + chunk_minutes * 60, last)
            current, _ = _demo(backend, expected_server)
            if current.login != account.login:
                raise ValueError("A conta mudou durante a coleta da fita.")
            ticks = backend.copy_ticks_range(symbol, a, b, backend.COPY_TICKS_ALL)
            if ticks is None:
                raise RuntimeError(f"O MT5 não retornou o bloco [{a * 1000}, {b * 1000}).")
            name = f"{a}-{b}.csv"
            path = stage / name
            rows, low, high = 0, None, None
            with path.open("x", encoding="ascii", newline="") as stream:
                writer = csv.writer(stream, lineterminator="\n")
                writer.writerow(("time_msc", "seq", *TICK_FIELDS[1:]))
                for tick in ticks:
                    msc = int(tick["time_msc"])
                    if msc == b * 1000:
                        continue
                    if not a * 1000 <= msc < b * 1000:
                        raise ValueError("O MT5 retornou uma cotação fora dos limites do bloco.")
                    row = (msc, float(tick["bid"]), float(tick["ask"]), float(tick["last"]),
                           int(tick["volume"]), int(tick["flags"]), float(tick["volume_real"]))
                    if not all(math.isfinite(value) for value in row):
                        raise ValueError("O MT5 retornou um valor de tick não finito.")
                    seq = coverage.add(row, trade_flags)
                    writer.writerow((msc, seq, *row[1:]))
                    low, high = msc if low is None else low, msc
                    rows += 1
            manifest["blocks"].append(dict(file=name, start_msc=a * 1000, end_msc=b * 1000,
                first_msc=low, last_msc=high, rows=rows, sha256=_sha(path)))
        current, _ = _demo(backend, expected_server)
        if current.login != account.login:
            raise ValueError("A conta mudou durante a coleta da fita.")
        manifest["coverage"] = coverage.summary()
        manifest["coverage"]["clock_minutes_in_window"] = (last + 59) // 60 - first // 60
        manifest["qualification"] = ("no_observed_quotes" if not coverage.counts["executable"]
                                     else "requires_clock_session_and_economic_evidence")
        manifest["manifest_hash"] = digest(manifest)
        (stage / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, allow_nan=False,
                                                       indent=2) + "\n", encoding="utf-8")
        stage.rename(target)
    return manifest


def verify(source):
    """Confere identidade e continuidade dos blocos antes de entregar a fita a um consumidor."""
    source = Path(source)
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    expected = manifest.pop("manifest_hash")
    if manifest["schema"] != SCHEMA or digest(manifest) != expected:
        raise ValueError("Identidade do manifesto de mercado divergente.")
    cursor = manifest["time"]["start_msc"]
    for block in manifest["blocks"]:
        name = f'{block["start_msc"] // 1000}-{block["end_msc"] // 1000}.csv'
        if (block["file"] != name or cursor != block["start_msc"] or cursor >= block["end_msc"]
                or _sha(source / name) != block["sha256"]):
            raise ValueError("A fita diverge do índice de blocos congelado.")
        cursor = block["end_msc"]
    if cursor != manifest["time"]["end_msc"]:
        raise ValueError("O índice não cobre todo o período congelado.")
    return dict(manifest, manifest_hash=expected)


def capture_configured(config, config_path, symbol, start, end, destination, *, expected_server, chunk_minutes=15):
    """Anexa a ponte ao terminal configurado que o operador já abriu."""
    from ..mt5_backend import get_mt5
    from ..robots import mt5_tools

    if not mt5_tools.terminal_running(config):
        raise ValueError("Abra a conta DEMO no terminal configurado antes da coleta.")
    terminal = mt5_tools.validate_terminal(config)
    prefix = mt5_tools.wine_prefix(config)
    data = mt5_tools.data_dir(config)
    portable = prefix is not None or terminal.parent.resolve() == data.resolve()
    backend = get_mt5(config_path)
    if backend is None or not backend.initialize(path=mt5_tools._windows_path(terminal, prefix), portable=portable):
        raise RuntimeError("A ponte não conseguiu anexar o terminal MT5 configurado.")
    _, selected = _demo(backend, expected_server)
    if selected.data_path.casefold() != mt5_tools._windows_path(data, prefix).casefold():
        raise ValueError("O terminal conectado usa outra pasta de dados.")
    return capture(backend, symbol, start, end, destination, expected_server=expected_server,
                   chunk_minutes=chunk_minutes)


def export_probe(source, destination, *, attempt):
    """Gera um Expert passivo, vinculado à fita congelada e a esta tentativa do Tester."""
    manifest = verify(source)
    symbol = manifest["symbol"]
    if not re.fullmatch(r"[A-Za-z0-9._#-]{1,31}", symbol) or not re.fullmatch(r"[a-f0-9]{32}", attempt):
        raise ValueError("Símbolo ou identificador de execução inválido para o Tester.")
    if not manifest["coverage"]["executable"]:
        raise ValueError("A fita de mercado não contém cotações executáveis.")
    binding = digest([manifest["manifest_hash"], attempt])
    template = Path(__file__).resolve().parents[1] / "robots" / "templates" / "UltraMarketProbe.mq5.tmpl"
    expert = Template(template.read_text(encoding="utf-8")).substitute(manifest=manifest["manifest_hash"],
        binding=binding, symbol=symbol, start=manifest["time"]["start_msc"], end=manifest["time"]["end_msc"])
    target = Path(destination)
    with target.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(expert)
    return dict(manifest_hash=manifest["manifest_hash"], symbol=symbol, binding=binding, source_sha256=_sha(target),
                tape=f"MT5XUltraMarket_{binding}.bin", tester_model="4", orders=False)


def export_demo_observer(source, destination, *, attempt):
    manifest = verify(source)
    if (manifest["symbol"] != "EURUSD" or manifest["source"]["server"] != "MetaQuotes-Demo"
            or not re.fullmatch(r"[a-f0-9]{32}", attempt)):
        raise ValueError("A observação é vinculada à fita EURUSD da MetaQuotes-Demo.")
    key = manifest["manifest_hash"]
    binding = digest([key, attempt, "demo_observe"])
    template = Path(__file__).resolve().parents[1] / "robots" / "templates" / "UltraMarketDemoObserve.mq5.tmpl"
    target = Path(destination)
    with target.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(Template(template.read_text(encoding="utf-8")).substitute(manifest=key, binding=binding))
    return dict(manifest_hash=key, binding=binding, source_sha256=_sha(target), symbol="EURUSD",
                policy="economics_and_holdout_unverified", orders=False)


def _csv_rows(source, manifest):
    for block in manifest["blocks"]:
        with (source / block["file"]).open("r", encoding="ascii", newline="") as stream:
            for row in csv.DictReader(stream):
                yield tuple(parse(row[field]) for parse, field in zip(FIELD_TYPES, TICK_FIELDS))


def _probe_rows(path):
    with path.open("rb") as stream:
        while data := stream.read(PROBE_RECORD.size):
            if len(data) != PROBE_RECORD.size:
                raise ValueError("A gravação do Tester acaba no meio de um tick.")
            yield PROBE_RECORD.unpack(data)


def diagnostics(source):
    """Recalcula a grade sem mudar a fita ou seu manifesto de aquisição."""
    source = Path(source)
    manifest = verify(source)
    declared = manifest["contract"]["current"]["trade_tick_size"]
    coverage = Coverage(Decimal(str(declared)) if declared else None)
    for row in _csv_rows(source, manifest):
        coverage.add(row, 8 | 16 | 32 | 64)
    return dict(snapshot=manifest["manifest_hash"], coverage=coverage.summary(),
                quote_grid_rule="nearest_tick_at_most_one_binary_ulp")


def compare_probe(source, tape, *, binding, agent_journal):
    """Compara o fluxo entregue em OnTick com a fita solicitada ao broker."""
    source, tape = Path(source), Path(tape)
    manifest = verify(source)
    records = [line.partition(PROBE_MARKER)[2].split("|")
               for line in agent_journal.splitlines() if PROBE_MARKER in line]
    if (len(records) != 2 or len(records[0]) != 6 or len(records[1]) != 5
            or records[0][:4] != ["BEGIN", manifest["manifest_hash"], binding, manifest["symbol"]]
            or records[1][:2] != ["END", binding]):
        raise ValueError("O journal deve vincular uma única execução do agente à fita e à tentativa.")
    _, _, _, _, build, mode = records[0]
    _, _, declared, ended, failed = records[1]
    if ended not in ("0", "1") or failed not in ("0", "1"):
        raise ValueError("O fechamento da gravação do Tester é inválido.")
    missing = object()
    first, equal_prefix = None, 0
    differences, flag_xors = Counter(), Counter()
    actual = expected = 0
    for i, (want, got) in enumerate(zip_longest(_csv_rows(source, manifest), _probe_rows(tape), fillvalue=missing)):
        actual += got is not missing
        expected += want is not missing
        if want is missing or got is missing:
            differences["tick"] += 1
            if first is None:
                first = dict(ordinal=i, field="tick", expected=None if want is missing else want,
                             observed=None if got is missing else got)
            continue
        fields = [key for key, a, b in zip(TICK_FIELDS, want, got) if a != b]
        differences.update(fields)
        if "flags" in fields:
            flag_xors[hex(want[5] ^ got[5])] += 1
        if fields and first is None:
            first = dict(ordinal=i, field=fields[0], expected=dict(zip(TICK_FIELDS, want)),
                         observed=dict(zip(TICK_FIELDS, got)))
        if first is None:
            equal_prefix += 1
    complete = actual == int(declared) and ended == "1" and failed == "0"
    status = "incomplete" if not complete else ("diverged" if first else "tick_stream_matches")
    return dict(status=status, manifest_hash=manifest["manifest_hash"], binding=binding,
                tape_sha256=_sha(tape), terminal_build=int(build), account_margin_mode=int(mode),
                first_divergence=first, expected_ticks=expected, observed_ticks=actual,
                matching_prefix=equal_prefix, passed_end=ended == "1", mismatch_counts=dict(differences),
                changed_flag_bits=dict(flag_xors), quotes_exact=bool(complete and actual
                    and not set(differences).difference({"flags"})), order_parity="not_exercised")
