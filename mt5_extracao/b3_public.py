"""Dados públicos da B3, sem corretora: histórico diário (COTAHIST) e negócios do dia (rapinegocios).

- COTAHIST (``COTAHIST_D<DDMMAAAA>.ZIP`` / ``COTAHIST_A<AAAA>.ZIP``): barras diárias do mercado à
  vista desde os anos 1990, em leiaute fixo de 245 colunas. Os preços não são ajustados por
  proventos ou desdobramentos (é o preço negociado no dia).
- Negócios (``tickercsv/<CÓDIGO>/<AAAA-MM-DD>``): cada negócio de um instrumento (ações e futuros,
  como WINZ26), disponível só para os pregões recentes; vira barras de 1 minuto ou maiores.

Os horários são os da B3 (Brasília), na base ``broker``. Os dados vão para um banco próprio
(``DEFAULT_DB``): a base de dados é vinculada a uma única fonte (``datasets.declare_source``).
"""
import io
import os
import re
import zipfile
from datetime import datetime

import pandas as pd
import requests

DEFAULT_DB = os.path.join("database", "b3_publico.db")
DOWNLOAD_DIR = os.path.join("database", "b3_arquivos")    # arquivos originais, para reimportar
SOURCE = {"provider": "b3_publico", "server": "B3"}
# Relógio da fonte: horário de Brasília (UTC-3, sem horário de verão desde 2019), gravado como publicado.
CLOCK = {"time_basis": "broker", "broker_utc_offset": -3}
TRADES_FIELDS = ("DataNegocio", "CodigoInstrumento", "AcaoAtualizacao", "PrecoNegocio", "QuantidadeNegociada",
                 "HoraFechamento", "CodigoIdentificadorNegocio")
COTAHIST_URL = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/{name}.ZIP"
TRADES_URL = "https://arquivos.b3.com.br/rapinegocios/tickercsv/{ticker}/{day}"
MAX_DOWNLOAD = 70_000_000                      # acima disso o download é manual (política do projeto)
# TPMERC: 010 = vista (lote padrão), 020 = fracionário
MARKETS = {"vista": "010", "fracionario": "020"}

_COLUMNS = ["time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume"]


def _read_member(source):
    """Bytes do arquivo de dados, aceitando o .ZIP baixado ou o arquivo já extraído."""
    raw = source if isinstance(source, bytes) else open(source, "rb").read()
    if raw[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            names = [n for n in archive.namelist() if not n.endswith("/")]
            if len(names) != 1:
                raise ValueError(f"ZIP da B3 com {len(names)} arquivos; esperado 1.")
            return archive.read(names[0])
    return raw


def parse_cotahist(source, symbols=None, market="vista"):
    """Barras diárias por código de negociação: {símbolo: DataFrame no formato do banco}."""
    wanted = {s.upper() for s in symbols} if symbols else None
    tpmerc = MARKETS[market]
    rows = []
    for line in _read_member(source).decode("latin-1").splitlines():
        if not line.startswith("01") or len(line) < 245 or line[24:27] != tpmerc:
            continue
        code = line[12:24].strip()
        if wanted is not None and code not in wanted:
            continue
        price = lambda a, b: int(line[a:b]) / 100  # noqa: E731 - preços com 2 casas implícitas
        rows.append((code, datetime.strptime(line[2:10], "%Y%m%d"), price(56, 69), price(69, 82),
                     price(82, 95), price(108, 121), int(line[147:152]), int(line[152:170])))
    frame = pd.DataFrame(rows, columns=["symbol", "time", "open", "high", "low", "close", "tick_volume",
                                        "real_volume"])
    frame["spread"] = 0
    return {code: group[_COLUMNS].sort_values("time").reset_index(drop=True)
            for code, group in frame.groupby("symbol")}


def parse_trades(source):
    """Negócios válidos de um arquivo ``NEGOCIOSAVISTA``: time, price, quantity, trade_id (+ attrs symbol)."""
    frame = pd.read_csv(io.BytesIO(_read_member(source)), sep=";", dtype=str)
    missing = [c for c in TRADES_FIELDS if c not in frame.columns]
    if missing:
        raise ValueError(f"Arquivo de negócios da B3 sem as colunas {', '.join(missing)} (cabeçalho inválido).")
    cancelled = set(frame.loc[frame["AcaoAtualizacao"] != "0", "CodigoIdentificadorNegocio"])
    frame = frame[(frame["AcaoAtualizacao"] == "0") & ~frame["CodigoIdentificadorNegocio"].isin(cancelled)]
    clock = frame["HoraFechamento"].str.zfill(9)
    time = pd.to_datetime(frame["DataNegocio"] + " " + clock.str[:6] + "." + clock.str[6:],
                          format="%Y-%m-%d %H%M%S.%f")
    symbols = frame["CodigoInstrumento"].unique()
    if len(symbols) > 1:
        raise ValueError(f"Arquivo de negócios com vários instrumentos: {', '.join(symbols)}.")
    trades = pd.DataFrame({"time": time.values,
                         "price": frame["PrecoNegocio"].str.replace(",", ".").astype(float).values,
                         "quantity": frame["QuantidadeNegociada"].astype("int64").values,
                         "trade_id": frame["CodigoIdentificadorNegocio"].values}).sort_values("time", kind="stable")
    trades.attrs["symbol"] = symbols[0] if len(symbols) else ""
    return trades


def trades_to_bars(trades, minutes=1):
    """Barras OHLC de ``minutes`` minutos; minutos sem negócio não viram barra."""
    if trades.empty:
        return pd.DataFrame(columns=_COLUMNS)
    grouped = trades.set_index("time").resample(f"{int(minutes)}min", label="left", closed="left")
    bars = grouped["price"].ohlc()
    bars["tick_volume"] = grouped["price"].count()
    bars["real_volume"] = grouped["quantity"].sum()
    bars = bars[bars["tick_volume"] > 0].reset_index()
    bars["spread"] = 0
    return bars[_COLUMNS]


def download(url, dest_dir, name, *, limit=MAX_DOWNLOAD, timeout=180):
    """Baixa ``url`` para ``dest_dir/name`` recusando arquivos acima de ``limit`` bytes."""
    os.makedirs(dest_dir, exist_ok=True)
    path = os.path.join(dest_dir, name)
    if os.path.exists(path):
        return path                            # já baixado: reimporta sem pedir de novo à B3
    partial = path + ".part"
    try:
        with requests.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()
            size = int(response.headers.get("Content-Length") or 0)
            if size > limit:
                raise ValueError(f"{name} tem {size / 1e6:.0f} MB, acima do limite de {limit / 1e6:.0f} MB: baixe "
                                 f"manualmente de {url} e importe o arquivo local.")
            received = 0
            with open(partial, "wb") as handle:
                for chunk in response.iter_content(1 << 20):
                    received += len(chunk)
                    if received > limit:
                        raise ValueError(f"{name} passou de {limit / 1e6:.0f} MB; baixe manualmente de {url}.")
                    handle.write(chunk)
    except BaseException:
        if os.path.exists(partial):
            os.remove(partial)                 # download interrompido não deixa arquivo pela metade
        raise
    if received == 0:
        os.remove(partial)
        raise ValueError(f"A B3 não tem {name} (resposta vazia: data sem pregão ou fora do período disponível).")
    os.replace(partial, path)
    return path


def cotahist_name(day=None, year=None):
    if year is not None:
        return f"COTAHIST_A{int(year)}"
    return f"COTAHIST_D{day:%d%m%Y}"


def trades_url(ticker, day):
    return TRADES_URL.format(ticker=ticker.upper(), day=f"{day:%Y-%m-%d}")


# Contratos futuros (prefixo + mês + ano): passo de preço e R$ por ponto, como a B3 especifica.
_FUTURES = {"WIN": (5.0, 0.20), "IND": (5.0, 1.00), "WDO": (0.5, 10.0), "DOL": (0.5, 50.0)}
_FUTURE_CODE = re.compile(r"^(WIN|IND|WDO|DOL)[FGHJKMNQUVXZ]\d{2}$")
_STOCK_CODE = re.compile(r"^[A-Z]{4}\d{1,2}$")


def spec_for(code):
    """Especificação do instrumento (``DatabaseManager.SPEC_FIELDS``) ou ``None`` se o código não é conhecido.

    Futuros: 1 contrato por lote, valor do passo = passo × R$ por ponto. Ações e ETFs: lote padrão
    de 100, passo de R$ 0,01. Sem swap; o spread real não vem nos arquivos e fica 0.
    """
    code = code.upper()
    base = {"spread": 0, "swap_long": 0.0, "swap_short": 0.0, "swap_mode": 0, "swap_rollover3days": 0,
            "currency_profit": "BRL"}
    match = _FUTURE_CODE.match(code)
    if match:
        tick, per_point = _FUTURES[match.group(1)]
        return {**base, "point": tick, "digits": 0 if tick >= 1 else 1, "tick_size": tick,
                "tick_value": tick * per_point, "contract_size": per_point, "volume_min": 1.0,
                "volume_step": 1.0, "volume_max": 10000.0}
    if _STOCK_CODE.match(code):
        return {**base, "point": 0.01, "digits": 2, "tick_size": 0.01, "tick_value": 0.01, "contract_size": 1.0,
                "volume_min": 100.0, "volume_step": 100.0, "volume_max": 10_000_000.0}
    return None


def save(db, symbol, timeframe_label, bars):
    """Grava as barras na base B3; ``False`` quando a base recusa (outra fonte, base de tempo)."""
    return db.save_ohlcv_data(symbol, timeframe_label, bars, source=dict(SOURCE))


def business_days(start, end):
    """Dias úteis (seg–sex) de ``start`` a ``end``; feriados aparecem como arquivo vazio."""
    return [d.date() for d in pd.bdate_range(start, end)] if start <= end else []
