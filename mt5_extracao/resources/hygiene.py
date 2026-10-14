"""
Higienização (T15.2): quais séries de barras são candidatas a sair da base, por quê, e quais estão protegidas.
Só leitura. Quem arquiva é `actions.archive_tables`.

Regras (registro `RULES`, aberto para novas frentes): cada uma recebe a série e o contexto e devolve o motivo
(texto) ou None. Proteções vencem as regras: série protegida nunca é candidata.

    found = scan(db, policy, selected=["PETR4"])
    found["candidates"]  # [{table, symbol, timeframe, rows, bytes, end, reasons: {código: texto}}]
    found["protected"]   # [{table, symbol, ..., protections: [texto]}]
    found["orphans"]     # {"symbol_tables": [...], "extraction_log": [...]}
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Optional

from sqlalchemy import inspect, text

from mt5_extracao import timeframes

log = logging.getLogger(__name__)

LABELS = {
    "sem_lucro": "Sem resultado positivo",
    "sem_estudo": "Não usado em estudos",
    "obsoleta": "Obsoleta",
    "vazia": "Vazia",
}


@dataclass
class Context:
    now: datetime
    obsolete_days: int
    favorites: set = field(default_factory=set)
    selected: set = field(default_factory=set)
    benchmarks: dict = field(default_factory=dict)       # ativo -> índice vinculado
    cells: dict = field(default_factory=dict)            # (ativo, timeframe) -> {robô: (lucro, retorno)}
    studied: set = field(default_factory=set)            # ativos com backtest ou validação

    @property
    def benchmark_targets(self) -> set:
        return {str(v).upper() for v in self.benchmarks.values() if v}


def _tf_name(label: str) -> Optional[str]:
    tf = timeframes.parse(label) if label else None
    return tf.name if tf else None


def _rule_no_profit(item, ctx: Context) -> Optional[str]:
    robots = ctx.cells.get((item["symbol"], _tf_name(item["timeframe"])))
    if robots and all(net <= 0 for net, _ in robots.values()):
        return f"{len(robots)} robô(s) testado(s) nesta série, nenhum com lucro no backtest mais recente"
    return None


def _rule_unstudied(item, ctx: Context) -> Optional[str]:
    if item["symbol"] not in ctx.studied:
        return "nenhum backtest ou validação neste ativo"
    return None


def _rule_obsolete(item, ctx: Context) -> Optional[str]:
    end = item.get("end")
    if end is None:
        return None
    age = (ctx.now - end).days
    if age > ctx.obsolete_days:
        return f"última barra há {age} dias (limite: {ctx.obsolete_days})"
    return None


def _rule_empty(item, ctx: Context) -> Optional[str]:
    return "tabela sem linhas" if not item.get("rows") else None


RULES: Dict[str, Callable] = {
    "sem_lucro": _rule_no_profit,
    "sem_estudo": _rule_unstudied,
    "obsoleta": _rule_obsolete,
    "vazia": _rule_empty,
}


def protections(item, ctx: Context) -> List[str]:
    symbol = item["symbol"]
    out = []
    if symbol in ctx.favorites:
        out.append("favorito")
    if symbol in ctx.selected:
        out.append("na seleção")
    if ctx.benchmarks.get(symbol):
        out.append(f"benchmark vinculado ({ctx.benchmarks[symbol]})")
    if symbol.upper() in ctx.benchmark_targets:
        out.append("usado como benchmark de outro ativo")
    robots = ctx.cells.get((symbol, _tf_name(item["timeframe"]))) or {}
    winners = [key for key, (net, _ret) in robots.items() if net > 0]
    if winners:
        out.append(f"backtest lucrativo ({', '.join(sorted(winners)[:3])})")
    return out


def build_context(db, obsolete_days: int, selected: Iterable[str] = (), now: Optional[datetime] = None) -> Context:
    """Lê favoritos, benchmarks, backtests e validações da base (o que faltar fica vazio)."""
    from mt5_extracao import asset_table
    from mt5_extracao.robots import store
    ctx = Context(now=now or datetime.now(), obsolete_days=obsolete_days, selected=set(selected or ()))
    try:
        prefs = asset_table.load_prefs(db) or {}
        ctx.favorites = set(prefs.get("favorites", []))
        ctx.benchmarks = dict(prefs.get("benchmarks", {}))
    except Exception as e:
        log.warning(f"Favoritos e benchmarks não lidos: {e}")
    try:
        runs = store.load_runs(db)
        ctx.cells = asset_table.profit_cells(runs)
        ctx.studied = {r["symbol"] for r in runs}
    except Exception as e:
        log.warning(f"Backtests não lidos: {e}")
    try:
        if inspect(db.engine).has_table("_strategy_validation"):
            with db.engine.connect() as conn:
                ctx.studied |= {r[0] for r in conn.execute(text("SELECT DISTINCT symbol FROM _strategy_validation"))}
    except Exception as e:
        log.warning(f"Validações não lidas: {e}")
    return ctx


def classify(inventory: List[dict], ctx: Context, sizes: Optional[Dict[str, int]] = None,
             rules: Optional[Iterable[str]] = None) -> dict:
    """Separa o inventário em candidatas (com motivos) e protegidas. `rules` limita as regras usadas."""
    active = {code: RULES[code] for code in (rules or RULES) if code in RULES}
    sizes = sizes or {}
    candidates, protected = [], []
    for item in inventory:
        row = {k: item.get(k) for k in ("table", "symbol", "timeframe", "rows", "start", "end")}
        row["bytes"] = sizes.get(item["table"])
        guards = protections(item, ctx)
        reasons = {}
        for code, rule in active.items():
            try:
                reason = rule(item, ctx)
            except Exception as e:      # uma regra quebrada não derruba a análise
                log.error(f"Regra de higienização {code} falhou em {item['table']}: {e}")
                reason = None
            if reason:
                reasons[code] = reason
        if guards:
            if reasons:
                protected.append({**row, "protections": guards, "reasons": reasons})
        elif reasons:
            candidates.append({**row, "reasons": reasons})
    candidates.sort(key=lambda c: (-(c["bytes"] or 0), c["table"]))
    return {"candidates": candidates, "protected": protected}


def find_orphans(db) -> dict:
    """Registros de controle que apontam para tabelas que não existem mais."""
    out = {"symbol_tables": [], "extraction_log": []}
    try:
        inspector = inspect(db.engine)
        existing = set(inspector.get_table_names())
        with db.engine.connect() as conn:
            if "_symbol_tables" in existing:
                out["symbol_tables"] = sorted(r[0] for r in conn.execute(
                    text("SELECT table_name FROM _symbol_tables")) if r[0] not in existing)
            if "_extraction_log" in existing:
                out["extraction_log"] = sorted(r[0] for r in conn.execute(
                    text("SELECT DISTINCT table_name FROM _extraction_log")) if r[0] not in existing)
    except Exception as e:
        log.error(f"Não foi possível procurar registros órfãos: {e}")
    return out


def scan(db, policy, selected: Iterable[str] = (), sizes: Optional[Dict[str, int]] = None,
         inventory: Optional[List[dict]] = None, now: Optional[datetime] = None,
         rules: Optional[Iterable[str]] = None) -> dict:
    """Análise completa: candidatas, protegidas e órfãos. `sizes` (tabela -> bytes) vem de storage.measure."""
    if db is None or getattr(db, "engine", None) is None:
        return {"candidates": [], "protected": [], "orphans": {"symbol_tables": [], "extraction_log": []}}
    inventory = db.data_inventory() if inventory is None else inventory
    ctx = build_context(db, policy.obsolete_days, selected, now)
    result = classify(inventory, ctx, sizes, rules)
    result["orphans"] = find_orphans(db)
    result["scanned_at"] = ctx.now
    return result
