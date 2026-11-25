"""Vincula as referências MQL5 e seus motores ao mesmo passe de carteira."""
from pathlib import Path, PurePosixPath
from decimal import Decimal
from posixpath import relpath
import re
from string import Template

from ..analytics.hypotheses import compile_definition
from ..laboratory import ultra_market, ultra_pattern_native
from ..laboratory.ultra_catalog import MARKET_NATIVE, MARKET_PATTERN
from ..experiments.schema import digest
from .catalog import library_dirs
from .ultra_runner import _sha


MARKER = "MT5X_ULTRA_PORTFOLIO|"
RISK_PCT = "0.5"
CAP_PCT = "1.0"
_TEMPLATES = Path(__file__).with_name("templates")
_NATIVE = PurePosixPath("Framework/HADOUKEN")
_ENGINE = _NATIVE / "Strategies/HDK_StrategyRobot.mqh"
_OBSERVATION = _ENGINE.with_name("HDK_StrategyObservation.mqh")
_EXECUTOR = _NATIVE / "Safety/HDK_SafeExecutor.mqh"
_INCLUDE = re.compile(r'^(\s*#include\s+)(?:<([^>]+)>|"([^"]+)")', re.MULTILINE)
_INPUT = re.compile(r"^input\s+(\w+)\s+(Inp\w+)\s*=\s*([^;\n]+);", re.MULTILINE)
_SYMBOLS = """
HDKSignal HDKExitRules HDKGuardTightens HDK_STRATEGY_ROBOT_MQH HDK_STRATEGY_OBSERVATION_MQH
HDK_STRATEGY_OBSERVE_ENTRIES HDK_STRATEGY_BARS HDK_STRATEGY_MIN_BARS HDK_STRATEGY_ATR
StrategyPosition StrategyStatus StrategyLoadBars StrategyDay StrategySessionEnd StrategyPartialKey
StrategyInitialKey StrategyPartialBlockReason StrategyInitialAnchor StrategyIgnored TryEnter
StrategyManageTick StrategyClosePosition StrategyOnBar StrategyInit StrategyTick StrategyTester
StrategyRules StrategySignal StrategyEntryTrace StrategyTraceAppend StrategyContextTrace
OnInit OnTick OnTester OnDeinit OnTimer OnChartEvent OnTradeTransaction
s09_anchor g_log g_risk g_exec g_trade_export g_rules g_bars g_last_bar g_name g_note g_shown g_episode
 g_panel g_panel_on g_guard_long g_guard_short g_entry_uncertain g_observed_bars
""".split()


def _replace(source, old, new):
    if source.count(old) != 1:
        raise ValueError(f"Contrato MQL5 do HADOUKEN mudou perto de {old!r}.")
    return source.replace(old, new)


def _engine(source, index):
    patches = (
        ('#include "HDK_StrategyObservation.mqh"', f'#include "portfolio_observation_{index}.mqh"'),
        ("double sl_dist = buy", "UltraEntryLevels(InpMagicNumber, buy, price, stop, target);\n   double sl_dist = buy"),
        ('   if(reason != "")\n   {\n#ifdef HDK_STRATEGY_OBSERVE_ENTRIES',
         '   if(reason=="" && UltraReconcile(reason))\n'
         '      g_exec.EntryIntent(g_episode>0 ? "consensus:"+IntegerToString(g_episode) : '
         'IntegerToString(g_last_bar)+":"+side,g_last_bar,reason);\n'
         '   if(reason != "")\n   {\n#ifdef HDK_STRATEGY_OBSERVE_ENTRIES'),
        ("   HDKExecutionResultV1 observed;", "   UltraPlan(InpMagicNumber,type,lot,price,stop,tp_dist,risk_money);\n"
         "   HDKExecutionResultV1 observed;"),
        ("   StrategyContextTrace(bar, s);", "   UltraDecision(InpMagicNumber,bar,g_bars.time[last],s.entry_long,"
         "s.entry_short,s.exit_long,s.exit_short,s.stop_long,s.stop_short,s.target_long,s.target_short);"),
        ("g_exec.ClosePosition(ticket, why, volume)", "UltraClose(g_exec,InpMagicNumber,ticket,why,volume)"),
        ("g_exec.ModifySL(ticket,entry)", 'UltraProtect(g_exec,InpMagicNumber,ticket,entry,"breakeven")'),
        ("g_exec.ModifySL(ticket, moved)", 'UltraProtect(g_exec,InpMagicNumber,ticket,moved,"trailing")'),
        ("g_exec.ModifySL(ticket, guard)", 'UltraProtect(g_exec,InpMagicNumber,ticket,guard,"move_guard")'),
        ("g_exec.CloseAll(g_risk.BlockReason(), failed);", "UltraCloseOwned(g_exec,InpMagicNumber,g_risk.BlockReason());"),
        ("      int failed = 0;\n", ""),
        ("InpDeviationPoints, 3,", "InpDeviationPoints, 1,"),
        ("if(want == 0 || want == closed_by_signal) return;",
         "if(want == 0 || want == closed_by_signal || (g_episode>0 && closed_by_signal!=0)) return;"),
        ("CHDKLogger       g_log;", "datetime         g_episode=0;\nCHDKLogger       g_log;"),
    )
    for old, new in patches:
        source = _replace(source, old, new)
    return source


def _qualified(source, index):
    tokens = re.compile(r"\b(?:" + "|".join(_SYMBOLS) + r"|Inp\w+)\b")
    return tokens.sub(lambda m: f"U{index}_{m[0]}", source)


class _Sources:
    def __init__(self, root, workspace, seed, tick):
        self.root = root
        self.workspace = workspace
        self.originals = {}
        self.files = {}
        self.seed = seed
        self.tick = tick

    def read(self, relative):
        source = self.root / relative
        self.originals[str(relative)] = _sha(source)
        return source.read_text(encoding="utf-8-sig")

    def write(self, relative, source):
        path = self.workspace / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(source)
        self.files[str(relative)] = _sha(path)

    def includes(self, source, origin, destination, engine=None):
        def include(match):
            angular, quoted = match[2], match[3]
            if angular and not angular.startswith("Framework/"):
                return match[0]  # Biblioteca padrão do MetaEditor.
            relative = PurePosixPath(angular) if angular else PurePosixPath(
                relpath("/" + str(origin.parent / quoted), "/"))
            if engine and relative == _ENGINE:
                installed = engine
            elif relative.name.startswith("portfolio_observation_"):
                return match[0]  # Derivada pelo gerador, na mesma pasta do motor.
            else:
                installed = self.header(relative)
            return match[1] + '"' + relpath(installed, destination.parent) + '"'

        return _INCLUDE.sub(include, source)

    def header(self, relative):
        installed = PurePosixPath("refs") / relative
        if str(installed) not in self.files:
            source = self.read(PurePosixPath("Include") / relative)
            self.write(installed, self.includes(source, relative, installed))
        return installed

    def owner(self, index, key, pattern):
        origin = PurePosixPath(f"Experts/HADOUKEN/Estrategias/HDK_{MARKET_NATIVE.get(key, 'Pattern')}.mq5")
        if key == "s09_reference":
            origin = PurePosixPath("Experts/HADOUKEN/Estrategias/HDK_S09_Referencia.mq5")
            self.read(origin)
            source = (_TEMPLATES / "UltraPortfolioS09.mqh.tmpl").read_text(encoding="utf-8")
        elif key == "super_atr_triple":
            origin = PurePosixPath("Experts/HADOUKEN/Estrategias/HDK_SuperATRTriplo.mq5")
            reference = self.read(origin)
            start = reference.index("bool TriploClosedPoint(")
            end = reference.index("\nvoid OnTick()", start)
            source = Template((_TEMPLATES / "UltraPortfolioSuperATR.mqh.tmpl").read_text(encoding="utf-8")).substitute(
                closed_point=reference[start:end], seed=self.seed)
        elif key == MARKET_PATTERN:
            native = (_TEMPLATES / "UltraPattern.mqh.tmpl").read_text(encoding="utf-8")
            common, marker, _ = native.partition("struct UltraKernel")
            if not marker:
                raise ValueError("Contrato de operadores ternários do Ultra indisponível.")
            local = (_TEMPLATES / "UltraPortfolioPattern.mqh.tmpl").read_text(encoding="utf-8")
            local = Template(local).safe_substitute(stop=pattern["stop_points"], target=pattern["target_points"])
            source = common + ultra_pattern_native.kernel(local, {"params": pattern}, index, self.tick)
        else:
            name = MARKET_NATIVE[key]
            origin = PurePosixPath(f"Experts/HADOUKEN/Estrategias/HDK_{name}/HDK_{name}.mq5")
            source = self.read(origin)
        overrides = {"InpRiskPct": RISK_PCT}
        if key == "vwap_bands":
            overrides.update(InpEntryBandPct="0.04", InpTargetBandPct="0.02", InpVolumeSource="1")
        effective = {}

        def freeze(match):
            kind, param, default = match.groups()
            value = overrides.get(param, default.strip())
            effective[param] = value
            return f"const {kind} {param} = {value};"

        source = _INPUT.sub(freeze, source)
        source = re.sub(r"^(?:#property |input group |#define HDK_STRATEGY_OBSERVE_ENTRIES).*\n", "", source,
                        flags=re.MULTILINE)
        if not set(overrides) <= effective.keys() or re.search(r"^input\b", source, re.MULTILINE):
            raise ValueError(f"Contrato de inputs MQL5 desconhecido em {key}.")
        effective.setdefault("InpExportTrades", "false")
        prefix = "const bool InpExportTrades=false;\n" if "InpExportTrades" not in source else ""
        engine = PurePosixPath("refs") / _ENGINE.with_name(f"portfolio_engine_{index}.mqh")
        observation = engine.with_name(f"portfolio_observation_{index}.mqh")
        # O arquivo de observação canônico usa o mesmo resultado de execução do Safety.
        # A versão de carteira guarda a amostra por barra e o recibo de cada entrada.
        self.read(PurePosixPath("Include") / _OBSERVATION)
        obs = """bool g_entry_uncertain=false;
void StrategyEntryTrace(const ENUM_ORDER_TYPE type,const HDKExecutionResultV1 &out,const string reason)
{
   UltraEntry(InpMagicNumber,type,out,reason);
}
"""
        self.write(observation, _qualified(obs, index))
        engine_text = _engine(self.read(PurePosixPath("Include") / _ENGINE), index)
        self.write(engine, _qualified(self.includes(engine_text, _ENGINE, engine), index))
        slot = PurePosixPath(f"owner_{index}.mqh")
        if key == "vwap_bands":
            ref = _ENGINE.with_name("HDK_VwapObservation.mqh")
            obs_ref = PurePosixPath("refs") / ref
            obs_text = self.read(PurePosixPath("Include") / ref)
            self.write(obs_ref, _qualified(self.includes(obs_text, ref, obs_ref), index))
        derived = self.includes(source, origin, slot, engine)
        self.write(slot, _qualified("#define HDK_STRATEGY_OBSERVE_ENTRIES\n" + prefix + derived, index))
        magic = int(effective["InpMagicNumber"])
        policy = {"inputs": effective}
        if pattern is not None:
            policy["pattern"] = pattern
        return dict(key=key, magic=magic, **policy, preset_sha256=digest([key, policy]),
                    inputs_sha256=digest(policy), reference=("analytics.pattern_engine" if pattern else str(origin)),
                    include=str(slot))


def export(config, snapshot, destination, *, attempt, strategies=None, pattern=None):
    manifest = ultra_market.verify(snapshot)
    if manifest["symbol"] != "EURUSD" or manifest["source"]["server"] != "MetaQuotes-Demo":
        raise ValueError("A carteira técnica exige EURUSD da MetaQuotes-Demo.")
    if not manifest["coverage"]["executable"] or not re.fullmatch(r"[a-f0-9]{32}", attempt):
        raise ValueError("A fita ou o identificador da tentativa é inválido para a carteira.")
    coverage = manifest["coverage"]
    if (coverage["rows"] != sum(block["rows"] for block in manifest["blocks"])
            or not manifest["time"]["start_msc"] <= coverage["last_msc"] < manifest["time"]["end_msc"]):
        raise ValueError("A contagem e o último tick precisam pertencer à janela congelada.")
    roots = [path for path in library_dirs(config) if (path / "Include" / _ENGINE).is_file()]
    if len(roots) != 1:
        raise ValueError("Configure uma única referência HADOUKEN para a carteira.")
    if pattern is not None:
        if (not isinstance(pattern, dict) or set(pattern) != {"definition", "apply_invalidation", "stop_points", "target_points"}
                or type(pattern["apply_invalidation"]) is not bool or any(type(pattern[k]) is not int or not 0 < pattern[k] <= 100000
                                                                          for k in ("stop_points", "target_points"))):
            raise ValueError("Declare definição, invalidação booleana e SL/TP de pesquisa em pontos.")
        body = compile_definition(pattern["definition"]).body
        if (manifest["symbol"] not in body["universe_definition"]["symbols"] or body["repetition_policy"] != "all"
                or body["timezone_calendar_versions"].get("clock") != "mt5_raw_clock"):
            raise ValueError("A hipótese de mercado exige EURUSD, relógio bruto MT5 e repetição sem dependência de sessão.")
        pattern = dict(pattern, definition=body)
    available = (*MARKET_NATIVE, *((MARKET_PATTERN,) if pattern is not None else ()))
    keys = available if strategies is None else tuple(strategies)
    if not keys or len(keys) != len(set(keys)) or set(keys) - set(available) or (pattern and MARKET_PATTERN not in keys):
        raise ValueError("Selecione estratégias de carteira distintas e disponíveis no catálogo.")
    destination = Path(destination)
    seed = manifest["coverage"]["first_msc"] // 60000 * 60
    contract = manifest["contract"]["current"]
    if Decimal(str(contract["point"])) != Decimal(str(contract["trade_tick_size"])):
        raise ValueError("A carteira técnica exige EURUSD com ponto igual ao tick de negociação.")
    sources = _Sources(roots[0], destination.parent, seed, Decimal(str(contract["trade_tick_size"])))
    safety = sources.header(_EXECUTOR)
    slots = [sources.owner(i, key, pattern if key == MARKET_PATTERN else None) for i, key in enumerate(keys)]
    if len({row["magic"] for row in slots}) != len(slots):
        raise ValueError("Há proprietários duplicados na carteira.")
    binding = digest([manifest["manifest_hash"], attempt])
    template_path = _TEMPLATES / "UltraMarketPortfolio.mq5.tmpl"
    closure = digest({"reference": sources.originals, "generated": sources.files, "host_template": _sha(template_path)})
    budgets = ";".join(f"{row['magic']}={RISK_PCT}" for row in slots)
    includes, init, ticks, settlement = [], [], [], []
    for i, row in enumerate(slots):
        includes.append(f'#include "{row["include"]}"\nCHDKPortfolioRiskGuard owner_{i};')
        init.append(f'''   if(U{i}_OnInit()!=INIT_SUCCEEDED ||
      !UltraBind(owner_{i},U{i}_g_exec,{row["magic"]},"{row["key"]}",
                "{row["preset_sha256"]}","{row["inputs_sha256"]}")) return INIT_FAILED;
   UltraRules({row["magic"]},U{i}_g_rules.target_r,U{i}_g_rules.partial_r,U{i}_g_rules.partial_fraction,
              U{i}_g_rules.breakeven_after_partial,U{i}_g_rules.trailing_atr,U{i}_g_rules.max_bars,U{i}_g_rules.eod_exit);''')
        ticks.append(f"   U{i}_OnTick();")
        settlement.append(f'      UltraCloseOwned(U{i}_g_exec,{row["magic"]},"fim da fita");')
    level_cases = []
    for row in slots:
        if row["key"] in ("s09_reference", MARKET_PATTERN):
            inputs = row["inputs"]
            level_cases.append(f'   if(magic=={row["magic"]})\n   {{\n'
                               f'      stop=price-(buy?1:-1)*{inputs["InpStopPoints"]}*_Point;\n'
                               f'      target=price+(buy?1:-1)*{inputs["InpTargetPoints"]}*_Point;\n   }}')
    template = Template(template_path.read_text(encoding="utf-8"))
    sources.write(PurePosixPath(destination.name), template.substitute(
        manifest=manifest["manifest_hash"], binding=binding, start=manifest["time"]["start_msc"],
        end=manifest["time"]["end_msc"], last=coverage["last_msc"], tick_count=coverage["rows"],
        closure=closure, safety=safety, cap=CAP_PCT, budgets=budgets,
        entry_levels="\n".join(level_cases),
        includes="\n".join(includes), init="\n".join(init), ticks="\n".join(ticks),
        settlement="\n".join(settlement)))
    return dict(manifest_hash=manifest["manifest_hash"], symbol="EURUSD", binding=binding, tester_model="4",
                source_sha256=sources.files[destination.name], source_closure_sha256=closure,
                source_files=sources.files, canonical_files=sources.originals, slots=slots, orders=True,
                slot_risk_pct=float(RISK_PCT), portfolio_cap_pct=float(CAP_PCT),
                window=dict(start_msc=manifest["time"]["start_msc"], last_msc=coverage["last_msc"], ticks=coverage["rows"]),
                tape=f"MT5XUltraMarket_{binding}.bin")
