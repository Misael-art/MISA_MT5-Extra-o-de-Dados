"""Vincula as referências MQL5 e seus motores ao mesmo passe de carteira."""
from pathlib import Path, PurePosixPath
from decimal import Decimal
from posixpath import relpath
import re
from hashlib import sha256
from string import Template

from ..analytics.hypotheses import compile_definition
from ..laboratory import ultra_market, ultra_pattern_native
from ..laboratory.ultra_market_parameters import validate_effective
from ..laboratory.ultra_catalog import MARKET_NATIVE, MARKET_PATTERN
from ..experiments.schema import digest
from ..strategies.indicators import PRICE_COMPARISON_REL_TOLERANCE
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
_LITERALS = {"bool": r"true|false", "int": r"-?[0-9]+", "long": r"[0-9]+",
             "datetime": r"\(datetime\)[0-9]+", "double": r"-?[0-9]+(?:\.[0-9]+)?"}
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


def _engine(source, index, local_risk=False):
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
        ("   StrategySignal(g_bars, s);", "   StrategySignal(g_bars, s);\n"
         "   UltraRaw(InpMagicNumber,bar,g_bars.time[0],g_bars.time[last],g_bars.n,s.entry_long,"
         "s.entry_short,s.exit_long,s.exit_short,s.stop_long,s.stop_short,s.target_long,s.target_short);"),
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
    if index == 0:
        source = _replace(source, "   g_bars.n = n;", "   UltraRates(rates,n);\n   g_bars.n = n;")
    if local_risk:
        anchor = '   CHDKVisualEntryHint visual_hint(g_name, "Fixa");\n'
        source = _replace(source, anchor + "   string reason = g_risk.BlockReason();",
                          anchor + "   string individual=UltraOwnerReason(InpMagicNumber);\n"
                          "   string reason = g_risk.BlockReason();\n   if(reason==\"\") reason=individual;")
    for old, new in patches:
        source = _replace(source, old, new)
    return source


def _qualified(source, index):
    tokens = re.compile(r"\b(?:" + "|".join(_SYMBOLS) + r"|Inp\w+)\b")
    return tokens.sub(lambda m: f"U{index}_{m[0]}", source)


def _bollinger_boundaries(source):
    epsilon = format(Decimal(str(PRICE_COMPARISON_REL_TOLERANCE)), "f")
    source = f"input double InpPriceCompareRelTolerance={epsilon};\n" + source
    source = _replace(source, "   bool lateral  = adx[i] < InpAdxMax;",
                      "   double epsilon=MathAbs(mid[i])*InpPriceCompareRelTolerance;\n"
                      "   PrintFormat(\"MT5X_ULTRA_BB_CONTEXT|%I64d|%I64d|%.17g|%.17g|%.17g|%.17g|%.17g\","
                      "ultra_msc,(long)b.time[i],mid[i],lower,upper,rsi[i],adx[i]);\n"
                      "   bool lateral  = adx[i] < InpAdxMax;")
    for old, new in (("b.low[i] <= lower", "b.low[i] <= lower + epsilon"),
                     ("b.high[i] >= upper", "b.high[i] >= upper - epsilon"),
                     ("b.close[i] >= mid[i]", "b.close[i] >= mid[i] - epsilon"),
                     ("b.close[i] <= mid[i]", "b.close[i] <= mid[i] + epsilon")):
        source = _replace(source, old, new)
    return source


def _squeeze_momentum(source):
    epsilon = format(Decimal(str(PRICE_COMPARISON_REL_TOLERANCE)), "f")
    source = f"input double InpMomentumRelTolerance={epsilon};\n" + source
    original = "   double momentum = b.close[i] - mid[i], before = b.close[i - 1] - mid[i - 1];"
    return _replace(source, original, original + "\n"
                    "   if(MathAbs(momentum)<=MathAbs(mid[i])*InpMomentumRelTolerance) momentum=0;\n"
                    "   if(MathAbs(before)<=MathAbs(mid[i-1])*InpMomentumRelTolerance) before=0;")


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
        content = source.encode("utf-8")
        if self.workspace is not None:
            path = self.workspace / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb") as stream:
                stream.write(content)
        self.files[str(relative)] = sha256(content).hexdigest()

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

    def owner(self, index, key, pattern, edits, frozen, *, portable=False, rule=None):
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
            source = common + ultra_pattern_native.kernel(local, {"params": pattern}, index, self.tick, market=True)
        else:
            name = MARKET_NATIVE[key]
            origin = PurePosixPath(f"Experts/HADOUKEN/Estrategias/HDK_{name}/HDK_{name}.mq5")
            source = self.read(origin)
        if key == "bb_rsi":
            source = _bollinger_boundaries(source)
        if key == "squeeze":
            source = _squeeze_momentum(source)
        overrides = {"InpRiskPct": RISK_PCT}
        if key == "vwap_bands":
            overrides.update(InpEntryBandPct="0.04", InpTargetBandPct="0.02", InpVolumeSource="1")
        overrides.update(edits)
        effective = {}

        def freeze(match):
            kind, param, default = match.groups()
            expected = overrides.get(param, default.strip())
            value = expected if frozen is None else frozen.get(param)
            if value != expected:
                raise ValueError(f"modules.{key}.inputs.{param}: valor fora da seleção suportada ({expected}).")
            relative_seed = portable and param == "InpSeedStart" and value == "(datetime)UltraSeed"
            if not relative_seed and (not isinstance(value, str) or not re.fullmatch(_LITERALS.get(kind, r"(?!)"), value)):
                raise ValueError(f"modules.{key}.inputs.{param}: literal MQL5 incompatível com {kind}.")
            effective[param] = value
            return f"const {kind} {param} = {value};"

        source = _INPUT.sub(freeze, source)
        source = re.sub(r"^(?:#property |input group |#define HDK_STRATEGY_OBSERVE_ENTRIES).*\n", "", source,
                        flags=re.MULTILINE)
        if not set(overrides) <= effective.keys() or re.search(r"^input\b", source, re.MULTILINE):
            raise ValueError(f"Contrato de inputs MQL5 desconhecido em {key}.")
        effective.setdefault("InpExportTrades", "false")
        if portable:
            validate_effective(key, effective)
        if frozen is not None and frozen != effective:
            raise ValueError(f"modules.{key}.inputs: parâmetros incompatíveis com a referência.")
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
        engine_text = _engine(self.read(PurePosixPath("Include") / _ENGINE), index, bool(rule and rule.get("risk")))
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
        if rule:
            policy["owner_rules"] = rule
        return dict(key=key, magic=magic, priority=index, **policy, preset_sha256=digest([key, policy]),
                    inputs_sha256=digest(policy), reference=("analytics.pattern_engine" if pattern else str(origin)),
                    include=str(slot))


def market_contract(manifest):
    for field, actual, expected in (
        ("symbol", manifest["symbol"], "EURUSD"),
        ("source.server", manifest["source"]["server"], "MetaQuotes-Demo"),
        ("source.mode", manifest["source"]["mode"], "demo"),
        ("account.margin_mode", manifest["account"]["margin_mode"], 2),
        ("account.currency", manifest["account"]["currency"], "USD"),
        ("account.currency_digits", manifest["account"]["currency_digits"], 2),
        ("time.basis", manifest["time"]["basis"], "mt5_raw_clock"),
    ):
        if actual != expected:
            raise ValueError(f"{field}: valor incompatível com o Tester de carteira ({actual!r}).")
    coverage = manifest["coverage"]
    if (not coverage["executable"] or coverage["rows"] != sum(block["rows"] for block in manifest["blocks"])
            or not manifest["time"]["start_msc"] <= coverage["last_msc"] < manifest["time"]["end_msc"]):
        raise ValueError("coverage: contagem, execução ou último tick inválido para a janela congelada.")
    if any(manifest["time"][name] % 86400000 for name in ("start_msc", "end_msc")):
        raise ValueError("time: a carteira exige dias completos no relógio bruto MT5.")
    contract = manifest["contract"]["current"]
    if Decimal(str(contract["point"])) != Decimal(str(contract["trade_tick_size"])):
        raise ValueError("contract.current.trade_tick_size: deve ser igual a point.")
    return dict(symbol=manifest["symbol"], data=manifest["source"], account=manifest["account"],
                specifications=manifest["contract"], clock=manifest["time"], timeframe="M1",
                session="broker_day_m1", tester_model=4, snapshot_sha256=manifest["manifest_hash"])


_COMPAT_SPEC = ("digits", "point", "trade_tick_size", "trade_contract_size", "volume_min", "volume_max",
                "volume_step", "volume_limit", "currency_base", "currency_profit", "currency_margin",
                "trade_calc_mode", "trade_exemode", "filling_mode", "order_mode", "trade_mode",
                "trade_stops_level", "trade_freeze_level", "margin_initial", "margin_maintenance",
                "margin_hedged", "margin_hedged_use_leg")


def compatibility(market):
    current = market["specifications"]["current"]
    return dict(symbol=market["symbol"], timeframe=market["timeframe"], clock_basis=market["clock"]["basis"],
                session=market["session"], tester_model=market["tester_model"], account=market["account"],
                data={key: market["data"][key] for key in ("provider", "server", "origin", "mode", "copy_mode")},
                specifications={field: current[field] for field in _COMPAT_SPEC},
                default_super_atr_seed="first_observed_minute_of_execution")


def _compatibility_check(expected, observed, path="preset.profile.market"):
    for key, value in expected.items():
        actual = observed.get(key)
        if isinstance(value, dict) and isinstance(actual, dict):
            _compatibility_check(value, actual, f"{path}.{key}")
        elif value != actual:
            raise ValueError(f"{path}.{key}: esperado {value!r}, observado {actual!r}.")
    if expected.keys() != observed.keys():
        raise ValueError(f"{path}: requisitos de compatibilidade incompletos.")


def _local_source(slots, sources):
    cases = []
    for row in slots:
        if risk := row.get("owner_rules", {}).get("risk"):
            limits = ",".join(str(risk.get(field, 0)) for field in
                              ("daily_loss_cents", "daily_profit_cents", "max_attempts", "max_losses"))
            cases.append(f'   if(magic=={row["magic"]}) return UltraReadOwnerRisk(magic,{limits});')
    template = Template((_TEMPLATES / "UltraMarketOwnerRisk.mqh.tmpl").read_text(encoding="utf-8"))
    sources.write(PurePosixPath("owner_risk.mqh"), template.substitute(cases="\n".join(cases)))


def resolve(config, manifest, *, strategies, pattern, edits, workspace=None, frozen=None, portable=False, owner_rules=None):
    market = market_contract(manifest)
    roots = [path for path in library_dirs(config) if (path / "Include" / _ENGINE).is_file()]
    if len(roots) != 1:
        raise ValueError("Configure uma única referência HADOUKEN para a carteira.")
    if pattern is not None:
        if (not isinstance(pattern, dict) or set(pattern) != {"definition", "apply_invalidation", "stop_points", "target_points"}
                or type(pattern["apply_invalidation"]) is not bool or any(type(pattern[k]) is not int or not 0 < pattern[k] <= 100000
                                                                          for k in ("stop_points", "target_points"))):
            raise ValueError("pattern: declare definition, apply_invalidation e SL/TP em pontos positivos.")
        body = compile_definition(pattern["definition"]).body
        if (manifest["symbol"] not in body["universe_definition"]["symbols"] or body["repetition_policy"] != "all"
                or body["timezone_calendar_versions"].get("clock") != "mt5_raw_clock"):
            raise ValueError("pattern.definition: exige EURUSD, mt5_raw_clock e repetition_policy=all.")
        pattern = dict(pattern, definition=body)
    available = (*MARKET_NATIVE, *((MARKET_PATTERN,) if pattern is not None else ()))
    keys = available if strategies is None else tuple(strategies)
    if not keys or len(keys) != len(set(keys)) or set(keys) - set(available) or (pattern and MARKET_PATTERN not in keys):
        raise ValueError("Selecione estratégias de carteira distintas e disponíveis no catálogo.")
    if set(edits) - set(keys):
        raise ValueError("parameters: há edição para um módulo fora da seleção.")
    seed = manifest["coverage"]["first_msc"] // 60000 * 60
    if portable:
        seed_edit = edits.get("super_atr_triple", {}).get("InpSeedStart")
        if seed_edit and not 0 < int(seed_edit.removeprefix("(datetime)")) <= seed:
            raise ValueError("parameters.super_atr_triple.seed_start_broker: deve preceder ou coincidir com a primeira barra.")
        if (edits.get("vwap_bands", {}).get("InpVolumeSource") == "2"
                and not manifest["coverage"]["positive_reported_volume"]):
            raise ValueError("parameters.vwap_bands.volume_source: a fita não contém volume de negócios.")
        seed = "UltraSeed"
    owner_rules = owner_rules or {}
    contract = manifest["contract"]["current"]
    sources = _Sources(roots[0], workspace, seed, Decimal(str(contract["trade_tick_size"])))
    safety = sources.header(_EXECUTOR)
    slots = [sources.owner(i, key, pattern if key == MARKET_PATTERN else None, edits.get(key, {}),
                           frozen[i]["inputs"] if frozen is not None else None, portable=portable,
                           rule=owner_rules.get(key)) for i, key in enumerate(keys)]
    if len({row["magic"] for row in slots}) != len(slots):
        raise ValueError("Há proprietários duplicados na carteira.")
    template_path = _TEMPLATES / "UltraMarketPortfolio.mq5.tmpl"
    if portable:
        _local_source(slots, sources)
        market = compatibility(market)
    closure = digest({"reference": sources.originals, "generated": sources.files, "host_template": _sha(template_path)})
    profile = dict(market=market, modules=slots, source_closure_sha256=closure,
                   canonical_files=sources.originals, portfolio_cap_pct=CAP_PCT)
    return profile, sources, safety


def export(config, snapshot, destination, *, attempt, strategies=None, pattern=None, preset=None, manifest=None,
           successor=None, switch_at=None, restore_at=None, policy_inbox=False):
    from ..laboratory import ultra_market_preset
    from . import ultra_market_revision

    manifest = ultra_market.verify(snapshot) if manifest is None else manifest
    if not re.fullmatch(r"[a-f0-9]{32}", attempt):
        raise ValueError("attempt: identificador inválido para a carteira.")
    if preset is not None and (strategies is not None or pattern is not None):
        raise ValueError("preset: não combine preset congelado com seleção de módulos ou hipótese.")
    if ((successor is None) != (switch_at is None)) or (successor is not None and preset is None) or (
            restore_at is not None and successor is None):
        raise ValueError("successor/switch_at_msc: informe a política inicial, o sucessor e o tick da troca.")
    destination = Path(destination)
    portable = False
    if preset is None:
        profile, sources, safety = resolve(config, manifest, strategies=strategies, pattern=pattern, edits={},
                                           workspace=destination.parent)
        preset = ultra_market_preset.seal(profile, parameters={}, previous=None)
    else:
        preset = ultra_market_preset.read(preset)
        portable = preset["schema"] == ultra_market_preset.PORTABLE
        observed = market_contract(manifest)
        options = {}
        if portable:
            _compatibility_check(preset["profile"]["market"], compatibility(observed))
            if manifest["coverage"]["off_grid"]:
                raise ValueError("coverage.off_grid: a fita contém cotações fora da grade congelada.")
            research = preset["research"]
            if research is not None and manifest["time"]["start_msc"] < research["training_end_msc"]:
                raise ValueError("preset.research.training_end_msc: a execução deve iniciar após o treino.")
            options.update(portable=True, owner_rules=preset["owner_rules"])
        elif preset["profile"]["market"] != observed:
            raise ValueError("preset.profile.market: fita ou especificação diferente do preset congelado.")
        slots = preset["profile"]["modules"]
        pattern = next((row["pattern"] for row in slots if row["key"] == MARKET_PATTERN), None)
        profile, sources, safety = resolve(config, manifest, strategies=[row["key"] for row in slots],
                                           pattern=pattern, edits=ultra_market_preset.edits(preset),
                                           workspace=destination.parent, frozen=slots, **options)
        if profile != preset["profile"]:
            field = next(key for key in profile if profile[key] != preset["profile"].get(key))
            raise ValueError(f"preset.profile.{field}: diverge da fita ou das referências fixadas.")
    slots = profile["modules"]
    coverage = manifest["coverage"]
    closure = profile["source_closure_sha256"]
    binding = digest([manifest["manifest_hash"], preset["preset_sha256"], attempt])
    window = dict(start_msc=manifest["time"]["start_msc"], last_msc=coverage["last_msc"], ticks=coverage["rows"])
    execution = {}
    revision = None
    if successor is not None:
        successor = ultra_market_revision.validate(preset, successor, window, switch_at)
        if restore_at is not None and (type(restore_at) is not int or not switch_at <= restore_at < window["last_msc"]):
            raise ValueError("restore_at_msc: deve ocorrer após a instalação e antes do fim da janela.")
        rebuilt = ultra_market_preset.portable(config, manifest, strategies=[row["key"] for row in slots],
            pattern=pattern, parameters=successor["parameters"], owner_rules=successor["owner_rules"],
            research=successor["research"], previous=preset)
        if rebuilt != successor:
            raise ValueError("successor.profile: o contrato diverge da revisão reconstruída.")
        header = destination.parent / safety
        extended = ultra_market_revision.executor_extension(header.read_text(encoding="utf-8")).encode("utf-8")
        header.write_bytes(extended)
        sources.files[str(safety)] = sha256(extended).hexdigest()
        revision = ultra_market_revision.fragments(successor, switch_at, restore_at)
        closure = digest([closure, sources.files[str(safety)], successor["preset_sha256"], switch_at, revision])
        binding = digest([binding, successor["preset_sha256"], switch_at, restore_at])
        execution["rotation"] = dict(switch_at_msc=switch_at, restore_at_msc=restore_at, successor=successor)
    delivery = None
    if policy_inbox:
        if not portable or revision:
            raise ValueError("A recepção posterior exige preset portátil e uma troca por passe.")
        from . import ultra_market_delivery
        delivery = ultra_market_delivery.fragments(slots, sources, destination.parent, safety)
        closure = digest([closure, sources.files, delivery])
        binding = digest([binding, closure])
        execution["policy_inbox"] = True
    budgets = ";".join(f"{row['magic']}={row['inputs']['InpRiskPct']}" for row in slots)
    includes, init, ticks, settlement = [], [], [], []
    for i, row in enumerate(slots):
        includes.append(f'#include "{row["include"]}"\nCHDKPortfolioRiskGuard owner_{i};')
        management = row.get("owner_rules", {}).get("management", {})
        rules = ultra_market_revision.management_code(i, management)
        init.append(f'''   if(U{i}_OnInit()!=INIT_SUCCEEDED ||
      !UltraBind(owner_{i},U{i}_g_exec,{row["magic"]},"{row["key"]}",
                "{row["preset_sha256"]}","{row["inputs_sha256"]}")) return INIT_FAILED;
{rules}   UltraRules({row["magic"]},U{i}_g_rules.target_r,U{i}_g_rules.partial_r,U{i}_g_rules.partial_fraction,
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
    host = (_TEMPLATES / "UltraMarketPortfolio.mq5.tmpl").read_text(encoding="utf-8")
    if delivery:
        helpers, declarations, ticks = delivery
        includes = [helpers, *includes, *declarations]
    if revision:
        helpers, declarations, next_init, ticks = revision
        includes.extend((helpers, *declarations))
        init.extend(next_init)
    if portable:
        seed = coverage["first_msc"] // 60000 * 60
        host = _replace(host, "bool UltraReconcile", f'const datetime UltraSeed=(datetime){seed};\n'
                        '#include "owner_risk.mqh"\n\nbool UltraReconcile')
        host = _replace(host, '   Print(UltraPrefix,"PRESET|"', _native_compatibility(manifest)
                        + '   Print(UltraPrefix,"PRESET|"')
        execution["execution_market"] = observed
        execution["execution_seed_broker_s"] = seed
    template = Template(host)
    sources.write(PurePosixPath(destination.name), template.substitute(
        manifest=manifest["manifest_hash"], binding=binding, start=manifest["time"]["start_msc"],
        end=manifest["time"]["end_msc"], last=coverage["last_msc"], tick_count=coverage["rows"],
        closure=closure, safety=safety, cap=profile["portfolio_cap_pct"], budgets=budgets,
        preset_sha=preset["preset_sha256"], preset_version=preset["version"],
        point=contract_literal(manifest, "point"), tick=contract_literal(manifest, "trade_tick_size"),
        lot_step=contract_literal(manifest, "volume_step"), digits=contract_literal(manifest, "digits"),
        entry_levels="\n".join(level_cases),
        includes="\n".join(includes), init="\n".join(init), ticks="\n".join(ticks),
        settlement="\n".join(settlement)))
    return dict(manifest_hash=manifest["manifest_hash"], symbol="EURUSD", binding=binding, tester_model="4",
                preset=preset, preset_sha256=preset["preset_sha256"], preset_version=preset["version"],
                source_sha256=sources.files[destination.name], source_closure_sha256=closure,
                source_files=sources.files, canonical_files=sources.originals, slots=slots, orders=True,
                slot_risk_pct=float(RISK_PCT), portfolio_cap_pct=float(CAP_PCT),
                window=window,
                tape=f"MT5XUltraMarket_{binding}.bin", **execution)


def _native_compatibility(manifest):
    checks = ("volume_min", "volume_max", "trade_contract_size", "trade_exemode", "filling_mode")
    dispatch = {field: "Integer" if field in ("trade_exemode", "filling_mode") else "Double" for field in checks}
    return "".join(
        f'   if(SymbolInfo{dispatch[field]}(_Symbol,SYMBOL_{field.upper()})!={contract_literal(manifest, field)})\n'
        f'   {{Print(UltraPrefix,"DENIED|contract.current.{field}");return INIT_FAILED;}}\n' for field in checks)


def contract_literal(manifest, field):
    return format(Decimal(str(manifest["contract"]["current"][field])), "f")
