"""
Bateria de segurança: confere o código e o preset do robô (estática) e as operações do último backtest
(estatística). Cada item devolve ✓ / ⚠ / ✗ com o motivo e o que fazer.

    report = run_battery(db, config, robot, robots, preset="Conservador")
    report.approved        # libera a conta demo (nenhum ✗; avisos não reprovam)
    report.allows_real     # só com a parte estática de fato conferida (robô com código-fonte)

A bateria não envia ordens nem fala com o MT5: lê arquivos e o que está gravado no banco. Aprovação não é
garantia de resultado: é o mínimo para o robô poder rodar em conta de demonstração.
"""
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import List, Optional

from . import catalog, contract, mql, store
from . import risk as risk_mod

OK, WARN, FAIL, NA = "ok", "aviso", "falha", "não avaliável"
ICONS = {OK: "✓", WARN: "⚠", FAIL: "✗", NA: "–"}
STATIC, STATISTICAL = "estática", "estatística"
APPROVED, REJECTED = "aprovada", "reprovada"

MIN_TRADES = 200
MAX_RUIN_PROBABILITY = 0.01      # chance de perder metade do capital nas simulações
LONG_LOSS_STREAK = 10
RECENT_SHARE = 0.3               # parte final do teste usada para conferir se o resultado se mantém
MIN_RECENT_TRADES = 30
DEFAULT_MAX_RISK_PCT = 1.0

_MAGIC = re.compile(r"magic", re.I)
_LINE_COMMENT = re.compile(r"//[^\n]*")


@dataclass
class Item:
    group: str
    id: str
    name: str
    status: str
    detail: str = ""
    fix: str = ""

    @property
    def icon(self) -> str:
        return ICONS[self.status]


@dataclass
class Report:
    robot_key: str
    preset: str = ""
    items: List[Item] = field(default_factory=list)
    run_id: str = ""
    created_at: str = ""
    check_id: str = ""

    def group_status(self, group: str) -> str:
        statuses = [i.status for i in self.items if i.group == group]
        if any(s == FAIL for s in statuses):
            return REJECTED
        return NA if all(s == NA for s in statuses) else APPROVED

    @property
    def static_status(self) -> str:
        return self.group_status(STATIC)

    @property
    def statistical_status(self) -> str:
        return self.group_status(STATISTICAL)

    @property
    def approved(self) -> bool:
        """Libera a conta demo: estatística aprovada e nada reprovado no código (caixa-preta passa só aqui)."""
        return self.statistical_status == APPROVED and self.static_status != REJECTED

    @property
    def allows_real(self) -> bool:
        """Conta real exige a parte estática conferida de verdade; sem código-fonte, nunca."""
        return self.approved and self.static_status == APPROVED

    def to_dict(self) -> dict:
        return {"check_id": self.check_id, "robot_key": self.robot_key, "preset": self.preset,
                "approved": self.approved, "allows_real": self.allows_real, "static_status": self.static_status,
                "statistical_status": self.statistical_status, "run_id": self.run_id, "created_at": self.created_at,
                "items": [dict(vars(i), icon=i.icon) for i in self.items]}


def _pct(value: float, digits: int = 1) -> str:
    return f"{value * 100:.{digits}f}%".replace(".", ",")


def _num(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}".replace(".", ",")


def _config_float(config, section: str, key: str, default: float) -> float:
    try:
        return config.getfloat(section, key, fallback=default) if config is not None else default
    except ValueError:
        return default


# --- estática -------------------------------------------------------------------------

def _preset_values(robot, preset: Optional[str]) -> dict:
    if not preset:
        return {}
    if preset not in robot.presets:
        raise ValueError(f"Preset desconhecido: '{preset}'. Disponíveis: " + (", ".join(robot.presets) or "nenhum") + ".")
    return robot.presets[preset]


def magic_number(robot, values: Optional[dict] = None) -> str:
    """Valor do parâmetro de magic number ('' se o robô não tem um)."""
    declared = contract.input_name(robot, "magic")
    item = next((i for i in robot.inputs if i.name == declared), None) if declared else None
    item = item or next((i for i in robot.inputs if _MAGIC.search(i.name)), None)
    return "" if item is None else str((values or {}).get(item.name, item.default)).split("|")[0].strip()


def _sources(robot) -> List[Path]:
    return [Path(p) for p in robot.files if p.lower().endswith((".mq5", ".mqh"))]


def _encoding_item(robot) -> Item:
    bad = []
    for path in _sources(robot):
        data = path.read_bytes()
        if data[:2] in (b"\xff\xfe", b"\xfe\xff"):
            continue
        try:
            data.decode("utf-8")
        except UnicodeDecodeError:
            bad.append(path.name)
    if bad:
        return Item(STATIC, "encoding", "Arquivos em UTF-8 ou UTF-16", WARN,
                    "codificação antiga (ANSI) em: " + ", ".join(bad) + "; acentos podem sair trocados no painel e nos logs",
                    "No MetaEditor: Arquivo > Salvar como > Codificação UTF-8.")
    return Item(STATIC, "encoding", "Arquivos em UTF-8 ou UTF-16", OK, f"{len(_sources(robot))} arquivo(s) conferido(s)")


def _orders_item(robot) -> Item:
    name = "Toda ordem passa pela camada de segurança"
    files = _sources(robot)
    safety = [p for p in files if "/safety/" in str(p).replace("\\", "/").lower()]
    loose = [p.name for p in files if p not in safety
             and mql.ORDER_CALL.search(_LINE_COMMENT.sub("", mql._BLOCK_COMMENT.sub("", mql.read_text(p))))]
    if loose and safety:
        return Item(STATIC, "orders", name, FAIL, "envio direto de ordem fora de Safety/ em: " + ", ".join(loose),
                    "Toda ordem deve passar pelo executor seguro (TryEnter / SafeExecutor).")
    if loose:
        return Item(STATIC, "orders", name, FAIL,
                    "o robô envia ordens direto (" + ", ".join(loose) + ") e não tem camada de segurança",
                    "Porte o robô para a camada Safety/ do HADOUKEN: stop obrigatório e limites conferidos a cada ordem.")
    if safety:
        return Item(STATIC, "orders", name, OK, "nenhum envio de ordem fora de Safety/")
    return Item(STATIC, "orders", name, WARN, "o código não envia ordens (painel ou robô sem entrada automática)",
                "Sem entradas não há o que testar: confirme se este é mesmo o arquivo que opera.")


def static_items(robot, config=None, robots=(), preset: Optional[str] = None) -> List[Item]:
    """Itens de código e preset. `robots` é o catálogo (para conferir se o magic number é exclusivo)."""
    max_risk = _config_float(config, "ROBOTS", "max_real_risk_pct", DEFAULT_MAX_RISK_PCT)
    if robot.kind == catalog.PYTHON:
        risk_pct = float(robot.risk.facts.get("risk_pct", 0.0))
        return [Item(STATIC, "stop", "Stop obrigatório", OK, "toda estratégia Python informa o stop junto com o sinal"),
                Item(STATIC, "risk", "Risco por operação dentro do teto", OK if risk_pct <= max_risk else FAIL,
                     f"{_num(risk_pct)}% por operação (teto {_num(max_risk)}%)",
                     "" if risk_pct <= max_risk else "Reduza [STRATEGY] risk_per_trade em config/config.ini.")]
    if not robot.has_source:
        return [Item(STATIC, "source", "Código-fonte disponível", NA,
                     "só há o arquivo compilado (.ex5): stop, limites e o que o robô envia não podem ser conferidos",
                     "Peça o .mq5. Sem ele o robô pode rodar em demo, nunca em conta real.")]
    values = _preset_values(robot, preset)
    rating = robot.preset_risks.get(preset, robot.risk) if preset else robot.risk
    facts, items = dict(rating.facts), []

    if contract.declared(robot):
        problems = contract.validate(robot)
        items.append(Item(STATIC, "contract", "Contrato do robô válido", FAIL if problems else OK,
                          "; ".join(str(p) for p in problems) if problems else f"contrato {contract.CONTRACT_VERSION}",
                          f"Corrija o robot.json (veja docs/sdk/contrato.md): mt5x robots validate "
                          f"{robot.key.split(':', 1)[1]}" if problems else ""))
        if not problems:
            facts.update(contract.facts(robot, values))       # valores lidos pelo nome declarado, sem adivinhar
    else:
        items.append(Item(STATIC, "contract", "Contrato do robô válido", WARN,
                          "o robô não declara contrato: risco e limites são lidos por nomes prováveis de parâmetros",
                          "Crie o robot.json com \"contract\": 1 (veja docs/sdk/contrato.md)."))

    compiled = robot.stage in robot.stages and robot.stages.index(robot.stage) >= robot.stages.index(catalog.COMPILED)
    items.append(Item(STATIC, "compile", "Compila sem erros", OK if compiled else FAIL,
                      "compilado no MetaEditor" if compiled else "ainda não compilado",
                      "" if compiled else f"Rode: mt5x robots compile {robot.key.split(':', 1)[1]}"))
    items.append(_encoding_item(robot))

    stop = facts.get("mandatory_stop")
    items.append(Item(STATIC, "stop", "Stop obrigatório",
                      OK if stop else FAIL if stop is False else WARN,
                      "o stop é sempre definido" if stop else "o robô pode operar sem stop" if stop is False
                      else "não encontrei um parâmetro de stop",
                      "" if stop else "Use um preset com stop maior que zero." if stop is False
                      else "Informe \"mandatory_stop\" na seção risk do robot.json se o stop é fixo no código."))

    averaging = facts.get("averaging")
    items.append(Item(STATIC, "averaging", "Sem martingale, grid ou preço médio",
                      FAIL if averaging else OK if averaging is False else WARN,
                      "aumenta a posição depois de uma perda" if averaging
                      else "preço médio bloqueado" if averaging is False
                      else "não encontrei parâmetro de martingale, grid ou preço médio",
                      "Desligue o aumento de posição no preset." if averaging else ""))
    items.append(_orders_item(robot))

    items.append(Item(STATIC, "dll", "Sem DLL externa", FAIL if robot.dll_imports else OK,
                      "importa " + ", ".join(robot.dll_imports) if robot.dll_imports else "nenhum #import de DLL",
                      "DLL é código fora do MT5, com acesso ao computador: remova ou use outro robô."
                      if robot.dll_imports else ""))

    magic = magic_number(robot, values)
    same = [r.name for r in robots if r.key != robot.key and r.kind == catalog.MQL5 and r.has_source
            and magic and magic_number(r) == magic]
    if not magic or magic == "0":
        items.append(Item(STATIC, "magic", "Magic number exclusivo", WARN,
                          "o robô não tem magic number (ou é zero): as operações dele se misturam às manuais",
                          "Defina um magic number diferente de zero no preset."))
    else:
        items.append(Item(STATIC, "magic", "Magic number exclusivo", FAIL if same else OK,
                          f"magic {magic} também usado por: " + ", ".join(same) if same else f"magic {magic}",
                          "Dê um magic diferente a cada robô: é por ele que cada um reconhece as próprias operações."
                          if same else ""))

    problems, notes = [], []
    if rating.level == risk_mod.HIGH and "contract" not in {i.id for i in items if i.status == OK}:
        problems.append("risco classificado como Alto (" + rating.summary + ")")
    risk_pct = facts.get("risk_pct")
    if risk_pct is not None:
        (problems if risk_pct > max_risk else notes).append(f"{_num(risk_pct)}% por operação (teto {_num(max_risk)}%)")
    daily = facts.get("daily_loss_pct")
    if daily is not None:
        (problems if daily <= 0 else notes).append("sem limite de perda diário" if daily <= 0
                                                   else f"limite diário {_num(daily)}%")
    max_dd = _config_float(config, "STRATEGY", "max_drawdown", 0.20) * 100
    drawdown = facts.get("max_drawdown_pct")
    if drawdown is not None:
        (problems if drawdown <= 0 or drawdown > max_dd else notes).append(
            f"drawdown máximo {_num(drawdown)}% (teto {_num(max_dd)}%)" if drawdown > 0 else "sem limite de drawdown")
    missing = [label for key, label in (("risk_pct", "risco por operação"), ("daily_loss_pct", "limite diário"),
                                        ("max_drawdown_pct", "drawdown máximo")) if facts.get(key) is None]
    if problems:
        items.append(Item(STATIC, "limits", "Risco e limites dentro dos tetos", FAIL, "; ".join(problems),
                          "Use um preset com risco menor, limite de perda diário e drawdown máximo dentro do teto."))
    elif missing:
        profile = f"perfil {facts['profile']}: os valores ficam dentro do código; " if facts.get("profile") else ""
        items.append(Item(STATIC, "limits", "Risco e limites dentro dos tetos", WARN,
                          profile + "não foi possível conferir: " + ", ".join(missing),
                          "Informe os valores na seção risk do robot.json (risk_pct, daily_loss_pct, max_drawdown_pct)."))
    else:
        items.append(Item(STATIC, "limits", "Risco e limites dentro dos tetos", OK, "; ".join(notes)))
    return items


# --- estatística ----------------------------------------------------------------------

def _profit_factor(pnl) -> float:
    gains, losses = sum(x for x in pnl if x > 0), -sum(x for x in pnl if x < 0)
    return gains / losses if losses > 0 else (float("inf") if gains > 0 else 0.0)


def _longest_loss_streak(pnl) -> int:
    longest = current = 0
    for value in pnl:
        current = current + 1 if value < 0 else 0
        longest = max(longest, current)
    return longest


def statistical_items(robot, run: Optional[dict], config=None, db=None) -> List[Item]:
    """Itens calculados sobre as operações do backtest `run` (o mais recente do robô)."""
    from mt5_extracao.strategies import research, specs
    from mt5_extracao.strategies.validation import monte_carlo
    slug = robot.key.split(":", 1)[1]
    if run is None:
        return [Item(STATISTICAL, "run", "Há um backtest gravado", FAIL, "nenhum backtest deste robô",
                     f"Rode: mt5x robots backtest {slug} --symbol <ativo> --tf <timeframe>")]
    trades = run.get("trades") or []
    where = f"{run['symbol']} {run['timeframe']}, {run['data_start'][:10]} a {run['data_end'][:10]}"
    if not trades and run["metrics"].get("trades"):
        return [Item(STATISTICAL, "run", "Há um backtest gravado", FAIL,
                     f"o backtest de {where} é de uma versão anterior e não guardou as operações",
                     f"Rode o backtest de novo: mt5x robots backtest {slug} --symbol {run['symbol']} --tf {run['timeframe']}")]
    criteria = research.criteria(config)
    pnl = [float(t[1]) for t in trades]
    initial = float(run["metrics"].get("initial_equity") or 0.0) or 1.0
    items = [Item(STATISTICAL, "run", "Há um backtest gravado", OK, where)]

    enough = len(pnl) >= MIN_TRADES
    items.append(Item(STATISTICAL, "trades", f"Pelo menos {MIN_TRADES} operações", OK if enough else FAIL,
                      f"{len(pnl)} operações", "" if enough else "Teste um período maior ou um timeframe menor."))
    pf = _profit_factor(pnl)
    pf_ok = pf > criteria.min_profit_factor
    items.append(Item(STATISTICAL, "profit_factor", f"Profit factor acima de {_num(criteria.min_profit_factor)}",
                      OK if pf_ok else FAIL, "profit factor " + ("sem perdas" if pf == float("inf") else _num(pf)),
                      "" if pf_ok else "O que o robô ganha não cobre o que perde com folga: não opere este ajuste."))
    expectancy = sum(pnl) / len(pnl) if pnl else 0.0
    items.append(Item(STATISTICAL, "expectancy", "Expectativa positiva com custos", OK if expectancy > 0 else FAIL,
                      f"{_num(expectancy)} por operação, em média",
                      "" if expectancy > 0 else "Na média cada operação perde dinheiro."))

    equity, returns = initial, []
    for value in pnl:
        returns.append(SimpleNamespace(return_pct=value / equity if equity > 0 else 0.0))
        equity += value
    p95 = monte_carlo(returns, criteria.mc_runs, 95.0, criteria.seed)
    p99 = monte_carlo(returns, criteria.mc_runs, 99.0, criteria.seed)
    dd_status = FAIL if not pnl or p95["dd_percentile"] > criteria.max_drawdown \
        else WARN if p99["dd_percentile"] > criteria.max_drawdown else OK
    items.append(Item(STATISTICAL, "drawdown", f"Drawdown de Monte Carlo até {_pct(criteria.max_drawdown, 0)}", dd_status,
                      f"p95 {_pct(p95['dd_percentile'])} · p99 {_pct(p99['dd_percentile'])} em {p95['runs']} simulações",
                      "" if dd_status == OK else "Reduza o risco por operação: embaralhando as operações, a queda passa do limite."))
    ruin = p95["ruin_probability"]
    items.append(Item(STATISTICAL, "ruin", "Chance de ruína desprezível", OK if pnl and ruin <= MAX_RUIN_PROBABILITY else FAIL,
                      f"{_pct(ruin)} das simulações perdem metade do capital (máximo {_pct(MAX_RUIN_PROBABILITY, 0)})",
                      "" if ruin <= MAX_RUIN_PROBABILITY else "Reduza o risco por operação."))

    if robot.kind == catalog.PYTHON:
        result = next((r for r in robot.results if (r["symbol"], r["timeframe"]) == (run["symbol"], run["timeframe"])),
                      None)
        fix = f"Rode: mt5x validate --symbols {run['symbol']} --tf {run['timeframe']} --strategies {slug}"
        if result is None:
            items.append(Item(STATISTICAL, "out_of_sample", "Fora da amostra (walk-forward)", FAIL,
                              f"sem validação gravada para {run['symbol']} {run['timeframe']}", fix))
            items.append(Item(STATISTICAL, "robustness", "Robustez com parâmetros ±20%", FAIL, "sem validação gravada", fix))
        else:
            items.append(Item(STATISTICAL, "out_of_sample", "Fora da amostra (walk-forward)",
                              OK if result["approved"] else FAIL,
                              f"profit factor {_num(result['profit_factor'])} em {result['trades']} operações fora da "
                              f"amostra; {_pct(result['positive_windows'])} das janelas com lucro",
                              "" if result["approved"] else "A validação reprovou: " + str(result["status"])))
            items.append(Item(STATISTICAL, "robustness", "Robustez com parâmetros ±20%", OK if result["robust"] else FAIL,
                              "o resultado se mantém com os parâmetros vizinhos" if result["robust"]
                              else "o resultado depende do ajuste exato dos parâmetros",
                              "" if result["robust"] else "Ajuste frágil costuma ser acaso: não opere."))
    else:
        recent = pnl[len(pnl) - int(len(pnl) * RECENT_SHARE):] if pnl else []
        recent_pf = _profit_factor(recent)
        if len(recent) < MIN_RECENT_TRADES:
            items.append(Item(STATISTICAL, "out_of_sample", "Resultado se mantém no fim do período", WARN,
                              f"só {len(recent)} operações nos últimos {_pct(RECENT_SHARE, 0)} do teste",
                              "Teste um período maior."))
        else:
            items.append(Item(STATISTICAL, "out_of_sample", "Resultado se mantém no fim do período",
                              OK if recent_pf > 1.0 else FAIL,
                              f"profit factor {'sem perdas' if recent_pf == float('inf') else _num(recent_pf)} nas últimas "
                              f"{len(recent)} operações ({_pct(RECENT_SHARE, 0)} finais do teste)",
                              "" if recent_pf > 1.0 else "O robô parou de funcionar na parte mais recente do teste."))
        items.append(Item(STATISTICAL, "robustness", "Robustez com parâmetros ±20%", WARN,
                          "não avaliado nesta versão para robôs MQL5 (exige repetir o Strategy Tester com os parâmetros "
                          "vizinhos)", "Repita o backtest com --param mudando cada parâmetro em ±20% e compare."))

    spec = specs.load_spec(db, run["symbol"]) if db is not None else None
    extra = spec.spread_points * spec.point * spec.money_per_price_unit if spec is not None and spec.known else 0.0
    if extra > 0 and all(len(t) > 2 and t[2] for t in trades) and trades:
        stressed = _profit_factor([float(t[1]) - extra * float(t[2]) for t in trades])
        items.append(Item(STATISTICAL, "cost_stress", "Aguenta o dobro do spread", OK if stressed > 1.0 else FAIL,
                          f"profit factor {'sem perdas' if stressed == float('inf') else _num(stressed)} com um spread "
                          f"a mais por operação ({_num(spec.spread_points, 0)} pontos)",
                          "" if stressed > 1.0 else "O lucro some com um custo um pouco maior: margem pequena demais."))
    else:
        items.append(Item(STATISTICAL, "cost_stress", "Aguenta o dobro do spread", WARN,
                          f"não avaliado: falta a especificação de contrato de {run['symbol']} (spread e valor do ponto)",
                          f"Rode: mt5x specs --symbols {run['symbol']} e repita a bateria."))

    streak = _longest_loss_streak(pnl)
    items.append(Item(STATISTICAL, "loss_streak", "Sequência de perdas suportável",
                      WARN if streak >= LONG_LOSS_STREAK else OK, f"maior sequência: {streak} perdas seguidas",
                      "Confira se você aguenta essa sequência sem desligar o robô." if streak >= LONG_LOSS_STREAK else ""))
    return items


# --- execução -------------------------------------------------------------------------

def run_battery(db, config, robot, robots=(), preset: Optional[str] = None) -> Report:
    """Roda a bateria sobre o backtest mais recente do robô e grava o resultado em `_robot_checks`."""
    runs = store.load_runs(db, robot.key)
    run = runs[0] if runs else None
    report = Report(robot.key, preset or "", run_id=run["run_id"] if run else "", check_id=uuid.uuid4().hex,
                    created_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    report.items = static_items(robot, config, robots, preset) + statistical_items(robot, run, config, db)
    store.save_check(db, report.to_dict())
    return report
