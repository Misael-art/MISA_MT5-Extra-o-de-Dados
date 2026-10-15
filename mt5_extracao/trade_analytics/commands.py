"""`mt5x trades ...` (Laboratório, Fase 16). Registrado em cli.build_parser por `add_parser`; a saída usa os auxiliares
do cli. Cada tarefa da Fase 16 acrescenta aqui o seu subcomando (T16.12 fecha o conjunto)."""
import argparse

MODE_KEYS = ("ponto_fixo", "alvo_r", "break_even", "trailing_atr", "trailing_barras", "gradiente_fixo",
             "gradiente_dinamico", "gradiente_direcional", "martingale", "anti_martingale")


def _print(message="") -> None:
    from mt5_extracao import cli
    cli._print(message)


def add_parser(sub) -> None:
    p = sub.add_parser("trades", help="Laboratório: livro de operações dos robôs (importar, consultar, analisar)")
    ts = p.add_subparsers(dest="trades_command", required=True)
    q = ts.add_parser("import", help="traz para o livro os backtests já gravados que ainda não estão nele")
    q.add_argument("--include-hidden", action="store_true", help="inclui execuções auxiliares (bateria, paridade)")
    ts.add_parser("runs", help="lista os runs do livro (robô, ativo, timeframe, operações, completo ou parcial)")

    q = ts.add_parser("enrich", help="calcula ou atualiza o contexto das operações no livro")
    _scope_arguments(q, required=False)

    q = ts.add_parser("report", help="relatório de operações pelas mesmas métricas do Laboratório")
    _scope_arguments(q, required=True)

    q = ts.add_parser("simulate", help="re-simula um run sobre as mesmas entradas e barras")
    q.add_argument("--run-id", action="append", required=True, help="run no livro (pode repetir)")
    q.add_argument("--mode", required=True, choices=MODE_KEYS, help="modo de execução a comparar")
    q.add_argument("--param", action="append", default=[], help="parâmetro do modo como nome=valor (pode repetir)")
    q.add_argument("--save", action="store_true", help="grava a re-simulação como resim:<modo> no livro")

    q = ts.add_parser("insights", help="procura contextos com diferença consistente fora da amostra")
    _scope_arguments(q, required=True)
    q.add_argument("--mode", action="append", choices=MODE_KEYS[1:], default=[],
                   help="re-simula este modo para procurar diferenças por contexto (pode repetir)")

    q = ts.add_parser("export", help="exporta uma proposta de regras para estudo, sem aplicá-las")
    _scope_arguments(q, required=True)
    q.add_argument("--rules-file", required=True, help="JSON com uma lista de regras (ou objeto com rules/regras)")
    q.add_argument("--rule", action="append", default=[], help="regra adicional em JSON (pode repetir)")
    q.add_argument("--tested", type=int, help="alternativas testadas; padrão: valor do arquivo ou dos insights")
    q.add_argument("--out-dir", help="pasta de saída (padrão: exports/propostas)")

    c = ts.add_parser("calendar", help="calendário econômico de várias fontes (CSV e MT5)")
    cs = c.add_subparsers(dest="calendar_command", required=True)
    q = cs.add_parser("import", help="importa uma fonte para o calendário unido")
    q.add_argument("--source", required=True, choices=("csv", "mt5"))
    q.add_argument("--path", help="arquivo (padrão: [SCREENER] events_file, ou a exportação mais recente do MT5)")
    cs.add_parser("status", help="eventos por fonte, período coberto e quantos de impacto alto")
    cs.add_parser("install", help="instala e compila o script MT5X_CalendarExport no MT5")
    q = cs.add_parser("export", help="pede ao MT5 a exportação do calendário (abre o terminal com o script)")
    q.add_argument("--days-back", type=int, help="padrão: [TRADE_ANALYTICS] calendar_days_back")
    q.add_argument("--days-forward", type=int, help="padrão: [TRADE_ANALYTICS] calendar_days_forward")
    q.add_argument("--countries", help="ex.: BR,US (padrão: [TRADE_ANALYTICS] calendar_countries; vazio = todos)")
    q.add_argument("--close-terminal", action="store_true", help="fecha o MT5 antes, se estiver aberto")
    p.set_defaults(func=run)


def _scope_arguments(parser, required: bool) -> None:
    scope = parser.add_mutually_exclusive_group(required=required)
    scope.add_argument("--robot", help="lente por robô (chave do catálogo)")
    scope.add_argument("--symbol", help="lente por ativo")


def run(args: argparse.Namespace, ctx) -> int:
    from mt5_extracao import cli
    from mt5_extracao.trade_analytics import importers, ledger
    out, data = cli._print, cli._data
    db = ctx.db
    if args.trades_command == "import":
        done = importers.import_existing_runs(db, include_hidden=args.include_hidden)
        out(f"{done['runs']} run(s) importado(s), {done['trades']} operação(ões); {done['skipped']} já estavam no "
            "livro ou são auxiliares.")
        data(**done)
        return cli.EXIT_OK
    if args.trades_command == "runs":
        items = ledger.runs(db)
        for r in items:
            out(f"{r['run_id'][:12]}\t{r['robot_key']}\t{r['symbol']}\t{r['timeframe']}\t{r['trades']} operações\t"
                f"{r['completeness']}")
        out(f"\n{len(items)} run(s) no livro.")
        data(runs=items)
        return cli.EXIT_OK
    if args.trades_command == "enrich":
        return _enrich(args, ctx)
    if args.trades_command == "report":
        return _report(args, ctx)
    if args.trades_command == "simulate":
        return _simulate(args, ctx)
    if args.trades_command == "insights":
        return _insights(args, ctx)
    if args.trades_command == "export":
        return _export(args, ctx)
    if args.trades_command == "calendar":
        return _calendar(args, ctx)
    raise ValueError(f"subcomando desconhecido: {args.trades_command}")


def _scope_filters(args) -> dict:
    if getattr(args, "robot", None):
        return {"robot_key": args.robot}
    if getattr(args, "symbol", None):
        return {"symbol": args.symbol}
    return {}


def _scope_name(args) -> str:
    if getattr(args, "robot", None):
        return "robot:" + args.robot
    if getattr(args, "symbol", None):
        return "symbol:" + args.symbol
    return "all"


def _load_lab(args, ctx):
    from mt5_extracao.trade_analytics import aggregate
    return aggregate.frame(ctx.db, ctx.config, **_scope_filters(args))


def _records(frame):
    """Converte tabelas pandas em registros que o serializador JSON do CLI consegue tratar."""
    if frame is None or frame.empty:
        return []
    return frame.astype(object).where(frame.notna(), None).to_dict("records")


def _money(value) -> str:
    if value is None:
        return "—"
    text = f"{abs(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return text if value >= 0 else "-" + text


def _enrich(args, ctx) -> int:
    from mt5_extracao import cli
    from mt5_extracao.trade_analytics import ledger
    lab = _load_lab(args, ctx)
    partial = int((lab["completeness"] == ledger.PARTIAL).sum()) if not lab.empty else 0
    payload = {"scope": _scope_name(args), "operations": len(lab), "runs": int(lab["run_id"].nunique()) if not lab.empty else 0,
               "complete": len(lab) - partial, "partial": partial,
               "calendar_covered": int(lab["event_impact"].notna().sum()) if "event_impact" in lab else 0,
               "features": [c for c in ("day_of_week", "time_bucket", "event_impact", "entry_volume", "atr_pct",
                                         "volatility_bucket") if c in lab]}
    if payload["operations"]:
        _print(f"{payload['operations']} operação(ões) enriquecida(s) em {payload['runs']} run(s): "
               f"{payload['complete']} completa(s), {payload['partial']} parcial(is).")
    else:
        _print("Nenhuma operação no livro para este escopo.")
    cli._data(enrichment=payload)
    return cli.EXIT_OK


def _report(args, ctx) -> int:
    from mt5_extracao import cli
    from mt5_extracao.trade_analytics import aggregate, ledger
    lab = _load_lab(args, ctx)
    runs = [r for r in ledger.runs(ctx.db) if (not args.robot or r["robot_key"] == args.robot) and
            (not args.symbol or r["symbol"] == args.symbol)]
    scope_table = aggregate.robot_view(lab, ctx.config) if args.robot else aggregate.asset_view(lab, ctx.config)
    dimensions = {}
    heatmaps = {}
    metrics = None
    if not lab.empty:
        metrics = aggregate.summarize(lab, aggregate._initial_equity(ctx.config), aggregate.min_trades(ctx.config))
        for dim in ("day_of_week", "time_bucket", "event_impact", "volatility_bucket"):
            dimensions[dim] = _records(aggregate.by_dimension(lab, [dim], ctx.config))
        heatmaps = {
            "time_by_day": _records(aggregate.by_dimension(lab, ["time_bucket", "day_of_week"], ctx.config,
                                                           ci=False)),
            "volatility_by_time": _records(aggregate.by_dimension(lab, ["volatility_bucket", "time_bucket"],
                                                                   ctx.config, ci=False)),
        }
    partial = int((lab["completeness"] == ledger.PARTIAL).sum()) if not lab.empty else 0
    payload = {"scope": _scope_name(args), "summary": metrics, "runs": runs, "scope_breakdown": _records(scope_table),
               "dimensions": dimensions, "heatmaps": heatmaps,
               "coverage": {"partial_operations": partial,
                            "calendar_operations": int(lab["event_impact"].notna().sum())
                            if "event_impact" in lab else 0}}
    if metrics is None:
        _print(f"Nenhuma operação no livro para {_scope_name(args)}.")
    else:
        _print(f"Relatório do Laboratório — {_scope_name(args)}")
        _print(f"  Operações: {metrics['n']} · acerto: {cli._fmt_pct(metrics['win_rate'])} · fator de lucro: "
               f"{cli._fmt_num(metrics['profit_factor'])} · expectativa: {_money(metrics['expectancy'])}")
        _print(f"  Resultado líquido: {_money(metrics['net'])} · queda máxima: "
               f"{cli._fmt_pct(metrics['max_drawdown_pct'])}")
        if metrics["ci_low"] is not None:
            _print(f"  Intervalo de 90% da expectativa: {_money(metrics['ci_low'])} a {_money(metrics['ci_high'])}")
        else:
            _print("  Intervalo de 90%: poucos dias para estimar")
        if partial:
            _print(f"  Aviso: {partial} operação(ões) parcial(is), sem horário de entrada.")
        if scope_table is not None and not scope_table.empty:
            _print("  Comparativo:")
            for row in scope_table.to_dict("records"):
                name = row.get("symbol") if args.robot else row.get("robot_key")
                _print(f"    {name} {row.get('timeframe', '')}: {row.get('n', 0)} operações, expectativa "
                       f"{_money(row.get('expectancy'))}")
    cli._data(report=payload)
    return cli.EXIT_OK


def _mode_params(items) -> dict:
    import math
    from mt5_extracao import cli
    params = cli._parse_params(items)
    if any(not key for key in params):
        raise ValueError("--param exige um nome antes de '='")
    if any(not math.isfinite(value) for value in params.values()):
        raise ValueError("--param exige valores numéricos finitos")
    return params


def _robot_for(db, config, robot_key):
    from mt5_extracao.robots import catalog
    robots = catalog.load_catalog(config, db)
    try:
        return catalog.get_robot(robots, robot_key)
    except ValueError:
        return None


def _simulate(args, ctx) -> int:
    from mt5_extracao import cli
    from mt5_extracao.trade_analytics import ledger
    from mt5_extracao.trade_analytics.execution import modes, runner
    params = _mode_params(args.param)
    unknown = sorted(set(params) - set(modes.MODES[args.mode].params))
    if unknown:
        raise ValueError(f"Parâmetro(s) {', '.join(unknown)} não pertence(m) ao modo {args.mode}.")
    results = []
    for run_id in dict.fromkeys(args.run_id):
        trades = ledger.load(ctx.db, run_id=run_id)
        if trades.empty:
            raise ValueError(f"Run {run_id} não está no livro de operações.")
        robot_key = str(trades.iloc[0]["robot_key"])
        robot = _robot_for(ctx.db, ctx.config, robot_key)
        compared = runner.compare(ctx.db, ctx.config, run_id, [args.mode], params={args.mode: params},
                                  save=args.save, robot=robot)
        item = compared[args.mode]
        result = item["result"]
        sequence = item.get("sequence")
        row = {"run_id": run_id, "robot_key": robot_key, "symbol": str(trades.iloc[0]["symbol"]),
               "timeframe": str(trades.iloc[0]["timeframe"]), "mode": args.mode, "name": item["name"],
               "metrics": item["metrics"], "study_only": bool(item.get("study_only")), "saved": bool(args.save)}
        if result is not None:
            row.update(skipped=result.skipped, approximate=result.approximate, adds=result.adds, capped=result.capped,
                       max_volume=result.max_volume)
        if sequence is not None:
            row.update(triggers=sequence["triggers"], max_streak=sequence["max_streak"],
                       max_loss=sequence["max_loss"], halted=sequence["halted"],
                       safety_notes=item.get("safety_notes", []))
        results.append(row)
        metrics = item["metrics"]
        _print(f"{run_id[:12]} · {item['name']}: {metrics['n']} operações, acerto "
               f"{cli._fmt_pct(metrics['win_rate'])}, fator de lucro {cli._fmt_num(metrics['profit_factor'])}, "
               f"expectativa {_money(metrics['expectancy'])}.")
        if item.get("study_only"):
            _print("  Só estudo: martingale/anti-martingale não é proposta para operação real.")
        if sequence is not None:
            _print("  Travas disparadas: " + ", ".join(f"{name}: {count}" for name, count in sequence["triggers"].items()))
    cli._data(simulations=results)
    return cli.EXIT_OK


def _time_key(value):
    import pandas as pd
    if value is None or pd.isna(value):
        return None
    stamp = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(stamp) else pd.Timestamp(stamp).isoformat()


def _mode_frame(run_id, base, item):
    from collections import defaultdict, deque
    import pandas as pd
    by_entry = defaultdict(deque)
    for row in base.sort_values("seq").to_dict("records"):
        key = _time_key(row.get("entry_time"))
        if key is not None:
            by_entry[key].append(int(row["seq"]))
    result = item.get("result")
    if result is not None:
        trades = result.trades
    else:
        frame = item["sequence"]["trades"]
        trades = frame.loc[~frame["skipped"]].itertuples(index=False)
    rows = []
    for trade in trades:
        key = _time_key(getattr(trade, "entry_time", None))
        if key is None or not by_entry[key]:
            continue
        rows.append({"run_id": run_id, "seq": by_entry[key].popleft(), "pnl": float(trade.pnl)})
    return pd.DataFrame(rows, columns=("run_id", "seq", "pnl"))


def _mode_frames(ctx, lab, wanted, strict=False):
    import pandas as pd
    from mt5_extracao.trade_analytics.execution import modes, runner
    wanted = list(dict.fromkeys(wanted))
    frames = {key: [] for key in wanted}
    skipped = []
    robot_cache = {}
    for run_id in lab["run_id"].drop_duplicates().tolist():
        base = lab[lab["run_id"] == run_id]
        robot_key = str(base.iloc[0]["robot_key"])
        if robot_key not in robot_cache:
            robot_cache[robot_key] = _robot_for(ctx.db, ctx.config, robot_key)
        robot = robot_cache[robot_key]
        available = modes.available(robot)
        unsupported = [key for key in wanted if key not in available]
        if unsupported:
            if strict:
                raise ValueError(f"Modo(s) {', '.join(unsupported)} indisponível(is) para {robot_key} no run {run_id}.")
            skipped.append({"run_id": run_id, "modes": unsupported})
            continue
        request = list(dict.fromkeys(["ponto_fixo", *wanted]))
        compared = runner.compare(ctx.db, ctx.config, run_id, request, robot=robot)
        for key in wanted:
            frames[key].append(_mode_frame(run_id, base, compared[key]))
    combined = {key: pd.concat(parts, ignore_index=True) if parts else
                pd.DataFrame(columns=("run_id", "seq", "pnl")) for key, parts in frames.items()}
    if strict:
        missing = [key for key, frame in combined.items() if frame.empty]
        if missing:
            raise ValueError(f"Não foi possível re-simular o(s) modo(s) {', '.join(missing)} neste escopo.")
    return combined, skipped


def _insights(args, ctx) -> int:
    from dataclasses import asdict
    from mt5_extracao import cli
    from mt5_extracao.trade_analytics import insights
    lab = _load_lab(args, ctx)
    if lab.empty:
        _print(f"Nenhuma operação no livro para {_scope_name(args)}.")
        cli._data(scope=_scope_name(args), insights=[], saved=0, runs_without_modes=[])
        return cli.EXIT_OK
    found = insights.discover(lab, ctx.config)
    skipped = []
    if args.mode:
        modes, skipped = _mode_frames(ctx, lab, args.mode)
        found.extend(insights.mode_insights(lab, modes, ctx.config))
    saved = insights.save(ctx.db, _scope_name(args), found)
    for item in found:
        _print(f"{item.state}: {item.text}")
    if not found:
        _print("Nenhum contexto atingiu a amostra mínima para gerar uma pista.")
    _print(f"{len(found)} insight(s); {saved} gravado(s) em {_scope_name(args)}.")
    cli._data(scope=_scope_name(args), insights=[asdict(item) for item in found], saved=saved,
              runs_without_modes=skipped)
    return cli.EXIT_OK


def _rules_from_args(args):
    import json
    from pathlib import Path
    try:
        source = json.loads(Path(args.rules_file).expanduser().read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Não foi possível ler --rules-file como JSON: {exc}") from exc
    if isinstance(source, list):
        rules, tested = source, None
    elif isinstance(source, dict):
        rules = source.get("rules", source.get("regras"))
        tested = source.get("tested", source.get("alternativas_testadas"))
    else:
        raise ValueError("--rules-file deve conter uma lista ou um objeto com rules/regras")
    if not isinstance(rules, list):
        raise ValueError("O JSON de regras deve conter uma lista em rules/regras")
    import json as json_module
    for raw in args.rule:
        try:
            rule = json_module.loads(raw)
        except json_module.JSONDecodeError as exc:
            raise ValueError(f"--rule deve ser um objeto JSON: {exc}") from exc
        if not isinstance(rule, dict):
            raise ValueError("Cada --rule deve ser um objeto JSON")
        rules.append(rule)
    if not rules or any(not isinstance(rule, dict) for rule in rules):
        raise ValueError("informe ao menos uma regra como objeto JSON")
    from mt5_extracao.trade_analytics import rules as rule_engine
    unknown = sorted({str(rule.get("tipo")) for rule in rules if rule.get("tipo") not in rule_engine.TYPES})
    if unknown:
        raise ValueError(f"Tipo(s) de regra desconhecido(s): {', '.join(unknown)}")
    return rules, tested


def _history_tested(db, scope) -> int:
    import json
    from mt5_extracao.trade_analytics import insights
    tested = []
    for row in insights.load(db, scope):
        try:
            tested.append(int(json.loads(row.get("data_json") or "{}").get("tested", 0)))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
    return max(tested, default=0)


def _export(args, ctx) -> int:
    from pathlib import Path
    from mt5_extracao import cli
    from mt5_extracao.trade_analytics import export, rules as rule_engine
    lab = _load_lab(args, ctx)
    if lab.empty:
        raise ValueError(f"Nenhuma operação no livro para exportar em {_scope_name(args)}.")
    rule_list, file_tested = _rules_from_args(args)
    for rule in rule_list:
        rule_engine.describe(rule)
    needed_modes = list(dict.fromkeys(rule["modo"] for rule in rule_list if rule.get("tipo") == "trocar_modo"))
    mode_frames = _mode_frames(ctx, lab, needed_modes, strict=True)[0] if needed_modes else {}
    try:
        evaluation = rule_engine.evaluate(lab, rule_list, ctx.config, mode_frames)
    except (KeyError, TypeError) as exc:
        raise ValueError(f"Regra inválida: {exc}") from exc
    tested = args.tested if args.tested is not None else file_tested
    if tested is None:
        tested = _history_tested(ctx.db, _scope_name(args))
    if int(tested) < 0:
        raise ValueError("--tested não pode ser negativo")
    folder = Path(args.out_dir).expanduser() if args.out_dir else None
    path = export.proposal(_scope_name(args), rule_list, evaluation, int(tested), folder=folder)
    _print(f"Proposta de estudo gravada em {path}. Ela não foi aplicada.")
    cli._data(proposal={"path": str(path), "scope": _scope_name(args), "rules": rule_list,
                        "tested": int(tested), "evaluation": evaluation})
    return cli.EXIT_OK


def _calendar(args, ctx) -> int:
    from mt5_extracao import cli
    from mt5_extracao.trade_analytics import calendar as cal
    from mt5_extracao.trade_analytics.calendar import mt5_export
    out, data = cli._print, cli._data
    config = ctx.config
    sub = args.calendar_command
    if sub == "import":
        count = cal.import_source(ctx.db, config, args.source, args.path)
        out(f"{count} evento(s) importado(s) da fonte {args.source}.")
        data(source=args.source, events=count)
        return cli.EXIT_OK
    if sub == "status":
        info = cal.status(ctx.db)
        for source, s in info.items():
            out(f"{source}: {s['events']} evento(s) de {s['first']:%Y-%m-%d} a {s['last']:%Y-%m-%d}, "
                f"{s['high_impact']} de impacto alto")
        if not info:
            out("Calendário vazio. Importe o CSV (--source csv) ou exporte do MT5 (mt5x trades calendar export).")
        merged = cal.merged(ctx.db)
        out(f"Unido: {len(merged)} evento(s) depois de juntar as fontes.")
        data(sources=info, merged_events=len(merged))
        return cli.EXIT_OK
    if sub == "install":
        result = mt5_export.install(config)
        out(f"Script instalado em {result['source']}: " + ("compilado." if result["ok"] else
                                                         "NÃO compilou: " + "; ".join(result["errors"])))
        data(**result)
        return cli.EXIT_OK if result["ok"] else cli.EXIT_FAIL
    if sub == "export":
        from mt5_extracao.robots import mt5_tools
        get = lambda key, default: config.get("TRADE_ANALYTICS", key, fallback=default)  # noqa: E731
        days_back = args.days_back or int(get("calendar_days_back", "1095") or 1095)
        days_forward = args.days_forward or int(get("calendar_days_forward", "30") or 30)
        countries = args.countries if args.countries is not None else get("calendar_countries", "")
        request = mt5_export.write_request(config, days_back, days_forward, countries,
                                           int(get("calendar_min_importance", "0") or 0))
        if args.close_terminal and mt5_tools.terminal_running(config):
            mt5_tools.close_terminal(config=config)
        mt5_export.start(config)
        out(f"Pedido gravado em {request}. O MT5 abre e roda o script; quando terminar, importe com "
            "`mt5x trades calendar import --source mt5`.")
        data(request=str(request))
        return cli.EXIT_OK
    raise ValueError(f"subcomando desconhecido: {sub}")
