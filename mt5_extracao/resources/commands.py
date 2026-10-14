"""`mt5x resources ...` (T15.4). Registrado em cli.build_parser por `add_parser`; a saída usa os auxiliares do cli."""
import argparse

from mt5_extracao.resources import actions, automation, files, hygiene, storage
from mt5_extracao.resources import policy as policy_mod


def add_parser(sub) -> None:
    p = sub.add_parser("resources", help="gestão de recursos: espaço da base e do disco, higienização, arquivo "
                                         "reversível e automação opt-in")
    rs = p.add_subparsers(dest="resources_command", required=True)
    rs.add_parser("status", help="tamanho da base, das tabelas, disco livre, crescimento e alertas")
    q = rs.add_parser("scan", help="séries candidatas a sair da base (sem lucro, sem estudo, obsoletas, vazias)")
    q.add_argument("--rules", default="", help="limita às regras, separadas por vírgula")
    q.add_argument("--selected", default="", help="ativos na seleção (protegidos), separados por vírgula")
    q = rs.add_parser("archive", help="arquiva (e retira da base) as tabelas indicadas; reversível com restore")
    q.add_argument("tables", nargs="*", help="nomes das tabelas (veja mt5x resources scan)")
    q.add_argument("--rules", default="", help="em vez de nomes: arquiva as candidatas destas regras")
    q.add_argument("--yes", action="store_true", help="confirma (sem ela nada é alterado)")
    rs.add_parser("archives", help="lista os arquivos que podem ser restaurados")
    q = rs.add_parser("restore", help="devolve à base a tabela de um arquivo")
    q.add_argument("path", help="caminho do arquivo (veja mt5x resources archives)")
    q.add_argument("--yes", action="store_true")
    q = rs.add_parser("vacuum", help="devolve ao disco o espaço livre dentro da base")
    q.add_argument("--yes", action="store_true")
    q = rs.add_parser("orphans", help="registros de controle de tabelas que não existem mais (--yes remove)")
    q.add_argument("--yes", action="store_true")
    rs.add_parser("auto", help="um ciclo de monitoramento (para o cron / Agendador de Tarefas); só age com "
                               "[RESOURCES] auto_enabled = true")
    q = rs.add_parser("backup", help="cópia de segurança conferida da base (SQLite), guardando as mais novas")
    q.add_argument("--yes", action="store_true")
    rs.add_parser("backups", help="lista as cópias de segurança")
    q = rs.add_parser("restore-backup", help="volta a base inteira a uma cópia (guarda antes o estado atual)")
    q.add_argument("path")
    q.add_argument("--yes", action="store_true")
    q = rs.add_parser("trim", help="apara séries: arquiva e retira as linhas mais velhas que --days (reversível)")
    q.add_argument("tables", nargs="*", help="tabelas; sem nomes, usa a retenção de [RESOURCES]")
    q.add_argument("--days", type=int, help="manter os últimos N dias")
    q.add_argument("--yes", action="store_true")
    q = rs.add_parser("files", help="logs e exportações mais velhos que [RESOURCES] logs_keep_days / "
                                    "exports_keep_days (--yes apaga)")
    q.add_argument("--yes", action="store_true")
    q = rs.add_parser("history", help="medições gravadas e ações executadas")
    q.add_argument("--limit", type=int, default=30)
    p.set_defaults(func=run)


def _rules(raw: str):
    rules = tuple(r.strip() for r in raw.split(",") if r.strip())
    unknown = [r for r in rules if r not in hygiene.RULES]
    if unknown:
        raise ValueError(f"regra(s) desconhecida(s): {', '.join(unknown)} (use {', '.join(hygiene.RULES)})")
    return rules or None


def run(args: argparse.Namespace, ctx) -> int:
    from mt5_extracao import cli
    out, data, err = cli._print, cli._data, cli._err
    db, config = ctx.db, ctx.config
    policy = policy_mod.load(config)
    for message in policy.errors:
        out("Aviso: " + message)
    hb = storage.human_bytes
    command = args.resources_command

    if command == "status":
        report = storage.measure(db, policy_mod.archive_dir(db, policy), config=config)
        storage.record_snapshot(db, report, policy.keep_snapshots_days)
        trend = storage.growth(storage.load_snapshots(db, 90))
        found = automation.alerts(report, policy)
        disk = report["disk"]
        out(f"Base ({report['dialect']}): {hb(report['db_bytes'])}; recuperável com otimizar: "
            f"{hb(report['reclaimable_bytes'])}")
        if disk:
            out(f"Disco ({disk['path']}): {hb(disk['free'])} livres de {hb(disk['total'])} ({disk['free_pct']:.1f}%)")
        if trend:
            out(f"Crescimento: {hb(trend['bytes_per_day'])}/dia" + (
                f"; disco cheio em ~{trend['days_to_full']:.0f} dias" if trend["days_to_full"] else ""))
        for label, info in report["dirs"].items():
            out(f"{label}: {hb(info['bytes'])} ({info['path']})")
        out("\nMaiores tabelas:")
        for t in report["tables"][:15]:
            out(f"  {t['table']}\t{t['kind']}\t{t['rows']} linhas\t{hb(t['bytes'])}" + ("*" if t["estimated"] else ""))
        for a in found:
            out(f"[{a['level']}] {a['text']}")
        for w in report["warnings"]:
            err(w)
        data(**{k: report[k] for k in ("db_bytes", "reclaimable_bytes", "disk", "tables", "dirs", "dialect")},
             growth=trend, alerts=found, automation={"enabled": policy.auto_enabled,
                                                     "last_cycle": automation.last_cycle(db)})
        return cli.EXIT_OK if report["db_bytes"] is not None else cli.EXIT_FAIL

    if command == "scan":
        sizes = {t["table"]: t["bytes"] for t in storage.measure(db, with_dirs=False)["tables"]}
        selected = [s.strip() for s in args.selected.split(",") if s.strip()]
        found = hygiene.scan(db, policy, selected, sizes, rules=_rules(args.rules))
        for c in found["candidates"]:
            out(f"{c['table']}\t{hb(c['bytes'])}\t" + "; ".join(f"{hygiene.LABELS.get(k, k)}: {v}"
                                                               for k, v in c["reasons"].items()))
        out(f"\n{len(found['candidates'])} candidata(s), "
            f"{hb(sum(c['bytes'] or 0 for c in found['candidates']))} no total; "
            f"{len(found['protected'])} protegida(s) apesar de cair em alguma regra.")
        orphans = found["orphans"]
        if any(orphans.values()):
            out(f"Registros órfãos: {len(orphans['symbol_tables'])} em _symbol_tables, "
                f"{len(orphans['extraction_log'])} tabela(s) em _extraction_log (mt5x resources orphans --yes).")
        data(**found)
        return cli.EXIT_OK

    if command == "archive":
        tables, reasons = list(args.tables), {}
        if args.rules:
            found = hygiene.scan(db, policy, rules=_rules(args.rules))
            reasons = {c["table"]: c["reasons"] for c in found["candidates"]}
            tables += list(reasons)
        if not tables:
            raise ValueError("informe as tabelas ou --rules")
        if not args.yes:
            out("Seriam arquivadas: " + ", ".join(tables))
            err("nada foi alterado: confirme com --yes")
            data(would_archive=tables)
            return cli.EXIT_USAGE
        results = actions.archive_tables(db, config, tables, origin="cli", reasons=reasons)
        for r in results:
            out(f"{r['table']}: " + (f"arquivada em {r['archive']} ({r['rows']} linhas)" if r["ok"]
                                     else f"NÃO arquivada: {r['error']}"))
        data(results=results)
        return cli.EXIT_OK if all(r["ok"] for r in results) else cli.EXIT_FAIL

    if command == "archives":
        items = actions.list_archives(db, config)
        for a in items:
            out(f"{a['path']}\t{a['table']}\t{a['rows']} linhas\t{hb(a['bytes'])}\t{a['archived_at'] or ''}")
        out(f"\n{len(items)} arquivo(s).")
        data(archives=items)
        return cli.EXIT_OK

    if command == "restore":
        if not args.yes:
            err("nada foi alterado: confirme com --yes")
            return cli.EXIT_USAGE
        result = actions.restore_archive(db, config, args.path, origin="cli")
        out(f"{result['table']}: restaurada ({result['rows']} linhas)" if result["ok"]
            else f"Não restaurada: {result['error']}")
        data(**result)
        return cli.EXIT_OK if result["ok"] else cli.EXIT_FAIL

    if command == "vacuum":
        if not args.yes:
            err("nada foi alterado: confirme com --yes")
            return cli.EXIT_USAGE
        result = actions.vacuum(db, config, origin="cli")
        out(f"Base: {hb(result['bytes_before'])} -> {hb(result['bytes_after'])}" if result["ok"]
            else f"Não otimizada: {result['error']}")
        data(**result)
        return cli.EXIT_OK if result["ok"] else cli.EXIT_FAIL

    if command == "orphans":
        if not args.yes:
            orphans = hygiene.find_orphans(db)
            out(f"_symbol_tables: {', '.join(orphans['symbol_tables']) or 'nenhum'}")
            out(f"_extraction_log: {', '.join(orphans['extraction_log']) or 'nenhum'}")
            data(orphans=orphans)
            return cli.EXIT_OK
        result = actions.clean_orphans(db, config, origin="cli")
        out(f"Removidos: {result['removed']}")
        data(**result)
        return cli.EXIT_OK

    if command == "auto":
        from mt5_extracao import notify
        result = automation.run_cycle(db, config, origin="auto",
                                      notify_fn=lambda title, text, level: notify.send(config, title, text, level))
        if not policy.auto_enabled:
            out("Automação desligada ([RESOURCES] auto_enabled = false): só medi e avaliei os alertas.")
        for a in result["alerts"]:
            out(f"[{a['level']}] {a['text']}")
        for d in result["actions"]:
            out(f"Ação automática: {d['action']}")
        data(alerts=result["alerts"], actions=result["actions"], db_bytes=result["report"]["db_bytes"])
        return cli.EXIT_OK

    if command == "backup":
        if not args.yes:
            err("nada foi feito: confirme com --yes")
            return cli.EXIT_USAGE
        result = files.backup(db, config, origin="cli")
        out(f"Cópia criada: {result['path']} ({hb(result['bytes'])})" if result["ok"]
            else f"Cópia não feita: {result['error']}")
        for old in result["pruned"]:
            out(f"Cópia antiga apagada: {old}")
        data(**result)
        return cli.EXIT_OK if result["ok"] else cli.EXIT_FAIL

    if command == "restore-backup":
        if not args.yes:
            err("nada foi alterado: confirme com --yes (pare extrações e backtests antes)")
            return cli.EXIT_USAGE
        result = files.restore_backup(db, config, args.path, origin="cli")
        out(f"Base trocada pela cópia. Estado anterior guardado em {result['safety_copy']}." if result["ok"]
            else f"Nada foi trocado: {result['error']}")
        data(**result)
        return cli.EXIT_OK if result["ok"] else cli.EXIT_FAIL

    if command == "trim":
        if args.tables:
            if not args.days or args.days < 1:
                raise ValueError("informe --days (manter os últimos N dias)")
            plan = {t: args.days for t in args.tables}
        else:
            plan = automation.retention_plan(storage.measure(db, with_dirs=False), policy)
            if args.days:
                plan = {t: args.days for t in plan}
        if not plan:
            raise ValueError("nada a aparar: informe as tabelas e --days, ou ligue a retenção em [RESOURCES]")
        if not args.yes:
            for table, days in plan.items():
                out(f"{table}: manteria os últimos {days} dias")
            err("nada foi alterado: confirme com --yes")
            data(would_trim=plan)
            return cli.EXIT_USAGE
        results = actions.trim_tables(db, config, plan, origin="cli")
        for r in results:
            out(f"{r['table']}: " + (f"{r['rows']} linha(s) aparada(s)" + (f" em {r['archive']}" if r["archive"] else "")
                                     if r["ok"] else f"NÃO aparada: {r['error']}"))
        data(results=results)
        return cli.EXIT_OK if all(r["ok"] for r in results) else cli.EXIT_FAIL

    if command == "backups":
        items = files.list_backups(db, config)
        for b in items:
            out(f"{b['path']}\t{hb(b['bytes'])}\t{b['created_at']:%Y-%m-%d %H:%M}")
        out(f"\n{len(items)} cópia(s). Para voltar a uma delas: feche o programa e copie o arquivo por cima da base.")
        data(backups=items)
        return cli.EXIT_OK

    if command == "files":
        if not args.yes:
            items = files.old_files(db, config)
            for f in items:
                out(f"{f['folder']}\t{f['path']}\t{hb(f['bytes'])}\t{f['modified']:%Y-%m-%d}")
            out(f"\n{len(items)} arquivo(s), {hb(sum(f['bytes'] for f in items))} (apague com --yes).")
            data(files=items)
            return cli.EXIT_OK
        result = files.clean_files(db, config, origin="cli")
        out(f"{result['removed']} arquivo(s) apagado(s), {hb(result['bytes'])} liberados.")
        for e in result["errors"]:
            err(e)
        data(**result)
        return cli.EXIT_OK if result["ok"] else cli.EXIT_FAIL

    if command == "history":
        snaps = storage.load_snapshots(db)[-args.limit:]
        for s in snaps:
            out(f"{s['taken_at']:%Y-%m-%d %H:%M}\t{hb(s['db_bytes'])}\tdisco livre {hb(s['disk_free'])}")
        acts = actions.load_actions(db, args.limit)
        if acts:
            out("\nAções:")
        for a in acts:
            out(f"{a['at']}\t{a['action']}\t{a['target'] or ''}\t{a['origin']}\t{'ok' if a['ok'] else 'falhou'}")
        data(snapshots=snaps, actions=acts)
        return cli.EXIT_OK
    raise ValueError(f"subcomando desconhecido: {command}")
