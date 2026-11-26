"""Entrega atômica de uma revisão posterior ao início de um passe do Tester DEMO."""
from hashlib import sha256
import json
import os
from pathlib import Path
import re
from uuid import uuid4

from ..experiments.schema import digest
from ..laboratory import ultra_market_preset as preset
from . import mt5_tools, ultra_market_revision


def instruction(initial, successor, selection, effective):
    old = initial["profile"]["modules"]
    if bool(successor) == bool(selection):
        raise ValueError("Informe uma política sucessora ou a seleção em caixa.")
    if successor:
        successor = preset.read(successor)
        if (successor["schema"] != preset.PORTABLE or successor["parent_sha256"] != initial["preset_sha256"]
                or successor["version"] != initial["version"] + 1
                or successor["profile"]["market"] != initial["profile"]["market"]
                or successor["profile"]["portfolio_cap_pct"] != initial["profile"]["portfolio_cap_pct"]):
            raise ValueError("A revisão não pertence à linhagem, mercado ou orçamento ativo.")
        research = successor["research"]
        if research and research["training_end_msc"] > effective:
            raise ValueError("O treino da revisão termina após a instalação.")
        new = {m["key"]: m for m in successor["profile"]["modules"]}
        if list(new) != [m["key"] for m in old if m["key"] in new]:
            raise ValueError("Preserve prioridade e selecione somente módulos compilados neste Expert.")
        policy_sha = successor["preset_sha256"]
    else:
        from ..laboratory.ultra_market_paired import choose
        if selection["chosen"] != "cash" or choose(selection["results"], selection["plan"])[0] != "cash":
            raise ValueError("A pesquisa deve ter encerrado em caixa.")
        new = {}
        policy_sha = digest(["suspend", initial["preset_sha256"], digest(selection)])
    rows = []
    for m in old:
        n = new.get(m["key"])
        minutes = 0
        if n:
            if any(m.get(k) != n.get(k) for k in ("magic", "reference", "pattern", "owner_rules")):
                raise ValueError("A entrega conserva titularidade, definição, risco e gestão.")
            changed = {k for k in m["inputs"].keys() | n["inputs"].keys() if m["inputs"].get(k) != n["inputs"].get(k)}
            if changed - ({"InpRangeMinutes"} if m["key"] == "orb" else set()):
                raise ValueError("A revisão dinâmica aceita o parâmetro orb.range_minutes.")
            if m["key"] == "orb":
                minutes = int(n["inputs"]["InpRangeMinutes"])
                if not 1 <= minutes <= 240:
                    raise ValueError("Janela ORB fora dos limites.")
            contract = {"inputs": n["inputs"], **{k: n[k] for k in ("pattern", "owner_rules") if k in n}}
            if n["inputs_sha256"] != digest(contract) or n["preset_sha256"] != digest([n["key"], contract]):
                raise ValueError("Hashes do módulo divergem dos parâmetros publicados.")
        rows.append([m["magic"], int(n is not None), (n or m)["preset_sha256"], (n or m)["inputs_sha256"], minutes])
    return policy_sha, rows


def install(config, receipt_path, *, successor=None, selection=None, effective):
    receipt = json.loads(Path(receipt_path).read_text(encoding="utf-8"))
    if (not receipt.get("policy_inbox") or receipt["status"] != "testing" or receipt["mode"] != "portfolio"
            or not re.fullmatch(r"[a-f0-9]{64}", receipt["binding"])
            or not receipt["window"]["start_msc"] <= effective < receipt["window"]["last_msc"]):
        raise ValueError("O recibo deve identificar um Tester em execução com recepção habilitada.")
    initial = preset.read(receipt["preset"])
    selected = json.loads(Path(selection).read_text(encoding="utf-8")) if selection else None
    policy_sha, rows = instruction(initial, successor, selected, effective)
    fields = [receipt["binding"], initial["preset_sha256"], initial["version"] + 1, effective, policy_sha]
    fields.extend(",".join(map(str, row)) for row in rows)
    payload = "|".join(map(str, fields))
    checksum = sha256(payload.encode("ascii")).hexdigest()
    common = mt5_tools.common_files_dir(config)
    if common is None:
        raise ValueError("FILE_COMMON do Tester não localizado.")
    destination = common / ("MT5XPolicy_" + receipt["binding"] + ".txt")
    temp = common / (uuid4().hex + ".tmp")
    try:
        with temp.open("x", encoding="ascii", newline="\n") as f:
            f.write(checksum + "|" + payload + "\r\n")
            f.flush()
            os.fsync(f.fileno())
        try:
            os.link(temp, destination)
        except FileExistsError:
            if destination.read_bytes() != temp.read_bytes():
                raise ValueError("Já foi publicada outra revisão para este passe.")
    finally:
        temp.unlink(missing_ok=True)
    return dict(status="published", binding=receipt["binding"], effective_msc=effective, policy_sha256=policy_sha,
                packet_sha256=checksum, owners=rows)


def fragments(slots, sources, directory, safety):
    def replace_file(name, transform):
        file = directory / name
        before = file.read_text(encoding="utf-8")
        after = transform(before)
        if before == after:
            raise ValueError(f"Ponto de integração da entrega ausente em {name}.")
        file.write_bytes(after.encode("utf-8"))
        sources.files[str(name)] = sha256(after.encode("utf-8")).hexdigest()
    replace_file(safety, ultra_market_revision.executor_extension)
    ticks = ["   PolicyPoll();"]
    declarations = []
    for i, slot in enumerate(slots):
        if slot["key"] == "orb":
            replace_file(slot["include"], lambda s: s.replace(f"const int U{i}_InpRangeMinutes =", f"int U{i}_InpRangeMinutes =", 1))
        marker = f"if(reason == \"\" && U{i}_g_entry_uncertain)"
        engine = next(k for k in sources.files if k.endswith(f"/portfolio_engine_{i}.mqh"))
        replace_file(engine, lambda s: s.replace(marker, f'if(reason=="" && PolicyBlocked({i})) reason="politica suspendeu novas entradas";\n   ' + marker, 1))
        declarations.append(f"CHDKPortfolioRiskGuard delivered_{i};")
        magic, key = slot["magic"], slot["key"]
        apply = f"U{i}_InpRangeMinutes=p_minutes[{i}];" if key == "orb" else ""
        ticks.append(f'''   if(p_active && !p_done[{i}] && !p_error)
   {{
      ulong id=PolicyPosition({magic});
      if(id!=0 || PolicyOrders({magic}) || U{i}_g_entry_uncertain)
      {{
         if(!p_held[{i}]) PolicyEvent({magic},"held",id,UltraPresetSha);
         p_held[{i}]=true;
      }}
      else if(!p_enable[{i}]) {{p_done[{i}]=true;PolicyEvent({magic},"removed",0,p_policy);}}
      else
      {{
         string why;
         if(UltraReconcile(why))
         {{
            if(!delivered_{i}.BindContext({magic},UltraBinding,"{key}","mt5x-ultra-eurusd",p_slot[{i}],
                UltraPresetVersion+1,UltraClosure,UltraBinarySha,p_inputs[{i}],why) ||
               !U{i}_g_exec.RestorePortfolio(GetPointer(delivered_{i}),why))
            {{p_error=true;PolicyEvent({magic},"error",0,p_policy);}}
            else {{{apply} p_done[{i}]=true;PolicyEvent({magic},"adopted",0,p_policy);}}
         }}
      }}
   }}
   U{i}_OnTick();''')
    template = Path(__file__).with_name("templates") / "UltraMarketDelivery.mqh.tmpl"
    helpers = template.read_text(encoding="utf-8").replace("@COUNT@", str(len(slots))).replace(
        "@MAGICS@", ",".join(str(s["magic"]) for s in slots)).replace("@ORB@",
        ",".join("true" if s["key"] == "orb" else "false" for s in slots))
    return helpers, declarations, ticks
