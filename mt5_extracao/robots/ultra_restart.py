"""Retomada do livro modelado em dois processos do terminal, com o checkpoint auditável."""
from hashlib import sha256
import json
from pathlib import Path
import re
import shutil

from ..laboratory import ultra_tester
from .mt5_tools import read_log_text


def read_parent(path, package_hash):
    if path is None:
        return None
    path = Path(path).resolve(strict=True)
    receipt = json.loads(path.read_text(encoding="utf-8"))
    if (path.name != "receipt.json" or not isinstance(receipt, dict)
            or receipt.get("origin") != "terminal_restart"
            or receipt.get("package_hash") != package_hash
            or not re.fullmatch(r"[0-9a-f]{32}", str(receipt.get("restart_id")))
            or not isinstance(receipt.get("checkpoint_files"), dict)):
        raise ValueError("O recibo informado não possui checkpoint desta campanha.")
    for name in ("partial.bin", "prefix.log"):
        file = path.parent / name
        if not file.resolve().is_relative_to(path.parent) or digest(file) != receipt["checkpoint_files"].get(name):
            raise ValueError("O conteúdo persistido do checkpoint diverge do recibo.")
    return path, receipt


def digest(path):
    return sha256(path.read_bytes()).hexdigest()


def complete(*, folder, data, exported, parent, invoke, marked, install, save, running):
    binding = exported["checkpoint_binding"]
    live = data / "MQL5" / "Files" / ("MT5XUltra_" + binding + ".bin")
    blob, prefix = folder / "partial.bin", folder / "prefix.log"
    if parent:
        parent_path, receipt = parent
        if receipt.get("checkpoint_binding") != binding:
            raise ValueError("O checkpoint pertence a outra revisão do código ou pacote.")
        shutil.copyfile(parent_path.parent / blob.name, blob)
        shutil.copyfile(parent_path.parent / prefix.name, prefix)
        install(blob, live, data)
    else:
        if live.exists():
            raise ValueError("O destino do checkpoint já está ocupado.")
        first, records = marked(invoke("save"), ultra_tester.RESTART_CONTROL)
        if not first or len(records) != 1 or not records[0].startswith("SAVED|" + binding + "|"):
            return None, save("partial_checkpoint_unavailable")
        if not live.is_file() or not live.read_bytes().startswith(binding.encode("ascii")):
            raise ValueError("O terminal não publicou o checkpoint identificado no journal.")
        shutil.copyfile(live, blob)
        shutil.copyfile(first, prefix)
    proof = {f.name: digest(f) for f in (blob, prefix)}
    if parent and proof != parent[1]["checkpoint_files"]:
        raise ValueError("Os arquivos mudaram durante a recuperação do checkpoint.")
    save("checkpoint_saved", restart_id=exported["restart_id"], checkpoint_binding=binding, checkpoint_files=proof)
    if running():
        return None, save("terminal_busy_after_checkpoint")
    if digest(live) != proof[blob.name]:
        raise ValueError("O checkpoint do terminal mudou antes da retomada.")
    last, records = marked(invoke("restore"), ultra_tester.RESTART_CONTROL)
    if not last or len(records) != 1 or not records[0].startswith("LOADED|" + binding + "|"):
        return None, save("restore_unconfirmed")
    combined = folder / "restart.journal.log"
    combined.write_text(read_log_text(prefix).rstrip("\r\n") + "\n" + read_log_text(last), encoding="utf-8")
    save("restored", suffix_journal=last.relative_to(folder).as_posix(), suffix_sha256=digest(last))
    return combined, None
