"""Caixa postal local MT5 → banco; gravação atômica, limitada e idempotente."""
import json
import hashlib
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from mt5_extracao.robots import mt5_tools
from . import snapshot, store

log = logging.getLogger(__name__)
MAX_BYTES = 4 * 1024 * 1024


def _read(path):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_BYTES:
        raise ValueError("Arquivo de estudo inválido ou acima do limite de4 MiB.")
    return json.loads(path.read_text(encoding="utf-8-sig"),
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError("Número não finito na captura.")))


def atomic(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    # Cada serviço tem um único consumidor; os nomes de resposta pertencem ao pedido.
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def escaped(value):
    return str(value).replace("\\", "\\\\").replace("\t", "\\t").replace("\r", "").replace("\n", "\\n")


def view(note):
    """Projeção de apresentação; a fotografia JSON do banco é a fonte autoritativa."""
    frozen = note["snapshot"]
    rows = [["NOTE", note["note_id"], note["revision"], note["note_text"], note["symbol"],
             note["timeframe"], note.get("bar_time_native") or frozen.get("bar_time_native") or note["bar_time_utc"], note["anchor_price"], note["snapshot_sha256"]],
            ["CAPTURE", frozen["captured_at_utc"], frozen["computed_at_utc"]]]
    for origin in ("extractor", "native"):
        for item in frozen[origin]:
            rows.append(["VALUE", origin, item["id"], item.get("name", item["id"]),
                         "" if item.get("value") is None else item["value"], item.get("state", "UNKNOWN"),
                         item.get("reason", ""), item.get("unit", "")])
    return "\n".join("\t".join(map(escaped, row)) for row in rows) + "\n"


class StudyService:
    def __init__(self, db, config, *, common_dir=None):
        self.db, self.config = db, config
        self.common_dir = Path(common_dir) if common_dir is not None else None
        self._lock = threading.Lock()

    def _list(self, root, request):
        if request.get("contract") != 1 or not all(isinstance(request.get(k), str) and 0 < len(request[k]) <= 160
                                                  for k in ("symbol", "timeframe", "magic")):
            raise ValueError("Escopo de notas inválido.")
        rows = store.list_notes(self.db, symbol=request["symbol"], timeframe=request["timeframe"], magic=request["magic"])
        lines = []
        for note in rows:
            complete = {**note, "snapshot": json.loads(note["snapshot_json"])}
            atomic(root / "notes" / (note["note_id"] + ".tsv"), view(complete))
            native = note.get("bar_time_native") or complete["snapshot"].get("bar_time_native") or note["bar_time_utc"]
            epoch = int(pd.Timestamp(native).timestamp())
            lines.append("\t".join(map(escaped, [note["note_id"], epoch, note["anchor_price"], note["revision"], note["note_text"]])))
        atomic(root / "workspaces" / (request["note_id"] + ".tsv"), "\n".join(lines) + ("\n" if lines else ""))
        return {"contract": 1, "request_id": request["request_id"], "note_id": request["note_id"], "state": "LISTED", "count": len(rows)}

    def poll(self, limit=8):
        """Sem tráfego, não abre o banco nem cria diretórios. Nunca roda ordens."""
        if not self._lock.acquire(blocking=False):
            return []
        try:
            common = self.common_dir or mt5_tools.common_files_dir(self.config)
            if common is None:
                return []
            root = common / "HDK_Study"
            if root.is_symlink() or not (root / "requests").is_dir() or (root / "requests").is_symlink():
                return []
            results = []
            for path in sorted((root / "requests").glob("*.request.json"))[:max(0, min(limit, 32))]:
                request_id = path.name.removesuffix(".request.json")
                if not store.IDENTIFIER.fullmatch(request_id):
                    continue
                response = {"contract": 1, "request_id": request_id, "state": "ERROR"}
                try:
                    request = _read(path)
                    if request.get("request_id") != request_id or not store.IDENTIFIER.fullmatch(str(request.get("note_id", ""))):
                        raise ValueError("O nome do pedido não corresponde ao conteúdo.")
                    if request.get("kind") == "note_list":
                        response = self._list(root, request)
                    else:
                        capture, captured_at = None, None
                        if request.get("kind") in ("note_create", "capture_preview"):
                            directory = root / "captures"
                            if directory.is_symlink():
                                raise ValueError("Pasta de capturas inválida.")
                            source = directory / (request["note_id"] + ".json")
                            capture = _read(source)
                            captured_at = datetime.fromtimestamp(source.stat().st_mtime, timezone.utc)
                        if request.get("kind") == "capture_preview":
                            frozen = store.freeze(self.db, request["note_id"], capture, captured_at)
                            note = dict(note_id=request["note_id"], revision=0, note_text="", snapshot=frozen,
                                        symbol=frozen["symbol"], timeframe=frozen["timeframe"], bar_time_utc=frozen.get("bar_time_utc") or "",
                                        bar_time_native=frozen.get("bar_time_native") or snapshot.native_clock(capture["bar_epoch"]),
                                        anchor_price=capture["anchor_price"], snapshot_sha256=hashlib.sha256(snapshot.json_text(frozen).encode()).hexdigest())
                            atomic(root / "captures" / (note["note_id"] + ".tsv"), view(note))
                            response = dict(contract=1, request_id=request_id, note_id=note["note_id"], revision=0, state="CAPTURED")
                        else:
                            response = store.apply(self.db, request, capture, captured_at)
                            note = store.get(self.db, request["note_id"])
                            atomic(root / "notes" / (note["note_id"] + ".tsv"), view(note))
                except (ValueError, TypeError, KeyError, OSError) as error:
                    response["message"] = str(error)
                except Exception:
                    # Falha de banco é transitória: conservar pedido e texto para nova tentativa.
                    log.exception("Não foi possível salvar a anotação; o pedido foi conservado.")
                    continue
                atomic(root / "replies" / (request_id + ".json"), snapshot.json_text(response))
                atomic(root / "replies" / (request_id + ".tsv"), "\t".join(map(escaped, [response["state"],
                       request_id, response.get("note_id", ""), response.get("revision", 0),
                       response.get("message", "Anotação salva no banco.")])) + "\n")
                destination = root / ("processed" if response["state"] in ("SAVED", "LISTED", "CAPTURED") else "rejected")
                destination.mkdir(parents=True, exist_ok=True)
                path.replace(destination / path.name)
                results.append(response)
            return results
        finally:
            self._lock.release()
