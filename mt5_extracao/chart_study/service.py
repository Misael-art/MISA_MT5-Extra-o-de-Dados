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

    def _catalog(self, root, request):
        from . import catalog
        rows = [[item["id"], item["name"], item["origin"], item["scale"], item["rule"], item["legend"],
                 snapshot.json_text(item["parameters"])] for item in catalog.entries()]
        atomic(root / "catalog.tsv", "\n".join("\t".join(map(escaped, row)) for row in rows) + "\n")
        return dict(contract=1, request_id=request["request_id"], note_id=request["note_id"], state="CATALOG", count=len(rows))

    def _series(self, root, request):
        from . import series
        revision = request.get("revision")
        layer = request["note_id"]
        if not layer.startswith("s_") or request["request_id"] != layer or isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
            raise ValueError("Identidade/revisão da série de estudo inválida.")
        directory = root / "series_sources"
        if directory.is_symlink():
            raise ValueError("Pasta de séries inválida.")
        capture = _read(directory / (layer + ".json"))
        if capture.get("revision") != revision:
            raise ValueError("A fonte da série mudou antes da leitura; solicite a revisão atual.")
        projection = series.build(capture, request.get("feature_id"))
        rows = [["META", revision, projection["feature"]["id"], projection["source_sha256"], projection["symbol"],
                 projection["timeframe"], projection["feature"]["scale"], projection["feature"]["legend"]]]
        rows += [["BAR", item["time"], "" if item["value"] is None else item["value"],
                  "" if item["direction"] is None else item["direction"], item["state"], item["reason"]] for item in projection["rows"]]
        atomic(root / "series" / (layer + ".tsv"), "\n".join("\t".join(map(escaped, row)) for row in rows) + "\n")
        return dict(contract=1, request_id=layer, note_id=layer, revision=revision, state="SERIES",
                    source_sha256=projection["source_sha256"], count=len(projection["rows"]),
                    message=f"Série atualizada · {sum(row['direction'] is not None for row in projection['rows'])}/{len(projection['rows'])} barras disponíveis")

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
                    before = path.lstat()
                except FileNotFoundError:
                    continue
                try:
                    request = _read(path)
                    if not isinstance(request, dict):
                        raise ValueError("O pedido de estudo precisa ser um objeto JSON.")
                    if isinstance(request, dict) and isinstance(request.get("revision"), int) and not isinstance(request.get("revision"), bool):
                        response["revision"] = request["revision"]
                    if request.get("request_id") != request_id or not store.IDENTIFIER.fullmatch(str(request.get("note_id", ""))):
                        raise ValueError("O nome do pedido não corresponde ao conteúdo.")
                    if request.get("contract") != 1:
                        raise ValueError("Contrato de estudo inválido.")
                    if request.get("kind") == "study_catalog":
                        response = self._catalog(root, request)
                    elif request.get("kind") == "study_series":
                        response = self._series(root, request)
                    elif request.get("kind") == "note_list":
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
                destination = root / ("processed" if response["state"] in ("SAVED", "LISTED", "CAPTURED", "CATALOG", "SERIES") else "rejected")
                destination.mkdir(parents=True, exist_ok=True)
                # Uma troca de indicador pode substituir o slot enquanto a
                # versão anterior é calculada. Não arquivar o pedido mais novo.
                try:
                    current = path.lstat()
                    if (current.st_ino, current.st_size, current.st_mtime_ns) == (before.st_ino, before.st_size, before.st_mtime_ns):
                        path.replace(destination / path.name)
                except FileNotFoundError:
                    pass
                results.append(response)
            return results
        finally:
            self._lock.release()
