"""Autoridade do adaptador Triplo; reutiliza kernel/inspeção, sem nova fórmula/store."""
import hashlib
import importlib.util
from pathlib import Path
import subprocess

from . import super_atr

HEADER = "Include/Framework/HADOUKEN/Indicators/HDK_SuperATRTriple.mqh"
ADAPTER = "Tools/research_super_atr/triple_model.py"


def source(framework_root, manifest_path=None):
    metadata, _single_curve_factory = super_atr.source(framework_root, manifest_path)
    root = Path(framework_root).expanduser().resolve()
    hashes = {}
    try:
        for relative in (HEADER, ADAPTER):
            expected = subprocess.run(["git", "-C", str(root), "show", metadata["framework_commit"]+":"+relative],
                                      capture_output=True, timeout=10, check=True).stdout
            raw = (root/relative).read_bytes()
            if super_atr._normalized(raw) != super_atr._normalized(expected):
                raise ValueError("Fonte Triplo difere do pin; reconcilie a composição sem alterar referências históricas.")
            hashes[relative] = hashlib.sha256(super_atr._normalized(raw)).hexdigest()
        spec = importlib.util.spec_from_file_location("_mt5x_pinned_super_atr_triple", root/ADAPTER)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    except (OSError, subprocess.SubprocessError) as error:
        raise ValueError("Composição Triplo indisponível; preserve o run e reconcilie o framework fixado.") from error
    result = dict(metadata, triple_contract_version=1, triple_header_sha256=hashes[HEADER],
                  triple_adapter_sha256=hashes[ADAPTER], qualification="PINNED_TRIPLE_HEADER_CPP_ADAPTER")
    return result, module.TripleModel
