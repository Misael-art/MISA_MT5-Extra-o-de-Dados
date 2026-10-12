"""Fase 8: o Python nunca envia ordens. Quem opera é o robô MQL5, dentro do MT5 (docs/robos.md)."""
import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
FORBIDDEN = re.compile(r"\border_(send|send_async|check)\b")


def test_python_code_never_sends_orders():
    files = list((REPO / "mt5_extracao").rglob("*.py")) + [REPO / "app.py"] + list((REPO / "scripts").rglob("*.py"))
    offenders = [str(p.relative_to(REPO)) for p in files if FORBIDDEN.search(p.read_text(encoding="utf-8"))]
    assert offenders == []
