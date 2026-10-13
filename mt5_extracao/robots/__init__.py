"""
Gestão de robôs (Fase 8): catálogo de Expert Advisors MQL5 e das estratégias Python.

O Python nunca envia ordens: quem opera é o robô MQL5 dentro do MT5. Veja docs/robos.md.
"""
from .catalog import (MQL5, PYTHON, STAGE_LABELS, STAGES, Robot, get_robot, import_library, import_robot,
                      load_catalog, robot_from_file, scan_library)
from .risk import RiskRating

__all__ = ["MQL5", "PYTHON", "STAGES", "STAGE_LABELS", "RiskRating", "Robot", "get_robot", "import_library", "import_robot",
           "load_catalog", "robot_from_file", "scan_library"]
