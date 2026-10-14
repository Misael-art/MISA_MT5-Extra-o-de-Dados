"""
Gestão de recursos (aba "Recursos" e `mt5x resources`): espaço da base e do disco, higienização, arquivo
reversível e automação opt-in. Sem Tkinter: a interface só desenha o que sai daqui.

    from mt5_extracao import resources
    report = resources.storage.measure(db, config)
    found = resources.hygiene.scan(db, config)
    resources.actions.archive_tables(db, config, ["petr4_1_minuto"], origin="ui")
    resources.automation.run_cycle(db, config, origin="auto")

Plano e regras: docs/propostas/GESTAO_RECURSOS.md (Fase 15).
"""
from mt5_extracao.resources import actions, automation, hygiene, policy, storage  # noqa: F401
