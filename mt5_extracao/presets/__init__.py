"""Presets versionados; compilar/importar não instala Expert nem inicia execução."""

# Mesma disponibilidade na UI e no workflow sem tela. O compilador continua
# validando versão/pin/inputs/closure, não basta estar nesta lista.
NATIVE_PRESET_KEYS = frozenset(("mql5:hdk_s09_referencia", "mql5:hdk_pullback_nativo",
    "mql5:hdk_stochema9", "mql5:hdk_vwapbands", "mql5:hdk_madrid", "mql5:hdk_hahilo"))
