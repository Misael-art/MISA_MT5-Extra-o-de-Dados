"""Composição operacional v1; não concede autorização nem escreve presets."""
from .adapter import BaseAdapter, base_composition, compatibility
from .schema import (CAPABILITIES_SCHEMA_VERSION, COMPOSITION_SCHEMA_VERSION,
                     Capabilities, Composition, CompositionError)

__all__ = ["BaseAdapter", "base_composition", "compatibility", "Composition",
           "CompositionError", "Capabilities", "COMPOSITION_SCHEMA_VERSION", "CAPABILITIES_SCHEMA_VERSION"]
