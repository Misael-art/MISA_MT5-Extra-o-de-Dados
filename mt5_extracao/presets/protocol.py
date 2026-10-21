"""Leitura integral dos marcadores de eventos reservados do diário nativo."""
import re


def strict_events(text, marker, pattern):
    """Prefixo MT5 é livre; toda ocorrência do marcador precisa ser interpretada inteira."""
    matches=list(pattern.finditer(text))
    positions=[m.start() for m in re.finditer(re.escape(marker),text)]
    if positions!=[m.start() for m in matches]:
        raise ValueError(f"Diário nativo contém marcador {marker} malformado; preserve a sessão para revisão.")
    for match in matches:
        end=text.find("\n",match.end())
        if text[match.end():len(text) if end<0 else end].strip():
            raise ValueError(f"Diário nativo contém conteúdo extra no marcador {marker}; preserve a sessão para revisão.")
    return matches
