"""Tokens visuais compartilhados pela interface Tk e pelos gráficos.

Os tamanhos de fonte são pontos tipográficos. O Tk aplica a escala de DPI do sistema; `font_scale`
é apenas o ajuste de leitura escolhido pela aplicação e não deve multiplicar o DPI novamente.
"""
import math
from types import MappingProxyType


FONT_FAMILY_PREFERENCE = (
    "Inter",
    "Noto Sans",
    "Segoe UI",
    "Ubuntu",
    "Lato",
    "Arial",
    "Liberation Sans",
)

READING_SCALE_OPTIONS = (1.0, 1.15, 1.3)
DEFAULT_READING_SCALE = 1.0

TYPOGRAPHY = MappingProxyType({
    "body": 11,
    "small": 10,
    "section": 13,
    "title": 18,
    "value": 16,
    "chart_tick": 9,
    "chart_label": 10,
    "chart_title": 11,
    "chart_tooltip": 10,
})

SPACING = MappingProxyType({
    "2xs": 2,
    "xs": 4,
    "sm": 8,
    "md": 12,
    "lg": 16,
    "xl": 24,
    "2xl": 32,
})

DIMENSIONS = MappingProxyType({
    "control_min_height": 34,
    "tree_row_height_factor": 1.8,
    "component_min_width": 260,
    "chart_min_height": 220,
})

TEXT_ROLES = ("ink", "ink2", "muted", "accent", "good", "bad", "warn")

LIGHT = MappingProxyType({
    "page": "#F3F5F8",
    "surface": "#FFFFFF",
    "field": "#FFFFFF",
    "ink": "#17212B",
    "ink2": "#465361",
    "muted": "#626F7C",
    "border": "#C9D3DE",
    "grid": "#E7EDF3",
    "axis": "#A1ADB9",
    "accent": "#1F5FA7",
    "accent_hover": "#194F8E",
    "on_accent": "#FFFFFF",
    "select": "#DDEBFA",
    "good": "#167044",
    "bad": "#B42318",
    "warn": "#865500",
    "up": "#167044",
    "down": "#B42318",
    "series": ("#286FBA", "#CE6128", "#20876B"),
})

DARK = MappingProxyType({
    "page": "#101720",
    "surface": "#18222E",
    "field": "#111922",
    "ink": "#EEF3F8",
    "ink2": "#C1CCD8",
    "muted": "#A2AFBC",
    "border": "#354454",
    "grid": "#2A3542",
    "axis": "#536273",
    "accent": "#79B4F7",
    "accent_hover": "#9CC9FC",
    "on_accent": "#0F1923",
    "select": "#203B58",
    "good": "#70D49A",
    "bad": "#FF918A",
    "warn": "#F0C36E",
    "up": "#70D49A",
    "down": "#FF918A",
    "series": ("#79B4F7", "#F19A65", "#56CCAA"),
})


def choose_font_family(available, fallback):
    """Escolhe uma fonte sem serifa instalada; usa a padrão do Tk como último recurso."""
    families = {str(name).casefold(): str(name) for name in available}
    for preferred in FONT_FAMILY_PREFERENCE:
        match = families.get(preferred.casefold())
        if match:
            return match
    return str(fallback)


def font_size(role, base_size, scale=DEFAULT_READING_SCALE):
    """Retorna tamanho em pontos, preservando bases maiores definidas pelo sistema."""
    if role not in TYPOGRAPHY:
        raise KeyError(f"Papel tipográfico desconhecido: {role}")
    base = max(TYPOGRAPHY["body"], int(base_size))
    ratio = base / TYPOGRAPHY["body"]
    return max(1, int(round(TYPOGRAPHY[role] * ratio * float(scale))))


def contrast_ratio(foreground, background):
    """Calcula a razão WCAG entre duas cores hexadecimais sRGB."""
    def luminance(color):
        channels = [int(color[index:index + 2], 16) / 255 for index in (1, 3, 5)]
        linear = [channel / 12.92 if channel <= 0.04045 else ((channel + 0.055) / 1.055) ** 2.4
                  for channel in channels]
        return sum(weight * channel for weight, channel in zip((0.2126, 0.7152, 0.0722), linear))

    first, second = sorted((luminance(foreground), luminance(background)), reverse=True)
    return (first + 0.05) / (second + 0.05)


ATTENTION_TINTS = MappingProxyType({"positive": 0.12, "negative": 0.035, "zero": 0.0, "absent": 0.0})


def result_attention_role(value):
    """Papel de apresentação pelo sinal de um resultado já recebido, sem estimar evidência."""
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return "absent"
    if isinstance(value, bool) or not math.isfinite(number):
        return "absent"
    return "positive" if number > 0 else "negative" if number < 0 else "zero"


def result_attention_colors(value, palette, background="surface"):
    """Destaque contido; ausência neutra e texto opaco com contraste preservado."""
    role = result_attention_role(value)
    base = palette[background]
    target = palette["accent"] if role == "positive" else palette["bad"] if role == "negative" else base
    weight = ATTENTION_TINTS[role]
    fill = "#" + "".join(f"{round(int(base[i:i + 2], 16) * (1 - weight) + int(target[i:i + 2], 16) * weight):02X}"
                         for i in (1, 3, 5))
    text = palette["ink"] if role == "positive" else palette["muted"] if role == "absent" else palette["ink2"]
    return fill, text
