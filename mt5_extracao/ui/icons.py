"""Ícones desenhados pelo programa (Pillow), para não depender das fontes que o Tk de cada máquina enxerga."""
import base64
import io
import tkinter as tk

from PIL import Image, ImageDraw

_SCALE = 4      # desenha maior e reduz: bordas suaves


def _photo(image: Image.Image, size: int) -> tk.PhotoImage:
    buffer = io.BytesIO()
    image.resize((size, size), Image.LANCZOS).save(buffer, format="PNG")
    return tk.PhotoImage(data=base64.b64encode(buffer.getvalue()))


def mt5_view(color: str, size: int = 20) -> tk.PhotoImage:
    """Uma janela com um gráfico de linha subindo: o botão "Abrir visão MT5"."""
    s = size * _SCALE
    image = Image.new("RGBA", (s, s), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)
    stroke = max(2, s // 11)
    left, top, right, bottom = s * 0.06, s * 0.14, s * 0.94, s * 0.86
    draw.rounded_rectangle((left, top, right, bottom), radius=s * 0.12, outline=color, width=stroke)
    bar = top + s * 0.17
    draw.line((left, bar, right, bar), fill=color, width=stroke)                       # barra de título
    points = [(s * 0.20, s * 0.72), (s * 0.38, s * 0.55), (s * 0.52, s * 0.64), (s * 0.80, s * 0.40)]
    draw.line(points, fill=color, width=stroke, joint="curve")                         # o gráfico
    r = stroke * 0.9
    draw.ellipse((points[-1][0] - r, points[-1][1] - r, points[-1][0] + r, points[-1][1] + r), fill=color)
    return _photo(image, size)


def information(color: str, size: int = 18) -> tk.PhotoImage:
    """Um i dentro de um círculo, independente dos glifos da fonte instalada."""
    s = size * _SCALE
    image = Image.new("RGBA", (s, s), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)
    stroke = max(2, s // 12)
    draw.ellipse((s * .08, s * .08, s * .92, s * .92), outline=color, width=stroke)
    draw.ellipse((s * .44, s * .25, s * .56, s * .37), fill=color)
    draw.line((s * .50, s * .46, s * .50, s * .75), fill=color, width=stroke)
    return _photo(image, size)
