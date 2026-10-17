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



def result_direction(direction: str, color: str, size: int = 18) -> tk.PhotoImage:
    """Seta de ganho/perda observados, sem confundir sinal com aprovação ou depender de glifo."""
    if direction not in ("positive", "negative"):
        raise ValueError("Direção do resultado desconhecida.")
    s = size * _SCALE
    image = Image.new("RGBA", (s, s), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)
    stroke = max(2, s // 9)
    if direction == "positive":
        path = [(s * .18, s * .78), (s * .78, s * .22)]
        head = [(s * .42, s * .22), path[-1], (s * .78, s * .58)]
    else:
        path = [(s * .18, s * .22), (s * .78, s * .78)]
        head = [(s * .42, s * .78), path[-1], (s * .78, s * .42)]
    draw.line(path, fill=color, width=stroke)
    draw.line(head, fill=color, width=stroke, joint="curve")
    return _photo(image, size)


def robot_action(action: str, color: str, size: int = 18) -> tk.PhotoImage:
    """Comandos de execução desenhados, com rótulos textuais sempre junto ao ícone."""
    if action not in ("play", "replay", "backtest", "stop", "selector"):
        raise ValueError("Comando de robô desconhecido.")
    s = size * _SCALE
    picture = Image.new("RGBA", (s, s), (0, 0, 0, 0))
    draw = ImageDraw.Draw(picture)
    stroke = max(2, s // 10)
    if action == "play":
        draw.polygon([(s*.25, s*.15), (s*.85, s*.5), (s*.25, s*.85)], fill=color)
    elif action == "stop":
        draw.rounded_rectangle((s*.2, s*.2, s*.8, s*.8), radius=s*.07, fill=color)
    elif action == "replay":
        draw.arc((s*.12, s*.12, s*.88, s*.88), 30, 315, fill=color, width=stroke)
        draw.polygon([(s*.12, s*.14), (s*.12, s*.48), (s*.46, s*.25)], fill=color)
    elif action == "selector":
        for x, y in ((.25, .65), (.5, .35), (.75, .55)):
            draw.line((s*x, s*.15, s*x, s*.85), fill=color, width=stroke//2)
            draw.ellipse((s*(x-.1), s*(y-.1), s*(x+.1), s*(y+.1)), fill=color)
    else:
        draw.line((s*.15, s*.12, s*.15, s*.85, s*.9, s*.85), fill=color, width=stroke)
        draw.line([(s*.28, s*.7), (s*.46, s*.45), (s*.64, s*.55), (s*.85, s*.22)], fill=color, width=stroke)
    return _photo(picture, size)
