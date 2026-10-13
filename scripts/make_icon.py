"""
Gera o ícone do aplicativo (mt5_extracao/assets/icon.png e icon.ico).

Só é preciso rodar de novo para mudar o desenho: python scripts/make_icon.py
Requer Pillow (já é dependência do projeto).
"""
from pathlib import Path

from PIL import Image, ImageDraw

SIZE = 1024  # desenha grande e reduz, para as bordas ficarem suaves
OUT = Path(__file__).resolve().parents[1] / "mt5_extracao" / "assets"

TOP, BOTTOM = (16, 42, 84), (9, 20, 46)          # fundo: azul-marinho em degradê
UP, DOWN, LINE = (38, 208, 148), (255, 99, 110), (255, 255, 255)


def draw_icon() -> Image.Image:
    gradient = Image.new("RGB", (SIZE, SIZE))
    pixels = ImageDraw.Draw(gradient)
    for y in range(SIZE):
        t = y / (SIZE - 1)
        pixels.line([(0, y), (SIZE, y)], fill=tuple(round(a + (b - a) * t) for a, b in zip(TOP, BOTTOM)))
    mask = Image.new("L", (SIZE, SIZE), 0)
    ImageDraw.Draw(mask).rounded_rectangle([40, 40, SIZE - 40, SIZE - 40], radius=220, fill=255)
    icon = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
    icon.paste(gradient, (0, 0), mask)

    draw = ImageDraw.Draw(icon)
    # Velas: (centro x, pavio topo, corpo topo, corpo base, pavio base, cor)
    candles = [(268, 520, 580, 760, 810, DOWN), (430, 430, 500, 700, 760, UP),
               (592, 340, 400, 600, 660, UP), (754, 220, 280, 500, 570, UP)]
    for x, wick_top, body_top, body_bottom, wick_bottom, color in candles:
        draw.rounded_rectangle([x - 9, wick_top, x + 9, wick_bottom], radius=9, fill=color)
        draw.rounded_rectangle([x - 52, body_top, x + 52, body_bottom], radius=22, fill=color)
    # Linha de tendência por cima das velas
    points = [(190, 700), (350, 610), (510, 470), (690, 400), (840, 250)]
    draw.line(points, fill=LINE, width=26, joint="curve")
    for px, py in (points[0], points[-1]):
        draw.ellipse([px - 13, py - 13, px + 13, py + 13], fill=LINE)
    draw.ellipse([points[-1][0] - 36, points[-1][1] - 36, points[-1][0] + 36, points[-1][1] + 36], fill=LINE)
    draw.ellipse([points[-1][0] - 18, points[-1][1] - 18, points[-1][0] + 18, points[-1][1] + 18], fill=UP)
    return icon


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    icon = draw_icon()
    icon.resize((256, 256), Image.LANCZOS).save(OUT / "icon.png")
    icon.resize((256, 256), Image.LANCZOS).save(
        OUT / "icon.ico", sizes=[(16, 16), (24, 24), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)])
    print(f"Ícone gerado em {OUT}")


if __name__ == "__main__":
    main()
