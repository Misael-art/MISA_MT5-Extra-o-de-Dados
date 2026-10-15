"""T17.1: tokens tipográficos, contraste e escala do tema Tk."""
import pytest

from mt5_extracao.ui.visual_tokens import (
    DARK,
    FONT_FAMILY_PREFERENCE,
    LIGHT,
    READING_SCALE_OPTIONS,
    SPACING,
    TEXT_ROLES,
    TYPOGRAPHY,
    choose_font_family,
    contrast_ratio,
    font_size,
)


def test_font_family_prefers_installed_sans_and_keeps_tk_fallback():
    assert choose_font_family({"LATO": "Lato", "Inter": "Inter"}, "fixed") == "Inter"
    assert choose_font_family({"Lato": "Lato"}, "fixed") == "Lato"
    assert choose_font_family({"fixed": "fixed"}, "fixed") == "fixed"
    assert FONT_FAMILY_PREFERENCE[0] == "Inter"


def test_typography_scale_uses_points_and_keeps_small_text_readable():
    assert font_size("body", base_size=7) == 11
    assert font_size("small", base_size=7) == 10
    assert font_size("title", base_size=11) == 18
    assert font_size("body", base_size=11, scale=1.15) > font_size("body", base_size=11)
    assert READING_SCALE_OPTIONS == (1.0, 1.15, 1.3)
    assert tuple(SPACING.values()) == (2, 4, 8, 12, 16, 24, 32)
    assert TYPOGRAPHY["chart_tick"] >= 9


@pytest.mark.parametrize("palette", [LIGHT, DARK])
def test_text_and_series_colors_meet_contrast_targets(palette):
    for role in TEXT_ROLES:
        for background in ("page", "surface", "field"):
            assert contrast_ratio(palette[role], palette[background]) >= 4.5, role
    assert contrast_ratio(palette["on_accent"], palette["accent"]) >= 4.5
    assert contrast_ratio(palette["ink"], palette["select"]) >= 4.5
    for color in palette["series"]:
        assert contrast_ratio(color, palette["surface"]) >= 3.0


tk = pytest.importorskip("tkinter")
tkfont = pytest.importorskip("tkinter.font")


@pytest.fixture
def root():
    try:
        window = tk.Tk()
    except tk.TclError as exc:
        pytest.skip(f"sem display gráfico: {exc}")
    yield window
    window.destroy()


def test_theme_selects_available_font_and_updates_scale_listeners(root):
    from mt5_extracao.ui.theme import Theme

    available = tkfont.families(root)
    fallback = tkfont.nametofont("TkDefaultFont").cget("family")
    theme = Theme(root)
    assert theme.font_family == choose_font_family(available, fallback)
    assert theme.font.actual("size") >= 11
    assert theme.font_small.actual("size") >= 10
    assert theme.dpi > 0 and theme.tk_scaling > 0

    changes = []
    theme.on_change(lambda: changes.append(theme.scale))
    original = theme.font.actual("size")
    assert theme.set_scale(1.15) is True
    assert theme.font.actual("size") > original
    assert changes == [1.15]
    assert theme.set_scale(1.15) is False
    with pytest.raises(ValueError, match="entre 0,85 e 1,5"):
        theme.set_scale(1.6)

    theme.toggle()
    assert isinstance(theme.colors, dict)
    assert theme.colors == DARK
    assert changes == [1.15, 1.15]
