"""Tema claro/escuro da interface: cores, fontes, estilos ttk e dicas (tooltips)."""
import tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk

LIGHT = {
    "page": "#f1f0ec", "surface": "#fcfcfb", "field": "#ffffff", "ink": "#0b0b0b", "ink2": "#52514e",
    "muted": "#898781", "border": "#d9d8d1", "grid": "#e1e0d9", "axis": "#c3c2b7",
    "accent": "#2a78d6", "accent_hover": "#256abf", "on_accent": "#ffffff", "select": "#cde2fb",
    "good": "#006300", "bad": "#b42b2b", "warn": "#8a5a00",
    "up": "#0ca30c", "down": "#d03b3b",
    "series": ("#2a78d6", "#eb6834", "#1baf7a"),
}
DARK = {
    "page": "#0d0d0d", "surface": "#1a1a19", "field": "#242422", "ink": "#f0efec", "ink2": "#c3c2b7",
    "muted": "#898781", "border": "#383835", "grid": "#2c2c2a", "axis": "#383835",
    "accent": "#3987e5", "accent_hover": "#5598e7", "on_accent": "#0d0d0d", "select": "#184f95",
    "good": "#0ca30c", "bad": "#e66767", "warn": "#fab219",
    "up": "#0ca30c", "down": "#d03b3b",
    "series": ("#3987e5", "#d95926", "#199e70"),
}


class Theme:
    """Aplica a paleta aos estilos ttk e aos widgets clássicos (Listbox, Text) registrados."""

    def __init__(self, root, dark=False):
        self.root = root
        self.dark = dark
        self.colors = DARK if dark else LIGHT
        self._classic = []    # widgets tk (não ttk) a recolorir quando o tema muda
        self._listeners = []  # funções chamadas depois da troca (ex.: redesenhar gráficos)
        base = tkfont.nametofont("TkDefaultFont")
        size = max(10, abs(int(base.cget("size"))))
        family = base.cget("family")
        base.configure(size=size)
        for name in ("TkTextFont", "TkMenuFont", "TkHeadingFont"):
            tkfont.nametofont(name).configure(family=family, size=size, weight="normal")
        self.font = base
        self.font_small = tkfont.Font(family=family, size=size - 1)
        self.font_bold = tkfont.Font(family=family, size=size, weight="bold")
        self.font_title = tkfont.Font(family=family, size=size + 5, weight="bold")
        self.font_value = tkfont.Font(family=family, size=size + 4, weight="bold")
        self.apply()

    def on_change(self, fn):
        self._listeners.append(fn)

    def toggle(self):
        self.dark = not self.dark
        self.colors = DARK if self.dark else LIGHT
        self.apply()
        for fn in self._listeners:
            fn()

    def classic(self, widget):
        """Registra um Listbox/Text para acompanhar o tema e já aplica as cores atuais."""
        self._classic.append(widget)
        self._paint(widget)
        return widget

    def _paint(self, widget):
        c = self.colors
        try:
            widget.configure(background=c["field"], foreground=c["ink"], highlightthickness=1,
                             highlightbackground=c["border"], highlightcolor=c["accent"],
                             selectbackground=c["select"], selectforeground=c["ink"], relief="flat", borderwidth=0)
            if isinstance(widget, tk.Text):
                widget.configure(insertbackground=c["ink"])
        except tk.TclError:
            pass

    def apply(self):
        c = self.colors
        root = self.root
        style = ttk.Style(root)
        style.theme_use("clam")
        root.configure(background=c["page"])
        style.configure(".", background=c["page"], foreground=c["ink"], fieldbackground=c["field"],
                        bordercolor=c["border"], lightcolor=c["border"], darkcolor=c["border"],
                        troughcolor=c["grid"], focuscolor=c["accent"], insertcolor=c["ink"],
                        selectbackground=c["select"], selectforeground=c["ink"], font=self.font)
        style.map(".", foreground=[("disabled", c["muted"])])

        style.configure("TFrame", background=c["page"])
        style.configure("Card.TFrame", background=c["surface"], relief="solid", borderwidth=1)
        style.configure("Surface.TFrame", background=c["surface"])
        style.configure("TLabel", background=c["page"], foreground=c["ink"])
        for name, opts in {
            "Surface.TLabel": {"foreground": c["ink"]},
            "Muted.TLabel": {"foreground": c["ink2"], "font": self.font_small},
            "Heading.TLabel": {"foreground": c["ink"], "font": self.font_bold},
            "Title.TLabel": {"foreground": c["ink"], "font": self.font_title},
            "Value.TLabel": {"foreground": c["ink"], "font": self.font_value},
            "Good.TLabel": {"foreground": c["good"], "font": self.font_bold},
            "Bad.TLabel": {"foreground": c["bad"], "font": self.font_bold},
            "Warn.TLabel": {"foreground": c["warn"], "font": self.font_bold},
        }.items():
            style.configure(name, background=c["surface"], **opts)
        style.configure("Page.Muted.TLabel", background=c["page"], foreground=c["ink2"], font=self.font_small)
        style.configure("Page.Title.TLabel", background=c["page"], foreground=c["ink"], font=self.font_title)

        style.configure("TButton", background=c["surface"], foreground=c["ink"], padding=(12, 6), relief="flat",
                        borderwidth=1)
        style.map("TButton", background=[("disabled", c["page"]), ("active", c["grid"])],
                  bordercolor=[("focus", c["accent"])])
        style.configure("Accent.TButton", background=c["accent"], foreground=c["on_accent"], font=self.font_bold,
                        bordercolor=c["accent"], lightcolor=c["accent"], darkcolor=c["accent"])
        style.map("Accent.TButton", background=[("disabled", c["grid"]), ("active", c["accent_hover"])],
                  foreground=[("disabled", c["muted"])])
        style.configure("Toolbutton", background=c["surface"], foreground=c["ink2"], padding=(10, 5), relief="flat")
        style.map("Toolbutton", background=[("selected", c["select"]), ("active", c["grid"])],
                  foreground=[("selected", c["ink"])])

        # Diálogos usam o fundo da página; dentro dos cartões vale a variante "Surface."
        for name, bg in (("TCheckbutton", c["page"]), ("TRadiobutton", c["page"]),
                         ("Surface.TCheckbutton", c["surface"]), ("Surface.TRadiobutton", c["surface"])):
            style.configure(name, background=bg, foreground=c["ink"], indicatorbackground=c["field"],
                            indicatorforeground=c["accent"])
            style.map(name, background=[("active", bg)], indicatorbackground=[("selected", c["field"])])
        style.configure("TEntry", padding=5)
        style.configure("TCombobox", padding=4, arrowcolor=c["ink2"], background=c["surface"])
        style.map("TCombobox", fieldbackground=[("readonly", c["field"])], foreground=[("readonly", c["ink"])],
                  selectbackground=[("readonly", c["field"])], selectforeground=[("readonly", c["ink"])])
        style.configure("TLabelframe", background=c["page"], bordercolor=c["border"], relief="solid", padding=8)
        style.configure("TLabelframe.Label", background=c["page"], foreground=c["ink2"], font=self.font_bold)
        style.configure("TPanedwindow", background=c["page"])
        style.configure("TSeparator", background=c["border"])

        style.configure("TNotebook", background=c["page"], borderwidth=0, tabmargins=(0, 0, 0, 0))
        style.configure("TNotebook.Tab", background=c["page"], foreground=c["ink2"], padding=(18, 9), borderwidth=0,
                        font=self.font_bold)
        style.map("TNotebook.Tab", background=[("selected", c["surface"])], foreground=[("selected", c["accent"])],
                  padding=[("selected", (18, 9))], expand=[("selected", (0, 0, 0, 0))],
                  lightcolor=[("selected", c["surface"])], bordercolor=[("selected", c["border"])])

        style.configure("Treeview", background=c["field"], fieldbackground=c["field"], foreground=c["ink"],
                        rowheight=int(self.font.metrics("linespace") * 1.7), borderwidth=0)
        style.map("Treeview", background=[("selected", c["select"])], foreground=[("selected", c["ink"])])
        style.configure("Treeview.Heading", background=c["surface"], foreground=c["ink2"], relief="flat",
                        font=self.font_small, padding=(6, 6))
        style.map("Treeview.Heading", background=[("active", c["grid"])])

        style.configure("TScrollbar", background=c["surface"], troughcolor=c["page"], bordercolor=c["page"],
                        arrowcolor=c["ink2"], relief="flat")
        style.configure("Horizontal.TProgressbar", background=c["accent"], troughcolor=c["grid"],
                        bordercolor=c["grid"], lightcolor=c["accent"], darkcolor=c["accent"])
        style.configure("Error.Horizontal.TProgressbar", background=c["down"], lightcolor=c["down"],
                        darkcolor=c["down"])
        style.configure("Collection.Horizontal.TProgressbar", background=c["up"], lightcolor=c["up"],
                        darkcolor=c["up"])

        # Listas suspensas do Combobox, menus e janelas novas (widgets clássicos)
        root.option_add("*TCombobox*Listbox.background", c["field"])
        root.option_add("*TCombobox*Listbox.foreground", c["ink"])
        root.option_add("*TCombobox*Listbox.selectBackground", c["select"])
        root.option_add("*TCombobox*Listbox.selectForeground", c["ink"])
        root.option_add("*Toplevel.background", c["page"])
        for key, value in (("background", c["surface"]), ("foreground", c["ink"]),
                           ("activeBackground", c["select"]), ("activeForeground", c["ink"])):
            root.option_add(f"*Menu.{key}", value)
        root.option_add("*Menu.relief", "flat")
        self._classic = [w for w in self._classic if self._alive(w)]
        for widget in self._classic:
            self._paint(widget)

    @staticmethod
    def _alive(widget):
        try:
            return bool(widget.winfo_exists())
        except tk.TclError:
            return False

    def style_menu(self, menu):
        c = self.colors
        menu.configure(background=c["surface"], foreground=c["ink"], activebackground=c["select"],
                       activeforeground=c["ink"], relief="flat", borderwidth=0)


def card(parent, title=None, hint=None, padding=14):
    """Bloco com borda e título. Retorna o frame de conteúdo (use .master para posicionar o cartão)."""
    outer = ttk.Frame(parent, style="Card.TFrame", padding=padding)
    if title:
        head = ttk.Frame(outer, style="Surface.TFrame")
        head.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(head, text=title, style="Heading.TLabel").pack(side=tk.LEFT)
        if hint:
            ttk.Label(head, text=hint, style="Muted.TLabel").pack(side=tk.LEFT, padx=(10, 0))
    body = ttk.Frame(outer, style="Surface.TFrame")
    body.pack(fill=tk.BOTH, expand=True)
    return body


class Tooltip:
    """Dica que aparece ao parar o mouse sobre o widget."""

    def __init__(self, widget, text, theme=None, wrap=340):
        self.widget, self.text, self.theme, self.wrap = widget, text, theme, wrap
        self._tip = None
        self._job = None
        widget.bind("<Enter>", self._schedule, add="+")
        widget.bind("<Leave>", self._hide, add="+")
        widget.bind("<ButtonPress>", self._hide, add="+")

    def _schedule(self, _event=None):
        self._cancel()
        self._job = self.widget.after(450, self._show)

    def _cancel(self):
        if self._job:
            try:
                self.widget.after_cancel(self._job)
            except tk.TclError:
                pass
            self._job = None

    def _show(self):
        if self._tip or not self.text:
            return
        colors = self.theme.colors if self.theme else LIGHT
        x = self.widget.winfo_rootx() + 12
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 6
        self._tip = tip = tk.Toplevel(self.widget)
        tip.wm_overrideredirect(True)
        tip.wm_geometry(f"+{x}+{y}")
        tk.Label(tip, text=self.text, justify=tk.LEFT, wraplength=self.wrap, background=colors["ink"],
                 foreground=colors["page"], padx=10, pady=7, borderwidth=0).pack()

    def _hide(self, _event=None):
        self._cancel()
        if self._tip:
            try:
                self._tip.destroy()
            except tk.TclError:
                pass
            self._tip = None


def scrolled_tree(parent, columns, widths, anchors=None, height=10, selectmode="browse"):
    """Treeview com barra de rolagem. Retorna (frame, tree)."""
    frame = ttk.Frame(parent, style="Surface.TFrame")
    tree = ttk.Treeview(frame, columns=columns, show="headings", height=height, selectmode=selectmode)
    anchors = anchors or ["w"] * len(columns)
    for col, width, anchor in zip(columns, widths, anchors):
        tree.heading(col, text=col, anchor=anchor)
        tree.column(col, width=width, anchor=anchor, stretch=True)
    bar = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
    tree.configure(yscrollcommand=bar.set)
    bar.pack(side=tk.RIGHT, fill=tk.Y)
    tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    return frame, tree


def fmt_int(value):
    """1234567 -> '1.234.567' (padrão brasileiro)."""
    return f"{int(value):,}".replace(",", ".")


def fmt_num(value, digits=2, suffix=""):
    if value is None:
        return "—"
    text = f"{value:,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return text + suffix
