"""Componentes ttk reutilizáveis de apresentação, sem regras de domínio.

Os contêineres expõem seus widgets internos para que as telas possam conservar
callbacks e cálculos próprios sem duplicar o padrão visual.
"""
import tkinter as tk
from tkinter import ttk

from mt5_extracao.ui.visual_tokens import SPACING


_STATUS = {
    "success": ("✓", "Success"),
    "warning": ("⚠", "Warning"),
    "error": ("×", "Error"),
    "info": ("ℹ", "Info"),
    "neutral": ("●", "Neutral"),
    "partial": ("◐", "Warning"),
    "loading": ("…", "Info"),
    "empty": ("○", "Neutral"),
    "unavailable": ("×", "Error"),
}
_STATUS_LABEL = {
    "success": "Concluído",
    "warning": "Aviso",
    "error": "Erro",
    "info": "Informação",
    "neutral": "Status",
    "partial": "Parcial",
    "loading": "Carregando",
    "empty": "Sem dados",
    "unavailable": "Indisponível",
}


def _check_action(label, command):
    if bool(label) != (command is not None):
        raise ValueError("A ação precisa de rótulo e callback juntos.")
    if command is not None and not callable(command):
        raise ValueError("O callback da ação precisa ser executável.")


class PageHeader(ttk.Frame):
    """Título e contexto da página, com uma área para ações reais da tela."""

    def __init__(self, parent, title, subtitle=None, **kwargs):
        super().__init__(parent, style="TFrame", **kwargs)
        self.title_label = ttk.Label(self, text=title, style="Page.Title.TLabel")
        self.title_label.pack(side=tk.LEFT)
        self.subtitle_label = None
        if subtitle:
            self.subtitle_label = ttk.Label(self, text=subtitle, style="Page.Muted.TLabel")
            self.subtitle_label.pack(side=tk.LEFT, padx=(SPACING["md"], 0), pady=(SPACING["xs"], 0))
        self.action_frame = ttk.Frame(self, style="TFrame")
        self.action_frame.pack(side=tk.RIGHT)

    def add_action(self, text, command, style=None, tooltip=None, theme=None):
        """Cria um botão somente quando a tela fornece a ação que ele executa."""
        _check_action(text, command)
        options = {"text": text, "command": command}
        if style:
            options["style"] = style
        button = ttk.Button(self.action_frame, **options)
        button.pack(side=tk.LEFT, padx=(SPACING["sm"], 0))
        if tooltip:
            from mt5_extracao.ui.theme import Tooltip

            Tooltip(button, tooltip, theme)
        return button


class FilterBar(ttk.Frame):
    """Faixa de filtros; controles ttk filhos mantêm foco e teclado nativos."""

    def __init__(self, parent, padding=SPACING["sm"], **kwargs):
        super().__init__(parent, style="Card.TFrame", padding=padding, **kwargs)


class ContentPanel(ttk.Frame):
    """Painel com título opcional e um corpo expansível."""

    def __init__(self, parent, title=None, subtitle=None, padding=SPACING["md"], **kwargs):
        super().__init__(parent, style="Card.TFrame", padding=padding, **kwargs)
        self.header = None
        self.title_label = None
        self.subtitle_label = None
        if title or subtitle:
            self.header = ttk.Frame(self, style="Surface.TFrame")
            self.header.pack(fill=tk.X, pady=(0, SPACING["sm"]))
            if title:
                self.title_label = ttk.Label(self.header, text=title, style="Heading.TLabel")
                self.title_label.pack(side=tk.LEFT)
            if subtitle:
                self.subtitle_label = ttk.Label(self.header, text=subtitle, style="Muted.TLabel")
                self.subtitle_label.pack(side=tk.LEFT, padx=(SPACING["sm"], 0))
        self.body = ttk.Frame(self, style="Surface.TFrame")
        self.body.pack(fill=tk.BOTH, expand=True)


class MetricCard(ContentPanel):
    """Métrica apresentada sem calcular ou transformar o valor recebido."""

    def __init__(self, parent, title, value="—", detail=None, hint=None, theme=None,
                 value_style="Value.TLabel", padding=SPACING["md"], **kwargs):
        super().__init__(parent, title=title, padding=padding, **kwargs)
        self.value_label = ttk.Label(self.body, text=str(value), style=value_style)
        self.value_label.pack(anchor="w")
        self.detail_label = ttk.Label(self.body, text=detail or "", style="Muted.TLabel", wraplength=260,
                                      justify=tk.LEFT)
        if detail:
            self.detail_label.pack(anchor="w", pady=(SPACING["2xs"], 0))
        self.hint = hint
        if hint:
            from mt5_extracao.ui.theme import Tooltip

            Tooltip(self.title_label, hint, theme)
            Tooltip(self.value_label, hint, theme)

    def set_value(self, value, detail=None, style=None):
        self.value_label.configure(text=str(value))
        if detail is not None:
            self.detail_label.configure(text=detail)
            if detail and not self.detail_label.winfo_manager():
                self.detail_label.pack(anchor="w", pady=(SPACING["2xs"], 0))
            elif not detail:
                self.detail_label.pack_forget()
        if style:
            self.value_label.configure(style=style)


class StatusBadge(ttk.Label):
    """Estado semântico com símbolo e texto explícito."""

    def __init__(self, parent, text, kind="neutral", icon=None, theme=None, **kwargs):
        if kind not in _STATUS:
            raise ValueError(f"Estado desconhecido: {kind}")
        self.kind = kind
        self.label_text = str(text)
        self.icon = icon if icon is not None else _STATUS[kind][0]
        self.theme = theme
        super().__init__(parent, style=f"{_STATUS[kind][1]}.Status.TLabel", **kwargs)
        self._refresh()

    def _refresh(self):
        icon = self.theme.glyphs(self.icon) if self.theme is not None else self.icon
        self.configure(text=f"{icon} {self.label_text}".strip())

    def set_status(self, text=None, kind=None, icon=None):
        if kind is not None:
            if kind not in _STATUS:
                raise ValueError(f"Estado desconhecido: {kind}")
            self.kind = kind
        if text is not None:
            self.label_text = str(text)
        self.icon = icon if icon is not None else _STATUS[self.kind][0]
        self.configure(style=f"{_STATUS[self.kind][1]}.Status.TLabel")
        self._refresh()


class Notice(ttk.Frame):
    """Aviso compacto, com severidade legível e ação opcional ligada a callback."""

    def __init__(self, parent, text, kind="info", action_text=None, command=None, theme=None, **kwargs):
        if kind not in _STATUS:
            raise ValueError(f"Estado desconhecido: {kind}")
        _check_action(action_text, command)
        style_kind = _STATUS[kind][1]
        super().__init__(parent, style=f"{style_kind}.Notice.TFrame", padding=SPACING["sm"], **kwargs)
        self.badge = StatusBadge(self, text=_STATUS_LABEL[kind], kind=kind, theme=theme)
        self.badge.pack(side=tk.LEFT, padx=(0, SPACING["sm"]))
        self.message = ttk.Label(self, text=text, style=f"{style_kind}.Notice.TLabel", wraplength=760,
                                 justify=tk.LEFT)
        self.message.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.action_button = None
        if action_text:
            self.action_button = ttk.Button(self, text=action_text, command=command)
            self.action_button.pack(side=tk.RIGHT, padx=(SPACING["sm"], 0))


class StatePanel(ContentPanel):
    """Apresenta estado vazio, carregamento, parcial ou indisponível."""

    def __init__(self, parent, title, message, kind="empty", action_text=None, command=None, theme=None,
                 padding=SPACING["lg"], **kwargs):
        if kind not in ("empty", "loading", "partial", "unavailable", "error"):
            raise ValueError(f"Estado de painel desconhecido: {kind}")
        _check_action(action_text, command)
        super().__init__(parent, title=title, padding=padding, **kwargs)
        self.kind = kind
        self.badge = StatusBadge(self.body, text=_STATUS_LABEL[kind], kind=kind, theme=theme)
        self.badge.pack(anchor="w")
        self.message = ttk.Label(self.body, text=message, style="Surface.TLabel", wraplength=760,
                                 justify=tk.LEFT)
        self.message.pack(anchor="w", fill=tk.X, pady=(SPACING["sm"], 0))
        self.action_button = None
        if action_text:
            self.action_button = ttk.Button(self.body, text=action_text, command=command)
            self.action_button.pack(anchor="w", pady=(SPACING["md"], 0))


class ChartPanel(ContentPanel):
    """Painel de gráfico com faixa opcional para qualidade ou disponibilidade."""

    def __init__(self, parent, title=None, subtitle=None, padding=SPACING["md"], **kwargs):
        super().__init__(parent, title=title, subtitle=subtitle, padding=padding, **kwargs)
        self.notice = None
        self.legend_frame = None
        self.legend_items = ()
        self.chart_area = ttk.Frame(self.body, style="Surface.TFrame")
        self.chart_area.pack(fill=tk.BOTH, expand=True)

    def set_notice(self, text=None, kind="info", theme=None):
        if self.notice is not None:
            self.notice.destroy()
            self.notice = None
        if text:
            self.notice = Notice(self.body, text, kind=kind, theme=theme)
            self.notice.pack(fill=tk.X, pady=(0, SPACING["sm"]), before=self.chart_area)

    def set_legend(self, items, theme=None):
        """Mostra uma legenda pequena com amostras de cor ou linha, fora da área dos dados."""
        items = tuple(items)
        if self.legend_frame is not None:
            self.legend_frame.destroy()
            self.legend_frame = None
        self.legend_items = items
        if not items:
            return
        self.legend_frame = ttk.Frame(self.body, style="Surface.TFrame")
        self.legend_frame.pack(fill=tk.X, pady=(0, SPACING["xs"]), before=self.chart_area)
        surface = theme.colors["surface"] if theme is not None else "#FFFFFF"
        for text, color, kind in items:
            if kind not in ("square", "line"):
                raise ValueError(f"Tipo de legenda desconhecido: {kind}")
            swatch = tk.Canvas(self.legend_frame, width=18, height=12, bg=surface, highlightthickness=0,
                               takefocus=False)
            if kind == "line":
                swatch.create_line(1, 6, 17, 6, fill=color, width=2)
                swatch.create_oval(7, 3, 11, 9, fill=color, outline=color)
            else:
                swatch.create_rectangle(3, 2, 15, 10, fill=color, outline=color)
            swatch.pack(side=tk.LEFT, padx=(0, SPACING["2xs"]))
            ttk.Label(self.legend_frame, text=text, style="Muted.TLabel").pack(side=tk.LEFT,
                                                                                padx=(0, SPACING["md"]))


class MatrixTable(ttk.Frame):
    """Tabela matricial rolável com fundo e texto configuráveis por célula."""

    def __init__(self, parent, columns, widths, theme, height=9, row_height=32, **kwargs):
        if not columns or len(columns) != len(widths):
            raise ValueError("Cada coluna da matriz precisa ter uma largura definida.")
        super().__init__(parent, style="Surface.TFrame", **kwargs)
        self.columns = tuple(columns)
        self.widths = tuple(int(width) for width in widths)
        self.theme = theme
        self.row_height = int(row_height)
        self.canvas = tk.Canvas(self, height=height * self.row_height, highlightthickness=0,
                                background=theme.colors["surface"], takefocus=False)
        self.canvas.grid(row=0, column=0, sticky="nsew")
        self.vertical = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.canvas.yview)
        self.vertical.grid(row=0, column=1, sticky="ns")
        self.horizontal = ttk.Scrollbar(self, orient=tk.HORIZONTAL, command=self.canvas.xview)
        self.horizontal.grid(row=1, column=0, sticky="ew")
        self.canvas.configure(yscrollcommand=self.vertical.set, xscrollcommand=self._set_xview)
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.content = tk.Frame(self.canvas, background=theme.colors["surface"], takefocus=False)
        self.window = self.canvas.create_window((0, 0), window=self.content, anchor="nw")
        self.headers = []
        self.row_headers = []
        self.cells = []
        self._build_header()
        self.content.bind("<Configure>", self._content_configured, add="+")
        self.canvas.bind("<Configure>", self._canvas_configured, add="+")
        for widget in (self.canvas, self.content):
            widget.bind("<MouseWheel>", self._mousewheel, add="+")
            widget.bind("<Button-4>", self._wheel_up, add="+")
            widget.bind("<Button-5>", self._wheel_down, add="+")

    def _build_header(self):
        colors = self.theme.colors
        for column, (text, width) in enumerate(zip(self.columns, self.widths)):
            self.content.grid_columnconfigure(column, minsize=width, weight=1 if column else 0)
            header = tk.Label(self.content, text=text, background=colors["surface"], foreground=colors["ink2"],
                              font=self.theme.font_bold, anchor="center", padx=6, pady=5,
                              highlightthickness=1, highlightbackground=colors["border"], takefocus=False)
            header.grid(row=0, column=column, sticky="nsew")
            self.headers.append(header)
        self.content.grid_rowconfigure(0, minsize=self.row_height)

    def _build_rows(self, count):
        for widget in self.content.grid_slaves():
            if int(widget.grid_info().get("row", 0)) > 0:
                widget.destroy()
        self.row_headers = []
        self.cells = []
        colors = self.theme.colors
        for row in range(count):
            row_index = row + 1
            self.content.grid_rowconfigure(row_index, minsize=self.row_height)
            row_header = tk.Label(self.content, text="", background=colors["surface"], foreground=colors["ink2"],
                                  font=self.theme.font, anchor="w", padx=8, pady=4,
                                  highlightthickness=1, highlightbackground=colors["border"], takefocus=False)
            row_header.grid(row=row_index, column=0, sticky="nsew")
            self.row_headers.append(row_header)
            row_cells = []
            for column in range(len(self.columns) - 1):
                cell = tk.Label(self.content, text="", background=colors["surface"], foreground=colors["ink"],
                                font=self.theme.font, anchor="center", padx=4, pady=4,
                                highlightthickness=1, highlightbackground=colors["surface"],
                                highlightcolor=colors["accent"], takefocus=True)
                cell.grid(row=row_index, column=column + 1, sticky="nsew")
                cell.bind("<FocusIn>", lambda event, r=row, c=column: self._reveal(r, c), add="+")
                cell.bind("<FocusOut>", self._restore_cell_border, add="+")
                cell.bind("<Button-1>", lambda _event, widget=cell: widget.focus_set(), add="+")
                cell.bind("<Left>", lambda event, r=row, c=column: self._move_focus(r, c - 1), add="+")
                cell.bind("<Right>", lambda event, r=row, c=column: self._move_focus(r, c + 1), add="+")
                cell.bind("<Up>", lambda event, r=row, c=column: self._move_focus(r - 1, c), add="+")
                cell.bind("<Down>", lambda event, r=row, c=column: self._move_focus(r + 1, c), add="+")
                cell.bind("<MouseWheel>", self._mousewheel, add="+")
                cell.bind("<Button-4>", self._wheel_up, add="+")
                cell.bind("<Button-5>", self._wheel_down, add="+")
                row_cells.append(cell)
            self.cells.append(row_cells)

    def set_rows(self, row_labels, rows):
        """Atualiza os rótulos e as tuplas (texto, fundo, primeiro plano) das células."""
        if len(row_labels) != len(rows) or any(len(row) != len(self.columns) - 1 for row in rows):
            raise ValueError("As linhas da matriz precisam corresponder aos cabeçalhos.")
        if len(self.cells) != len(rows):
            self._build_rows(len(rows))
        colors = self.theme.colors
        self.canvas.configure(background=colors["surface"])
        self.content.configure(background=colors["surface"])
        for header in self.headers:
            header.configure(background=colors["surface"], foreground=colors["ink2"],
                              highlightbackground=colors["border"])
        for row_index, (label, row) in enumerate(zip(row_labels, rows)):
            self.row_headers[row_index].configure(text=str(label), background=colors["surface"],
                                                  foreground=colors["ink2"], highlightbackground=colors["border"])
            for cell, (text, background, foreground) in zip(self.cells[row_index], row):
                cell.configure(text=str(text), background=background, foreground=foreground,
                               highlightbackground=background, highlightcolor=colors["accent"])
        self.after_idle(self._sync_geometry)

    def _content_configured(self, _event=None):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _canvas_configured(self, event):
        width = max(self.content.winfo_reqwidth(), event.width)
        self.canvas.itemconfigure(self.window, width=width)
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _sync_geometry(self):
        width = max(self.content.winfo_reqwidth(), self.canvas.winfo_width())
        self.canvas.itemconfigure(self.window, width=width)
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _set_xview(self, first, last):
        self.horizontal.set(first, last)
        if float(last) - float(first) >= 0.999:
            self.horizontal.grid_remove()
        else:
            self.horizontal.grid()

    def _move_focus(self, row, column):
        if 0 <= row < len(self.cells) and 0 <= column < len(self.columns) - 1:
            self.cells[row][column].focus_set()
        return "break"

    def _reveal(self, row, column):
        self.cells[row][column].configure(highlightbackground=self.theme.colors["accent"])
        x0 = sum(self.widths[:column + 1])
        x1 = x0 + self.widths[column + 1]
        y0 = (row + 1) * self.row_height
        y1 = y0 + self.row_height
        bbox = self.canvas.bbox("all") or (0, 0, 1, 1)
        self._reveal_axis(self.canvas.xview, self.canvas.xview_moveto, x0, x1,
                          max(1, bbox[2] - bbox[0]))
        self._reveal_axis(self.canvas.yview, self.canvas.yview_moveto, y0, y1,
                          max(1, bbox[3] - bbox[1]))

    @staticmethod
    def _reveal_axis(view, move, start, end, total):
        first, last = (float(value) for value in view())
        start_fraction, end_fraction = start / total, end / total
        viewport = last - first
        if start_fraction < first:
            move(max(0, start_fraction))
        elif end_fraction > last:
            move(max(0, end_fraction - viewport))

    @staticmethod
    def _restore_cell_border(event):
        event.widget.configure(highlightbackground=event.widget.cget("background"))

    def _mousewheel(self, event):
        if event.delta:
            self.canvas.yview_scroll(-1 if event.delta > 0 else 1, "units")
        return "break"

    def _wheel_up(self, _event):
        self.canvas.yview_scroll(-1, "units")
        return "break"

    def _wheel_down(self, _event):
        self.canvas.yview_scroll(1, "units")
        return "break"


class TablePanel(ContentPanel):
    """Painel de conteúdo tabular com cabeçalhos, colunas e rolagem explícitos."""

    def __init__(self, parent, title, columns, widths, anchors=None, height=10, selectmode="browse",
                 xscroll=False, subtitle=None, padding=SPACING["md"], **kwargs):
        super().__init__(parent, title=title, subtitle=subtitle, padding=padding, **kwargs)
        from mt5_extracao.ui.theme import scrolled_tree

        self.table_frame, self.tree = scrolled_tree(self.body, columns, widths, anchors=anchors,
                                                    height=height, selectmode=selectmode, xscroll=xscroll)
        self.table_frame.pack(fill=tk.BOTH, expand=True)


class ProgressPanel(ContentPanel):
    """Progresso/capacidade com valor já preparado pela tela, sem inferência de domínio."""

    def __init__(self, parent, title, value, maximum=100, detail=None, cancel_text=None, on_cancel=None,
                 padding=SPACING["md"], **kwargs):
        _check_action(cancel_text, on_cancel)
        if maximum <= 0:
            raise ValueError("O máximo do progresso precisa ser maior que zero.")
        super().__init__(parent, title=title, padding=padding, **kwargs)
        self.progress = ttk.Progressbar(self.body, orient=tk.HORIZONTAL, mode="determinate", maximum=maximum,
                                        value=value)
        self.progress.pack(fill=tk.X)
        row = ttk.Frame(self.body, style="Surface.TFrame")
        row.pack(fill=tk.X, pady=(SPACING["xs"], 0))
        self.detail_label = ttk.Label(row, text=detail or "", style="Muted.TLabel")
        self.detail_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.cancel_button = None
        if cancel_text:
            self.cancel_button = ttk.Button(row, text=cancel_text, command=on_cancel)
            self.cancel_button.pack(side=tk.RIGHT, padx=(SPACING["sm"], 0))

    def set_progress(self, value, detail=None):
        self.progress.configure(value=value)
        if detail is not None:
            self.detail_label.configure(text=detail)


class ComparisonSummary(ContentPanel):
    """Resumo lado a lado dos valores fornecidos pela tela, sem ranqueá-los."""

    def __init__(self, parent, title, items, columns=3, padding=SPACING["md"], **kwargs):
        if columns < 1:
            raise ValueError("O resumo precisa de pelo menos uma coluna.")
        super().__init__(parent, title=title, padding=padding, **kwargs)
        self.items = []
        self.columns = columns
        self.set_items(items)

    def set_items(self, items):
        normalized = list(items)
        if any(not isinstance(item, (tuple, list)) or len(item) not in (2, 3) for item in normalized):
            raise ValueError("Cada item deve conter rótulo, valor e detalhe opcional.")
        for widget in self.body.winfo_children():
            widget.destroy()
        self.items = normalized
        for column in range(self.columns):
            self.body.columnconfigure(column, weight=1, uniform="comparison")
        for index, item in enumerate(self.items):
            label, value = item[:2]
            detail = item[2] if len(item) == 3 else ""
            cell = ttk.Frame(self.body, style="Surface.TFrame", padding=SPACING["sm"])
            cell.grid(row=index // self.columns, column=index % self.columns, sticky="nsew")
            ttk.Label(cell, text=label, style="Muted.TLabel", wraplength=220).pack(anchor="w")
            ttk.Label(cell, text=str(value), style="Value.TLabel", wraplength=220).pack(anchor="w")
            if detail:
                ttk.Label(cell, text=detail, style="Muted.TLabel", wraplength=220, justify=tk.LEFT).pack(anchor="w")


class DetailsPanel(ttk.Frame):
    """Painel recolhível acionável por mouse, Enter ou Espaço."""

    def __init__(self, parent, title, expanded=True, padding=SPACING["md"], **kwargs):
        super().__init__(parent, style="Card.TFrame", padding=padding, **kwargs)
        self.expanded = bool(expanded)
        self.header = ttk.Frame(self, style="Surface.TFrame")
        self.header.pack(fill=tk.X)
        self.title_label = ttk.Label(self.header, text=title, style="Heading.TLabel")
        self.title_label.pack(side=tk.LEFT)
        self.toggle_button = ttk.Button(self.header, command=self.toggle, takefocus=True)
        self.toggle_button.pack(side=tk.RIGHT)
        self.body = ttk.Frame(self, style="Surface.TFrame")
        self._sync()

    def _sync(self):
        self.toggle_button.configure(text="Ocultar detalhes" if self.expanded else "Mostrar detalhes")
        if self.expanded:
            self.body.pack(fill=tk.BOTH, expand=True, pady=(SPACING["sm"], 0))
        else:
            self.body.pack_forget()

    def toggle(self):
        self.expanded = not self.expanded
        self._sync()
