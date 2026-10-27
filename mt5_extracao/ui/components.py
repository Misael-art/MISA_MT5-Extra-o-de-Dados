"""Componentes ttk reutilizáveis de apresentação, sem regras de domínio.

Os contêineres expõem seus widgets internos para que as telas possam conservar
callbacks e cálculos próprios sem duplicar o padrão visual.
"""
import tkinter as tk
from tkinter import ttk, font as tkfont

from mt5_extracao.ui.visual_tokens import SPACING, result_attention_colors


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
        self.chart_area = ttk.Frame(self.body, style="Surface.TFrame")
        self.chart_area.pack(fill=tk.BOTH, expand=True)

    def set_notice(self, text=None, kind="info", theme=None):
        if self.notice is not None:
            self.notice.destroy()
            self.notice = None
        if text:
            self.notice = Notice(self.body, text, kind=kind, theme=theme)
            self.notice.pack(fill=tk.X, pady=(0, SPACING["sm"]), before=self.chart_area)


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
        self.header_canvas = tk.Canvas(self, height=self.row_height, highlightthickness=0,
                                       background=theme.colors["surface"], takefocus=False)
        self.header_canvas.grid(row=0, column=0, sticky="ew")
        self.header_frame = tk.Frame(self.header_canvas, background=theme.colors["surface"])
        self.header_window = self.header_canvas.create_window((0, 0), window=self.header_frame, anchor="nw")
        self.canvas = tk.Canvas(self, height=height * self.row_height, highlightthickness=0,
                                background=theme.colors["surface"], takefocus=False)
        self.canvas.grid(row=1, column=0, sticky="nsew")
        self.vertical = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.canvas.yview)
        self.vertical.grid(row=1, column=1, sticky="ns")
        self.horizontal = ttk.Scrollbar(self, orient=tk.HORIZONTAL, command=self.canvas.xview)
        self.horizontal.grid(row=2, column=0, sticky="ew")
        self.canvas.configure(yscrollcommand=self.vertical.set, xscrollcommand=self._set_xview)
        self.rowconfigure(1, weight=1)
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
            # Sem peso: as larguras de corpo e cabeçalho são definidas juntas em _sync_geometry.
            self.content.grid_columnconfigure(column, minsize=width, weight=0)
            header = tk.Label(self.header_frame, text=text, background=colors["surface"], foreground=colors["ink2"],
                              font=self.theme.font_bold, anchor="center", padx=6, pady=5,
                              highlightthickness=1, highlightbackground=colors["border"], takefocus=False)
            header.grid(row=0, column=column, sticky="nsew")
            self.headers.append(header)
        self.header_frame.grid_rowconfigure(0, minsize=self.row_height)

    def _build_rows(self, count):
        for row in range(len(self.cells)):
            self.content.grid_rowconfigure(row, minsize=0)
        for widget in self.content.grid_slaves():
            widget.destroy()
        self.row_headers = []
        self.cells = []
        self._focus_cell = None
        colors = self.theme.colors
        for row in range(count):
            row_index = row
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
                                highlightcolor=colors["accent"], takefocus=(row == 0 and column == 0))
                cell.grid(row=row_index, column=column + 1, sticky="nsew")
                cell.bind("<FocusIn>", lambda event, r=row, c=column: self._reveal(r, c), add="+")
                cell.bind("<FocusOut>", self._restore_cell_border, add="+")
                cell.bind("<Button-1>", lambda _event, widget=cell: widget.focus_set(), add="+")
                cell.bind("<Left>", lambda event, r=row, c=column: self._move_focus(r, c - 1), add="+")
                cell.bind("<Right>", lambda event, r=row, c=column: self._move_focus(r, c + 1), add="+")
                cell.bind("<Up>", lambda event, r=row, c=column: self._move_focus(r - 1, c), add="+")
                cell.bind("<Down>", lambda event, r=row, c=column: self._move_focus(r + 1, c), add="+")
                cell.bind("<Home>", lambda event, c=column: self._move_focus(0, c), add="+")
                cell.bind("<End>", lambda event, c=column: self._move_focus(len(self.cells) - 1, c), add="+")
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
        self.header_canvas.configure(background=colors["surface"])
        self.header_frame.configure(background=colors["surface"])
        self.content.configure(background=colors["surface"])
        for header in self.headers:
            header.configure(background=colors["surface"], foreground=colors["ink2"],
                              highlightbackground=colors["border"])
        for row_index, (label, row) in enumerate(zip(row_labels, rows)):
            self.row_headers[row_index].configure(text=str(label), background=colors["surface"],
                                                  foreground=colors["ink2"], highlightbackground=colors["border"])
            for cell, (text, background, foreground) in zip(self.cells[row_index], row):
                cell.configure(text=str(text), background=background, foreground=foreground,
                               highlightbackground=colors["accent"] if self.focus_get() is cell else background,
                               highlightcolor=colors["accent"])
        self.after_idle(self._sync_geometry)

    def _content_configured(self, _event=None):
        self._sync_geometry()

    def _canvas_configured(self, _event=None):
        self._sync_geometry()

    def column_widths(self, available=None):
        """Mesma largura por coluna no cabeçalho e no corpo; sobra vai às colunas de valores."""
        natural = []
        for column, minimum in enumerate(self.widths):
            widgets = [self.headers[column]] + ([row[column - 1] for row in self.cells] if column
                                                else list(self.row_headers))
            natural.append(max([minimum] + [widget.winfo_reqwidth() for widget in widgets]))
        available = self.canvas.winfo_width() if available is None else available
        extra = max(0, available - sum(natural))
        if extra and len(natural) > 1:
            share, rest = divmod(extra, len(natural) - 1)
            natural = [natural[0]] + [width + share + (i < rest) for i, width in enumerate(natural[1:])]
        return natural

    def _sync_geometry(self):
        widths = self.column_widths()
        if getattr(self, "_applied_widths", None) != widths:
            self._applied_widths = widths
            for column, width in enumerate(widths):
                self.content.grid_columnconfigure(column, minsize=width, weight=0)
                self.header_frame.grid_columnconfigure(column, minsize=width, weight=0)
        total = sum(widths)
        body_height = max(1, self.content.winfo_reqheight())
        height = max(self.row_height, *(header.winfo_reqheight() for header in self.headers))
        self.canvas.itemconfigure(self.window, width=total)
        self.header_canvas.itemconfigure(self.header_window, width=total)
        # Regiões explícitas e idênticas: a mesma fração de xview vale para cabeçalho e corpo.
        self.canvas.configure(scrollregion=(0, 0, total, body_height))
        self.header_canvas.configure(height=height, scrollregion=(0, 0, total, height))
        self.header_canvas.xview_moveto(self.canvas.xview()[0])


    def _set_xview(self, first, last):
        self.horizontal.set(first, last)
        self.header_canvas.xview_moveto(float(first))
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
        cell = self.cells[row][column]
        previous = getattr(self, "_focus_cell", None)
        if previous is not None and previous is not cell and previous.winfo_exists():
            previous.configure(takefocus=False)
        cell.configure(takefocus=True)
        self._focus_cell = cell
        x0, y0 = cell.winfo_x(), cell.winfo_y()
        x1, y1 = x0 + cell.winfo_width(), y0 + cell.winfo_height()
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

    def __init__(self, parent, title, expanded=True, padding=SPACING["md"],
                 information=False, on_toggle=None, theme=None, **kwargs):
        if on_toggle is not None and not callable(on_toggle):
            raise ValueError("O callback de expansão precisa ser executável.")
        super().__init__(parent, style="Card.TFrame", padding=padding, **kwargs)
        self.expanded = bool(expanded)
        self.information = information
        self.on_toggle = on_toggle
        self.theme = theme
        self.information_icon = None
        self.header = ttk.Frame(self, style="Surface.TFrame")
        self.header.pack(fill=tk.X)
        self.header.columnconfigure(0, weight=1)
        self.title_label = ttk.Label(self.header, text=title, justify=tk.LEFT,
                                     style="Muted.TLabel" if information else "Heading.TLabel")
        self.title_label.grid(row=0, column=0, sticky="w")
        self.toggle_button = ttk.Button(self.header, command=self.toggle, takefocus=True,
                                        style="Toolbutton" if information else "TButton")
        self.toggle_button.grid(row=0, column=1, sticky="ne", padx=(SPACING["xs"], 0))
        self.toggle_button.bind("<Return>", self._activate)
        self.toggle_button.bind("<Escape>", self.collapse)
        self.header.bind("<Configure>", self._wrap_title, add="+")
        self.body = ttk.Frame(self, style="Surface.TFrame")
        self._sync()
        if information:
            self._refresh_information_icon()
            if theme:
                theme.on_change(self._refresh_information_icon)

    def _refresh_information_icon(self):
        if not self.toggle_button.winfo_exists():
            return
        from mt5_extracao.ui import icons

        font = self.theme.font if self.theme else tkfont.nametofont("TkDefaultFont", root=self)
        color = (self.theme.colors["ink2"] if self.theme else
                 ttk.Style(self).lookup("Toolbutton", "foreground") or "black")
        rgb = self.winfo_rgb(color)
        color = "#" + "".join(f"{channel // 257:02x}" for channel in rgb)
        self.information_icon = icons.information(color, size=max(14, int(font.metrics("linespace") * .8)))
        self.toggle_button.configure(image=self.information_icon, compound=tk.LEFT)

    def _wrap_title(self, event):
        self.title_label.configure(wraplength=max(1, event.width - self.toggle_button.winfo_reqwidth() - SPACING["xs"]))

    def _activate(self, _event=None):
        self.toggle_button.invoke()
        return "break"

    def _sync(self):
        text = "Ocultar detalhes" if self.expanded else "Mostrar detalhes"
        if self.information:
            text = "Recolher" if self.expanded else "Detalhes"
        self.toggle_button.configure(text=text)
        if self.expanded:
            self.body.pack(fill=tk.BOTH, expand=True, pady=(SPACING["sm"], 0))
        else:
            self.body.pack_forget()

    def toggle(self):
        self.set_expanded(not self.expanded)

    def set_expanded(self, expanded):
        expanded = bool(expanded)
        if expanded == self.expanded:
            return
        focus = self.focus_get()
        if not expanded and focus is not None and str(focus).startswith(str(self.body) + "."):
            self.toggle_button.focus_set()
        self.expanded = expanded
        self._sync()
        if self.on_toggle:
            self.on_toggle(self.expanded)

    def collapse(self, _event=None):
        self.set_expanded(False)
        self.toggle_button.focus_set()
        return "break"


class ScrollableContent(ttk.Frame):
    """Deixa a área de trabalho percorrível quando o gráfico e os dados excedem a janela."""

    def __init__(self, parent, theme):
        super().__init__(parent, style="Surface.TFrame")
        self.theme = theme
        self.canvas = tk.Canvas(self, highlightthickness=1, borderwidth=0, takefocus=True)
        self.scrollbar = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        self.content = ttk.Frame(self.canvas, style="Surface.TFrame")
        self._window = self.canvas.create_window((0, 0), window=self.content, anchor="nw")
        self._geometry_sync = None
        self.bind("<Destroy>", self._destroyed, add="+")
        self.canvas.grid(row=0, column=0, sticky="nsew")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        self.content.bind("<Configure>", self._content_changed, add="+")
        self.canvas.bind("<Configure>", self._canvas_changed, add="+")
        self.canvas.bind("<Prior>", lambda event: self._page(-1))
        self.canvas.bind("<Next>", lambda event: self._page(1))
        self._apply_theme()
        theme.on_change(self._apply_theme)

    def _page(self, direction):
        self.canvas.yview_scroll(direction, "pages")
        return "break"

    def _apply_theme(self):
        if self.canvas.winfo_exists():
            c = self.theme.colors
            self.canvas.configure(background=c["surface"], highlightbackground=c["border"],
                                  highlightcolor=c["accent"])


    def _canvas_changed(self, event):
        self.canvas.itemconfigure(self._window, width=event.width)
        self._content_changed()

    def _content_changed(self, _event=None):
        if self._geometry_sync is None:
            self._geometry_sync = self.after_idle(self._sync_content)

    def _destroyed(self, event):
        if event.widget is self and self._geometry_sync is not None:
            self.after_cancel(self._geometry_sync)
            self._geometry_sync = None

    def _sync_content(self):
        # A largura pode alterar o requisito de altura dos rótulos que quebram linhas.
        # Mantém o agendamento marcado durante o cálculo para não reentrar pelo Configure.
        self.content.update_idletasks()
        self._geometry_sync = None
        inset = 2 * int(self.canvas.cget("highlightthickness"))
        height = max(self.content.winfo_reqheight(), self.canvas.winfo_height() - inset)
        if int(float(self.canvas.itemcget(self._window, "height"))) != height:
            self.canvas.itemconfigure(self._window, height=height)
        bounds = self.canvas.bbox("all")
        if bounds:
            self.canvas.configure(scrollregion=bounds)
        self._sync_scrollbar()

    def _sync_scrollbar(self):
        available = self.canvas.winfo_height() - 2 * int(self.canvas.cget("highlightthickness"))
        needs_scroll = self.content.winfo_reqheight() > available
        if needs_scroll and not self.scrollbar.winfo_manager():
            self.scrollbar.grid(row=0, column=1, sticky="ns")
        elif not needs_scroll and self.scrollbar.winfo_manager():
            self.scrollbar.grid_remove()


def configure_result_tags(tree, theme):
    """Hierarquia de resultados com seleção nativa claramente distinta dos tons suaves."""
    style = ttk.Style(tree)
    style.configure("ResultAttention.Treeview.Cell", padding=(SPACING["xs"], 0, SPACING["sm"], 0))
    style.map("ResultAttention.Treeview", background=[("selected", theme.colors["accent"])],
              foreground=[("selected", theme.colors["on_accent"])])
    tree.configure(style="ResultAttention.Treeview")
    for role, value in (("positive", 1), ("negative", -1), ("zero", 0), ("absent", None)):
        background, foreground = result_attention_colors(value, theme.colors, background="field")
        tree.tag_configure(f"result.{role}", background=background, foreground=foreground)


class AreaNavigation(ttk.Frame):
    """Nomes completos das páginas quando as abas não cabem na largura atual."""
    def __init__(self, parent, notebook, theme):
        super().__init__(parent)
        self.notebook, self.theme = notebook, theme
        self.original_style = notebook.cget("style")
        self._job = None
        self._key = None
        ttk.Label(self, text="Área:", style="Page.Muted.TLabel").pack(side=tk.LEFT, padx=(0, 8))
        self.combo = ttk.Combobox(self, state="readonly", width=1)
        self.combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.combo.bind("<<ComboboxSelected>>", self._choose)
        notebook.bind("<Configure>", self._schedule, add="+")
        notebook.bind("<<NotebookTabChanged>>", self._selected, add="+")
        self.bind("<Destroy>", self._destroyed, add="+")
        theme.on_change(self._schedule)
        self._schedule()

    def _destroyed(self, event):
        if event.widget is self and self._job is not None:
            self.after_cancel(self._job)
            self._job = None

    def _schedule(self, event=None):
        if self.winfo_exists() and self._job is None:
            self._job = self.after_idle(self._layout)

    def _selected(self, event=None):
        tabs = self.notebook.tabs()
        selected = self.notebook.select()
        if selected in tabs:
            self.combo.configure(values=tuple(self.notebook.tab(tab, "text") for tab in tabs))
            self.combo.current(tabs.index(selected))
        self._schedule()

    def _choose(self, event=None):
        index = self.combo.current()
        tabs = self.notebook.tabs()
        if 0 <= index < len(tabs):
            self.notebook.select(tabs[index])

    def _layout(self):
        self._job = None
        names = tuple(self.notebook.tab(tab, "text") for tab in self.notebook.tabs())
        width = self.notebook.winfo_width()
        if width <= 1:
            return
        needed = sum(self.theme.font_bold.measure(name) + 2 * SPACING["xl"] + 4 for name in names)
        key = width, needed, names, self.theme.dark
        if key == self._key:
            return
        self._key = key
        self.combo.configure(values=names)
        if needed > width:
            ttk.Style(self).layout("AreaOnly.TNotebook.Tab", [])
            self.notebook.configure(style="AreaOnly.TNotebook")
            self.pack(fill=tk.X, before=self.notebook, pady=(0, 6))
        else:
            if self.focus_get() is self.combo:
                self.notebook.focus_set()
            self.pack_forget()
            self.notebook.configure(style=self.original_style)
        tabs = self.notebook.tabs()
        selected = self.notebook.select()
        if selected in tabs:
            self.combo.current(tabs.index(selected))


class AdaptiveStatusBar(ttk.Frame):
    """Duas mensagens originais, com detalhe integral quando o espaço é insuficiente."""
    def __init__(self, parent, theme):
        super().__init__(parent)
        self.theme = theme
        self.status_text, self.base_text = "Status: Iniciando...", ""
        self._job = None
        self._key = None
        self.status_label = ttk.Label(self, style="Page.Muted.TLabel")
        self.base_label = ttk.Label(self, style="Page.Muted.TLabel")
        self.details_button = ttk.Button(self, text="Detalhes...", command=self.show_details)
        self.columnconfigure(0, weight=1)
        self.bind("<Configure>", self._schedule, add="+")
        self.bind("<Destroy>", self._destroyed, add="+")
        theme.on_change(self._schedule)
        self._schedule()

    def _destroyed(self, event):
        if event.widget is self and self._job is not None:
            self.after_cancel(self._job)
            self._job = None

    def set_status(self, text):
        if text is not None:
            self.status_text = str(text)
            self._layout()
            self._schedule()

    def set_base(self, text):
        if text is not None:
            self.base_text = str(text)
            self._layout()
            self._schedule()

    def _schedule(self, event=None):
        if self.winfo_exists() and self._job is None:
            self._job = self.after_idle(self._layout)

    def _fit(self, text, width):
        # O texto integral permanece no modelo e no detalhe, sem alterar seu significado.
        font = self.theme.font_small
        single = text.replace("\r", " ").replace("\n", " ").replace("\t", " ")
        if font.measure(single) <= width:
            return single
        low, high = 0, len(single)
        while low < high:
            middle = (low + high + 1) // 2
            if font.measure(single[:middle] + "…") <= width:
                low = middle
            else:
                high = middle - 1
        return single[:low] + "…"

    def _layout(self):
        if self._job is not None:
            self.after_cancel(self._job)
            self._job = None
        width = self.winfo_width()
        if width <= 1:
            return
        font = self.theme.font_small
        status_width = font.measure(self.status_text.replace("\r", " ").replace("\n", " ").replace("\t", " "))
        base_width = font.measure(self.base_text.replace("\r", " ").replace("\n", " ").replace("\t", " "))
        key = width, self.status_text, self.base_text, status_width, base_width
        if key == self._key:
            return
        self._key = key
        multiline = any(char in self.status_text + self.base_text for char in "\n\r\t")
        clipped = max(status_width, base_width) > width - 8 or multiline
        stacked = status_width + base_width + 16 > width or clipped
        reserve = self.details_button.winfo_reqwidth() + 8 if clipped else 0
        if stacked:
            status = self._fit(self.status_text, max(1, width - reserve - 8))
            base = self._fit(self.base_text, max(1, width - 8))
            self.status_label.grid(row=0, column=0, columnspan=2, sticky="w")
            self.base_label.grid(row=1, column=0, columnspan=3, sticky="w")
        else:
            status, base = self.status_text, self.base_text
            self.status_label.grid(row=0, column=0, columnspan=1, sticky="w")
            self.base_label.grid(row=0, column=1, columnspan=1, sticky="e", padx=(8, 0))
        self.status_label.configure(text=status)
        self.base_label.configure(text=base)
        if clipped:
            self.details_button.grid(row=0, column=2, sticky="e", padx=(8, 0))
        else:
            self.details_button.grid_remove()

    def show_details(self):
        window = tk.Toplevel(self.winfo_toplevel())
        window.title("Status da aplicação")
        window.geometry("800x500")
        window.minsize(400, 240)
        window.transient(self.winfo_toplevel())
        caption = ttk.Label(window, text="Mensagens no momento da abertura", style="Page.Muted.TLabel", wraplength=760)
        caption.pack(fill=tk.X, padx=12, pady=8)
        caption.bind("<Configure>", lambda e: caption.configure(wraplength=max(1, e.width - 4))
                     if int(float(caption.cget("wraplength"))) != max(1, e.width - 4) else None)
        ttk.Button(window, text="Fechar", command=window.destroy).pack(side=tk.BOTTOM, anchor="e", padx=12, pady=8)
        window.bind("<Escape>", lambda event: window.destroy())
        body = ttk.Frame(window, padding=12)
        body.pack(fill=tk.BOTH, expand=True)
        text = self.theme.classic(tk.Text(body, wrap="word", padx=8, pady=8, font=self.theme.font))
        bar = ttk.Scrollbar(body, command=text.yview)
        text.configure(yscrollcommand=bar.set)
        bar.pack(side=tk.RIGHT, fill=tk.Y)
        text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        text.insert("1.0", self.status_text + "\n\n" + self.base_text)
        text.configure(state="disabled")
        return window
