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
        self.chart_area = ttk.Frame(self.body, style="Surface.TFrame")
        self.chart_area.pack(fill=tk.BOTH, expand=True)

    def set_notice(self, text=None, kind="info", theme=None):
        if self.notice is not None:
            self.notice.destroy()
            self.notice = None
        if text:
            self.notice = Notice(self.body, text, kind=kind, theme=theme)
            self.notice.pack(fill=tk.X, pady=(0, SPACING["sm"]), before=self.chart_area)


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
