"""
Organização da janela do MetaTrader 5 (T12): fecha as janelas laterais (Observação do Mercado, Navegador, Caixa de
Ferramentas e Testador de Estratégia) e esconde as barras de ferramentas, para o gráfico do robô e o painel
ocuparem a tela inteira em qualquer resolução.

Não há função MQL5 para isso e o projeto não usa DLL: o que este módulo faz é editar `Config\\terminal.ini` com o
terminal FECHADO, antes de o programa abri-lo. O arquivo original é guardado uma vez
(`terminal.ini.antes-do-mt5x`) e `restore` devolve a visibilidade que as janelas tinham nele, sem tocar no resto do
arquivo (ele também guarda opções do terminal, que podem ter mudado depois). Depois de aberto, o usuário reabre o
que quiser pelo menu Exibir; o terminal só grava a escolha dele se for fechado normalmente.

Cada parte pode ser desligada em `[MT5]` (ou em Arquivo > Configurações, na interface): `layout_hide_navigation`,
`layout_hide_toolbox`, `layout_hide_toolbars` e `layout_own_profile`. `manage_layout` é o interruptor geral.
`panel_layout` escolhe a disposição do painel do HADOUKEN (parâmetro `InpPanelLayout` do robô). Além do início de um
robô, `view.open_view` ("Abrir visão MT5") aplica tudo isso quando o usuário pedir.

Os identificadores foram lidos do terminal build 6258 (fechando cada janela e comparando o arquivo). Num build
que use outros, nada é alterado: seção que não existe é ignorada.
"""
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

log = logging.getLogger(__name__)

# Cada par de identificadores é um par de janelas. Visto no terminal real (build 6258, 2026-10-13): 32846 é o
# Testador de Estratégia e 32841 a Caixa de Ferramentas. Qual do outro par é o Navegador não foi conferido.
TOOLBOX_PANES = {"32841": "Caixa de Ferramentas / Testador", "32846": "Caixa de Ferramentas / Testador"}
NAVIGATION_PANES = {"32843": "Observação do Mercado / Navegador", "32845": "Observação do Mercado / Navegador"}
SIDE_PANES = dict(TOOLBOX_PANES, **NAVIGATION_PANES)
TOOLBARS = {"32900": "barra de ferramentas", "32931": "barra de ferramentas", "32901": "barra de ferramentas"}
ALL_PANES = dict(SIDE_PANES, **TOOLBARS)
BACKUP_SUFFIX = ".antes-do-mt5x"
PROFILE = "HADOUKEN"               # perfil de gráficos próprio: o perfil do usuário (Default) nunca é tocado
OFF_FILE = "HDK_LAYOUT_OFF.txt"    # gravado pelo botão TELA do painel: enquanto existir, nada é organizado
SEED_FILE = "HDK_LAYOUT_SEED.txt"  # avisa o painel de que há um gráfico auxiliar para ele fechar
# (tipo, tamanho) do período no arquivo .chr do MT5
_PERIODS = {"M1": (0, 1), "M2": (0, 2), "M3": (0, 3), "M4": (0, 4), "M5": (0, 5), "M6": (0, 6), "M10": (0, 10),
            "M12": (0, 12), "M15": (0, 15), "M20": (0, 20), "M30": (0, 30), "H1": (1, 1), "H2": (1, 2), "H3": (1, 3),
            "H4": (1, 4), "H6": (1, 6), "H8": (1, 8), "H12": (1, 12), "D1": (2, 1), "W1": (3, 1), "MN1": (4, 1)}
# Gráfico auxiliar MAXIMIZADO (window_type=3). Com ele no perfil, o gráfico que o terminal abre para o robô também
# nasce maximizado; sem ele, nasce numa janela pequena. Um arquivo só com as primeiras linhas não bastou no teste.
_SEED = """<chart>
id=0
symbol={symbol}
period_type={period_type}
period_size={period_size}
digits=5
tick_size=0.000000
scale_fix=0
scale_fixed_min=0.000000
scale_fixed_max=0.000000
scale_fix11=0
scale_bar=0
scale_bar_val=1.000000
scale=16
mode=1
fore=0
grid=1
volume=0
scroll=1
shift=0
shift_size=18.413598
fixed_pos=0.000000
ticker=1
ohlc=0
one_click=0
one_click_btn=1
bidline=1
askline=0
lastline=0
days=0
descriptions=0
tradelines=1
tradehistory=1
window_left=0
window_top=0
window_right=461
window_bottom=61
window_type=3
floating=0
floating_left=0
floating_top=0
floating_right=0
floating_bottom=0
floating_type=1
floating_toolbar=1
floating_tbstate=
background_color=0
foreground_color=16777215
barup_color=65280
bardown_color=65280
bullcandle_color=0
bearcandle_color=16777215
chartline_color=65280
volumes_color=3329330
grid_color=10061943
bidline_color=10061943
askline_color=255
lastline_color=49152
stops_color=255
windows_total=1

<window>
height=100.000000
objects=0

<indicator>
name=Main
path=
apply=1
show_data=1
scale_inherit=0
scale_line=0
scale_line_percent=50
scale_line_value=0.000000
scale_fix_min=0
scale_fix_min_val=0.000000
scale_fix_max=0
scale_fix_max_val=0.000000
expertmode=0
fixed_height=-1
</indicator>
</window>
</chart>
"""
_SECTION = re.compile(r"^\[Pane-(\d+)\]\s*$")


def ini_path(data_dir) -> Path:
    return Path(data_dir) / "Config" / "terminal.ini"


def _read(path: Path):
    raw = path.read_bytes()
    encoding = "utf-16" if raw[:2] in (b"\xff\xfe", b"\xfe\xff") else "utf-8-sig"
    return raw.decode(encoding), encoding


def hide(text: str, ids) -> tuple:
    """Põe IsVisible=0 nas seções [Pane-<id>] pedidas. Devolve (texto novo, ids alterados)."""
    newline = "\r\n" if "\r\n" in text else "\n"
    current, changed, out = None, [], []
    for line in text.split(newline):
        match = _SECTION.match(line)
        if match:
            current = match.group(1)
        elif line.startswith("["):
            current = None
        elif current in ids and line.strip() == "IsVisible=1":
            line = "IsVisible=0"
            changed.append(current)
        out.append(line)
    return newline.join(out), changed


def visibility(text: str) -> Dict[str, str]:
    """{identificador: "0" ou "1"} das seções [Pane-<id>] do arquivo."""
    current, found = None, {}
    for line in text.replace("\r\n", "\n").split("\n"):
        match = _SECTION.match(line)
        if match:
            current = match.group(1)
        elif line.startswith("["):
            current = None
        elif current and line.strip() in ("IsVisible=0", "IsVisible=1"):
            found[current] = line.strip()[-1]
    return found


def _set_visibility(text: str, values: Dict[str, str]) -> tuple:
    """Grava IsVisible das seções pedidas. Devolve (texto novo, ids cujo valor mudou)."""
    newline = "\r\n" if "\r\n" in text else "\n"
    current, changed, out = None, [], []
    for line in text.split(newline):
        match = _SECTION.match(line)
        if match:
            current = match.group(1)
        elif line.startswith("["):
            current = None
        elif current in values and line.strip() in ("IsVisible=0", "IsVisible=1"):
            wanted = "IsVisible=" + values[current]
            if line.strip() != wanted:
                line = wanted
                changed.append(current)
        out.append(line)
    return newline.join(out), changed


def _backup(path: Path) -> Path:
    return path.with_name(path.name + BACKUP_SUFFIX)


def apply(data_dir, toolbars: bool = True, navigation: bool = True, toolbox: bool = True) -> List[str]:
    """
    Fecha no terminal.ini as janelas pedidas: `navigation` (Observação do Mercado e Navegador), `toolbox` (Caixa de
    Ferramentas e Testador) e `toolbars` (barras de ferramentas). O que este módulo tinha escondido antes e não é
    mais pedido volta a aparecer como estava. O terminal precisa estar fechado. Devolve a lista do que foi
    escondido agora (vazia se já estava ou se o arquivo não existe).
    """
    path = ini_path(data_dir)
    if not path.is_file():
        return []
    text, encoding = _read(path)
    wanted = dict(NAVIGATION_PANES if navigation else {}, **(TOOLBOX_PANES if toolbox else {}),
                  **(TOOLBARS if toolbars else {}))
    new_text, changed = hide(text, wanted)
    backup = _backup(path)
    given_back = []
    if backup.is_file():         # parte desligada depois de já ter sido escondida: volta ao que o usuário tinha
        before = visibility(_read(backup)[0])
        new_text, given_back = _set_visibility(new_text, {i: v for i, v in before.items()
                                                           if i in ALL_PANES and i not in wanted})
    if not changed and not given_back:
        return []
    if changed and not backup.exists():
        backup.write_bytes(path.read_bytes())            # guarda o arranjo do usuário, uma vez só
    path.write_bytes(new_text.encode(encoding))
    names = sorted({wanted[i] for i in changed})
    if names:
        log.info("Janela do MT5 organizada: escondido " + ", ".join(names))
    if given_back:
        log.info("Janela do MT5: de volta como estava " + ", ".join(sorted({ALL_PANES[i] for i in given_back})))
    return names


def restore(data_dir) -> bool:
    """
    Devolve às janelas e barras a visibilidade que tinham antes da primeira organização (a cópia guardada). O resto
    do terminal.ini fica como está: ele também guarda opções do terminal, que podem ter mudado depois. True se
    havia o que devolver.
    """
    path = ini_path(data_dir)
    backup = _backup(path)
    if not backup.is_file():
        return False
    if path.is_file():
        text, encoding = _read(path)
        before = visibility(_read(backup)[0])
        new_text, changed = _set_visibility(text, {i: v for i, v in before.items() if i in ALL_PANES})
        if changed:
            path.write_bytes(new_text.encode(encoding))
    else:
        path.write_bytes(backup.read_bytes())
    backup.unlink()
    return True


def has_arrangement(data_dir) -> bool:
    """Há um arranjo anterior guardado (a janela foi organizada e ainda não foi devolvida)?"""
    return _backup(ini_path(data_dir)).is_file()


def enabled(config, common_files=None) -> bool:
    """A organização vale se [MT5] manage_layout não for false E o painel não a tiver desligado (botão TELA)."""
    if config is not None and not config.getboolean("MT5", "manage_layout", fallback=True):
        return False
    return not panel_off(common_files)


def panel_off(common_files) -> bool:
    """O botão TELA do painel do HADOUKEN desligou a organização?"""
    return common_files is not None and (Path(common_files) / OFF_FILE).is_file()


def set_panel_off(common_files, off: bool) -> None:
    """Liga de novo (ou desliga) a organização do mesmo jeito que o botão TELA do painel: pelo arquivo que ele grava."""
    target = Path(common_files) / OFF_FILE
    if off:
        target.write_text("organizacao da janela desligada\r\n", encoding="ascii")
    else:
        target.unlink(missing_ok=True)


# chave de [MT5] -> (atributo de Options, rótulo, explicação). A ordem é a da tela de configurações.
OPTION_KEYS = (
    ("layout_hide_navigation", "navigation", "Fechar a Observação do Mercado e o Navegador",
     "As duas janelas da lateral esquerda do MetaTrader 5."),
    ("layout_hide_toolbox", "toolbox", "Fechar a Caixa de Ferramentas e o Testador de Estratégia",
     "As duas janelas da parte de baixo do MetaTrader 5."),
    ("layout_hide_toolbars", "toolbars", "Esconder as barras de ferramentas",
     "As barras de botões no alto da janela. O menu Exibir do MetaTrader 5 as traz de volta."),
    ("layout_own_profile", "own_profile", "Abrir o gráfico do robô sozinho e maximizado",
     "Usa um perfil de gráficos próprio (\"HADOUKEN\"); os seus perfis não são tocados. Desligado, o gráfico do robô "
     "é aberto no perfil em que o terminal estava, e cada início acrescenta um gráfico a ele."),
    ("view_panel", "panel", "Mostrar o painel do HADOUKEN no gráfico",
     "Em \"Abrir visão MT5\" o painel entra no gráfico, na conta de demonstração conferida. Precisa do perfil de "
     "gráficos próprio (a opção acima)."),
)
MASTER_KEY = "manage_layout"

# Disposição do painel do HADOUKEN: valor de [MT5] panel_layout -> (rótulo, código de ENUM_HDK_PANEL_LAYOUT)
PANEL_KEY, PANEL_INPUT = "panel_layout", "InpPanelLayout"
PANEL_LAYOUTS = {"auto": ("Automática (pelo tamanho do gráfico)", 0), "compact": ("Compacta (telas pequenas)", 1),
                 "normal": ("Normal (uma coluna)", 2), "wide": ("Ampla (duas colunas, telas grandes)", 3)}
_PANEL_LINE = re.compile(r"^" + PANEL_INPUT + r"=\d+\s*$", re.M)


def panel_layout(config) -> str:
    """A disposição escolhida em [MT5] panel_layout ('auto' se a chave falta ou tem um valor desconhecido)."""
    value = (config.get("MT5", PANEL_KEY, fallback="auto") if config is not None else "auto").strip().lower()
    if value not in PANEL_LAYOUTS:
        log.warning(f"[MT5] {PANEL_KEY} = {value!r} não existe (use auto, compact, normal ou wide): usando auto.")
        return "auto"
    return value


PANEL_BLOCK_KEY = "panel_demo_blocked"
PANEL_HOURS_KEY, HOURS_INPUT = "panel_any_hours", "InpUseHours"


def panel_any_hours(config) -> bool:
    """
    A pessoa pediu que o painel opere em qualquer horário ([MT5] panel_any_hours)? O padrão é não: vale a janela
    de horário do painel, que é contada no HORÁRIO DO SERVIDOR da corretora, não no do computador.
    """
    if config is None:
        return False
    try:
        return config.getboolean("MT5", PANEL_HOURS_KEY, fallback=False)
    except ValueError:
        log.warning(f"[MT5] {PANEL_HOURS_KEY} com valor inválido em config.ini: usando o padrão (false).")
        return False


def server_offset_hours(server_epoch, now_utc_epoch, max_age: float = 300.0):
    """
    Diferença, em horas (múltiplos de meia hora), entre o relógio do servidor da corretora e o UTC, a partir do
    horário da última cotação de um ativo. O MT5 entrega esse horário como se fosse UTC, mas ele está no fuso do
    servidor. None se a cotação não cai perto de uma meia hora cheia (está velha: mercado fechado). Uma cotação
    parada há um número quase exato de meias horas não se distingue de outro fuso: quem chama deve conferir que a
    cotação está andando (veja ui/settings.py).
    """
    if not server_epoch:
        return None
    diff = float(server_epoch) - float(now_utc_epoch)
    offset = round(diff / 1800.0) / 2.0
    if abs(diff - offset * 3600.0) > max_age or abs(offset) > 14:
        return None
    return offset


def panel_blocked(config) -> bool:
    """
    A pessoa religou o bloqueio do painel manual em demo ([MT5] panel_demo_blocked)? O padrão é não: em conta de
    demonstração o painel manual opera (decisão do dono do projeto, 2026-10-13).
    """
    if config is None:
        return False
    try:
        return config.getboolean("MT5", PANEL_BLOCK_KEY, fallback=False)
    except ValueError:      # na dúvida sobre o que a pessoa quis, vale o lado seguro: bloqueado
        log.warning(f"[MT5] {PANEL_BLOCK_KEY} com valor inválido em config.ini: tratando como bloqueado.")
        return True


def panel_code(name: str) -> int:
    return PANEL_LAYOUTS[name][1]


def profile_dir(data_dir) -> Path:
    return Path(data_dir) / "MQL5" / "Profiles" / "Charts" / PROFILE


def has_profile(data_dir) -> bool:
    """O perfil de gráficos próprio já tem algum gráfico (um robô já foi iniciado pelo programa)?"""
    return any(profile_dir(data_dir).glob("chart*.chr"))


def set_panel_input(data_dir, code: int) -> int:
    """
    Grava a disposição do painel nos gráficos do perfil próprio que têm um robô com o parâmetro `InpPanelLayout`
    (o terminal precisa estar fechado). Só o perfil "HADOUKEN" é tocado. Devolve quantos gráficos mudaram.
    """
    changed = 0
    for chart in sorted(profile_dir(data_dir).glob("chart*.chr")):
        try:
            text, encoding = _read(chart)
        except (OSError, UnicodeError) as e:
            log.debug(f"Gráfico ilegível, deixado como está ({chart}): {e}")
            continue
        new_text = _PANEL_LINE.sub(lambda m: f"{PANEL_INPUT}={code}" + ("\r" if m.group(0).endswith("\r") else ""), text)
        if new_text != text:
            chart.write_bytes(new_text.encode(encoding) if encoding != "utf-16" else _utf16_raw(new_text))
            changed += 1
    return changed


def _utf16_raw(text: str) -> bytes:
    return b"\xff\xfe" + text.encode("utf-16-le")


@dataclass
class Options:
    enabled: bool = True
    navigation: bool = True
    toolbox: bool = True
    toolbars: bool = True
    own_profile: bool = True
    panel: bool = True


def _flag(config, key: str) -> bool:
    try:
        return config.getboolean("MT5", key, fallback=True)
    except ValueError:               # valor que não é sim/não: vale o padrão, como se a chave faltasse
        log.warning(f"[MT5] {key} com valor inválido em config.ini: usando o padrão (true).")
        return True


def options(config) -> Options:
    """O que a organização faz, lido de [MT5]. Chave ausente (ou inválida) = ligada, que é o comportamento original."""
    if config is None:
        return Options()
    return Options(enabled=_flag(config, MASTER_KEY), **{attr: _flag(config, key) for key, attr, _, _ in OPTION_KEYS})


def status(config, data_dir=None, common_files=None) -> dict:
    """
    Retrato para a interface e para `mt5x mt5 layout`: {"enabled" (opção geral), "panel_off" (botão TELA),
    "active" (vai organizar na próxima abertura), "panel_layout", "has_profile" (o perfil próprio já tem gráfico),
    "options": {chave: bool}, "will_hide": [nomes],
    "has_arrangement", "hidden_now": [nomes do que está escondido no terminal.ini]}.
    """
    opts = options(config)
    off = panel_off(common_files)
    wanted = dict(NAVIGATION_PANES if opts.navigation else {}, **(TOOLBOX_PANES if opts.toolbox else {}),
                  **(TOOLBARS if opts.toolbars else {}))
    hidden_now, arrangement = [], False
    if data_dir is not None:
        path = ini_path(data_dir)
        arrangement = has_arrangement(data_dir)
        if path.is_file():
            try:
                seen = visibility(_read(path)[0])
                hidden_now = sorted({name for i, name in ALL_PANES.items() if seen.get(i) == "0"})
            except (OSError, UnicodeError) as e:
                log.debug(f"Não foi possível ler {path}: {e}")
    return {"enabled": opts.enabled, "panel_off": off, "active": opts.enabled and not off,
            "panel_layout": panel_layout(config), "panel_blocked": panel_blocked(config),
            "panel_any_hours": panel_any_hours(config),
            "has_profile": data_dir is not None and has_profile(data_dir),
            "options": {key: getattr(opts, attr) for key, attr, _, _ in OPTION_KEYS},
            "will_hide": sorted(set(wanted.values())), "has_arrangement": arrangement, "hidden_now": hidden_now}


def _utf16(text: str) -> bytes:
    return b"\xff\xfe" + text.replace("\n", "\r\n").encode("utf-16-le")


def prepare_profile(data_dir, symbol: str, timeframe: str, common_files=None) -> str:
    """
    Monta o perfil de gráficos do HADOUKEN com um único gráfico auxiliar maximizado e devolve o nome do perfil,
    para o .ini de início (`[Charts] ProfileLast`). O que havia nesse perfil é substituído; os outros perfis não
    são tocados. Com `common_files`, deixa o aviso para o painel fechar o gráfico auxiliar.
    """
    if timeframe not in _PERIODS:
        raise ValueError(f"Timeframe sem equivalente no perfil do MT5: {timeframe}")
    period_type, period_size = _PERIODS[timeframe]
    folder = Path(data_dir) / "MQL5" / "Profiles" / "Charts" / PROFILE
    folder.mkdir(parents=True, exist_ok=True)
    for old in folder.glob("chart*.chr"):
        old.unlink()
    (folder / "chart01.chr").write_bytes(_utf16(_SEED.format(symbol=symbol, period_type=period_type,
                                                             period_size=period_size)))
    (folder / "order.wnd").write_bytes(_utf16("chart01.chr\n"))
    if common_files is not None and Path(common_files).is_dir():
        (Path(common_files) / SEED_FILE).write_text("grafico auxiliar criado pelo mt5x\r\n", encoding="ascii")
    return PROFILE


def user_chart(data_dir) -> tuple:
    """
    (ativo, timeframe) do primeiro gráfico do perfil em que o terminal estava (`[Charts] ProfileLast` de
    Config\\common.ini; "Default" se não houver), ou ("", ""). Só leitura: os perfis do usuário não são tocados.
    """
    data_dir = Path(data_dir)
    profile = "Default"
    try:
        common, _ = _read(data_dir / "Config" / "common.ini")
        match = re.search(r"^ProfileLast=(.+?)\s*$", common, re.M)
        if match and match.group(1) != PROFILE:
            profile = match.group(1)
    except (OSError, UnicodeError):
        pass
    periods = {value: name for name, value in _PERIODS.items()}
    for chart in sorted((data_dir / "MQL5" / "Profiles" / "Charts" / profile).glob("chart*.chr")):
        try:
            text, _ = _read(chart)
        except (OSError, UnicodeError):
            continue
        symbol = re.search(r"^symbol=(\S+)\s*$", text, re.M)
        kind, size = re.search(r"^period_type=(\d+)\s*$", text, re.M), re.search(r"^period_size=(\d+)\s*$", text, re.M)
        if symbol:
            period = periods.get((int(kind.group(1)), int(size.group(1))), "") if kind and size else ""
            return symbol.group(1), period
    return "", ""


def startup_lines(profile: str) -> str:
    """Trecho do .ini de início que abre o terminal no perfil dado."""
    return f"[Charts]\r\nProfileLast={profile}\r\n"
