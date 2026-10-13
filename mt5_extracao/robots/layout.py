"""
Organização da janela do MetaTrader 5 (T12): fecha as janelas laterais (Observação do Mercado, Navegador, Caixa de
Ferramentas e Testador de Estratégia) e esconde as barras de ferramentas, para o gráfico do robô e o painel
ocuparem a tela inteira em qualquer resolução.

Não há função MQL5 para isso e o projeto não usa DLL: o que este módulo faz é editar `Config\\terminal.ini` com o
terminal FECHADO, antes de o programa abri-lo. O arquivo original é guardado uma vez
(`terminal.ini.antes-do-mt5x`) e `restore` o devolve. Depois de aberto, o usuário reabre o que quiser pelo menu
Exibir; o terminal só grava a escolha dele se for fechado normalmente.

Os identificadores foram lidos do terminal build 6258 (fechando cada janela e comparando o arquivo). Num build
que use outros, nada é alterado: seção que não existe é ignorada.
"""
import logging
import re
from pathlib import Path
from typing import List

log = logging.getLogger(__name__)

SIDE_PANES = {"32841": "Caixa de Ferramentas / Testador", "32846": "Caixa de Ferramentas / Testador",
              "32843": "Observação do Mercado / Navegador", "32845": "Observação do Mercado / Navegador"}
TOOLBARS = {"32900": "barra de ferramentas", "32931": "barra de ferramentas", "32901": "barra de ferramentas"}
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


def apply(data_dir, toolbars: bool = True) -> List[str]:
    """
    Fecha as janelas laterais (e, com `toolbars`, esconde as barras de ferramentas) no terminal.ini. O terminal
    precisa estar fechado. Devolve a lista do que foi escondido (vazia se já estava ou se o arquivo não existe).
    """
    path = ini_path(data_dir)
    if not path.is_file():
        return []
    text, encoding = _read(path)
    wanted = dict(SIDE_PANES, **(TOOLBARS if toolbars else {}))
    new_text, changed = hide(text, wanted)
    if not changed:
        return []
    backup = path.with_name(path.name + BACKUP_SUFFIX)
    if not backup.exists():
        backup.write_bytes(path.read_bytes())            # guarda o arranjo do usuário, uma vez só
    path.write_bytes(new_text.encode(encoding))
    names = sorted({wanted[i] for i in changed})
    log.info("Janela do MT5 organizada: escondido " + ", ".join(names))
    return names


def restore(data_dir) -> bool:
    """Devolve o terminal.ini guardado antes da primeira organização. True se havia o que devolver."""
    path = ini_path(data_dir)
    backup = path.with_name(path.name + BACKUP_SUFFIX)
    if not backup.is_file():
        return False
    path.write_bytes(backup.read_bytes())
    backup.unlink()
    return True


def enabled(config, common_files=None) -> bool:
    """A organização vale se [MT5] manage_layout não for false E o painel não a tiver desligado (botão TELA)."""
    if config is not None and not config.getboolean("MT5", "manage_layout", fallback=True):
        return False
    return not (common_files is not None and (Path(common_files) / OFF_FILE).is_file())


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


def startup_lines(profile: str) -> str:
    """Trecho do .ini de início que abre o terminal no perfil dado."""
    return f"[Charts]\r\nProfileLast={profile}\r\n"
