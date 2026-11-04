"""
Explica, em uma frase com a ação de correção, por que o MetaTrader 5 não conectou. Sem Tkinter.

O conector tenta várias formas de conectar e, quando todas falham, só sabe dizer isso. O motivo mais comum não está
nele: o terminal abre, mas não entra na conta porque o computador está sem internet (visto no host em 2026-10-13:
o log do MT5 dizia "no connection to MetaQuotes-Demo" e o programa mostrava seis estratégias que falharam).
"""
import socket
import time
from typing import Callable, Iterable, Optional, Tuple

# Endereços públicos estáveis, por IP: não dependem de DNS, que também some quando a rede cai.
PROBES = (("1.1.1.1", 443), ("8.8.8.8", 53), ("9.9.9.9", 53))
NO_INTERNET, NOT_LOGGED, NOT_OPEN = "sem internet", "sem conta", "não abriu"
BACKEND_UNAVAILABLE, CONNECTION_ERROR = "canal indisponível", "conexão não concluída"
STARTUP_WAIT_SECONDS = 60.0


def wait_ready(check, timeout=STARTUP_WAIT_SECONDS, *, cancelled=None, clock=time.monotonic, sleep=time.sleep):
    """Espera cooperativa por uma condição curta, sem relançar terminal ou fazer login."""
    deadline=clock()+max(0,timeout)
    while True:
        if cancelled is not None and cancelled():return False
        if check():return True
        remaining=deadline-clock()
        if remaining<=0:return False
        sleep(min(.25,remaining))


def internet_ok(timeout: float = 2.5, probes: Iterable[Tuple[str, int]] = PROBES,
                connect: Optional[Callable] = None) -> bool:
    """True se pelo menos um dos endereços responde. Não envia nada: só abre e fecha a conexão."""
    connect = connect or socket.create_connection
    for address in probes:
        try:
            connect(address, timeout=timeout).close()
            return True
        except OSError:
            continue
    return False


def diagnose(terminal_running: bool, online: bool, backend_available: bool = True) -> str:
    if not online:
        return NO_INTERNET
    if not backend_available:
        return BACKEND_UNAVAILABLE
    return NOT_LOGGED if terminal_running else NOT_OPEN


def explain(reason: str, windows: bool = False) -> str:
    """O texto para a pessoa: o que aconteceu e o que fazer."""
    if reason == NO_INTERNET:
        return ("Este computador está sem internet, por isso o MetaTrader 5 não consegue entrar na conta.\n\n"
                "Confira o Wi-Fi ou o cabo de rede e depois clique em \"Verificar conexão\". Enquanto isso, a base "
                "de dados já extraída continua disponível nas abas Minha base e Análise.")
    if reason == NOT_LOGGED:
        return ("O MetaTrader 5 abriu, mas não entrou na conta: a corretora não respondeu, ou a conta ou a senha "
                "não valem mais.\n\nOlhe o canto inferior direito da janela do MetaTrader 5 (ele mostra o estado da "
                "conexão). Se pedir, entre na conta por Arquivo > Login; depois clique em \"Verificar conexão\".")
    if reason == BACKEND_UNAVAILABLE:
        return ("Não foi possível estabelecer o canal de comunicação com o MetaTrader 5 no prazo de espera. "
                "O terminal pode estar aberto; isso ainda não confirma a conexão com o aplicativo.\n\n" +
                ("Confira a instalação do módulo MetaTrader5 e o Diagnóstico do ambiente." if windows else
                 "Confira a ponte em Arquivo > Configurações > Diagnóstico do ambiente ou com ./scripts/mt5-bridge.sh status.") +
                " Depois clique em \"Verificar conexão\". A base já extraída continua disponível.")
    if reason == CONNECTION_ERROR:
        return ("Não foi possível concluir a verificação da conexão. O terminal pode continuar iniciando; "
                "este aviso não confirma uma falha de abertura.\n\nVeja o motivo em Atividade e no Diagnóstico do ambiente; "
                "depois clique em \"Verificar conexão\".")
    hint = ("Confira se ele abre normalmente pelo atalho." if windows else
            "Confira a ponte com ./scripts/mt5-bridge.sh status e, se estiver parada, ./scripts/mt5-bridge.sh start.")
    return ("Não foi possível confirmar a abertura do MetaTrader 5 no prazo de espera. Ele pode continuar iniciando.\n\n" + hint + "\nO diagnóstico completo está em Arquivo > Configurações > "
            "Diagnóstico do ambiente (ou no comando 'mt5x doctor').")
