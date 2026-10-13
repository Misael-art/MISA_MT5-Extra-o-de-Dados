"""
Abre a conta de demonstração gratuita no assistente do MetaTrader 5.

Roda no Python do Windows (nativo ou dentro do Wine) e usa só a biblioteca
padrão. Não simula teclado/mouse do desktop: fala direto com os controles do
assistente pelos IDs (iguais em qualquer idioma), então funciona em Wayland,
Xvfb e no Windows.

Quem chama é `python -m mt5_extracao.bootstrap demo-account`, que já pediu a
permissão do usuário e abriu o terminal. Uso direto:

    python mt5_demo_account.py --first Nome --last Sobrenome --email a@b.c
                               --phone 11912345678 --birth 1990-01-01

Códigos de saída: 0 = conta aberta, 1 = erro, 3 = assistente não apareceu,
4 = o formulário recusou os dados (preencha na tela do MT5).
"""
import argparse
import ctypes
import ctypes.wintypes as wt
import sys
import time

# IDs dos controles do assistente (validados no MT5 build 6257)
ID_NEXT, ID_FINISH = 12324, 12325
ID_COMPANY_LIST = 10729          # página 1: lista de empresas
ID_RADIO_DEMO = 10468            # página 2: "abrir uma conta de demonstração"
ID_FIRST, ID_LAST, ID_EMAIL, ID_PHONE = 10022, 10728, 10032, 10031   # página 3
ID_BIRTH, ID_AGREE = 10732, 11106
ID_STATUS = 10108                # página 4: resultado

WM_SETTEXT, WM_COMMAND, EN_CHANGE = 0x000C, 0x0111, 0x0300
BM_GETCHECK, BM_CLICK = 0x00F0, 0x00F5
MOUSE_DOWN, MOUSE_UP, KEY_UP = 0x0002, 0x0004, 0x0002

EXIT_OK, EXIT_ERROR, EXIT_NO_WIZARD, EXIT_REJECTED = 0, 1, 3, 4


def _api():
    user32 = ctypes.windll.user32
    user32.SendMessageW.argtypes = [wt.HWND, ctypes.c_uint, ctypes.c_void_p, ctypes.c_void_p]
    return user32


class Wizard:
    def __init__(self, user32):
        self.u = user32
        self._enum = ctypes.WINFUNCTYPE(ctypes.c_bool, wt.HWND, wt.LPARAM)

    def _class(self, hwnd):
        buf = ctypes.create_unicode_buffer(256)
        self.u.GetClassNameW(hwnd, buf, 256)
        return buf.value

    def _children(self, hwnd):
        found = []
        self.u.EnumChildWindows(hwnd, self._enum(lambda c, _l: found.append(c) or True), 0)
        return found

    def item(self, dialog, ctrl_id):
        """Controle visível com esse ID (pode estar numa página filha), ou None."""
        for child in self._children(dialog):
            if self.u.GetDlgCtrlID(child) == ctrl_id and self.u.IsWindowVisible(child):
                return child
        return None

    def find(self):
        """Janela do assistente: um diálogo visível que tem Avançar ou Finalizar."""
        tops = []
        self.u.EnumWindows(self._enum(lambda h, _l: tops.append(h) or True), 0)
        for hwnd in tops:
            if self.u.IsWindowVisible(hwnd) and self._class(hwnd) == "#32770":
                if self.item(hwnd, ID_NEXT) or self.item(hwnd, ID_FINISH):
                    return hwnd
        return None

    def wait(self, timeout):
        end = time.time() + timeout
        while time.time() < end:
            hwnd = self.find()
            if hwnd:
                return hwnd
            time.sleep(1)
        return None

    def click(self, dialog, ctrl_id):
        self.u.SendMessageW(self.item(dialog, ctrl_id), BM_CLICK, 0, 0)

    def enabled(self, dialog, ctrl_id):
        hwnd = self.item(dialog, ctrl_id)
        return bool(hwnd and self.u.IsWindowEnabled(hwnd))

    def set_text(self, dialog, ctrl_id, value):
        hwnd = self.item(dialog, ctrl_id)
        text = ctypes.c_wchar_p(value)
        self.u.SendMessageW(hwnd, WM_SETTEXT, 0, ctypes.cast(text, ctypes.c_void_p))
        # O formulário valida o campo ao receber EN_CHANGE
        self.u.SendMessageW(self.u.GetParent(hwnd), WM_COMMAND, (EN_CHANGE << 16) | ctrl_id, hwnd)

    def set_birth(self, dialog, day, month, year):
        """
        O campo de data é um controle próprio do MT5 que só aceita entrada real:
        clica em cada parte (dia, mês, ano) e digita. Supõe o ano por último
        (dd/mm/aaaa ou mm/dd/aaaa); use dia == mês para não depender da ordem.
        """
        hwnd = self.item(dialog, ID_BIRTH)
        kernel32 = ctypes.windll.kernel32
        target = self.u.GetWindowThreadProcessId(hwnd, None)
        me = kernel32.GetCurrentThreadId()
        self.u.AttachThreadInput(me, target, True)
        try:
            self.u.SetForegroundWindow(dialog)
            self.u.SetFocus(hwnd)
            rect = wt.RECT()
            self.u.GetWindowRect(hwnd, ctypes.byref(rect))
            height = rect.bottom - rect.top
            scale = height / 19.0          # 19 px = altura do campo a 96 DPI
            for offset, digits in ((40, "%04d" % year), (20, "%02d" % month), (6, "%02d" % day)):
                self.u.SetCursorPos(rect.left + int(offset * scale), rect.top + height // 2)
                time.sleep(0.2)
                self.u.mouse_event(MOUSE_DOWN, 0, 0, 0, 0)
                self.u.mouse_event(MOUSE_UP, 0, 0, 0, 0)
                time.sleep(0.3)
                for ch in digits:
                    self.u.keybd_event(ord(ch), 0, 0, 0)
                    self.u.keybd_event(ord(ch), 0, KEY_UP, 0)
                    time.sleep(0.15)
        finally:
            self.u.AttachThreadInput(me, target, False)


def _wait_until(condition, timeout):
    end = time.time() + timeout
    while time.time() < end:
        if condition():
            return True
        time.sleep(0.5)
    return False


def run(args, wizard=None, log=print):
    wizard = wizard or Wizard(_api())
    dialog = wizard.wait(args.wait)
    if not dialog:
        log("O assistente de abertura de conta não apareceu.")
        return EXIT_NO_WIZARD

    year, month, day = (int(p) for p in args.birth.split("-"))
    deadline = time.time() + args.timeout
    filled = False
    while time.time() < deadline:
        dialog = wizard.find()
        if not dialog:
            # O assistente fechou depois de "Finalizar"
            return EXIT_OK if filled else EXIT_NO_WIZARD
        if wizard.item(dialog, ID_FINISH):
            if wizard.enabled(dialog, ID_FINISH):
                log("Conta de demonstração aberta. Finalizando o assistente.")
                wizard.click(dialog, ID_FINISH)
                filled = True
            time.sleep(2)
        elif wizard.item(dialog, ID_FIRST) and wizard.item(dialog, ID_AGREE):
            if filled:                      # pedido enviado: aguardando o servidor
                time.sleep(2)
                continue
            log("Preenchendo o formulário da conta de demonstração.")
            wizard.set_text(dialog, ID_FIRST, args.first)
            wizard.set_text(dialog, ID_LAST, args.last)
            wizard.set_text(dialog, ID_EMAIL, args.email)
            wizard.set_text(dialog, ID_PHONE, args.phone)
            wizard.set_birth(dialog, day, month, year)
            agree = wizard.item(dialog, ID_AGREE)
            if wizard.u.SendMessageW(agree, BM_GETCHECK, 0, 0) != 1:
                wizard.click(dialog, ID_AGREE)
            if not _wait_until(lambda: wizard.enabled(dialog, ID_NEXT), 10):
                log("O MT5 recusou algum dado do formulário (campo em vermelho).")
                return EXIT_REJECTED
            wizard.click(dialog, ID_NEXT)
            filled = True
            time.sleep(3)
        elif wizard.item(dialog, ID_RADIO_DEMO):
            radio = wizard.item(dialog, ID_RADIO_DEMO)
            if wizard.u.SendMessageW(radio, BM_GETCHECK, 0, 0) != 1:
                wizard.click(dialog, ID_RADIO_DEMO)
            wizard.click(dialog, ID_NEXT)
            time.sleep(3)
        elif wizard.item(dialog, ID_COMPANY_LIST):
            wizard.click(dialog, ID_NEXT)   # a empresa padrão já vem selecionada
            time.sleep(3)
        else:
            time.sleep(1)
    log("Tempo esgotado esperando o assistente do MT5.")
    return EXIT_ERROR


def build_parser():
    p = argparse.ArgumentParser(description="Abre a conta demo no assistente do MetaTrader 5.")
    p.add_argument("--first", required=True)
    p.add_argument("--last", required=True)
    p.add_argument("--email", required=True)
    p.add_argument("--phone", required=True)
    p.add_argument("--birth", required=True, help="AAAA-MM-DD")
    p.add_argument("--wait", type=int, default=120, help="segundos esperando o assistente aparecer")
    p.add_argument("--timeout", type=int, default=180, help="segundos para concluir o assistente")
    return p


if __name__ == "__main__":
    sys.exit(run(build_parser().parse_args()))
