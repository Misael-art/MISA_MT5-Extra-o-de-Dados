"""Explicação de por que o MetaTrader 5 não conectou (o caso real: o Wi-Fi do host caiu e o programa só dizia que
seis estratégias falharam)."""
from mt5_extracao import connection_help as ch


class _Socket:
    def close(self):
        pass


def test_internet_check_tries_every_address_and_only_opens_a_connection():
    tried = []

    def connect(address, timeout):
        tried.append((address, timeout))
        if address[0] != "9.9.9.9":
            raise OSError("rede inalcançável")
        return _Socket()
    assert ch.internet_ok(timeout=1.0, connect=connect)
    assert [a for a, _ in tried] == list(ch.PROBES) and all(t == 1.0 for _, t in tried)

    def down(address, timeout):
        raise TimeoutError("sem resposta")
    assert not ch.internet_ok(connect=down)
    assert ch.internet_ok(connect=lambda address, timeout: _Socket())


def test_diagnosis_puts_the_network_first():
    assert ch.diagnose(terminal_running=True, online=False) == ch.NO_INTERNET
    assert ch.diagnose(terminal_running=False, online=False) == ch.NO_INTERNET
    assert ch.diagnose(terminal_running=True, online=True) == ch.NOT_LOGGED
    assert ch.diagnose(terminal_running=False, online=True) == ch.NOT_OPEN


def test_each_explanation_says_what_happened_and_what_to_do():
    assert "sem internet" in ch.explain(ch.NO_INTERNET) and "Verificar conexão" in ch.explain(ch.NO_INTERNET)
    assert "não entrou na conta" in ch.explain(ch.NOT_LOGGED) and "Arquivo > Login" in ch.explain(ch.NOT_LOGGED)
    assert "mt5-bridge.sh start" in ch.explain(ch.NOT_OPEN) and "mt5-bridge" not in ch.explain(ch.NOT_OPEN, windows=True)
    assert "Diagnóstico do ambiente" in ch.explain(ch.NOT_OPEN, windows=True)


def test_readiness_wait_has_deadline_and_cooperative_cancellation():
    now=[0.];checks=[]
    def sleep(seconds):now[0]+=seconds
    def ready():checks.append(now[0]);return now[0]>=.5
    assert ch.wait_ready(ready,1,clock=lambda:now[0],sleep=sleep)
    assert now[0]==.5 and len(checks)==3
    assert not ch.wait_ready(lambda:False,.5,clock=lambda:now[0],sleep=sleep)
    assert now[0]==1
    assert not ch.wait_ready(lambda:True,1,cancelled=lambda:True,clock=lambda:now[0],sleep=sleep)


def test_backend_failure_and_uncertain_opening_do_not_claim_terminal_did_not_open():
    assert ch.diagnose(True,True,backend_available=False)==ch.BACKEND_UNAVAILABLE
    assert ch.diagnose(False,False,backend_available=False)==ch.NO_INTERNET
    for reason in (ch.NOT_OPEN,ch.BACKEND_UNAVAILABLE,ch.CONNECTION_ERROR):
        assert 'O MetaTrader 5 não abriu.' not in ch.explain(reason)
    assert 'ponte' in ch.explain(ch.BACKEND_UNAVAILABLE) and 'ponte' not in ch.explain(ch.BACKEND_UNAVAILABLE,windows=True)
