"""Navegador real (Chrome headless): carga por file://, tema sem recarga e ausência de laço.

Pulado sem Chrome/Chromium. Só a cópia temporária do teste perde o CSP para a sonda
inline rodar; o documento gerado e o renderer não mudam.
"""
import re
import shutil
import subprocess
from datetime import datetime, timedelta, timezone

import pytest

from mt5_extracao.dashboard import document, generator, sections

BROWSERS = ("google-chrome-stable", "google-chrome", "chromium-browser", "microsoft-edge-stable")
PROBE = """<script>
setTimeout(() => {
  const b = document.querySelector('#theme-toggle'), seen = [];
  const bg = () => getComputedStyle(document.body).backgroundColor;
  seen.push(b.textContent + '|' + bg());
  for (let i = 0; i < 3; i++) { b.click(); seen.push(b.textContent + '|' + bg() + '|' + document.querySelectorAll('.chart svg').length); }
  document.documentElement.setAttribute('data-probe', seen.join(';'));
}, 800);
</script></html>"""


def browser():
    for name in BROWSERS:
        path = shutil.which(name)
        if path:
            try:
                subprocess.run([path, "--version"], capture_output=True, timeout=10, check=True)
                return path
            except (OSError, subprocess.SubprocessError):
                continue
    pytest.skip("Chrome/Chromium indisponível para o teste de navegador real")


def page(tmp_path):
    from tests.test_dashboard_hypothesis import record
    cut = datetime.now(timezone.utc)
    from tests.test_dashboard_document import fixture
    snapshot = fixture()                     # surpresa com série: gráfico ECharts cru, redesenhado ao trocar tema
    cut = datetime.fromisoformat(snapshot["as_of_utc"])
    data = dict(snapshot)
    data.pop("integrity")
    data["errors"] = []
    data["sections"].append(sections.hypothesis(data, record(available_at_utc=(cut - timedelta(days=1)).isoformat())))
    html, _ = document.render(generator.seal(data))
    html = re.sub(r'<meta http-equiv="Content-Security-Policy" content="[^"]*">', "", html, count=1)
    target = tmp_path / "pagina.html"
    target.write_text(html[:html.rindex("</html>")] + PROBE, encoding="utf-8")
    return target


def test_file_url_loads_and_theme_cycles_without_reload_or_hang(tmp_path):
    path = browser()
    target = page(tmp_path)
    result = subprocess.run([path, "--headless=new", "--disable-gpu", "--no-sandbox", "--window-size=940,700",
                             f"--user-data-dir={tmp_path / 'perfil'}", "--virtual-time-budget=5000", "--dump-dom",
                             target.as_uri()], capture_output=True, text=True, timeout=60)
    assert 'data-ready="true"' in result.stdout, result.stderr[-500:]
    probe = re.search(r'data-probe="([^"]*)"', result.stdout)
    assert probe, "sonda não concluiu: possível laço no redesenho de tema"
    steps = probe.group(1).split(";")
    assert [s.split("|")[0] for s in steps] == ["Tema: automático", "Tema: claro", "Tema: escuro", "Tema: automático"]
    assert steps[1].split("|")[1] != steps[2].split("|")[1]                    # cores mudam sem recarregar
    assert len({s.split("|")[2] for s in steps[1:]}) == 1                      # gráficos não duplicam nem somem
