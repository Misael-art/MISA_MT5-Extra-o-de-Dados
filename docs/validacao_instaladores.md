# Validação dos instaladores com o MT5 real (T0.3 e T2.6)

Registro das execuções manuais que o CI não cobre (download e instalação do MT5, Wine,
ponte com o MT5 real). Roteiro em [PLANO_DE_TRABALHO.md](PLANO_DE_TRABALHO.md), seções T0.3 e T2.6.

## T0.3 — Instaladores

| Ambiente | Data | Comando | Resultado |
|---|---|---|---|
| BigLinux/Manjaro (família Arch), KDE Wayland, Wine 11.17, máquina de uso (não é VM limpa) | 2026-10-12 | `./install.sh --yes` | ✅ com as correções abaixo. `doctor`: 20 OK, 1 aviso (`pandas-ta` opcional), 0 falhas; ponte ATIVA; MetaTrader5 5.0.6231, terminal build 6257 |
| Reexecução (mesma máquina) | 2026-10-12 | `./install.sh --yes` (2ª e 3ª vez) | ✅ etapas puladas, `config.ini` preservado |
| Windows 11 (usuário comum) | — | `install.bat` | ⏳ pendente |
| Windows 10 sem winget | — | `install.bat` | ⏳ pendente |
| Windows com MT5 de corretora | — | `install.bat` | ⏳ pendente |
| Ubuntu 24.04 desktop | — | `./install.sh` | ⏳ pendente |
| Ubuntu 22.04 servidor (sem X) | — | `./install.sh --yes` | ⏳ pendente |
| Debian 12 | — | `./install.sh` | ⏳ pendente |
| Fedora 40 | — | `./install.sh` | ⏳ pendente |

### Falhas encontradas e corrigidas (Linux, 2026-10-12)

1. **Download do MT5 dava 404.** A URL `.../metaquotes.software.corp/metatrader5/mt5setup.exe` não existe;
   a correta é `.../metaquotes.software.corp/mt5/mt5setup.exe`. Corrigido em `linux.sh` e `windows.ps1`
   (no Windows só a URL foi conferida; o instalador não foi executado).
2. **Pacotes do sistema sem terminal.** Sem TTY o `sudo` não tinha como pedir a senha. O instalador agora
   só pede permissão se faltar pacote (Arch) e, sem terminal, usa a janela de senha do sistema (`pkexec`).
   Também deixou de usar `pacman -Sy` (atualização parcial).
3. **Conexão: 5 tentativas erradas antes da certa.** No Wine, `initialize(path=<pasta>)` falha com
   `IPC initialize failed, Process create failed`; só o caminho do `terminal64.exe` funciona. Pela ponte,
   essa estratégia agora é a primeira.
4. **Terminal novo não tem conta.** O MT5 build 6257 abre um assistente em vez de criar a conta demo, e
   `initialize` fica em `IPC timeout`. Novo comando `python -m mt5_extracao.bootstrap demo-account`
   (chamado pelo instalador) abre a conta de demonstração gratuita; o `doctor` ganhou o item "Conta no MT5".
   Testado de ponta a ponta partindo de um terminal sem conta: 20 s.
5. **Reexecutar o instalador com o MT5 aberto travava** em `wineserver -w` (que espera todos os processos
   do Wine). Agora não espera quando o terminal ou a ponte estão em execução.

6. **Preços de outro ativo gravados como `WIN$N` (integridade).** `WIN$N` não existe na `MetaQuotes-Demo`,
   mas a autocorreção de nomes removia o `$` e achava `WINN` (ação dos EUA, ~US$ 35): a extração gravava
   esses preços em `win_n_1_minuto` e informava "Sucesso". A variação que remove o `$` foi retirada; agora
   a extração falha e a CLI explica que o servidor não tem o símbolo e o que fazer.
7. **Credenciais do `.env` nunca eram usadas.** O parâmetro `auto_login` do conector não fazia nada. Agora,
   com `MT5_LOGIN`/`MT5_PASSWORD`/`MT5_SERVER` preenchidos, o app entra nessa conta sozinho; se o login
   falhar, a conexão falha (não segue na conta errada). Validado só com o MT5 falso: falta testar com a
   conta de uma corretora.

### Extração real (conta MetaQuotes-Demo, pela ponte)

| Verificação | Resultado |
|---|---|
| `mt5x symbols --group "*EURUSD*"` | ✅ (12.306 símbolos na conta) |
| `mt5x extract --symbols EURUSD --tf M1 --from 2026-10-05 --to 2026-10-09` | ✅ 7.200 barras em 14 s |
| Reextrair o mesmo período | ✅ continua 7.200 linhas, sem duplicatas |
| `mt5x update --symbols EURUSD --tf M1` | ✅ base avançou até o minuto atual (7.581 linhas) |

Não validado: símbolos da B3 (`WIN$N`, `WDO$N`, `PETR4`...). Conferido no MT5 real que a `MetaQuotes-Demo`
não tem nenhum deles; é preciso a conta de uma corretora brasileira (veja docs/instalacao.md).

## T2.6 — Paridade Windows × Linux

Script: `scripts/parity_check.py` (requer `pyarrow`).

| Lado | Data | Comando | Resultado |
|---|---|---|---|
| Linux (ponte) | 2026-10-12 | `parity_check.py extract --symbol EURUSD --date 2026-10-09 --out linux.parquet` | ✅ 1.440 barras |
| Windows | — | mesmo comando, `--out windows.parquet`, mesma conta/corretora | ⏳ pendente |
| Comparação | — | `parity_check.py compare linux.parquet windows.parquet` | ⏳ pendente |
