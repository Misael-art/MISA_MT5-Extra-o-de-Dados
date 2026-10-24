# Continuidade AIA-01 — SDK para agentes, 2026-10-23

O código está no GitHub, em duas PRs empilhadas e ainda sem merge em master:

- [AppPR207](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/207),
  branch `codex/ai-workflows`, implementação instalada
  `56226ce92148c84fe5875f2fd5e54645a35cee04`.
- [HPR75](https://github.com/Misael-art/projeto-indicadores-mt5/pull/75),
  branch `codex/ai-workflows`, `b730ed4a8df74f5f20eb6e289798d0232c370994`.
- Base de revisão do App: `codex/ai-workflows-base`,
  `1d5ce2296dfb48e884c0175dcca05aa7d4801885`, merge Git das frentes publicadas
  BD19c1/fe92ed13 e PR205/60e8870. Não sobrescreveu trabalho do pilar de dados.
  H75 depende de H74/e29a2c6. Não fazer cherry-pick de arquivos inteiros de bases
  antigas nem substituir risco/executor/contratos para compor a interface.

Leia AGENTS.md, PLANO, continuidade e [docs/agente.md](../agente.md) antes de editar.
A tarefa atual é SDK/CLI. Indicador estratégico, sua série/armazenamento, OMNI e
estratégia operacional estão registrados como próximos contratos, não implementados.

## Retomar no novo host

Clone ambos os repositórios e use os ramos acima (master ainda não contém esta
entrega). Se um ramo for removido, a referência da PR conserva a ponta:

```sh
git clone --branch codex/ai-workflows https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados.git app
git clone --branch codex/ai-workflows https://github.com/Misael-art/projeto-indicadores-mt5.git hadouken
# alternativa em clone já existente:
git fetch origin pull/207/head:codex/ai-workflows  # repositório App
# no repositório H: git fetch origin pull/75/head:codex/ai-workflows
```

Instale pelo procedimento AGENTS/README adequado ao SO. Recrie ambiente/MT5/Wine;
não migre prefixos inteiros. Reconfigure caminhos locais de ROBOTS/library_dirs e
credenciais somente em .env. O pin manual45e31481977d09f1 exige a closure H75
completa e Canvas padrão instalado. Outros perfis nativos conservam seus pins:
use os snapshots correspondentes; uma nova closure não recebe qualificação automática.

Use o complemento privado já preservado no SD conforme o
[roteiro de recuperação](RECUPERACAO_POS_FORMATACAO_20261021.md), se precisar do
histórico existente. AIA-01 não criou base de mercado que valha migrar; SQLite de QA
era temporário. Não inventar elegibilidade/proveniência de dados restaurados.
O adicional AIA-01 no SD contém somente continuidade e evidência visual compacta;
o source permanece no GitHub. Não contém credenciais, ambientes ou prefixos Wine.

```sh
python -m mt5_extracao.cli --json commands
python -m mt5_extracao.cli --json workflow methods
python -m mt5_extracao.cli --config config/config.ini --json workflow serve
```

O guia descreve JSONL, manifesto, SDK Python, confirmação PLAY, sessões múltiplas,
gráficos/estilos/indicadores/PNG, presets, dados/laboratório/backtest/deploy existentes,
recibos incertos e encerramento. Não usar mouse/teclado como substituto do contrato.
REPLAY screenshot usa explicitamente janela própria pela API do SO; não é só o chart.
No Tester, introspecção de inputs do indicador ficou indisponível e é declarada;
o recibo obrigatório do robô em PresetExecution continua sem relaxamento.

## O que foi conferido

- CLI real DEMO:40 pedidos PLAY e15 REPLAY, carteira inalterada e nenhuma ordem.
  Gráfico cinza/10curvas, padrão20/SMA/1..10 e custom14/EMA/.5..5 em M5/M15,
  EMA/ATR, capturas, remoção própria e recusas de gráfico alheio/fechar principal/
  campo de ordem. Readback43 inclui grupos e31 valores matemáticos conferidos no
  PLAY; no Tester `inputs_verified=false` explícito. PNG1280×800 conferido.
- Framework136 (baseline134), checker166, compilações MetaEditor/Wine0erros/0avisos.
- App100 dirigidos finais,237 regressões (sobrepostos), wheel/recurso MQL e import
  instalado; doctor20OK/1pandas-ta opcional/0falhas.
- Nova execução ampla:2952pass/251skip em2971.93s, sem DISPLAY, BLAS/OMP=1.
  Ela começou antes dos últimos refinamentos, que foram cobertos pelos100 testes
  dirigidos; CI do último HEAD tem conferência própria. Não somar as seleções.
- Execução ampla inicial2893pass/251skip/3timeouts60s; rechecagem1pass/2timeouts.
  `test_newest_run_first_and_period_start` também falhou por timeout na base Git
  congelada1d5ce229, antes do SDK. A causa de desempenho não foi corrigida nem os
  testes afrouxados. Guardar como limitação de carga/tempo, não afirmar regressão
  causada pelo SDK nem desaparecimento definitivo do problema.
- Prévia imutável código App56226ce/Hb730 ativada pelo atalho/menu; configuração,
  .env, dados, PID/token da ponte existente e referências aceitas preservados.
  Aplicativo foi iniciado pelo desktop e o CLI instalado consultado sem abrir Tk.
- 13 perfis próprios encerrados e conciliados antes da remoção;11.151.593.590 bytes
  lógicos removidos, logs/recibos preservados. Árvore temporária da base removida.
  Nenhum processo alheio sinalizado; WIP/venv/DB/estudos do proprietário preservados.

[Manifesto de fontes/evidência](../evidencias/AIA_01_SDK_AGENTES_20261023.json).
Logs brutos privados do host: logs/ai-workflows-20261023; após formatação, use o
manifesto versionado e o pequeno repasse SD. Não inferir recibo carregado/negociação
pela mera instalação/exportação/compilação ou por PNG.

## Pendências e próximo passo

1. Conferir CI do último HEAD e fazer integração serial com suas dependências;
   PRs/revisão/merge são estados diferentes de instalação experimental.
2. Investigar os timeouts herdados em snapshot/decodificação Pandas/runner sem
   alterar critérios financeiros, Budget ou desativar testes para passar.
3. Windows físico: instalação, PrintWindow, fontes/DPI, múltiplos monitores,
   EOF/Ctrl-C e recuperação. As simulações não substituem o host físico.
4. AIA-02: demais ferramentas de Estudo/indicadores de terceiros e play/pause do
   Tester. Concorrência entre controllers/instâncias e lifecycle de todos os EAs
   precisam de qualificação própria; o limite/idempotência atual é por instância.
5. Depois do SDK, consolidar contrato do OMNI/indicador estratégico/armazenamento e
   estratégia operacional. Nenhuma promoção financeira/live nesta tarefa.

O canal de desenvolvimento é experimental; canais app-current/hadouken-current/
Safety aceitos continuam preservados. Usar o retorno descrito em
[TESTAR_DESENVOLVIMENTO](../TESTAR_DESENVOLVIMENTO.md) se houver necessidade.
