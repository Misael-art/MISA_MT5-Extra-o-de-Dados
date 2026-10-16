# ORG05 — continuidade da integração visual

Escopo registrado antes da implementação documental. Reconciliar o painel de
status e a continuidade com os PRs visuais efetivamente integrados, respectivos
SHAs, revisão técnica, checks e operação no host. Preservar registros históricos
e separar o estado atual das medições anteriores. Nenhuma mudança de runtime,
configuração, schema ou dados nesta tarefa.

Aceite: confirmar estado remoto de cada PR; citar testes e smokes sem esconder
falhas, tentativas não aceitas ou limitações; registrar preservação e diagnóstico;
manter pendências da UI, pesquisa e contas explícitas. Revisão técnica própria
será identificada como tal, sem alegação de revisão independente. PRs #79/#80,
releases e operações em conta continuam separados da autorização de integração UI.

Ambiente próprio instalado; baseline a13c2c6:856 aprovados/9 ignorados em833,45 s,
saída0, antes das alterações. A base foi reconciliada com a principal após a
baseline, preservando as integrações existentes.

## Estado verificado da integração

Snapshot da principal `eed5f34`, em 2026-10-16. Cada PR abaixo foi revisado
tecnicamente pelo agente executor, teve os checks conferidos no head final,
foi integrado separadamente e confirmado no remoto. Não houve revisão por
outro agente. CI foi usado como evidência adicional à leitura dos diffs.

| PR | Tarefa | Merge | Operação no host após integrar |
|---|---|---|---|
| [#81](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/81) | T17.7: referência/população temporal | `91744ff` | 263 operações reais identificadas por Saída |
| [#82](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/82) | T17.9: filtros e rolagem | `fb7dad7` | 144 DPI, controles acessíveis em 1280/1920 |
| [#83](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/83) | T17.12a: ausência, zero e perda | `b739fa2` | 15 séries reais com valores exibidos iguais às métricas |
| [#84](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/84) | T17.12: atenção em Horários/Minha base | `0bcbbb0` | Dois temas, contraste ≥4,5 e teclado na matriz de 37 linhas |
| [#85](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/85) | T17.8: timeline sem suavização | `d52fe51` | Pontos exatos; demonstração sintética identificada |
| [#86](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/86) | T17.15: sincronização da conexão nos testes | `bc56b41` | Dois cenários de recuperação aprovados na principal |
| [#87](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/87) | T17.14: altura útil da análise | `a13c2c6` | Altura/rolagem verificadas em 144 DPI após resize |
| [#88](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/88) | T17.17: detalhe legível | `3471dbe` | Detalhe acima do mínimo, quatro larguras, 144 DPI |
| [#89](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/89) | T17.16: contexto do Backtest | `b9e54c9` | Execução histórica, atualização e resultado anterior verificados |
| [#93](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/93) | T17.20: sincronização da restauração nos testes | `eed5f34` | Dois cenários falsos aprovados na principal, 8,75 s |

Os smokes executaram o módulo da instalação principal e sua venv no host,
com cópia consistente dos dados reais e configuração isolada que desativa a
abertura do terminal. Configuração e bases do usuário foram preservadas.
As sessões de inspeção foram encerradas com saída 0. O diagnóstico separado
da principal consultou a ponte em modo somente leitura: 21 OK, um aviso e
nenhuma falha. Isso não certifica todas as combinações de ambiente ou uso.

As suítes locais e seus SHAs estão registrados na continuidade de cada tarefa.
Os checks finais de merge foram conferidos nos respectivos PRs. Validação
manual com MT5 no Windows continua pendência própria; os jobs Windows do CI
usam provedores falsos e instaladores sem MT5.

## Implementações ainda na cadeia de integração

- [#90](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/90), T17.13:
  retorno de mercado distingue alta, queda, zero e ausência. Suíte reconciliada
  `a893f54`: 866 aprovados/9 ignorados, saída 0. O job Windows falhou duas vezes
  no teste antigo de restauração, com feedback ainda pendente após 0,6 s.
  A correção foi integrada no #93 e recebida no `0a23687`; checks novos em curso. Não se repetirá o SHA antigo indefinidamente.
- [#91](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/91), T17.10:
  zeradas separadas de perdas. Suíte `85b81ef`: 875 aprovados/9 ignorados,
  saída 0. BBRSI real: 4.071 ganhos, 6.366 perdas, 340 zeradas; o gráfico anterior
  mostrava 6.706 perdas. Ledger com hash idêntico antes/depois; SQL independente
  confirma as 48 faixas exportadas. Recebeu o pai corrigido em `fd94893`; CI novo em curso. Integração depende do #90.
- [#92](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/92), T17.11:
  grade horária explícita. Suíte `3b4e6d1`: 886 aprovados/9 ignorados, saída 0.
  Dados reais: 263 operações, 48 posições, 37 medidas e quatro segmentos;
  valores observados preservados. Recebeu o pai corrigido em `06c92d3`; CI novo em curso. Integração depende do #91.

A suíte documental c3d3785 terminou com 860 aprovados/9 ignorados em
591,95 s, saída 0. A execução anterior foi interrompida pelo reboot do host
às 19:16:57 UTC e não foi contada como aprovação. O #93 tem suíte própria
861/9, CI integral aprovado e as oito verificações originais preservadas;
nenhum arquivo real de terminal foi restaurado pelos testes.

Após o reboot, o diagnóstico encontrou a ponte parada. Foi iniciado apenas
o servidor RPC de leitura, cujo startup importa o SDK sem chamar initialize,
login ou funções de negociação. O doctor seguinte confirmou 21 OK/1 aviso/
0 falhas. Configuração do usuário e o hash do diff T17.6 foram reconferidos.

O novo pedido de detalhes recolhíveis com ícone de informação é T17.21,
separado da geometria da Análise T17.18. Os anexos temporários não estão
disponíveis; nenhuma imagem ausente foi alegada como inspecionada. Detalhes,
critérios longos e explicações secundárias devem liberar área sem esconder
avisos sintéticos, custos incompletos, denominadores ou contexto primário.
Ambos os recortes ainda aguardam implementação neste snapshot.

## Falhas e limitações preservadas

O #85 tinha uma falha local intermitente de conexão, também presente na base;
o #86 corrigiu a sincronização e teve suíte e checks aprovados. O primeiro job
Windows do #86 teve timeout em fixture SQL inalterada; uma repetição no mesmo
SHA passou, e a tentativa anterior permanece registrada.

No #88, Windows revelou quatro pixels de espaçamento duplicado. A correção
preservou as exigências de largura e passou no Linux/Windows. Uma repetição
local perdeu o Xvfb e ignorou 74 testes gráficos extras: esse resultado não
foi aceito. O roteiro foi corrigido e o mesmo código repetido com 858 aprovações
/9 ignorados. Nenhum teste foi desativado para obter aprovação.

A inspeção visual do relatório HTML permanece pendente: a navegação fileURL
foi bloqueada pelo navegador interno. O arquivo foi gerado pela CLI com saída
0; isso não substitui a conferência renderizada nem autoriza contornar a política.

Permanecem recortes para clipping da Análise em DPI alto, aplicação do princípio
visual aos resultados restantes, aviso de origem parcial, densidade de alguns
rótulos e proveniência dos exports. Relações mantém aviso explícito de dados
sintéticos enquanto DTO/produtor/serviço reais da F18 não estiverem ligados.

## Pesquisa e autorização operacional

O smoke instalado do #89 repetiu EMAATR em EURUSD H1, no intervalo já exposto:
129 operações, PF 0,637667 e resultado de −23.984,94 na unidade do teste,
iguais à execução anterior. Contagem e soma do ledger reconciliadas. Ao mudar
o período, o resultado passou de seleção atual para anterior. Trata-se de
verificação funcional em histórico, sem alegação OOS ou de meta alcançada.

A campanha PF líquido ≥1,9 aguarda aceite da UI e contratos de dados/custos.
A referência de pesquisa exige reserva OOS intocada, ao menos 200 operações,
60 sessões, 30 perdas, IC 95% publicado com limite inferior de PF acima de 1,
expectativa líquida positiva e limites de risco pré-registrados. Dados já
expostos não serão apresentados como nova reserva independente.

A soma descritiva de vários runs não representa uma carteira executável única;
risco e custos precisam de proveniência e capital coerentes. Comissões/taxas
incompletas e PF sem perdas exigem tratamento próprio antes de aprovação.
Não houve operação em conta, publicação de pacote ou release. PRs #79/#80
continuam separados desta autorização de integração visual.
