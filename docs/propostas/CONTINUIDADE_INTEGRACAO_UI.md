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

Snapshot da principal `854af32`, em 2026-10-16. Cada PR abaixo foi revisado
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
| [#90](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/90) | T17.13: retorno de mercado | `7a13d57` | EURUSD H1 real: −0,898355% preservado, estados nos dois temas |
| [#91](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/91) | T17.10: ganhos/perdas/zeradas | `dda98b8` | BBRSI: 4.071 ganhos, 6.366 perdas e 340 zeradas; SQL/ledger reconciliados |
| [#92](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/92) | T17.11: grade horária/lacunas | `24faa35` | 48 posições, 37 medidas, 11 lacunas, quatro segmentos; valores exatos |
| [#95](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/95) | T17.21: detalhes recolhíveis | `8bacadf` | Seis detalhes fechados, ícone visível, mouse/teclado, seleção/aquecimento e dados preservados |
| [#97](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/97) | T13.9: PF definido sem teto artificial | `aacd05f` | 276 runs, 14 vazios indefinidos; PF real 0,87 preservado; projeção não altera SQLite |
| [#96](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/96) | T17.18: Análise/cabeçalho responsivos | `323decb` | EURUSD H1: 1.537 barras, dois temas e DPI, 1920/1280/900, altura limitada/hover/rolagem |
| [#98](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/98) | T17.22: abertura offline explícita | `7925b8b` | **Lançador principal real** com configuração/base existentes, Minha base/Análise/Relações, detalhes por mouse/Escape, Sair normal0 |
| [#99](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/99) | T17.23: agregação e comprovação | `e8988d9` | **Lançador principal real**: 10.445 BBRSI/31 runs, campos ausentes/queda com capital, ícone mouse/Escape, números reconciliados com a base real e Sair0 |
| [#100](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/100) | T13.10: estudoPF19 limitado | `854af32` | Oito casos reais modelados, todos negativos; origem/protocolo inalterados; CLI/help/import instalados sem SDK/Tk e sem nona tentativa |


Os smokes executaram o módulo da instalação principal e sua venv no host,
com cópia consistente dos dados reais e configuração isolada que desativa a
abertura do terminal. Configuração e bases do usuário foram preservadas.
As sessões de inspeção foram encerradas com saída 0. Além dos perfis, o #98 foi aberto pelo `./run.sh --offline` na principal, com backup consistente prévio da base, configuração real e display próprio. O hash da configuração e a lista de terminais permaneceram idênticos (nenhum terminal aberto). Minha base mostrou 15 séries; Análise abriu EURUSD H1 por seleção e botão reais; Relações conservou o aviso sintético e o detalhe expandiu/recolheu. O diagnóstico separado
da principal consultou a ponte em modo somente leitura: 21 OK, um aviso e
nenhuma falha. Isso não certifica todas as combinações de ambiente ou uso.

As suítes locais e seus SHAs estão registrados na continuidade de cada tarefa.
Os checks finais de merge foram conferidos nos respectivos PRs. Validação
manual com MT5 no Windows continua pendência própria; os jobs Windows do CI
usam provedores falsos e instaladores sem MT5.

## Integração e evidências recentes

Os PRs #90/#91/#92 receberam a correção test-only #93 e foram integrados com os checks finais aprovados. No #90, as duas falhas anteriores no teste antigo de restauração com espera fixa de 0,6 s foram preservadas; o #93 sincroniza pelo callback e pela atualização observáveis. No #91 a contagem anterior de 6.706 perdas incluía 340 zeradas; a correção preservou o ledger e reconciliou 48 faixas com SQL independente.

Suítes finais: #95 890/9 em636,45s; #97 931/9 em729,91s no d921540; #96 939/9 em763,62s no b0b68f9; #98 947/9 em830,26s no d012758. Todos com saída0 e checks finais aprovados, inclusive Windows. No #96, dois heads falharam em Windows por alocação excessiva do canvas; a solução final mede viewport/fonte e limita o contêiner. O teste novo também reproduziu a falha no fonte antigo, sem afrouxar limites. Uma suíte intermediária do #98 foi interrompida ao se observar rótulo de conta e extração habilitada no offline; o head final corrige os dois.

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
Ambos foram implementados, testados, integrados (#95/#96) e operados no módulo instalado. O ícone Unicode não aparecia na fonte do host; o #95 desenha o símbolo na infraestrutura existente, sem dependência nova. Não houve uso de imagens ausentes.

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

O clipping da Análise foi resolvido no #96. O aviso parcial incorretamente atribuído a MQL5 e a contextualização da agregação/queda foram integrados no #99: suíte956/9 em902,53s, saída0; CI final8777a49 aprovado incluindo Windows; revisão técnica própria e lançador instalado conferidos. O perfil privado de QA tinha10.777 operações/32runs e queda3.163,7%; a base original do usuário tem10.445/31 e queda3.235,1%. A diferença é uma execução histórica adicional na cópia de QA, não divergência de cálculo. O roteiro instalado corrigiu a expectativa ao consultar a população original: PF0,30 e demais números iguais ao núcleo. Contexto essencial permanece visível, detalhes recolhidos por padrão e F16 oculto em Relações sintéticas. Restam expansão da hierarquia visual a superfícies secundárias, densidade de alguns rótulos e metadados não disponíveis nos registros antigos. Relações mantém aviso explícito de dados
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

## Escopo do próximo ciclo

O aceite crítico da UI foi registrado depois do lançador instalado #99, com ressalvas explícitas: F18 sintética identificada, custos/moeda antigos desconhecidos, WindowsMT5 manual e refinamentos secundários pendentes. O estudo T13.10/PR100 então executou as oito tentativas pré-registradas, em15,87s de comando: zero candidatos, todos os resultados modelados negativos, maiorPF0,875849 (DonchianM15,669operações). Não é OOS independente nem PF líquido certificado. Relatório, oito livros/logs/hashes e próximo experimento estão no PR próprio; implementação e resultados foram integrados no #100 após963/9 em764,57s, saída0, revisão própria e CIexato271b integral aprovado incluindoWindows. A CLI instalada tem o mesmo hash do código que executou os oito casos; help/import passaram sem carregar SDK/Tk ou executar nova tentativa. Nenhuma ampliação da busca foi feita após o fracasso da meta.

Revisão somente leitura do ecossistema reconfirmou 11 fontes Safety idênticos entre origem e cópia 1.5.0. O exportador MQL5 ainda emite cinco colunas (T16.2b pendente). A chamada legada direta no plugin de risco, linha503, permanece registrada; a busca não encontrou include desse plugin nos MQ5/MQH locais, mas isso não o exclui da regra nem prova ausência de consumidores externos. Há ainda chamadas CTrade legadas fora do executor. Não se alterou proteção nem publicou release; aceites operacionais/demonstração/netting/Windows da Safety continuam pendentes.


Na abertura principal #99, o hash da configuração permaneceu igual e nenhum terminal foi aberto. Comparação lógica contra backup imutável anterior ao #98 confirmou dados brutos, runs, ledger e especificações preservados. Só o cache derivado ganhou369features e3runs de cache ao abrir BBRSI; não se apagou/reescreveu dado primário. A verificação principal anterior #98 havia mantido todas as27tabelas idênticas.


T16.2b está implementada no [PR12 do HADOUKEN](https://github.com/Misael-art/projeto-indicadores-mt5/pull/12), **draft e sem integração**: recorte sobre masterfc4d531, preservando a branch original480d310/T12.3; nove fontes dessa base compilaram sem erros/avisos. Componente de fonte idêntico passou17checksMQL5 e CSVsintético→ledger, mas cinco probes do Tester isolado falharam191 antes de carregar a estratégia. Isso não prova HistoryAPI/CSVreal nem paridade dos seis. As tentativas com cache demo tinham egressobloqueado; o cache temporário sensível foi removido, origem preservada e nenhuma conta recebeu ordem/autenticação. Não forçar integração/release com esse aceite faltante.
