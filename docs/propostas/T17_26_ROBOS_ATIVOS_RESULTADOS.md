# T17.26 — Ativos testados e sinal do resultado nos cards

Pedido de 2026-10-17: mostrar quantos ativos cada Expert/robô testou, em quantos teve resultado positivo, e usar ícones distintos para lucro e prejuízo. Recorte separado da recuperação do Laboratório, PR #102.

## Política registrada antes da implementação

- Contar cada símbolo não vazio uma vez. Repetições, presets e timeframes não multiplicam o ativo.
- Classificar pelo último backtest daquele ativo, segundo `created_at` e `run_id`, no mesmo contrato de ordenação do catálogo. O critério fica visível no card. Não escolher retrospectivamente o melhor teste.
- Mais de zero operações e `net_profit` finito: positivo, negativo ou zerado pelo saldo gravado. Sem operações ou saldo disponível é **sem resultado**, não prejuízo. Valores inválidos e booleanos não são números financeiros.
- Excluir execuções auxiliares de bateria e paridade, como já faz o catálogo. A validação OOS continua no headline existente, com seu veredito; esta contagem descreve backtests e não aprova estratégia.
- Cálculo puro no catálogo. Widgets só apresentam contagens; não somar moedas, não recalcular PF, risco ou rentabilidade.
- Ícones de direção desenhados pelo sistema de ícones existente (Pillow), sem depender dos glifos da fonte. Texto acompanha os símbolos; azul favorável, negativo discreto e ausência sem destaque. Preservar clique, seleção, fontes e rolagem.

## Ordem e verificação

1. Instalador/dev em ambiente exclusivo Python 3.11. A branch parte do SHA `0c5f74b`, exatamente o baseline geral já executado para este pedido (**967 aprovados, 9 pulos existentes**, saída 0); todas as versões de pacotes foram comparadas e são idênticas. Foco inicial do catálogo e três fluxos reais de widgets: **24 aprovados**, repetição com saída 0. A primeira rodada também passou os 24 testes, mas seu Xvfb não encerrou no prazo; falha de limpeza mantida separadamente.
2. Implementar propriedade de resumo e testes para ativos repetidos, inversão do sinal no teste recente, zero, ausência, sem operações, valores inválidos e sem alteração dos registros recebidos.
3. Implementar ícones e apresentação nos cards; validar textos/contagens, clique e troca de tema, nomes/dígitos longos e geometria.
4. Comparar com consulta SQL independente da base real; inspecionar ambos os temas e resoluções usando cópia consistente, depois a instalação conectada à DEMO.
5. Reconciliar esta branch com o master depois da integração do Laboratório, preservando ambas as linhas do plano e da continuidade. Executar suíte e CI na composição final, revisão própria, merge normal e smoke instalado.

## Aceite

- [x] Ativos distintos e sinal do último teste corretos; política legível.
- [x] Sem resultado difere de zero e perda; não altera headline, estágio ou aprovação.
- [x] Ícones e textos acompanham tema/resize, com ausência discreta.
- [x] Consulta SQL independente e captura real confirmam as contagens na cópia consistente; falta repetir o lançador instalado após integração.
- [ ] Suíte, doctor, CI exato, revisão e operação instalada documentados no PR.

Não muda execução, exposição, configuração ou dados históricos. Não envia ordens. Windows físico com MT5 continua pendente, distinto do CI Windows.

## Observação → correção → evidência

| Fluxo/ação | Esperado e observado | Gravidade/tarefa | Correção e verificação |
|---|---|---|---|
| Robôs: inspecionar abrangência dos testes | Antes só último teste e quantidade de execuções; ativos e saldo por ativo ausentes | Legibilidade/análise; T17.26 | Resumo puro deduplica símbolos pelo teste mais recente; 13 casos de núcleo aprovados, sem mutar registros ou confundir validação |
| Robôs: diferenciar ganho, perda e ausência | Falta de sinal por ativo dificultava analisar abrangência | Legibilidade; T17.26 | Setas vetoriais rasterizadas pelo sistema existente, textos e tons discretos; regressão verifica tema, resize, ícone vivo e seleção do detalhe |
| Base real: comparar com SQL | 15 robôs, com históricos repetidos e testes sem operações | Verificação de dados; T17.26 | Consulta independente com ROW_NUMBER por robô/símbolo coincide exatamente; cópia contém os cinco relatórios nativos preservados recuperados na T17.25, não campanha nova |

Baseline: 967/9 no SHA inicial idêntico; ambiente preparado e pacotes comparados. Foco: **37 aprovados** e uma regressão de UI que passou após corrigir o preparo do próprio teste (faltavam campos obrigatórios de `save_run`); a primeira falha permanece no log, sem ajuste no produto para contorná-la. Os 24 focados iniciais também passaram. Cópia consistente da base real: **12 casos aprovados**, dois temas × duas resoluções × três robôs (MQL5 com ganho/perda/ausência, Python com dez ativos, Guardião sem resultado), encerramento normal e comparação SQL exata dos 15 robôs. Inspeção visual confirmou textos, setas e ausência sem destaque.

Evidências privadas fora do Git: `t1726-baseline-reuse.json`, `t1726-baseline-focused-repeat.*`, `t1726-focused-v1.*`, `t1726-new-ui-fixed.*`, `t1726-ui-realdata-v1/visual-acceptance.json` e `independent-SQL-counts.json`. Revisão própria, suíte e CI no SHA final composto, confirmação remota do merge e operação instalada devem constar no PR; não substituídos pelas capturas privadas. Não houve mudança de `.sh`, dependência, configuração, risco ou schema. Não foram enviadas ordens.


## Composição e verificação nativa antecipada

O Laboratório foi integrado separadamente no PR #102, merge `2ec2688`, após 984 aprovados/9 pulos existentes, seis jobs de CI no SHA4cfa447 e revisão dos cinco apontamentos externos (quatro corrigidos, recuperação por Atualizar documentada no quinto). A instalação foi atualizada por fast-forward e seu runtime conferido com o testado. A composição desta tarefa preserva ambas as linhas do plano, continuidade e checklist; os conflitos foram exclusivamente documentais. Os três arquivos de runtime dos cards mantêm os hashes testados em4704180. Suíte e CI serão repetidos no SHA composto desta branch.

Antecipação com aplicação real e SDK nativo conectado à mesma conta DEMO, usando cópia consistente:12 casos aprovados, SQL exato para15 robôs, encerramento0, mesma instância do terminal e0posições/0ordens. O primeiro verificador auxiliar apontava para um banco inexistente; a falha permanece preservada, e a repetição corrigida é a evidência válida. Não substitui a verificação de ./run.sh e da base instalada após o merge.

CI preliminar4704180:seis jobs aprovados. Sourcery recusou revisão adicional por cota do serviço, portanto SKIPPED não foi tratado como revisão aprovada. Revisão própria (não independente) registrada por patch/hash; nova revisão própria da composição e checks exatos permanecem gates do PR.
