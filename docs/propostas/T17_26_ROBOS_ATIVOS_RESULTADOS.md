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

- [ ] Ativos distintos e sinal do último teste corretos; política legível.
- [ ] Sem resultado difere de zero e perda; não altera headline, estágio ou aprovação.
- [ ] Ícones e textos acompanham tema/resize, com ausência discreta.
- [ ] Consulta SQL independente e captura real confirmam as contagens.
- [ ] Suíte, doctor, CI exato, revisão e operação instalada documentados no PR.

Não muda execução, exposição, configuração ou dados históricos. Não envia ordens. Windows físico com MT5 continua pendente, distinto do CI Windows.
