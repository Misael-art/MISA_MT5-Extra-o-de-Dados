# G19.16 — alvo probabilístico no painel manual

Painel HADOUKEN7.10 reconhecido pela closure exata `352de5268dcd32ae`, mantendo os
seis pins anteriores. A integração App acrescenta somente esse pin; não relaxa
reconhecimento por key/kind/fonte, contas, controles de risco ou Experts automáticos.
O instalador/launcher PLAY/REPLAY e a correção de monitores permanecem iguais.

No Expert, **Visual → Alvo probabilístico** liga o cartão de forma independente
do Candle Time. Há três tamanhos junto ao relógio e leitura no painel se não
couber. Seta cinza estável, verde para melhora e vermelha para piora; intensidade
acompanha a variação, não confiança estatística. Com posição: variação de
Pr(TP primeiro)−Pr(SL primeiro). Sem posição: índice de pressão de alta.

Os11 novos inputs do grupo7 são revisados nas propriedades do Expert: conjunto
10/20/30, período/média, peso por proximidade, horizonte, histórico, amostra mínima,
gestão, parcial/final e fração. Padrões10/20/SMA, horizonte12 barras fechadas,
histórico3000, mínimo30. `InpProbManage=false`; habilitação explícita só emDEMO.
Parcial80%, saída90%, fração50%. Limiar100% é recusado.

O modelo reutiliza a catraca Super ATR e separa pressão de probabilidade.
Probabilidade é frequência condicional de primeira passagem ao SL/TP **já
existentes**, com blocos históricos maduros não sobrepostos, pressão semelhante,
distâncias em ATR e suavização Dirichlet. São quatro categorias: TP, SL, timeout
e ambos na mesma barra (ambíguo). Sem SL/TP/amostra/dados não há gestão. Não cria
novos níveis ou entradas. O menor ticket próprio é mostrado; a política avalia
cada posição magic/símbolo. Nenhum comportamento automático é ligado pelo botão
visual. O cálculo não lê barra em formação.

A política chama somente o executor existente. Grava intenção antes do pedido,
observa redução de volume e não confunde bool com fill. Mantém SL, prioridades
de risco e fim de horário. Mínimo/step/volume residual são respeitados; vários
tickets mínimos permitem fechar um como parcial de cesta. Não certifica async,
concorrência, todas as retentativas internas, autoria de redução externa ou
restauração de Global Variables após formatação.

[Contrato matemático e continuidade no HADOUKEN](https://github.com/Misael-art/projeto-indicadores-mt5/blob/codex/alvo-probabilistico/Documentation/HADOUKEN_2/propostas/G19_16_ALVO_PROBABILISTICO.md).
A nova closure precisa acompanhar o fonte. Não há nova distribuição Safety;
seus consumidores/pins anteriores não ganham qualificação automática.

Validação: testes reais MQL em shim de plataforma e QA visual nativo sintético,
sem ordens. Não demonstram calibração financeira ou parciais contra broker.
Dados anteriormente explorados não viram holdout intocado. Calibração/OOS,
resultado após custos, Tester/servidor DEMO, Windows físico e distribuição Safety
continuam pendentes. O novo pin não autoriza negociação REAL.
