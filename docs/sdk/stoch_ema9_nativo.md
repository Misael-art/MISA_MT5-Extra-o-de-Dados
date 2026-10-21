# S10 — consumidor de presets e conferência nativa

T19.38, variante declarada Estocástico lento/EMA: a matemática Python está em
[stoch_ema9.md](stoch_ema9.md). O perfil MQL5 fechado acrescenta Default preservado,
Manual nomeado/revisionado, exportação e conferência pelo serviço de execução já usado
por S09/Pullback. Os oito parâmetros e o orçamento por saldo são editáveis; lotes fixos,
parciais, BE, filtros e saídas temporais não são capacidades deste Expert.

## Identidade e proteção

Fonte HADOUKEN `9a003dd051f4b40f9c6f1effcf749aecb0122f51`, S10 `1.30`, fingerprint
`26545f544c784c39`. Nome/tipo/quantidade dos 15 inputs, versão, origem e closure são
exigidos antes de abrir o editor. Somente o include externo exato `Canvas/Canvas.mqh`
é aceito; a biblioteca padrão instalada continua necessária à compilação. O novo
`HDK_StrategyObservation.mqh` acompanha a closure. Versões/pins anteriores de S09 e
Pullback permanecem intactos. Manifesto lateral não concede suporte a outro fonte.

Risco por saldo composto até 0,5%; uma posição, SL obrigatório, perdas diária/semanal
2%/4%, DD10%, bloqueio de averaging e buffer de margem20% do motor existente. O preset
não edita essas proteções. S10 exige Strategy Tester + conta DEMO no próprio OnInit;
replay não libera execução ao vivo. `operational_acceptance=false` permanece explícito.

O offset da EMA usa pontos do ativo. O alvo opcional usa o fechamento do sinal como
referência proposta; zero desliga. O executor mantém sua normalização e cálculo por
distância sobre a cotação de envio/reenvio, podendo alterar níveis finais em movimento
de preço. O trace não prova igualdade exata entre nível proposto e fill/SL/TP final.
Os overrides Python de alvo/stop existem separadamente e não são anunciados como
capacidades nativas: alvo `fixed_points` Python parte do fill, diferente deste preset.

## Conferência e limites

O wrapper da Session lê os 15 inputs após OnInit, no Tester DEMO, e o serviço compara
os valores ao pacote/.set/INI efetivamente carregado. Fonte/binário compilado e pacote
exportado, por si sós, não constituem esse recibo. O serviço exige isolamento, modo
visual real no replay, livro completo, SL, fills e saldo reconciliados. Ausência de TP
é permitida somente neste perfil quando `target_points=0`; um TP inesperado, ou TP
ausente quando solicitado, reprova a conferência. Os consumidores anteriores ainda
exigem seus alvos físicos.

As janelas OHLC fechadas registradas em `HDK_STRATEGY_CONTEXT/BAR` são reconstruídas
uma a uma. O consumidor recalcula a variante Python em cada janela, incluindo sua
semente. Publicação incremental depende de todo o prefixo anterior idêntico. Revisão
antiga exige nova janela completa; divergência no contexto reprova replay mesmo quando
sinais/resultado coincidem. Isso não equipara a janela nativa deslizante1500 a um
backtest Python de prefixo fixo, nem certifica Profit/iStochastic ou fontes externas.

`execution_state` é obrigatório: aceito/parcial/incerto nunca conta como ausência de
fill ou aceite completo. A trava nativa é de instância, posterior ao retorno de
OpenObserved. O método expõe a resposta final da chamada e conserva retries anteriores
do executor; não certifica ausência de incerteza em todas as tentativas. Histórico e
reconciliação continuam necessários. Restart, múltiplas instâncias/carteira e recuperação
assíncrona permanecem fora desta qualificação. Níveis de guard no contexto são propostas,
sem recibo de `ModifySL`. O modelo Python normaliza tick, mas não dispõe de stops/freeze
no SymbolSpec e não substitui a recusa/aceite do broker.

## Evidência desta entrega

A fundação Python foi preservada pelo commit `d4b084de0e73e2b99ded3d734957288954643385`
e composição Git; seus240 testes focados/171 ampliados são reutilizados nos arquivos
inalterados. A seleção final de integração passou em203 testes, com Tk/Xvfb. Os testes próprios cobrem limiares,
janela revisada, ambos os lados, Manual/reabrir/exportar, recusa de dados/estado incerto,
TP desligado e divergência no replay. Session simulada não é passagem nativa.

Em cópia consistente da base real, EURUSD/H1, seleção2026-09-01..30 com aquecimento e
operações desde15/09:528 barras, cinco posições, resultado −819,29 sobre capital100000,
risco0,25%. Livro e reabertura conciliados. Especificação histórica não estabelecida:
não é aprovação financeira, paridade nativa ou alegação de perda máxima. A tentativa
anterior até15/10 foi recusada por conter só288 barras; resultado preservado. O backtest
EMA negativo de T8.7 permanece intacto. Cache S10 continua inelegível.

Expert e wrapper gerado têm compilações isoladas MetaEditor6276/Wine,0 erros/0 avisos,
sem terminal/Tester/ordens; processos próprios encerrados e contexto liberado. Recibo
real, novo backtest/replay nativos, painel visual, Windows físico e live DEMO continuam
pendentes. A instalação do proprietário só recebe o conjunto após revisão e CI final;
nenhuma branch WIP é promovida como release.


Correção da revisão: o consumidor exige crescimento incremental de zero ou uma barra
na contagem, como o produtor, e testa a rolagem de1500. Toda ocorrência reservada de
SIGNAL/CONTEXT/BAR deve ser interpretada; linha truncada ou conteúdo extra reprova a
sessão, preservando prefixos normais do diário MT5. Isso não certifica completude quando
pares inteiros CONTEXT+BAR se perdem: não há contador/hash de sequência do produtor.
Os flags de fonte/guard/cache não são promovidos por essa conferência.

O CI39548192469 da ponta ce09fe6 foi recusado (contagens antigas do catálogo e
fixture de re-simulação com especificação desconhecida). A correção conserva uma
lista explícita dos oito robôs Python nas fixtures e testa o catálogo inteiro.
O re-simulador T16.6 não recebe as séries de níveis EMA: `ExitPolicy.from_rules`
agora recusa Move Guard dependente do sinal, em vez de perder essa gestão ao
projetar regras. Isso não desliga o Move Guard no backtester nem altera o replay
nativo; qualificação da re-simulação com essa série permanece pendente. Os casos
legados mantêm comparação de saídas/PnL, e o caso novo exige a recusa explícita,
sem pular teste. Correção validada em58 testes focados com Tk/Xvfb e14 de
re-simulação após a seleção tipada da especificação. Novo CI final continua
obrigatório; falhas anteriores e Windows físico não são declarados aprovados.
