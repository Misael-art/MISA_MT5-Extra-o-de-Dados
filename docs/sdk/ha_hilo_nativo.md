# HA/HiLo — presets e propostas por janela

T19.40/S11, variante declarada em [ha_hilo.md](ha_hilo.md). Perfil nativo1.20,
HADOUKEN `6b4f9ec8800afcd50158e5991dfde89a14b28264`, integrado em
`ec550d77caae74e9f5f4ffe30cea5ccd29621624`, fingerprint `5dad3bbabab0343f`.
Exige fonte/versão/pin e os10 inputs com nomes/tipos/quantidade exatos, revalidados
antes do editor e na exportação. Único include externo aceito: `Canvas/Canvas.mqh`
exato, biblioteca padrão necessária no destino. Observador compartilhado acompanha
a closure; não há novo release Safety nem alteração dos pins anteriores.

Default preservado, Manual nomeado/revisionado, três parâmetros e orçamento por
saldo composto até0,5%. `hilo_period`2..500, `hilo_offset` somente0,
`stop_buffer_points`0..2147483647, todos inteiros. Não oferece overrides nativos
de stop/alvo, lotes fixos, parciais/BE, trailing, filtros ou saídas temporais.

## Variante e execução

HA/HiLo conserva estado inicial indefinido e só define HiLo após rompimento
estrito da SMA anterior; mantém estado entre as médias, atualizando a SMA ativa.
Não é o indicador gráfico HiLo já existente, cujas convenções de seed/igualdade
diferem. HA inicia em (O0+C0)/2 e usa recursão de meia soma; OHLC inválido/overflow
envenena o prefixo sem reparo. Warmup300 fixo não garante linha para período500
nem estado após perder a virada inicial da janela1500. Recálculo por janela não
é equivalência ao frame Python fixo, Profit ou estado global após restart.

HAclose acima da linha e de HAopen pede compra/saída da venda; abaixo de ambos,
venda/saída da compra. Condição de nível, sem exigir nova borda. Neutro mantém a
posição. SL inicial fixado na linha do sinal ±buffer; não acompanha a linha futura.
HA sintético não vira fill. Sem TP: o serviço permite ausência de alvo SOMENTE
neste perfil e exige TPzero no livro. A exceção S10/target_points0 continua própria;
VWAP/Madrid/legados conservam seus requisitos. SL positivo permanece obrigatório.

OnInit exige Tester+DEMO. Uma posição, risco composto até0,5%, diária2%/semanal4%/
DD10%, buffer de margem20% e averaging bloqueado permanecem no motor existente.
Reversão é sequencial: tenta fechar, relê posição, só tenta entrada oposta se
estiver sem posição e as proteções permitirem. Remanescente/partial impede nova
entrada; stop inválido ou recusa pode deixar sem posição. Não entrega ordem
ReversePosition atômica, garantia de sempre posicionado, recuperação assíncrona,
restart, concorrência ou suporte operacional netting/hedging.

## Leitura e limites da evidência

Leitor estrito compartilhado reconstrói cada janela OHLC e exige todos os markers,
count anterior/anterior+1, clocks e prefixos válidos. O callback Python existente
é aplicado antes de seu filtro público de entrada; ATR permanece indisponível no
canal não utilizado por esta referência. Converte buffer pelo point registrado,
valida ambos os níveis antes de publicar propostas e depois confere o filtro SL
versus close real do motor. Sem sinal bruto, stops/TP/guards são zero. Entrada
recusada pelo lado do SL pode conservar stops e intenção de saída reconstruída.
Comparação de níveis admite arredondamento do trace e poucos ULP do double.

`exit_intent_from_reference` é fórmula reconstruída: contexto é anterior à gestão
e não contém flags de saída, pedido, retcode ou resultado de fechamento. Não
atribuir um close automaticamente a essa intenção. Livro/histórico e correlação
causal próprios continuam necessários; `reversal_execution_qualified=false` e
`execution_levels_qualified=false`. Replay compara a referência além do livro/
sinais/saldo. Reconciliar esses artefatos não concede aceite de reversão/servidor.

Mesmo OpenObserved, mesmo envio: aceito/parcial/incerto travam novas entradas da
instância sem suspender gestão. Resposta final não relata todas as retentativas;
PARTIAL opened0 não significa ausência de fill. Sem contador/hash encadeado,
pares inteiros perdidos em janelas constantes podem escapar. Fonte/ModifySL/
broker não qualificados. Recibo real de inputs, .set/INI carregado, DEMO, isolamento,
fills e saldo continuam obrigatórios. Exportar/compilar não recarrega um Expert.

## Verificações

Núcleo515a3b9 composto por Git, seus três runtimes preservados byte a byte.
Evidências reutilizadas:80 próprios,432 na seleção final do núcleo e três GUI
afetados; seleções sobrepõem. Consumidor:220 testes de integração Tk/Xvfb, incluindo
todos os perfis anteriores, catálogo/CLI/re-simulação, serviço com/sem buffer,
TP inesperado, SL obrigatório, respostas truncadas3/5/6/incertas e seed/estado da
rolagem1500. Plataformas/Session simuladas não são terminal ou broker.
Doctor com configuração existente --no-probe:20OK/1opcional/0falhas.

Expert e wrapper real de recibo gerado pela aplicação: MetaEditor6276/Wine0erros/
0avisos; contexto liberado, nenhum próprio remanescente observado, principal
preservado. Fonte/pin/10 inputs conferidos em Default/Manual exportado/relido numa
base isolada, sem recibo carregado. Histórico Python nessa cópia:EURUSD/H1setembro2026,
528barras,24operações, resultado−382,223, risco0,25%, salvo/reaberto/conciliado.
Sem aprovação financeira/paridade; especificação histórica desconhecida. Identidade
genérica pode ser `frozen_reference/cache_eligible=true`; gate preservado, sem novo
aceite de reuso operacional/nativo/live. Re-simulação usa SL/livro já gravados e
não recria sinais HiLo ou entradas inversas alternativas.

S10 e VWAP já foram integrados/instalados sob seus próprios checks, com configuração,
ponte e terminal preservados. O resultado negativo S10 foi reaberto na aplicação.
Este consumidor HA/HiLo ainda depende de revisão/CI final e instalação correspondente;
Tester/replay real, painel, Windows físico e DEMO operacional seguem pendentes.
