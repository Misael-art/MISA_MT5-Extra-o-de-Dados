# S09 — variante mínima declarada, T19.37 inicial

`s09_reference` reutiliza Strategy, EMA, composição, sizing/backtester, ledger e CSV.
Cruzamento EMA20/50 após 250 barras fechadas de aquecimento; SL100/TP200 pontos
ancorados no preço executado, antes do lote, pelas políticas T19.18/19. Entradas
seguintes ao sinal fechado. Padrões são referência editável, não metas universais.
Empate anterior permite cruzar; empate atual não é sinal. Sem saída no cruzamento
oposto, parcial, BE, trailing, filtro EMA200 ou stop ATR. Os seis robôs antigos
mantêm seus defaults e regras. `ema_atr` não é renomeado nem declarado equivalente.

A EMA parte do primeiro close do snapshot de aquecimento e conserva essa âncora.
Ausência, preço não positivo ou não finito invalida o prefixo, sem preencher buracos.
Acrescentar barras futuras não altera sinais confirmados. Corrigir a fonte exige
nova evidência/rematerialização pelo consumidor; runs anteriores são preservados.
A seleção do snapshot e início de negociação devem acompanhar o preset/contexto.

Políticas explícitas da composição podem substituir o SL/TP da estratégia, pelo
mesmo serviço existente. Modos sem suporte são recusados. Este recorte não concede
suporte nativo à composição: o mapper/perfil T19.23 deve mapear cada input/unidade,
validar os limites do Expert e ligar o recibo real antes da exportação/conferência.
Risco da simulação Python pertence ao BacktestConfig/composição; ele não qualifica
por si só as guardas financeiras nativas e não é alterado silenciosamente.

Evidência `docs/evidencias/T19_37_S09_PYTHON_20261019.json`: referência Decimal
independente, causalidade por prefixos, ausência, parâmetros, cálculo/trades/CSV,
Default de composição igual à estratégia e quatro sinais iguais aos registrados
no backtest nativo do mesmo snapshot. O equivalente H usa B08 inicial e limita-se
ao Tester DEMO: dois negócios protegidos, livro/saldo reconciliados; não é paridade
financeira Python/nativa, replay visual, Manual gerado ou aceite estatístico.

As lacunas do exemplo S09 original (períodos/gatilhos/unidades/precedência, BE,
trailing, janela, painel e zeragem) permanecem declaradas; esta é uma variante
mínima para o piloto, sem prometer fidelidade à fonte. Não cria framework novo,
estratégia Pullback ou presets automáticos. Pullback continua bloqueado no adaptador;
Windows físico permanece pendente. Nenhum negócio ao vivo é autorizado.
