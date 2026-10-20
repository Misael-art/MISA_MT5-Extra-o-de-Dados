# T19.16 / TM06 — Donchian, rompimento e relógio observado

Depende de T19.7/I10 (M#157) e de T19.15/TM05 integrados. Perfil novo S09v1.50,
33 inputs, fingerprintb39a586b622e4713, H runtimeeaaa059. Perfis anteriores e
resultados continuam legíveis; nenhuma conversão automática do Manual/Pullback.

A variante usa o canal anterior20: max(high)/min(low) de `[t-20,t-1]`, excluindo
high/low de t. Renovação é **close fechado** acima do upper para buy, abaixo do
lower para sell. Wick e empate não renovam. Após pelo menos um rompimento favorável
observado durante a posição, close dentro do canal anterior, limites inclusivos,
permite fechar quando o tempo sem renovação atinge o prazo. Não se presume retorno
sem rompimento anterior. Não é a estratégia Donchian de entrada nem um novo preset.

O relógio sem renovação começa no instante conhecido do close que rompe; nunca na
origem retrospectiva da barra. A idade total continua desde o primeiro fill. Teto absoluto positivo é obrigatório quando Donchian está ativo, sem extensão por renovação. Nenhum
reset por parcial/reinício; prazos anteriores e ER têm prioridade, SL permanece.
Adições/gradientes não são admitidos. A ausência de cobertura/âncora impede somente
a regra de canal; outros prazos/proteções continuam. Não se reinicia um contador
perdido. Nativo persiste âncora/último contexto e registra o journal em logs próprios.
Correção recebida é observação nova; não apaga confirmação passada nem reancora
retroativamente. Contexto igual/reinício não adia relógio.

Python avalia no fechamento e executa na abertura seguinte, preservando SL/TP no gap;
nativo observa closes anteriores à barra atual recebida e pode atingir o prazo entre
ticks com o último close conhecido. Motores continuam distintos. O journal nativo
conserva janela, close e instante conhecido. O consumidor confere a cadeia do relógio,
min/max por Decimal independente e função Donchian canônica, ownership e preenchimento
confirmado pelo controlador compartilhado. Replay compara também as observações.

Manual opt-in com prazo em segundos; salvar/reabrir/exportar, inputs carregados,
aba de contexto conhecido, decisão/confirmação e consulta persistida. Sem ativação
para Pullback, conta REAL, live DEMO, carteira ou gradientes. Cache inelegível; S09
somente Tester e Windows físico pendente específico.

Testes dirigidos dos dois motores, registry/UI/recibos, buy/sell, N−1/N/N+1,
ausência, não romper/empate, renovações, reinício/parcial/hash/cobertura e confirmação
falha aprovados. Provedor: 27 Tools, verificador estático123 e compilação isolada.
Aceite nativo próprio e checks obrigatórios pendentes; não repetir a campanha ER.
