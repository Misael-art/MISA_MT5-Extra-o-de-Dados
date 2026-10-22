# G19.5 — notas e fotografia dos indicadores

Análise → Estudo → Pena: clicar em um candle congela barras e buffers no MT5,
abre o editor e pede a fotografia ao extrator. Ela aparece abaixo do texto,
com itens do extrator destacados primeiro e páginas para consultar todos.
Salvar confirma a gravação no banco; editar o texto conserva a fotografia.
Reabrir notas recupera o escopo ativo/período/magic pelo banco.

O indicador de pena acompanha o ponteiro no gráfico, sem DLL ou troca do
cursor do sistema operacional. O editor oferece várias linhas, + Linha e
navegação. Atualmente o ponto de ancoragem é na área principal do gráfico.
Os textos longos do resumo têm conteúdo completo na dica. Cartão e painel
respeitam seus limites; etiquetas de trades preservam também o cartão.

O extrator precisa estar aberto na aba Robôs (pode-se continuar usando outras
abas depois). A sincronização usa o ciclo já existente dos cards, com trabalho
em segundo plano a cada2 s; sem pedido não abre banco nem cria diretórios.
Se o extrator estiver fechado, o pedido fica no Common/Files/HDK_Study e o
editor informa que aguarda confirmação. Falha transitória do banco conserva
o pedido. Não declarar salva uma nota apenas por existir um arquivo local.

## Banco e imutabilidade

Migração aditiva: `_chart_study_notes` tem `note_text` em coluna própria,
símbolo/TF/candle/preço, magic, revisão e `snapshot_json`/SHA256. A tabela
`_chart_study_captures` conserva a fotografia antes de o usuário salvar o texto;
`_chart_study_requests` torna a gravação idempotente. Não renomear ou remover
tabelas antigas. Cache TSV pode ser reconstruído a partir do banco.

A captura nativa copia até2000 barras terminando no candle escolhido e os
buffers dos38 indicadores nativos com seus parâmetros efetivos. Ausência, aquecimento ou EMPTY_VALUE ficam explícitos; não usar zero como substituto.
O extrator reutiliza `analytics.indicator_registry.iter_core` com exatamente
esse prefixo congelado. Todos os seus indicadores incluem valor/estado,
origem, fórmula, parâmetros e versão. Não há motor financeiro paralelo.

Horários separados: candle MT5, captura observada (mtime do arquivo atômico),
cálculo do extrator e gravação/edição. Buffers observados numa consulta a uma
barra histórica não provam disponibilidade histórica de um indicador que
repinta. A reabertura não consulta barras novas nem recalcula indicadores.
Prévia e nota salva compartilham os mesmos bytes e hash da fotografia.

Pedidos JSON versionados, identificadores limitados e arquivos de até4 MiB.
Temporário → rename para publicação; recibo só depois do commit do banco.
Um pedido repetido devolve o mesmo recibo; colisão de conteúdo é recusada.
Edição exige a revisão atual, evitando sobrescrever outra janela. Texto até
20000 caracteres. Sem execução de ordens, credenciais ou dependência nova.

## Validação

Base exata7b2062d4 previamente validada:2892 testes/52 pulados/3 avisos.
24 testes dirigidos aprovados (mais2 de descoberta de Common no Wine/Windows simulado): captura sem futuro, ausência de valores,
bases antigas preservadas, prévia/salvamento idênticos, edição imutável,
conflitos, repetição, recuperação de cache, falha transitória e UI. Ambiente
reaplicado no worktree com atalhos operacionais restaurados; doctor16/2/0
(pandas-ta opcional e ponte desabilitada no perfil isolado).

Ensaio nativo inicial com a App consumindo pedidos reais:212 controles
aprovados/zero falhas em96 DPI, com Unicode, múltiplas linhas, prévia antes
de salvar, duas revisões e reabertura pelo banco. Dados sintéticos, sem conta
ou ordens. Refinamento visual:216 controles aprovados/zero falhas em96 DPI, incluindo orientação vertical e etiquetas fora do cartão. Mesmos216 controles aprovados em144 DPI. Suíte completa da App:2908 aprovados/52 pulados/3 avisos existentes de layout (44min47s); os2 testes adicionais de caminhos passaram separadamente.
Windows físico permanece pendência, não comprovado neste host Linux/Wine.

## Calendário nativo

MQL datetime é decodificado explicitamente como relógio naive do servidor nas fórmulas. bar_time_native guarda esse horário; bar_time_utc fica ausente quando o offset não é conhecido. Coluna nova aditiva, sem reescrever fotografias antigas nem seu SHA256. Captura/cálculo reais continuam em UTC. Revalidação completa da App em execução após esse ajuste;19 testes de núcleo aprovados, incluindo captura legada.
