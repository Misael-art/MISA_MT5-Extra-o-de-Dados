# Continuidade AIA-02a — 2026-10-24

Entrega independente do SDK, sobre AIA-01, sem editar o WIP do proprietário ou
os fontes das outras frentes de dados/painel.

- App: branch `codex/ai-study-catalog`, código instalado
  `49c6629101305d1c0a53e9da55005da8ef40049d`, [PR208](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/208).
- Base: AppPR207/`codex/ai-workflows`, `f4ca1776a4fe3414c6d057c2b6a5f302e73ebf22`.
- H permanece HPR75/`codex/ai-workflows`,
  `b730ed4a8df74f5f20eb6e289798d0232c370994`. Nenhum novo commit H/Safety nesta tarefa.
- Prévia do atalho/menu usa App49c6629. Configuração, .env, dados, H e canais
  aceitos preservados; ponte existente reutilizada, sem reinício. Um processo UI
  anterior pode continuar aberto; não encerrá-lo cegamente.

Leia AGENTS/ROADMAP/PLANO/continuidade, [guia](../agente.md) e
[contrato de Estudo](../sdk/study_cli.md). No novo host, clone os ramos acima;
master ainda não recebeu estes merges. Alternativa: `git fetch origin
pull/208/head:codex/ai-study-catalog` no App e `pull/75/head` no H. Recrie o
ambiente pelo instalador do SO; não copie venv/prefixos Wine. Ajuste os caminhos
locais e mantenha credenciais somente em .env. Execute o CLI a partir desse
checkout/instalação, sem importar um WIP antigo por engano.

## Entregue e verificado

`study.catalog` descobre os38 nativos da própria closure, sem criar handles.
`study.indicator.add` usa a mesma fábrica/defaults/leases, respeitando ownership,
64 tentativas e remoção por `indicator.remove`. Retorna hash do header, metadados,
parâmetros observados e todos os buffers em shift1; não duplica fórmulas no Python.
O hash do catálogo identifica bytes da origem, não redefine o fingerprint do robô.

O canal também espera a limpeza inicial antes de abrir um auxiliar do mesmo
símbolo/TF do painel. A falha inicial foi preservada: o gráfico M5 desapareceu e
o pedido foi recusado por ownership. A espera pelo marcador resolveu a reprodução
sem alterar/remover o marcador nem editar o host. Timeout continua incerto;
`chart.receipt` consulta progresso/ACK sem repetir o efeito.

QA CLI real PLAY DEMO:102 pedidos,38 adições/readbacks/remoções; Bands/SAR sobre
candles e MACD separado em PNG1280×800 inspecionado. Carteira inalterada, nenhuma
ordem, retorno0. MetaEditor/Wine0erros/0avisos, terminal6283 observado.
Fractals e Ichimoku não tinham TODOS os buffers disponíveis na última barra
fechada; isso foi devolvido como null/indisponível, sem fabricar valor.

Baseline focado80; dirigidos finais108 e regressão176 (seleções sobrepostas).
Helpers MQL reais executados em shim com plataforma/catálogo simulados; catálogo
presente/ausente, IDs e cinco casos do predicado de espera. Wheel inclui os dois
recursos MQL e import instalado foi conferido. Doctor20OK/1pandas-ta opcional/0falhas.
Baseline amplo f4ca177 congelado:2966aprovados/251pulados em2121.08s.
Suíte ampla após a mudança em49c6629:2994aprovados/251pulados em2206.57s,
sem DISPLAY e com BLAS/OMP=1;0falhas nas duas execuções.
Resultados e CI são registrados separadamente na
[evidência](../evidencias/AIA_02A_ESTUDO_CLI_20261023.json), sem somar seleções.

Dois perfis próprios removidos após encerramento/reconciliação:1.821.811.172 bytes
lógicos. Logs, fontes instrumentadas, recusas e capturas preservados; nenhum
processo alheio sinalizado. Árvore própria de testes pós-mudança removida depois
do término:1.235.160.835 bytes lógicos adicionais. Não remover por suposição
a pasta pytest padrão da baseline sem identificação conclusiva de posse. Não
apagar DB/venv/temporários de outros agentes.
O adicional organizado no SD, `Backups/MT5X/2026-10-24-ai-study-catalog`, contém
somente continuidade/evidência e a captura. Source no GitHub; não há base de
mercado criada por este recorte que justifique migração.

## Pendências e fila

1. Conferir a CI de cada HEAD e integrar serialmente as PRs/dependências. O timeout
   Windows da base em BD18a/commitSQLite é da fronteira de dados: não atribuir
   causa ao SDK, ocultar o erro ou alterar o código dessa frente sem coordenação.
2. SDK: demais ferramentas de Estudo, parâmetros tipados dos presets nativos,
   terceiros/projeções Python/anotações/lápis e controle de reprodução do Tester.
   Nenhum fallback por mouse/teclado, nova fila/cache ou gestor financeiro.
3. Windows físico, DPI/monitores e concorrência/lifecycle entre controllers têm
   qualificação própria. Compilação/PNG/readback não são aceite financeiro.
4. OMNI: consolidar seu contrato antes de implementação. Indicador estratégico:
   fechar cálculo/estado/projeção e sua evidência. Armazenamento: integrar pelo
   contrato da frente de dados, sem criar outro store paralelo. Estratégia
   operacional: recorte separado com critérios e validação próprios.

O pin manual45e31481977d09f1 requer a closure H75 completa/Canvas padrão. Outros
perfis preservam pins/snapshots; uma nova closure não é promoção automática.
Parâmetros observados dos indicadores mantêm `inputs_verified=false`; recibos do
robô em PresetExecution não foram relaxados. Não inferir causalidade histórica,
ausência de repaint ou paridade externa de valores observados agora.

Este repasse cobre AIA-02a. Antes de uma formatação, recolher também as pontas e
os repasses dos agentes ainda trabalhando em dados/painel; não perder WIP deles.
