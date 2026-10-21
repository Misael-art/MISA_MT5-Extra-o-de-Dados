# T12.6/PM-12 — distribuição coerente do painel

O painel compacto foi integrado ao HADOUKEN pelas PRs32/36/37/38/39, nesta ordem.
O consumidor legado33 e seu aceite operacional têm acompanhamento próprio; não
confundir a integração visual com a confirmação de ordens no servidor.

## Instalação do conjunto completo

A Safety1.8.0 acrescenta PanelHost, ChartVisual, CandleTime e VisualTradeBook ao
manifesto e sincroniza GuardianPanel/PanelUi. Atualizar somente GuardianPanel deixa
includes ausentes. Canvas pertence à biblioteca padrão do MT5. Risco, executor,
contratos e registry permanecem byte a byte da Safety1.7.0; SDK1 e sidecars conservam
seus contratos. Cada versão continua exigindo a escolha explícita da distribuição
correspondente; uma versão diferente não é instalada sem `--upgrade-safety`.

O instalador confere **todos** os fontes antes de copiar o primeiro. Quando `safety.json`
declara `sha256`, o mapa deve abranger exatamente a lista `files`, e cada conteúdo deve
coincidir. Ausência ou divergência recusa a distribuição com ação de restauração,
preservando os includes e marcador já instalados. A cópia usa os bytes conferidos.
Isso não é rollback de uma falha de disco durante a escrita; erros de I/O continuam
exigindo recuperação. Manifestos antigos sem hashes mantêm o comportamento compatível.
O marcador `MQL5/Include/mt5x-safety.json` conserva os hashes quando fornecidos.

## Evidências e limites

- Baseline dirigido:10 testes de Safety/posse aprovados. Depois:15 no recorte e38 com scaffold/ferramentas/SDK, incluindo
  idempotência, hashes, preservação diante de último header ausente/divergente e mapa
  incompleto, com caminhos Windows/Linux simulados. Não é Windows físico.
- Smoke da distribuição1.8: quinze headers `MT5X/Safety`, sem `Framework/` disponível,
  MetaEditor6276/Wine,0erros/0avisos; dois testes próprios de grafo/manifesto/hashes.
  Nenhum terminal/Tester/ordem iniciado, contexto liberado e principal preservado.
- CI e revisão técnica de cada PR continuam obrigatórios para integração. Os resultados
  visuais44/113/142 das entregas são QA com dados sintéticos e fontes identificados.
- Reconexão da API ao principal6276 confirmou DEMO sem reiniciar terminal/ponte.
  Perfis privados autorizaram, mas não sincronizaram nos ensaios delimitados; nenhum
  Tester ou operação foi iniciado nessas tentativas. Não repetir sem nova pré-condição.

Pausar/zerar e a gestão permanecem no executor/RiskGuard do Expert. Escopo é magic em
todos os seus ativos; leitura de posições externas não concede autoridade. Finalizar
permanece indisponível e Lote/SL/TP são prévias somente leitura. S09, Pullback Nativo e
adaptador mantêm suas restrições Tester-only; cache de execução segue inelegível.
Ao vivo DEMO, Windows físico, muitos gráficos/DPI e capacidades adicionais têm aceites
específicos. Compilar não prova que o terminal carregou o binário novo.
