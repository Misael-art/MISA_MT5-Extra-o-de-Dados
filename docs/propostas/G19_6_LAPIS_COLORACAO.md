# G19.6 — catálogo e séries para lápis/coloração

O catálogo de estudo publica11 estratégias e52 indicadores do extrator,
com parâmetros, origem, aquecimento, escala e regra explícitos. O painel os
mostra primeiro e destacados, seguido dos38 nativos do MT5. Estratégias usam
os generate_signals existentes sobre o prefixo capturado, sem backtester,
posições ou execução de ordens. Indicadores usam o registry canônico.

Séries de até2000 barras usam calendário naive do servidor, revisão monotônica,
valores/indisponibilidades explícitos e publicação atômica. O pedido estável
é renovado pelo painel; a App evita arquivar um pedido mais novo que chegou
durante o cálculo anterior. O serviço usa o ciclo já existente da aba Robôs;
sem resposta, o painel informa a espera. Notas/fotografias são independentes.

Descoberta de robôs inclui o ativo HDK_StudyCandles.mq5 no fingerprint apenas
quando o fechamento de includes exige HDK_StudyLayers.mqh. Instalação privada
preserva o caminho Indicators/HADOUKEN/Study e compila a dependência antes do
Expert ou adaptador manual. Falha de compilação interrompe a preparação.
Pacotes antigos sem esse include continuam sem essa dependência.

O lápis nativo persiste traços por magic/ativo/período, reabre e desfaz somente
traços próprios. A coloração usa OHLC real e conserva indicadores externos.
Nenhuma dessas ferramentas utiliza executor ou altera limites de gestão.

## Verificação

98 testes dirigidos aprovados (catálogo/geradores, causalidade, ausência,
calendário, corrida entre revisões, descoberta/fingerprint, instalação e falha
da dependência, caminhos simulados Wine/Windows). Doctor16 OK/2 avisos/0 falhas
no perfil isolado. Suíte completa final:2930 aprovados/52 pulados/3 avisos existentes de layout
em48min37s, sobre a base G19.5 validada com2911 testes/52 pulados. Instalador aplicado com os atalhos operacionais
restaurados. Nenhum script shell alterado.

Adaptador manual real do SDK e ativo indicador compilados, fonte do painel
idêntica à biblioteca e contexto liberado sem processos próprios restantes.
Ensaio nativo com consumidor App real:302 controles/zero falhas em96 e144 DPI;
abrange todos os38 nativos, RSI do extrator, catálogo prioritário, persistência,
troca de fonte e preservação de indicador/desenho externos. Dados sintéticos,
sem conta ou ordens. Windows físico e instalação final permanecem pendências.
