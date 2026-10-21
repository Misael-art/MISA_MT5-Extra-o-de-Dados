# VWAP Bands — presets e conferência nativa

T19.36/S01, variante percentual declarada em [vwap_bands.md](vwap_bands.md).
O consumidor oferece Default preservado e Manual nomeado/revisionado, edição dos
quatro parâmetros da estratégia e orçamento por saldo até0,5%, exportação e
conferência pelo serviço existente de backtest/replay. Não oferece lotes fixos,
parciais, BE, trailing, filtros ou saídas temporais que este Expert não possui.

## Fonte e semântica

HADOUKEN `dc789710f57a54260c0243ceb1716b66ae523ea9` (PR46 integrada em
`11bbbd7633de2fa6b2666b06f521d19d2142359e`), VWAP1.20, fingerprint
`7139d746ab6b2c1d`. Exige fonte, versão, fingerprint, nomes/tipos/quantidade exata
dos11 inputs e closure antes de abrir o editor. A única dependência externa
aceita é `Canvas/Canvas.mqh`, cuja biblioteca padrão deve estar instalada.
Inclui `HDK_VwapObservation.mqh` e o observador compartilhado; S10/S09/Pullback
mantêm seus pins e contratos. Não requer nova distribuição Safety.

HLC3 ponderado por uma série de volume; bandas são percentuais, não desvios
padrão. O toque usa high/low contra a banda calculada no fechamento da própria
barra. Entrada é a mercado na barra seguinte; não é ordem limite nem prova do
toque na banda vigente intrabar. Stop é offset em pontos além da banda de entrada;
TP é a banda de alvo fixada no sinal. **Banda de alvo zero é VWAP central COM TP**.
O serviço continua exigindo SL e TP positivos no livro deste perfil. A exceção
de TP desligado do S10 não se aplica ao VWAP.

Primeiro dia iniciado no meio do dia é indisponível. Meia-noite ou mudança
observada de data habilita a primeira barra disponível; não prova dia completo,
sessão/leilão ou ausência de lacunas. Horários são calendário do servidor, sem
inferência de UTC/DST. Auto nativo escolhe uma série para toda a janela1500,
por qualquer real positivo nela, inclusive em data anterior. Python usa o
prefixo fixo conhecido; não se presume equivalência entre esses percursos.

## Proteções e evidência

OnInit exige Strategy Tester e conta DEMO antes do motor. Uma posição, risco por
saldo composto até0,5%, limites diária2%/semanal4%/DD10%, buffer de margem20% e
bloqueio de averaging permanecem no risco/executor existentes. Não há novo gestor
nem envio paralelo. O observador compartilhado distingue bool de fill e trava
novas entradas após aceito/parcial/incerto nesta instância. Resposta final não
certifica todas as retentativas anteriores; restart/concorrência não qualificados.

O leitor reconstrói separadamente cada janela OHLC e real/ticks do mesmo snapshot,
conciliando horários, quantidade, fonte solicitada e escolhida. Recalcula a variante
Python sobre essa janela com a fonte efetivamente registrada. Revisão/fonte nova
exige reset dos volumes; indisponibilidade limpa o prefixo e só aceita contexto
neutro, sem inferir volumes. Toda ocorrência de SIGNAL/CONTEXT/BAR/VOLUME reservada
precisa ser interpretada integralmente. Diário truncado ou divergente reprova;
replay compara o contexto além de sinais/livro/saldo. A extração do leitor OHLC
compartilhado preserva a validação S10 e sua invariável incremental de zero/uma barra.

Contextos descrevem propostas antes do envio/gestão, sem comprovar níveis finais,
ModifySL ou corretora. Não há contador/hash encadeado do produtor: perda de pares
inteiros em janelas constantes pode não ser detectada. `source_qualified=false`,
`execution_levels_qualified=false`, `external_parity=false`, cache inelegível e
`operational_acceptance=false` permanecem. Recibo real de inputs, .set/INI carregado,
DEMO, isolamento, fills e saldo continuam obrigatórios no serviço.

## Verificações desta entrega

Fundação Python48feaf8 preservada por Git:110 novos testes e240 de composição e
regressão reutilizados com os mesmos fontes/fixtures. Integração:152 testes com
Tk/Xvfb, incluindo Default/Manual/reabrir/exportar, orçamento, TP central obrigatório,
duas fontes/Auto, rolagem1500, troca de fonte, indisponibilidade e registros truncados.
Session simulada não é execução no terminal. Doctor20OK/1aviso opcional/0falhas.

Expert e wrapper de recibo/observação gerado pela aplicação compilados em
MetaEditor6276/Wine0erros/0avisos, sem terminal/Tester/ordens; nenhum processo próprio
remanescente observado, contexto liberado e principal preservado. Fonte real/pin e
os11 inputs também conferidos em Default/Manual exportado e relido numa base isolada.
Isso não prova inputs realmente carregados num teste.

Na cópia consistente da base real EURUSD/M15, setembro2026, aquecimento e operação
desde15/09: Auto foi recusado porque volume real sózeros permanece capacidade
desconhecida no gate de dados. A matemática aceitar zeros não qualifica essa fonte;
o fallback de outros indicadores tem outro contrato e não foi usado como waiver.
Nova escolha explícita `volume_source=1`, ticks elegíveis, mesmas bandas/risco0,25%:
2112barras, zero operações, resultado0, salvo/reaberto e livro conciliado. Não é
aceite de negociação, aprovação financeira, fonte certificada ou paridade nativa.
O resultado negativo S10/EMA existente permanece preservado. Auto Python permanece
restrito quando essa capacidade é desconhecida; selecione ticks explicitamente
somente quando os dados requeridos forem elegíveis.

Tester/replay real deste consumidor, painel, Windows físico e live DEMO permanecem
pendentes. Instalação do proprietário recebe apenas o conjunto revisado com CI final
verde; exportação ou compilação não recarregam um Expert.

Correção de integração: o corretivo S10b4bdf24 foi composto por Git, preservando o
histórico. A fixture explícita do catálogo inclui os nove Python;59 focados adicionais
de CLI/catálogo/UI/re-simulação passaram. VWAP sem Move Guard conserva comparação
do re-simulador base com o backtester, inclusive saídas e PnL; gestão por séries de
sinal, como S10/Madrid, permanece recusada nesse re-simulador sem contexto próprio.
Novo CI sobre esta ponta é obrigatório; a falha anterior do S10 não foi contornada.
