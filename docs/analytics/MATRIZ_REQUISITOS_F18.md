# Matriz de requisitos — Fase 18

Extensão planejada em 2026-10-15. Estes requisitos não alteram o estado das entregas F13/F16 e não declaram nenhuma implementação concluída.

| Requisito | Tarefa | Evidência de aceite exigida |
|---|---|---|
| Colunas consultáveis de indicadores e relações sem alterar barras brutas | T18.1/T18.6 | Registry, persistência aditiva, projeções/exports e DTO versionado |
| Indicador × indicador e × retorno futuro | T18.2 | Método, alvo/horizonte, IC, amostra, maturidade e ajustes de testes |
| Compra/venda, gain/loss/empate e utilidade para Expert | T18.3 | Ledger completo, custos, baseline e população sem seleção só de winners |
| Convergências de dois fatores | T18.4 | Utilidade incremental OOS contra fatores isolados e Expert original |
| Mapa temporal e deterioração | T18.5/T18.8 | Ranking no treino, nenhuma célula usa futuro; presença/evidência/frescor separados |
| Pré-processamento incremental | T18.5 | Revisões, invalidação, idempotência, recuperação, write_guard e medição |
| Presets candidatos e leitura por agentes | T18.6/T18.7 | Manifesto de pesquisa com proveniência, sem auto-apply nem alteração de risco |
| UI dinâmica e legível | T18.8 | Temas, contraste, legendas, teclado, zoom, amostra e estados ausentes |
| API posterior somente leitura | T18.9 | Equivalência com consulta local, limites de acesso e nenhuma escrita/ordem |

Especificação detalhada: [Relações entre indicadores e aderência temporal](../propostas/RELACOES_INDICADORES_SETUP.md).
