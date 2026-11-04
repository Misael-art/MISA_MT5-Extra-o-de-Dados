# Retomada da integração — 21/10/2026

**Nova conferência do host:** [continuidade validada](docs/repasse/RETOMADA_VALIDADA_20261021.md)
registra clones novos, SD/restauração em leitura, ambiente preservado, testes,
checks atuais e o próximo recorte de correção do REPLAY. O SO encontrado não é
declarado reinstalado sem evidência.

**Reinstalação do host:** leia primeiro o [roteiro de recuperação](docs/repasse/RECUPERACAO_POS_FORMATACAO_20261021.md) e o [estado da preservação](docs/repasse/RECUPERACAO_ESTADO_20261021.json). Fontes conferidos no GitHub; complemento privado de desenvolvimento salvo e restaurado em ensaio a partir do SD preservado pelo proprietário, em `Backups/MT5X/2026-10-21-desenvolvimento` (cerca de142 MB). Banco útil, configuração, bundles, WIP e contexto essencial foram mantidos; Wine/caches/ambientes são recriados. Novo SO e aceites operacionais continuam pendentes.

Comece pelo [repasse completo](docs/repasse/INTEGRACAO_20261021.md): entregas integradas e instaladas, fila de PRs, testes, restrições e próximos passos. O [resumo verificável da limpeza](docs/evidencias/REPASSE_LIMPEZA_20261021.json) registra o escopo e as medições sem dados privados.

- Instalação no corte do repasse: aplicação PR171/ac7af2c3, HADOUKEN ec550d77 e Safety3988df13/v1.8.0.
- PRs172–177 publicadas e não integradas/instaladas; ordem de retomada172→173→174→175→176→177. Conferir checks da ponta exata antes de agir; o snapshot documental não concede aprovação futura.
- O pedido de parada/limpeza foi cumprido. Este PR publica o repasse; não altera a instalação nem reinicia as jornadas operacionais.
- Não aplicar reset/clean/checkout forçado no checkout principal antigo. WIP, dados, configurações, backups, `.venv` e worktrees publicadas foram preservados.

No host original, recibos e instruções operacionais detalhados estão em `logs/integracao-pm12-20261020/`: `current-handoff.json`, `NEXT_ACTIONS.md`, `REPASSE_COMPLETO.md`, `cleanup-summary-20261021.json`, `cleanup-manifest-20261021.json` e `handoff-final-verification.json`. Esses arquivos são locais e ignorados; não forçar inclusão no Git. No SD, `continuity/context-and-wip.tar.zst` conserva o repasse principal, seus scripts/recibos e cinco WIPs reais. As grandes cópias privadas/regeneráveis não foram migradas; veja o escopo do roteiro.

A sincronização privada/Tester, o replay e os aceites operacionais continuam pendentes. Compilação, exportação e testes simulados não comprovam Expert carregado, execução do broker, Windows físico ou autorização para conta REAL.
