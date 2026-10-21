# Retomada da integração — 21/10/2026

Comece pelo [repasse completo](docs/repasse/INTEGRACAO_20261021.md): entregas integradas e instaladas, fila de PRs, testes, restrições e próximos passos. O [resumo verificável da limpeza](docs/evidencias/REPASSE_LIMPEZA_20261021.json) registra o escopo e as medições sem dados privados.

- Instalação no corte do repasse: aplicação PR171/ac7af2c3, HADOUKEN ec550d77 e Safety3988df13/v1.8.0.
- PRs172–177 publicadas e não integradas/instaladas; ordem de retomada172→173→174→175→176→177. Conferir checks da ponta exata antes de agir; o snapshot documental não concede aprovação futura.
- O pedido de parada/limpeza foi cumprido. Este PR publica o repasse; não altera a instalação nem reinicia as jornadas operacionais.
- Não aplicar reset/clean/checkout forçado no checkout principal antigo. WIP, dados, configurações, backups, `.venv` e worktrees publicadas foram preservados.

No host original, recibos e instruções operacionais detalhados estão em `logs/integracao-pm12-20261020/`: `current-handoff.json`, `NEXT_ACTIONS.md`, `REPASSE_COMPLETO.md`, `cleanup-summary-20261021.json`, `cleanup-manifest-20261021.json` e `handoff-final-verification.json`. Esses arquivos são locais e ignorados; não forçar inclusão no Git. Cópias privadas grandes foram compactadas com hash verificado e podem ser recuperadas pelo manifesto para um novo caminho privado, sem recalcular históricos.

A sincronização privada/Tester, o replay e os aceites operacionais continuam pendentes. Compilação, exportação e testes simulados não comprovam Expert carregado, execução do broker, Windows físico ou autorização para conta REAL.
