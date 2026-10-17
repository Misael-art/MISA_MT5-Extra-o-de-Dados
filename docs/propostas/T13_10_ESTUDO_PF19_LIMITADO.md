# T13.10 — Estudo PF19 limitado, reprodutível e retrospectivo

Recorte de pesquisa próprio. Instalador/dev isolados e baseline947/9, saída0, em912,43s concluídos antes de editar. Preparação de código não inicia avaliação de estratégia; executar somente após aceite crítico da UI e protocolo registrado antes de resultados novos.

## Ordem e aceite

1. Registrar universo/hipótese/períodos/exposição/custos/risco/orçamento/aceite em protocolo JSON versionado. As séries atuais e resultados antigos já foram vistos: NÃO são OOS independente. Não afrouxar critérios nem recriar uma reserva ao mudar seu nome.
2. Ferramenta offline usa somente o backtester, estratégias, especificações e PF existentes. Copia consistentemente SQLite para execução privada; não conecta SDK/terminal/conta e não muda configuração, sinais, parâmetros ou risco. Oito tentativas máximas e nenhum otimizador, conforme protocolo. Registrar falhas/limites/consumo, trades e hashes.
3. PF por operação depois dos custos **modelados**, distinguido de líquido certificado. Comissão zero configurada não prova isenção; moeda de lucro não prova moeda de conta. Perfil de spread23dias medido depois de parte da janela não é calibração ponto-no-tempo. Publicar limitações e suporte efetivo.
4. Incerteza por blocos de cinco sessões de mercado, incluindo sessões sem operações, somando GP/GL dos PnLs de cada trade dentro dos dias. Nunca calcular PF dos saldos diários. Duas mil reamostragens, seed fixa, IC95%; denominador indefinido não sustenta aprovação. Descrever dependência/seleção não corrigida.
5. Testes de cálculo/invariância/denominador, gates administrativos, orçamento e execução reprodutível; suíte/doctor/CI/revisão e PR próprio. Operar estudo somente após UI aceita. Relatório real dos oito casos, sem ocultar fracassos; versão/hash/preset/períodos/custos/amostra/sessões/perdas/expectativa/DD/concentração/subperíodos/stress/limitações e próximo experimento.

A condição de dados já expostos e custos não certificados impede aprovação nesta rodada, mesmo se algum PF pontual superar1,9. Critérios originais (200operaçõesOOS/60sessões/30perdas, PF≥1,9, limite inferiorIC95>1, expectativa líquida positiva, risco/coerência/estabilidade/custos/MT5) são preservados. Se nenhum candidato elegível, registrar resultado negativo de demonstração e próximo teste independente, sem executar conta.

Sem nova dependência/configuração/schema; Windows de primeira classe. Relatório de pesquisa é artefato reproduzível, não recomendação ou autorização financeira. T16.2b/Safety e PRs79/80 mantêm revisão e escopos separados.
