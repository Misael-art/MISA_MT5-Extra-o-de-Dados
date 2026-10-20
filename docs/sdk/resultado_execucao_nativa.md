# B07 — Resultado nativo observado e SDK1

Vínculo da tarefa B07 entre mt5x, HADOUKEN e mt5x-safety. Não muda runtime Python,
SDK1, sidecars T19.2 ou liberação do backend nativo. A API de origem está em H
`Safety/HDK_ExecutionResult.mqh`/`SafeExecutor.OpenObserved`, com cópia versionada
S1.7.0 sobre1.6.0. Branches locais1.4/1.5 permanecem preservadas e fora da entrega.
Manifesto contrato1/include_dir único continuam aceitos pelo serviço existente.

A observação opt-in usa a mesma chamada Open/OrderSend, sem cálculo, preço, risco,
limite ou retentativa paralelos. O bool antigo permanece igual. O DTO simples1
expõe estado, instante, tentativa final, tickets, retcodes e request_id. Preço e
volume preenchidos só existem com `fill_known`; ausente é EMPTY_VALUE. O receptor
precisa tratar versão/estado/unidade, conservando envelope com símbolo/magic/plano
mais identidade da sessão. Request_id0 observado no Tester é ausente; não fabricar
um identificador ou inferir tipo/resultado por um ticket ambíguo.

Distinções: recusado, recotação, aceito ainda sem fill, preenchido total/parcial
e incerto. `attempts=0` separa recusa antes de OrderSend de uma resposta após envio.
Timeout/conexão/código estranho não são prova de rejeição: não autorizam nova
entrada. A política interna de retentativas do executor legado não foi alterada;
esta API não cria mais tentativas. Uma resposta não substitui a reconciliação do
histórico/ledger e não confirma eventos futuros.

Validação H: dois testes focados e checker, CI único39347476261 na fonte
`7ede8f18fc92b6347a721f9a91143bf13a315b2c` aprovado; fechamento documental.
MetaEditor sem erros/avisos para QA, smoke e seis Experts consumidores do header
alterado. SDK1 instala11arquivos e depois0. Cinco vetores no Tester exercitam
parcial/recusa/recotação/aceitação/incerto; não são preenchimentos parciais reais
da corretora. Uma entrada e saída próprias passam pelas guardas/executor reais
do Tester DEMO, com volume/preço/dono conferidos no histórico,0falhas. Relatório
e aplicação registram1operação fechada/1entrada comSL; HistoryDealsTotal3 é
contagem bruta de histórico, não3trades.

Provas versionadas em H: `B07_RESULTADO_NATIVO_20261019.json`,
`B07_SDK1_COMPAT_20261019.json`, `B07_PROJECAO_LEGADA_20261019.json` e
`B07_COMPILACAO_SEIS_20261019.json`. Fonte/fingerprints Safety mudaram: não afirmar
fonte idêntica ou nova paridade financeira. Projeção dos comandos do Open legado
preserva cálculos, guardas e retornos, ao retirar somente a observação. Evidência
financeira anterior mantém seu escopo; não houve campanha numérica duplicada.

PRs vinculadas: [HADOUKEN #21](https://github.com/Misael-art/projeto-indicadores-mt5/pull/21)
e [Safety #6](https://github.com/Misael-art/mt5x-safety/pull/6). Fonte H
`7ede8f18fc92b6347a721f9a91143bf13a315b2c`, fechamento documental
`3490a5e6684e696a332d84bb4e6c3013df43d96e`; S `92c52085dfd4ee7f5f504afc2f22bfdf5223a09d`.
Integrar H→S→este vínculo depois da revisão/checks obrigatórios. Código Python permanece idêntico a master9a2a79c e à fonte
aprovada da PR133: estes Markdown não exigem repetir os seis checks/suítes.

Nenhum novo Default/Manual/preset carregado ou S09 de UI é entregue por B07.
Um negócio de QA não é o percurso operacional. Próximo: consumidor nativo mínimo
B08/T19.23, S09 comSL/TP fixos, Default→Manual nomeado→backtest→preset carregado→
replay com negócios reconciliados na aplicação. Simulação Python, Tester nativo
e operação ao vivo DEMO são modalidades distintas; nenhuma ordem ao vivo foi
feita. Pullback/parcial/BE continuam bloqueados até T19.57; Windows físico específico.
