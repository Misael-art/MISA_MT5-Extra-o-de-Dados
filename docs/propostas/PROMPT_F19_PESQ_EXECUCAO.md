Implemente no MT5X o plano de pesquisa de sinais, operacionais e presets da Fase 19. Comece pela F19-PESQ-01 e avance pelas 17 fichas conforme as dependências e os critérios de aceite registrados no projeto. A entrega esperada é código integrado, testes, evidências e continuidade atualizada. O plano já está escrito.

## Onde começar

Use o checkout existente do MT5X. No host Windows em que o plano foi preparado, o App fica em `E:\misael\Projects\MT5X\repos\app`. A composição também contém `repos/hadouken` e `repos/safety`. Em outro host, resolva a raiz pelo registro do projeto.

Leia os `AGENTS.md` aplicáveis, `CONTINUAR_AQUI.md` e, a partir da raiz do App:

- `docs/propostas/PESQUISA_SINAIS_E_OPERACIONAIS.md` — especificação das 17 fichas, regras, exemplos e aceites.
- `docs/PLANO_DE_TRABALHO.md` — seção “Fila de pesquisa de sinais e operacionais”, com responsáveis, dependências e situação.
- `docs/ROADMAP.md` — seção “Pesquisa de sinais de abertura, padrões e gestão de ciclos” e prioridades corretivas vigentes.
- `docs/dados/DIRETRIZ_BASE_ESTRATEGICA.md`, `docs/propostas/PLANO_F19_PILARES_PRESETS.md` e `docs/propostas/PLANO_F20_ROBUSTEZ.md` — contratos de dados, composição, execução, risco e validação.

O registro documental foi publicado no PR #373 do repositório `Misael-art/MISA_MT5-Extra-o-de-Dados`, commit `572a1eb89a6cdc90e4dc2b4cc9feb93c341721fe`, originalmente sobre a base do PR #372. Confira o estado atual do checkout, das branches, dos PRs e dos pins antes de editar. Preserve o trabalho em andamento. Se uma ficha já tiver implementação parcial ou PR próprio, confronte essa implementação com o aceite e complete a lacuna no local que já responde por ela.

## Primeira entrega — F19-PESQ-01

Comece agora pela qualificação da abertura do WIN por contrato e pregão.

1. Inspecione a estrutura de sessão existente, o importador B3, a identificação e vigência dos instrumentos, a elegibilidade dos dados, a fidelidade dos ticks e seus consumidores. Identifique onde o App conhece a abertura esperada, onde registra a primeira barra observada e o que acontece quando as duas divergem.
2. Implemente o diagnóstico no fluxo canônico. Registre fonte, contrato e vencimento, sessão, fuso, abertura esperada e observada, cobertura M1 e de ticks, lacunas e motivo de elegibilidade ou exclusão. Trate leilão, spread, agressor e fila conforme a evidência disponível. Preserve a distinção entre dado ausente e valor zero.
3. Cubra com fixtures uma abertura completa, primeira barra às 09h07, minuto ausente dentro da caixa, sessão especial e troca de vencimento. Para a caixa nominal 09h00–09h15, a chegada da primeira barra às 09h07 deve produzir um diagnóstico explícito de cobertura. A janela não pode ser deslocada silenciosamente para 09h07–09h22. A faixa só fica disponível depois do fechamento da última barra exigida.
4. Entregue um manifesto de cobertura por pregão, com dias elegíveis, excluídos e respectivos motivos. Verifique que o consumidor consegue distinguir esses estados. Se houver histórico WIN elegível no host, exercite o fluxo oficial com um recorte identificado e limitado. Se faltar histórico, registre exatamente a cobertura ausente e até onde as fixtures comprovam o comportamento.
5. Atualize a linha F19-PESQ-01 com arquivos alterados, testes, evidência, PR/SHA e situação real. Use esse resultado para orientar o livro de oportunidades e a paridade do ORB.

## Sequência de implementação

Use a tabela de dependências do projeto como referência. A numeração abaixo identifica o conteúdo de cada frente.

- **01–03 — Dados e método.** Qualifique a sessão do WIN, registre o ciclo completo de cada oportunidade e congele o protocolo de comparação. O livro precisa incluir sinais vetados, expirados, rejeitados, parcialmente preenchidos e executados, além dos pregões sem operação.
- **04–06 — ORB.** Reconcilie a referência Python/HADOUKEN. Depois isole os filtros de largura da caixa e distância do rompimento. Por fim, compare stop, alvo, parcial, break-even e trailing nas mesmas entradas e com risco comparável. Contabilize tanto as perdas evitadas quanto os ganhos descartados.
- **07 — Open Strike.** Confira a fórmula das bandas e o instante do sinal. Compare entrada direta com ordem stop pendente, incluindo validade, virada de sessão, gaps e corrida entre cancelamento e preenchimento.
- **08–10 — Primeiro reteste, CHoCH H4 e FVG.** Formalize eventos causais e controles pareados. Diferencie fechamento contrário de quebra de swing confirmado e meio da vela de impulso de meio da zona FVG. Avalie o sinal antes de adicionar uma grade.
- **11–15 — Gestão de ciclos.** Comece pelo contrato comum de grade, risco, margem, parciais e reconciliação. Depois implemente e compare médio para frente com reposição, entrada por banda com âncora congelada, lote assimétrico e hedge bidirecional. Verifique comportamento em reinício, fill parcial, eventos fora de ordem, gap e limite de exposição. Qualifique cada modo de conta efetivamente suportado.
- **16 — Harmônico à vista.** Avalie caixa mais estoque marcado a mercado. Compare com comprar e manter e com aportes parcelados sob os mesmos fluxos de capital. Inclua arredondamento, custos, eventos corporativos e queda abaixo do piso da grade.
- **17 — Gerador e promoção.** Para cada candidato elegível, vincule a definição ao Expert, à revisão, aos inputs e às capacidades. Percorra pesquisa, pacote, reabertura, `.set`, Tester DEMO e reconciliação. A ficha 17 pode avançar por candidato quando suas dependências estiverem satisfeitas. Default, Manual, Defensivo, Fator de Lucro e Ultra têm finalidades e evidências próprias.

## Regras para implementar e validar

- Reutilize os donos existentes. O App extrai, qualifica, pesquisa e gera presets. O HADOUKEN materializa sinal e política operacional no MT5. O Safety protege risco e execução. Preserve compatibilidade com dados e presets anteriores e siga o fluxo de revisão dos componentes fixados.
- Para cada ficha, identifique o comportamento de referência antes da mudança. Execute as regressões pertinentes, acrescente testes para a nova regra e valide pelo ponto de entrada usado pelo projeto. Quando houver paridade Python/MQL5, localize a primeira divergência por evento e reconcilie sinal, veto, ordem, fills, posição, custos e caixa. Um PF agregado parecido não encerra uma divergência de execução.
- Trate os vídeos como origem das hipóteses. Antes de observar o resultado, registre controle, variante, população, custos, risco, métrica de decisão e reserva temporal. Use apenas informações disponíveis no instante da decisão. Se a mudança afetar preenchimento, margem, capital disponível ou entradas posteriores, reexecute a cadeia completa.
- Para qualquer alegação de PF19, aplique integralmente `docs/pesquisa/PF19_PROTOCOLO_20261016.json`. Para o Defensivo, fixe previamente qual perda deve diminuir e qual deterioração da expectativa é aceitável. Para o Ultra, compare a seleção condicional com a melhor política estática escolhida no treino.
- Aplique os gates do host antes de builds, suítes longas ou Tester. Execute uma carga pesada por vez e use o ambiente oficial do projeto. A validação nativa deve usar Tester DEMO isolado. Não envie ordens em conta real nem use dados privados como fixtures.
- Mantenha distintos os estados de implementação, teste por fixture, aceite nativo e evidência econômica. Uma hipótese reprovada com evidência é uma entrega válida. Quando faltar dado inédito, conta compatível ou aceite nativo, registre o bloqueio exato, preserve o que foi verificado e avance nas fichas independentes já elegíveis.

## Continuidade e entrega

Conclua cada incremento revisável pelo fluxo Git do projeto. Atualize a linha da ficha em `docs/PLANO_DE_TRABALHO.md` e o ponto de retomada em `CONTINUAR_AQUI.md`. Atualize o roadmap quando mudar a situação de um marco ou uma dependência. Registre o identificador da ficha, arquivos, testes executados e resultados, ambiente, fonte e período dos dados, recibo reproduzível, PR/SHA, limitações e próxima ficha elegível.

Comece pela inspeção do código e dos testes da F19-PESQ-01. Prossiga enquanto houver trabalho autorizado e dependências satisfeitas. Ao encerrar, deixe explícito o que outro agente já pode usar, o que foi comprovado e de qual ficha ele deve retomar.
