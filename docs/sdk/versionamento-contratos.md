# B-11 — Versionamento e migração dos contratos

Política do recorte M19-B integrada pelas PRs M132/7e2178a e H14/d018f6c. Preserva o SDK1 e permite definir a
composição canônica em T19.2 sem esperar todo o Composer. Esta política não
implementa contratos nativos, módulos de execução ou autorização para operar.

## Formatos independentes

| Formato | Versão/papel | Compatibilidade |
|---|---|---|
| `<Expert>.robot.json` / `robot.json` | `contract:1`, declaração SDK existente | Campos e papéis atuais permanecem fechados; sem campos novos informais |
| Composição operacional | `composition_schema_version:1`, artefato lateral de T19.2 | Descreve sinal, entrada, orientação, ganho, proteção, filtros, dimensionamento e parciais |
| Capacidades | `capabilities_schema_version:1`, artefato lateral de T19.2 | Vincula suporte e inputs explícitos ao robô/versão/fingerprint e ao ambiente consumidor |
| Pacote de preset | Formato de T19.25 | Referencia composição/capacidades; não substitui o contrato SDK |
| Tipos MQL5 B-02–B-07 | Versões próprias, entregues pelos respectivos donos | Não existem só porque há composição serializada em Python |

Um robô legado não é reescrito nem migrado automaticamente. Falta de declaração
lateral não impede seus caminhos atuais; impede afirmar suporte a uma capacidade
nova. Nenhuma inferência pelo nome de input, por semelhança de estratégia ou pela
existência do simulador autoriza gerar parâmetro executável. O adaptador base usa
apenas contratos/mapeamentos explícitos e preserva os valores efetivos legados.

## Mudança e leitura

1. SDK1 continua sendo lido e validado sem alteração dos seus campos. Uma mudança
   incompatível requer outro número de contrato, leitor específico e guia de
   migração; não se publica SDK2 nesta entrega.
2. Leitores conferem versão e tipo antes de interpretar campos. Versão desconhecida,
   bool no lugar de inteiro, campo desconhecido ou valor não finito têm erro
   acionável; não converter silenciosamente para versão conhecida nem ignorar campo.
3. Cada formato lateral tem versão inteira explícita. A versão é parte da
   serialização/hash. Mudança incompatível sobe a versão; extensões só entram por
   schema e validação declarados, preservando os artefatos antigos.
4. Migração futura é explícita e preserva o original: cria novo artefato/revisão,
   registra versão de origem/destino e perdas de significado (ou recusa a migração).
   Nunca sobrescreve presets/bases do usuário nem herda validação automaticamente.
5. Identidade da configuração, rótulo do usuário, fingerprint do robô e hash da
   evidência são distintos. Datas, caminhos pessoais e credenciais não entram na
   composição semântica portável. T19.50 mantém a convenção de nomes.
6. Capacidade declarada, compatibilidade técnica, prova automatizada, ensaio nativo
   e autorização operacional são estados diferentes. UI/pacote mantém o estado e
   motivo reais. Capacidade ausente tem motivo, inclusive em rascunhos.

## Origem e distribuição nativa

A origem dos tipos do framework é o HADOUKEN, em
`Include/Framework/HADOUKEN/Contracts/` quando B-02–B-05 forem implementadas;
`RiskDecision` e `ExecutionResult` pertencem à camada Safety. Não se adiciona lógica
financeira, negociação ou executor aos tipos. Structs MQL5 copiáveis seguem a regra
sem strings/arrays; espelho Python e teste de equivalência cabem à integração B-10.

Adota-se a direção já proposta: duas pastas, uma biblioteca versionada, com origem
HADOUKEN e espelho distribuído por mt5x-safety. **A distribuição atual não muda**:
`robots/safety_lib.py` continua lendo um `include_dir`; nenhum campo informal é
adicionado a `safety.json`. A entrega das duas pastas requer schema de distribuição,
versão da biblioteca, instalador compatível, hashes e testes próprios em M/H/S,
vinculados às tarefas B-06/B-07/B-10. Só então um consumidor pode exigir essa versão.
Nenhuma substituição de includes do terminal ocorre nesta política.

## Ordem do primeiro consumidor

B-11 → T19.2 (schema/capacidades/adaptador base, sem prometer execução modular nativa)
→ contratos/adaptador nativos realmente usados → T19.25/50/55/26 → ensaio/aceite
M19-B/T20.30. B-09 completo e dois módulos de cada tipo não são requisitos de uma
cópia Manual do modo base; a montagem nativa ampliada conserva seu próprio aceite.
S09/T19.37 deve declarar sua variante e prova; EMA/ATR não recebe equivalência
silenciosa com outro setup. T19.7, Ultra e seleção automática mantêm o backlog.

## Verificação e limites

O leitor SDK atual já recusa versão2/9 e campo extra; os casos estão em
`tests/test_robots_contract.py`, incluindo a mensagem da CLI. Reaproveitar a execução
CI da fonte registrada enquanto os fontes/testes permanecerem idênticos; esta
política altera somente Markdown. Não justifica reinstalação, nova bateria,
recompilação ou jornada nativa. O comportamento dos novos leitores laterais é
verificado na própria T19.2. A política fica em PR vinculada nos dois repositórios;
integração respeita a autorização e os checks do fluxo. Windows físico permanece
pendente e não é condição de definição do formato.
