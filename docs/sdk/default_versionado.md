# Default versionado — T19.55 inicial

`presets.defaults.ensure_default` cria um snapshot imutável dos defaults efetivos
S09 no banco já selecionado. Scope identifica robô/versão/fingerprint, perfil,
ativo/TF e configuração padrão. ID determinístico do scope e unicidade no banco
fazem reabertura, repetição e criação concorrente idempotentes. Fonte/perfil/contexto
novo cria outro Default; revisões anteriores continuam legíveis, sem sobrescrever
arquivo, fonte, preset do usuário ou registro antigo.

Tabela aditiva `_preset_revisions` preserva manifesto, .set, identidade/contexto e
hashes. Não altera tabelas de mercado, runs ou finanças. O pacote segue fechado v1,
sem concessão de aceite; compilado não significa conferido nativamente. Risco do
Default é0.5% efetivo do perfil nativo, distinto do default genérico Python, explícito
no snapshot. SL/TP/períodos e restrições continuam os aprovados no mapper/Safety.

`StoredPreset` entrega cópias JSON, composição e nome canônico T19.50. Pacote para
execução só reabre com fonte/versão compatíveis; snapshot antigo permanece consultável
mesmo quando a fonte atual difere. Não migrar, rematerializar ou executar silenciosamente.
Default não tem edição pública; o consumidor T19.26 deve clonar como Manual nomeado.

`_preset_selections` mantém ID/revisão por robô/ativo/TF. Criar/reabrir Default não
muda seleção existente; contexto diferente não pode receber revisão alheia. O consumidor
pode escolher Default se não há seleção, preservando escolha anterior na atualização.
Nenhuma preferência dos cards/arquivos legados é modificada por esta API.

87 testes focados aprovados: idempotência, concorrência, snapshot/fonte imutáveis,
reabertura, seleção e contexto, versão de fonte anterior preservada, dados existentes
intactos e consumidores de pacote/nomes/mapper/SDK1. Teste PostgreSQL no job existente.
A API não instala/inicia Expert, cria UI, exporta no usuário ou aprova operação.
Manual/exportação/replay/UI são consumidores seguintes; Pullback bloqueado e Windows
físico pendente. Não é o catálogo completo de presets ou aceite F19.
