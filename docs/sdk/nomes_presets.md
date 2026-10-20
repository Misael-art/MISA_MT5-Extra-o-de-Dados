# Convenção de nomes dos presets — T19.50 inicial

Formato v1: `Robo__Ativo__TF__Categoria__[NomeManual]__Orientacao__vNNN__Hash.set`.
`NomeManual` só aparece na categoria Manual. Exemplo:
`HDK-S09-Referencia__EURUSD__M5__Manual__Sao-Joao__Normal__v001__7edeaabfb546.set`.

O nome escolhido permanece Unicode no rótulo/manifesto; slug portátil ASCII vai
para o arquivo. ID estável, revisão, hash operacional e evidência não são nomes.
Hash usa12 caracteres do SHA-256 canônico completo (mínimo previsto8), sem gerar
hash a partir do rótulo. Reexportação da mesma identidade/revisão/configuração é
estável; arquivo/revisão anterior não é substituído por este serviço puro.

Default usa nome automático e rótulo `Default · Padrão do código`; Manual exige
nome1–80 caracteres. Sem nome exportável, o rascunho não gera .set. Orientação é
Normal/Inverso; o rótulo não concede suporte à inversão. Categorias automáticas
continuam indisponíveis no compilador inicial. Leitura reconhece Defensivo,
FatorLucro/alias Maximalista e Ultra, sem implementar seus geradores.

Slug preserva letras/dígitos, translitera acentos e separa palavras por hífen;
controles, resultado vazio e reservados Windows CON/PRN/AUX/NUL/COM1–9/LPT1–9
são recusados com ação. Stem máximo180 caracteres, sem truncar hash/identidade.
Comprimento do caminho completo é responsabilidade do exportador; Windows físico
permanece pendente. A mesma regra pura vale em ambos os sistemas.

`collision_key` ignora caixa. João/joao e símbolos/nomes com a mesma representação
portátil podem colidir: o serviço de persistência/exportação deve recusar colisão
entre identidades e nunca sobrescrever arquivo divergente. Nome distinto não
qualifica parâmetros, e parâmetros iguais de presets diferentes não fundem IDs.

`inspect_name` reconhece a convenção/alias e conserva o filename original. Legado
fora da convenção continua identificado como legado, sem renomeação obrigatória
ou escrita automática. Importação/persistência são consumidores T19.26; isto não
instala, executa ou aprova preset.51 testes focados aprovados, incluindo compilador
consumido. UI, Default persistido, Manual salvo e replay seguem pendentes nesta PR.
