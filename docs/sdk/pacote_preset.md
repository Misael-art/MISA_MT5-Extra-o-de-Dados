# Pacote de preset S09 — T19.25 inicial

`presets.compiler` transforma composição canônica + identidade tipada + contexto
opcional de teste em manifesto fechado v1 e bytes `.set` UTF-16. Consome somente
S09NativeAdapter da fonte aprovada; nenhum sidecar externo concede capacidades.
Reutiliza `mt5_tools.write_tester_set` em diretório temporário próprio e o leitor
`mql.read_text/parse_set`, conferindo a ida/volta. Não instala nem inicia Expert,
não grava em diretório do usuário e não inventa formato concorrente de .set.

`PresetIdentity` separa ID, revisão, categoria, nome de apresentação e Default de
origem. Categorias iniciais: draft, Default e Manual; o Manual exige nome. Os nomes
padronizados de exportação são responsabilidade T19.50; este pacote usa somente
`parameters.set` interno. Default persistido e clone/edição/importação são T19.55/26.

`TestContext` registra datas ISO de dias completos, modelo suportado pelo serviço
existente e depósito **simulado explícito**. Não altera conta nem limites. Ausente,
o pacote permanece sem janela, útil como rascunho. Ativo/TF vêm da composição;
Safety, fonte, tipos/unidades e conversões vêm do perfil instalado. O consumidor
deve gerar o INI pelo serviço existente, mantendo esse contexto; o pacote não é
um comando de operação ao vivo. Cobertura/história efetiva só vêm do ensaio nativo.

`config_hash` conserva seu significado operacional e versão anteriores.
`package_hash` identifica todos os bytes/metadados/contexto. `execution_hash` inclui
composição, perfil/fonte, inputs, contexto e revisão; mudar somente o nome de
apresentação preserva essa identidade de execução, sem reescrever evidências.
Editar parâmetros, revisão ou contexto muda a identidade pertinente.

`load_package` valida schema fechado, recompila o contrato com a fonte atual e
confere exatamente o manifesto e .set. Fonte/versão desconhecida, input omitido,
duplicado, alterado, extra, arquivo divergente ou aprovação informal embutida são
recusados. Preserve o original para leitura/diagnóstico e gere nova revisão; não
converter silenciosamente arquivo legado. Os leitores/contrato SDK1 e .set legados
continuam inalterados; este loader trata somente pacotes novos canônicos UTF-16.

60 testes focados aprovados: ida/volta, precisão/unidade, contexto, identidade,
alteração/fonte, nome de apresentação, rascunho e contratos/SDK consumidores.
Ainda não é preset salvo/selecionável na UI, exportação com nome final ou prova de
carregamento nativo. Esses consumidores deverão ligar Session/recibo, executar
negócios e conciliar os resultados; nenhum campo deste pacote concede aceite.
Pullback bloqueado, execução ao vivo indisponível e Windows físico pendente.
