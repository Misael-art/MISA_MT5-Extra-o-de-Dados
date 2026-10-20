# Perfil S09 nativo — T19.23, recorte do primeiro percurso

`composition.native.S09NativeAdapter` mapeia somente a variante mínima aprovada
na HADOUKEN #23 (`d60e421`), robô `mql5:hdk_s09_referencia` versão1.00, fingerprint
SDK1 `91ff0ec8a7bba77e`. Fonte, includes, inputs, versão e contexto são conferidos;
edição após construir o adaptador é detectada ao gerar o snapshot. Sidecar importado
não concede suporte, nem substitui a autoridade instalada. SDK1 é preservado.

EMAs/aquecimento/pontos inteiros mapeiam explicitamente os inputs homônimos.
Fração de risco ×100 vira percentual:0.005→0.5%; não converter implicitamente o
número para outro significado. O Default efetivo do perfil nativo usa0.005, distinto
do default genérico do backtester Python; não sobrescrever uma composição existente
para caber no teto. Acima0.5%, compound=false, input int fracionário/fora de faixa,
filtros extras, inversão, limite/stop de entrada, alvoR e parcial são recusados.

Só entrada de mercado, uma posição, ganho/proteção da estratégia ou em pontos fixos,
sem parcial/BE/trailing, condicional desativada. Política explícita de saída prevalece
sobre o parâmetro correspondente da estratégia; `inactive_paths` torna essa precedência
visível. Não há duas origens para o mesmo input. Contexto, versão e ordem são mantidos;
SL/TP mínimos, tick, lote, margem e guardas permanecem validados no executor nativo.

A identidade pública do preset e hash canônico da composição são inputs reais;
`receipt_fields`/`expected_native_inputs` alimentam a conferência opt-in do Session
existente. O snapshot declara conversões e restrições fixas da fonte, sem inventar
inputs para desativá-las. Não envia ordem, instala Expert, escreve .set, concede
aceite ou habilita PLAY. O pacote/Default/Manual e replay devem consumir esse mesmo
serviço e comprovar os novos campos de identidade efetivamente carregados.

Evidência `docs/evidencias/T19_23_PERFIL_S09_20261019.json`: referência nativa anterior
com dois negócios e oito inputs operacionais iguais; novos campos de identidade
aguardam o percurso do pacote/replay. Testes determinísticos cobrem tipos/unidades,
limites, defaults, overrides, incompatibilidade/edição, perfis externos e gates.
Este recorte não migra os seis Experts, não conclui todas as capacidades T19.23,
não certifica paridade financeira nem operação ao vivo. Pullback permanece bloqueado
até corrigir/validar a parcial T19.57. Windows físico pendente.
