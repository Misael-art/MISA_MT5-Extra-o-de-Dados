# PM12 — perfis dos Experts com painel próprio

S09 1.90 e Pullback Nativo 1.10 usam o PanelHost dentro do seu próprio adaptador.
Os perfis são adicionais: S09 1.00–1.80 e Pullback Nativo 1.00 continuam com pins,
parâmetros e capacidades anteriores. Default permanece preservado; Manual nomeado
usa a exportação, reabertura e conferência de inputs existentes.

| Fonte H1024cee0 | Fingerprint | Inputs | Perfil/capacidades |
|---|---|---:|---|
| S09 1.90 | b069f6b52c76c77d | 44 | revisão10; mesmas capacidades da1.80 |
| Pullback Nativo1.10 | e1b14494047477b2 | 23 | revisão3; mesmas capacidades da1.00 |

Não surgem entradas manuais, editor de risco, BE manual ou boleta. No Tester visual,
pausar bloqueia novas entradas e conserva a gestão parcial/temporal já existente;
a zeragem exige confirmação e mantém o escopo do magic do host. O painel não destrava
uma execução congelada que exige reconciliação. Inputs e limites não aumentam.

A closure contém todos os headers do projeto. `Canvas/Canvas.mqh` pertence à
biblioteca padrão MT5: o catálogo o registra como externo; somente esse nome exato
é permitido nesses novos perfis. Header próprio ou outro include ausente bloqueia.
A compilação no destino continua exigindo a biblioteca padrão instalada; esta exceção
não concede compilação, carregamento nem aceite operacional por si só.

Validação local: baseline8 testes não gráficos; depois89 testes com Tk em Xvfb,
incluindo perfil/pins, UI Default/Manual nomeado/reabrir/exportar, inputs e isolamento.
O catálogo real das duas fontes confirmou fingerprints,44/23 inputs e exportação dos
valores esperados. É conferência estática; não é recibo dos inputs carregados.
Os dois Experts compilaram0erros/0avisos no MetaEditor6276/Wine em árvore isolada.

Dependências: H#33 e H#44, distribuição completa Safety/Canvas e revisão/checks desta
PR. `native_tester_demo_only` e `operational_acceptance=false` permanecem. Tester
visual integrado, conferência de inputs carregados desta revisão, execução ao vivo,
cache e Windows físico têm aceites separados e continuam pendentes.
