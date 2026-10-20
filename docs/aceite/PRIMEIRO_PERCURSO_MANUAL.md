# T20.30 inicial — primeiro percurso Manual histórico

Estado: percurso funcional histórico observado e runtime integrado em master pelas
PRs #138–#146, após revisão e seis checks obrigatórios do respectivo código final. Este relatório não conclui toda F19/F20,
a seleção automática T19.35, nem todos os requisitos Q01–Q16.

## O que a pessoa consegue fazer

Na aba **Robôs**, importar a biblioteca HADOUKEN integrada (layout Experts/Include),
selecionar `HDK_S09_Referencia` e escolher Ativo / TF. **Default / Manual**, BACKTEST
ou REPLAY abrem o editor do recorte. O Default é somente leitura; **Salvar como
Manual** exige nome e mostra o nome do arquivo. A cópia conserva a origem Default.

O Manual permite EMA curta/longa, aquecimento e SL/TP fixos. **Salvar revisão** não
reescreve a anterior. **Reabrir** recupera a revisão selecionada; renomear conserva
ID e evidência operacional. Datas e capital são explicitamente simulados. Exportar
publica manifesto e .set UTF-16 pelo serviço existente, sem sobrescrever conteúdo
divergente. Importar Manual usa o loader do mesmo contrato; não converte .set legado.

**Backtest nativo** e **Replay nativo** executam o pacote exportado em perfil próprio.
A aplicação confere o arquivo apontado no INI e os inputs efetivamente lidos no MQL5.
**Cancelar esta sessão** e o STOP existente usam o mesmo gerenciamento de processos.
A consulta aos resultados salvos mostra negócios, inputs e sinais com origem,
confirmação e disponibilidade; recupera também os arquivos/CSV do run depois de
reabrir a interface. A marcação temporal usa o relógio do servidor nativo.

Não editar configurações/credenciais do usuário para reproduzir a prova: usar banco
e pasta de exportação de teste. O ensaio importou a biblioteca integrada em base
privada; os worktrees locais anteriores e o config.ini do usuário foram preservados.
Não executar PLAY. O perfil S09 exige Tester DEMO e bloqueia execução ao vivo.

## Prova registrada

[Recibo completo](../evidencias/T19_29_MANUAL_NATIVO_20261020.json) e
[serviço de conferência](../sdk/conferencia_preset_nativo.md).

| Item | Observado |
|---|---|
| Estratégia | S09 referência mínima EMA, versão 1.00; FP `91ff0ec8a7bba77e` |
| Preset | Manual “Piloto S09”, revisão 1, origem Default preservada |
| Contexto | EURUSD/M5, 2026-10-14→15, cada tick, capital simulado 100000 USD |
| Parâmetros | EMA20/50, aquecimento250 barras, SL100/TP200 pontos, risco0,5% por saldo |
| Pacote | Mesmo execution_hash e SHA-256 do .set em ambas as passagens |
| Carregamento | Dez inputs realmente lidos/iguais; Tester/DEMO e Visual lidos no MQL5 |
| Backtest nativo | Dois negócios encerrados, quatro sinais; zero entradas sem SL |
| Replay nativo | Dois negócios encerrados, quatro sinais; Visual efetivo=true |
| Conferência | CSV, livro e saldo conciliados; negócios/sinais semânticos idênticos ao backtest |
| Resultado local | 1792,85 USD; sem perdas na amostra, sem aprovação estatística |
| Causalidade | Origem < confirmação ≤ disponibilidade; duas entradas e duas recusas por uma posição máxima |
| Reabertura | Outra execução da UI consultou a evidência e recuperou arquivos/CSV pelo run salvo |
| Ponte/principal | Mesmos processos antes/depois, conta DEMO conectada; sem initialize/shutdown/login/ordem ao vivo |

O backtest aprovado foi reutilizado. Duas tentativas visuais em Xvfb separado
falharam com `BadWindow` antes do Expert iniciar; twm não resolveu. Repetir somente
o replay na tela :0 usada pelo Wine resolveu a inicialização. São falhas preservadas,
não ensaios com resultado financeiro nem busca de janela/parâmetros lucrativos.

O visualizador próprio permaneceu depois do launcher. A prova original mantém
`resources_released=false`; um recibo adicional confirma seu encerramento pelo
`Session._stop_owned` existente, sem tocar ponte/terminal principal. O caminho normal
agora chama esse mecanismo após importar o relatório, antes de liberar contexto.
Os 82 testes pertinentes após a correção cobrem isolamento e ciclo de vida; não foi
necessário recalcular negócios para testar o mesmo mecanismo de encerramento.

## Modalidades separadas

| Modalidade | Alcance do aceite |
|---|---|
| Simulação Python | Motor OHLC/fills modelados e referências automatizadas existentes; não é o replay MT5. Não se afirma igualdade financeira Python↔MT5 |
| Histórico nativo | Backtest e Replay Visual do Manual com negócios e resultado reconciliado na aplicação |
| Ao vivo DEMO | Não exercitado; latência, rede, fila/fill e recuperação ao vivo continuam pendências próprias |
| REAL | Não operada nem autorizada pelo recorte |

O histórico da corretora não tem revisão qualificada; cobertura de ticks reais não
foi informada. Modelo “cada tick” e qualidade de histórico100% não certificam ticks
reais completos. Cache qualificado=false, promoção estatística=false e autorização
ao vivo=false. Dois negócios não demonstram desempenho esperado, robustez ou OOS.
Tempos locais de sessão não são metas universais.

## Matriz Q01–Q16 do recorte

| Q | Cobertura usada e limite explícito |
|---|---|
| Q01 | Referências existentes e saldo/CSV/livro nativos conciliados; não certifica todos os cálculos/custos/cenários |
| Q02 | Fonte/composição/pacote identificados; revisão histórica/specs vigentes da corretora não qualificados |
| Q03 | Modelo e qualidade efetivos preservados; realticks desconhecido; sem afirmar fila/agressor |
| Q04 | Mapper fechado S09 e versões/inputs; combinações não suportadas recusadas; não habilita seis EAs em composição |
| Q05 | Safety com SL, risco por saldo e uma posição; não certifica margem compartilhada, grids, parcial ou fill parcial |
| Q06 | Sinais/entradas/saídas/volume/preços/SL/TP e resultado conciliados; primeira divergência não é ocultada; seis pares legados não recebem novo aceite |
| Q07 | Janela funcional fixa, sem treinamento/otimização/reserva consultada para ajuste; protocolo estatístico global continua próprio |
| Q08 | Sem seleção Defensivo/FatorLucro/Ultra; PF indefinido/atividade mínima não são promoção |
| Q09 | Sem ablação/FRP/OOS ou novas campanhas numéricas |
| Q10 | Pacote/identidade/revisão/configuração/execução separados; rótulo não muda execution_hash; evidência imutável e sem autorização de ordem |
| Q11 | Sem acompanhamento prospectivo ou alarmes calibrados; ao vivo não exercitado |
| Q12 | Duas falhas anteriores preservadas; cancelamento/ausência/incerteza cobertos por fakes; limpeza própria exercitada; não certifica restart/fills/rede ao vivo |
| Q13 | Um robô/ativo/posição; sem carteira síncrona ou soma de curvas como capital compartilhado |
| Q14 | UI real, tabelas/inputs/sinais/consulta, dois temas, teclado, reflow/rolagem; gráfico/inspetor e todos os modos/layouts T20.22/23 não concluídos |
| Q15 | Session/lease/isolamento/retensão existentes, sem jobs duplicados; cache qualificado continua inelegível por revisão histórica desconhecida |
| Q16 | Contratos/SDK existentes e fontes preservados; não certifica terceiros, restauração completa ou release Windows físico |

### UI exercitada sem repetir cálculos

[Guia do consumidor](../presets/T20_22_PERCURSO_MANUAL_UI.md). Seis testes de widgets
com serviços reais e fronteira MT5 falsa cobrem edição/persistência/bloqueios e zero
negócios sem aceite. O UIManager/card/editor reais também executaram o percurso nativo.
A consulta posterior operou claro1280×800/DPI96, escuro900×620/DPI96,
claro820×700/DPI144 e escuro1280×800/DPI144; DPI solicitado no Tk e valor efetivo
registrados, sem alegar teste de hardware físico. PageDown, Tab, seleção/resultado
persistidos e campos não salvos após troca de tema passaram, sem erros de callback.
A conversão de `wraplength` vazio foi corrigida após falha real dessa consulta.

## Integração e continuidade

HADOUKEN #15→#23, Safety #5→#6 e Extrator #136→#146 estão integrados.
A #139 entrou após #137. A #140 incorporou a #138 por ancestralidade real, mantendo
seu HEAD/commits; GitHub confirma ambas integradas. Depois, #141→#142→#143→#144→#145→#146
seguiram suas dependências. Cada árvore final ficou inteira idêntica à fonte com
seis checks SUCCESS. [Recibo de integração](../evidencias/T20_30_INTEGRACOES_20261020.json)
conserva HEADs/árvores/checks/merges. Não houve cópia indiscriminada, force, reset ou
substituição de trabalho local. O fechamento somente documental reaproveita os
checks do runtime final; sem repetir suíte/workflow/jornada por documentação.


Pullback continua bloqueado no novo adaptador até correção/validação da parcial
(T19.57). Não se bloqueia o piloto S09 por isso. Windows físico permanece pendência
específica (DPI/foco/NTFS/visual/cancelamento em host real); não bloqueia a entrega no
Linux. Este aceite funcional não conclui catálogo, Analytics, presets automáticos,
Ultra, estratégias complexas ou todas as fichas T19/T20.


### Registro por PR

| PR | Fonte aprovada (6 checks) | Merge |
|---|---|---|
| [#139](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/139) | `8bc9b3515b04d7048072df7e52fa8c40245b890b` | `0b62de5dbb9b69d72b25dc0678c8d2369431a1b2` |
| [#140](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/140) | `0aa9e337d39a3b1b165a28a708df1a4f8ed21978` | `b69d112771acd8e350d710c725a79e03bdc3838a` |
| [#141](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/141) | `f5fd0f4b2165cec38bf6682fef8c41338d9123ad` | `fe9e202311b3844d92d33402701d98b73ecc5f19` |
| [#142](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/142) | `83eeafffde3913010133a43269afdd7a98b12731` | `b3cb9089b32392757cfc79ff7c8f36863f533515` |
| [#143](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/143) | `c6aeae003af455d3c78526df454324fb64c7123b` | `e8f765fa117ecfe58e12a0744354566361fd0cf9` |
| [#144](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/144) | `5dbeae28e2c4b85920d786a3f729c3ffbbdbe408` | `dd2d2d163447e2ea131403f71921be5d239855ec` |
| [#145](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/145) | `e20fd793084bf08a2eb0dc65fae9bd16c2e931b0` | `27dfb759ccd8d7371f79c6af2ae50176810df3ae` |
| [#146](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/146) | `85118159628b39ff1e0f237d60729e490ae78e63` | `50e54714735de2f266efae07d83ba957665ebd96` |

O recibo #138 tem merge interno `f369dff7108150a043a10f4183846eb343397a0b` e chegou a master pela #140 (`b69d112771acd8e350d710c725a79e03bdc3838a`), com a árvore consolidada aprovada.


O fechamento encontrou grupos de tarefas no painel (T19.17–23, T19.25–29 e
T19.36–47), não linhas individuais para cada recorte. A diferença foi registrada
na PR147; os grupos foram preservados e somente a cobertura entregue foi atualizada.

O check Windows144 anterior falhou em dois testes que liam o manifesto UTF-8 com
a codificação padrão; o loader real já usa UTF-8. Três leituras da fixture ficaram
explícitas e dois casos passaram. Os HEADs finais144/145/146 têm seis checks SUCCESS;
a propagação mudou somente tests/test_manual_presets.py. A prova de UI produzida
no d8a4844 e os negócios nativos continuam compatíveis: runtime inalterado após
esse ajuste de teste, sem nova campanha nativa.
