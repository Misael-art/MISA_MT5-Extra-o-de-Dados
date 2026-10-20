# T20.30 inicial — primeiro percurso Manual histórico

Estado: percurso funcional observado no host; publicação em master aguarda a cadeia
#138–#146 e os checks do código final. Este relatório não conclui toda F19/F20,
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

HADOUKEN #15→#23 e Safety #5→#6 já integrados. Extrator #136→#137 integrado;
#138/#139/#140→#141→#142→#143→#144→#145→#146 aguardam seus checks finais e integração.
A dependência do recibo #138 está em ancestral real da #140; sua composição usa merge
Git e não cópia de arquivos. O fechamento registrará os merges e a identidade entre
o runtime integrado e os commits efetivamente aprovados. Alteração apenas documental
não invalida os checks do código; mudança relevante é verificada proporcionalmente.

Pullback continua bloqueado no novo adaptador até correção/validação da parcial
(T19.57). Não se bloqueia o piloto S09 por isso. Windows físico permanece pendência
específica (DPI/foco/NTFS/visual/cancelamento em host real); não bloqueia a entrega no
Linux. Este aceite funcional não conclui catálogo, Analytics, presets automáticos,
Ultra, estratégias complexas ou todas as fichas T19/T20.
