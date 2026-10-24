# Prompt — agente do Laboratório e ferramentas gráficas

Você dará continuidade ao projeto **Misael-art/MISA_MT5-Extra-o-de-Dados**,
na frente Laboratório. O proprietário testou o atalho central em24/10/2026 e
reportou falhas e recursos ausentes. **O Laboratório não está concluído.** Execute
correções e expansão em recortes verificáveis, uma tarefa por PR, sem encerrar na
proposta ou numa captura sintética. Há outro agente no pilar de dados e um agente
paralelo para o Expert Super ATR; respeite as posses abaixo.

## Fonte e leitura obrigatória

Clone/fetch o repositório e obtenha `codex/lab-superatr-frentes`; ela contém este
prompt e o mapa documental sobre `codex/lab-multiframe`:
`af717fa90bd4d514fb226622862bca572ae41f88`. Runtime anterior
`7c66feda06eb065b5cd591fd22ba5b602cffa624`, PR210 em rascunho, sobre AppPR208;
dependências AppPR207/208 e HPR75. Não presuma que tudo está em master ou que
o diretório principal do host está limpo. Confira ancestry, remotos e prévia real.

Leia AGENTS.md integral, ROADMAP, PLANO_DE_TRABALHO, continuidade,
[backlog e acordo entre frentes](../propostas/LAB_SUPER_ATR_PENDENCIAS_20261024.md),
[repasse anterior](T20_22_LABORATORIO_CONTINUIDADE_20261024.md),
[SDK atual](../sdk/laboratorio_series.md), `docs/agente.md`, BD06/07/18/19c1,
T18/F18, T20.22/23 e o plano de comparação multi-TF. Confira também
G19.13 no HADOUKEN: `Documentation/HADOUKEN_2/propostas/G19_13_LAB_COMPARACAO_MULTITEMPOS.md`
na closure HPR75, `b730ed4a8df74f5f20eb6e289798d0232c370994`.

## Posse e forma de trabalhar

Crie branch `codex/` e worktree próprias. O núcleo desta frente é
`mt5_extracao/laboratory/`, `ui/laboratory.py`, `ui/laboratory_series.py`,
`ui/relations.py`, `ui/charts.py`, componentes/tema/tokens e testes próprios.
Não edite o WIP da raiz, ingestão, schema/store/query/produtores/registry canônico
de Dados, fontes do painel ou do Expert paralelo. Para CLI/SDK/catálogos/centrais
compartilhados combine dono e entregue delta para composição serial.

Reutilize ambiente/configuração existentes quando disponíveis; host novo usa
instalador previsto no AGENTS. Registre baseline e testes proporcionais antes e
depois, doctor e CI por HEAD; não desative testes nem altere Dados para esconder
timeout. A ampla anterior terminou143 após imprimir3014/259; seleção nova20/15
terminou0 e Tk16 passou. Preserve esses limites, sem chamá-los de aceite do usuário.

## Trabalho a executar

1. **Reprodução pelo atalho:** identifique commit da release, config/base/contexto,
   seleção, período, fonte, TFs e caminho de lançamento. Sem expor credenciais ou
   copiar a base inteira, reproduza Séries e confluências, incluindo gráfico vazio
   com detalhe uma barra/NaN. Capture erro/cobertura/warmup e callback de fundo.
   Corrija a causa no consumidor, mantendo gates de integridade. Um estado sem
   dados deve informar por quê e como corrigir, não exibir cinza como sucesso.
2. **Catálogo real:** Indicador × indicador e Indicador × alvo ainda mostram lista
   curta. `ui/relations.py` contém fixture sintética; não amplie a fixture como
   solução. Descubra todas as séries/variantes/outputs presentes pelo registry e
   serviço de Dados; busca, paginação, parâmetros, disponibilidade e motivo de
   bloqueio. Analytics52, Estudo nativo38 e séries do banco não são equivalentes.
   Consuma BD18a/BD19c1 com `available_at/revision`; não replique cálculo/store.
3. **Horários:** alinhe cabeçalho/corpo por uma geometria coerente e sincronize
   rolagem, resize, DPI, maximizar/restaurar e troca de monitor. Teste as bordas de
   todas as colunas com rolagem horizontal/vertical; não basta a primeira captura.
4. **Sistema gráfico:** modernize Dia do mês e faça inventário das demais visões.
   Fundo clean, tons intensos para magnitude forte e gradiente para tons claros
   fracos, escala divergente consistente e neutro visível. Dados ausentes não são
   zero. Valores horizontais legíveis; tire `n=…` repetido de cada barra e coloque
   amostra, amostra pequena e cobertura em legenda contextual/tooltip/detalhes.
   Preserve sinais de perdas, PF indefinido, moeda/custos e entrada/saída faltantes.
5. **Comparações distintas:** implemente a) mesmo ativo com estratégias/robôs
   diferentes; b) mesmo robô/preset de **revisão exata** entre ativos; c) mesma
   estratégia/params entre TFs. Compare contexto, snapshots, intervalo, capital,
   unidades/moeda, custos e motor; não use último run como prova de preset igual.
   Preço/sinais/eventos/equity precisam de legendas e fontes diferentes. Curvas de
   intenções não são trades. Reuse runs existentes e Budget/BatchRunner; análise
   cara é explícita, cancelável e não disparada ao abrir uma aba.
6. **Previsão:** separe correlação contemporânea, concordância de direção e
   antecipação. Fixe evento-alvo/horizonte/lag e regra de confirmação; use apenas
   dados disponíveis na decisão, rótulos maduros e avaliação cronológica fora da
   amostra. Mostre denominadores, eventos sem par, cobertura, incerteza e controles
   preço/fase/distância; linhas/TFs repetidos não são amostras independentes.
   Sem qualificação, rotule exploratório. Não prometa90–100% ou lucro por associação.
7. **Bancada do ativo:** candles com seleção robusta de todos os indicadores
   qualificados da base, múltiplas instâncias/params, sobreposição ou subjanela
   conforme unidade, legenda/ocultar/remover. Cursor/zoom/janela comuns entre TFs,
   escala comum ou local rotulada. Ferramentas de linha, horizontal/vertical,
   região e notas, edição/exclusão/desfazer, salvar/exportar/reabrir estudo ligado
   ao snapshot. Reuse ferramentas de Estudo quando pertinente e exponha operações
   por SDK/CLI, sem automação de mouse como API e sem limitar tudo ao MT5 externo.
8. **Monitores e presets:** botão ampliar/restaurar cada gráfico, layout amplo
   para monitor/janela acima1920×1080 e rolagem em tela pequena. Reconheça o monitor
   real de lançamento, coordenadas negativas/DPI e retorno sem dock. Preserve
   seleção/cursor/params ao ampliar. Reuse Default/Manual/compiler/store existentes;
   seis perfis nativos estavam bloqueados por pin/closure. Qualifique por perfil,
   sem remover gate ou gerar preset operacional a partir de correlação exploratória.

## Integração com as outras frentes

Dados continua dono de cálculo/persistência Super ATR e TFs derivados M7/M9.
Consuma o envelope acordado no backlog; mostre indisponível enquanto não houver
série qualificada. Não simule TF, fuso ou volume real para preencher gráfico.
Expert Super ATR continua dono do consumidor operacional; suas linhas e eventos
chegam pelo contrato versionado. Laboratório não envia ordens nem cria gestor.

## Aceite e publicação

- Testes de descoberta por nova definição, dados reais elegíveis e negativos
  (warmup/NaN/constante/revisão/lacuna), erro de worker e seleção obsoleta.
- Regressão de geometria/rolagem e inspeção visual claro/escuro, handheld e monitor
  amplo; leitura de todos os rótulos e restauração. Capturas saneadas sem conta.
- Cada comparação comprova identidade do preset/estratégia/dados e distingue
  intenções de execução. SDK/CLI reproduz a seleção e o JSON/export da UI.
- UI serve dados reais pelo serviço; fixture identificada fica só para QA.
  Testes sintéticos não encerram o bug encontrado no atalho do proprietário.
- Commit, push e PR por tarefa; atualizar mapa de pendências com evidência e
  limites. Compor apenas commits congelados, mantendo WIPs de Dados/Expert.
- Publicar a release testada no destino estável `mt5x/app-development/run.sh`
  segundo `docs/TESTAR_DESENVOLVIMENTO.md`, usado pelo atalho central e menu.
  Reusar dados/config/.env/venv, preservar rollback e canal aceito. Reabrir pelo
  atalho e verificar o runtime carregado e os percursos corrigidos. Sem mudar
  runtime, uma entrega documental não requer reinstalação.
- Não finalizar com “Laboratório concluído” enquanto houver LAB-01…10 sem aceite.
  Windows físico, paridade externa, calibração e aceites financeiros mantêm gates
  próprios. Limpe somente temporários/processos identificados como seus.
