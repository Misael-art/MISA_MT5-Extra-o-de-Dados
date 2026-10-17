# T17.24 — Bateria persistida sem erro ao selecionar o robô

## Escopo registrado antes da implementação

O clique no card do painel HADOUKEN termina em `KeyError: 'icon'` dentro de
`RobotDetail.show` → `battery_text`. A reprodução com cópia consistente da base
real encontrou oito baterias do painel afetadas. Ao consultar uma bateria ligada
a uma execução cujo PF é indefinido, `store.load_checks` acrescenta o item
`profit_factor_contract`, de reprovação, sem a chave `icon` que o consumidor exige.
Os itens gravados normalmente por `Report.to_dict` já possuem essa chave.

A correção completa esse item na projeção de leitura. Preserva a reprovação por
PF indefinido, seus motivos e a ação de refazer a bateria. Nenhuma bateria antiga,
aprovação gravada, execução ou tabela do usuário será reescrita. Não altera
cálculos, critérios de aprovação, presets, risco, Experts ou operações em conta.

Dependências: T8.6 e T13.9. Uma tarefa e um PR, sem outras melhorias da interface.

### Ajuste do escopo após inspeção da mesma bateria

Com o ícone corrigido, a inspeção real em 1280×800 mostrou que o texto longo da
bateria cortava o motivo final e a orientação de recuperação. Antes de alterar
esse trecho, fica registrado o uso do `ScrollableContent` já existente somente
na aba Bateria, com teste de acesso ao fim do texto em janela pequena e grande.
A suíte intermediária foi interrompida para essa revisão e não vale como aceite.

## Arquivos

- `mt5_extracao/robots/store.py`.
- `mt5_extracao/ui/robot_detail.py` (rolagem da própria bateria).
- `tests/test_ui_main_window.py`.
- Este plano, painel de status, continuidade e checklist de Windows.

## Passos

1. Preservar o erro e a base real; instalar ambiente/dev e executar a suíte antes
   de alterar o produto. Reproduzir a falha por persistência/leitura e pelo clique
   no card, incluindo ausência de operações e amostra sem perdas.
2. Completar o contrato do item `profit_factor_contract` com seu ícone de falha
   e reutilizar a área rolável para alcançar o motivo final da bateria.
3. Acrescentar regressões de renderização e de seleção do card/aba Bateria em
   claro e escuro, verificando que a projeção rejeita e os registros continuam iguais.
4. Rodar os testes focados, a suíte completa e o doctor; operar a interface com
   os dados reais e repetir o fluxo pelo aplicativo instalado conectado à DEMO.
5. Registrar resultados, revisão própria, CI no SHA final, integração remota e
   verificação da versão instalada. Windows real permanece uma pendência explícita.

## Critérios de aceite

- [x] O clique no card e a aba Bateria não geram `KeyError` nem erro de callback.
- [x] PF indefinido continua reprovado, com motivo e orientação visíveis no texto.
- [x] A consulta repetida não acumula itens nem modifica os registros históricos.
- [x] O fim de uma bateria longa fica acessível pela rolagem, em claro e escuro.
- [x] Regressão falha no código anterior e passa com a correção.

O aceite de integração exige a suíte completa após a correção, CI no SHA final,
revisão e verificação da versão instalada. Esses resultados e o estado de
integração são registrados no PR da tarefa; os testes focados abaixo não
substituem esses gates.

## Verificação

Ambiente Linux instalado com `./install.sh --yes --no-mt5`, dependências dev e
display Xvfb próprio. Nenhum teste automatizado exige MT5 ou conta reais.

```bash
.venv/bin/python -m pytest -q tests/test_profit_factor.py tests/test_robots_battery.py tests/test_ui_main_window.py
.venv/bin/python -m pytest -q
.venv/bin/python -m mt5_extracao.bootstrap doctor
```

A conferência manual no host usa os dados existentes, sem executar novas
baterias financeiras ou ordens. Evidências privadas não entram no repositório.

## Evidências da correção

- Baseline sem a alteração: **963 aprovados e 9 pulos existentes**, saída 0,
  antes da implementação. Ambiente/dev instalados em worktree isolada.
- Reprodução anterior: três falhas por `KeyError: 'icon'`, inclusive o callback
  do clique; a cópia real reproduziu o defeito nas oito baterias do painel.
- Após a correção: **51 testes de PF/bateria** e **quatro regressões de UI**
  aprovados. O teste da largura também falhou antes de ajustar a quebra de linha
  à largura útil. Page Down é exercitado com o foco no canvas.
- Interface real, em modo local sobre cópia consistente da base: 15 robôs ×
  claro/escuro × 1280×800/1920×1080, **60 seleções**, com motivo final acessível
  e oito capturas geradas, com inspeção do texto longo nas quatro combinações.
  Baterias, execuções e ledger permaneceram
  idênticos na comparação antes/depois. O processo da aplicação encerrou com 0.
- Doctor da instalação operacional: **21 OK, um aviso (`pandas-ta` opcional
  ausente), nenhuma falha**. Na worktree sem MT5, a falta deliberada do terminal
  é registrada separadamente, com saída 1 por essa pendência de instalação.
- As suítes intermediárias interrompidas durante a inspeção da bateria não
  contam como aceite. A suíte completa final e o CI são condições do merge.
- A conferência conectada à DEMO pelo `run.sh` instalado pertence ao smoke de
  integração registrado no PR; a cópia local acima não é apresentada como esse smoke.
- Windows físico permanece pendente no checklist; CI Windows é evidência distinta.
