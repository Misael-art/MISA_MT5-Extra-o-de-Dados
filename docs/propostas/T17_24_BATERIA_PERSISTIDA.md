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

## Arquivos

- `mt5_extracao/robots/store.py`.
- `tests/test_ui_main_window.py`.
- Este plano, painel de status, continuidade e checklist de Windows.

## Passos

1. Preservar o erro e a base real; instalar ambiente/dev e executar a suíte antes
   de alterar o produto. Reproduzir a falha por persistência/leitura e pelo clique
   no card, incluindo ausência de operações e amostra sem perdas.
2. Completar o contrato do item `profit_factor_contract` com seu ícone de falha.
3. Acrescentar regressões de renderização e de seleção do card/aba Bateria em
   claro e escuro, verificando que a projeção rejeita e os registros continuam iguais.
4. Rodar os testes focados, a suíte completa e o doctor; operar a interface com
   os dados reais e repetir o fluxo pelo aplicativo instalado conectado à DEMO.
5. Registrar resultados, revisão própria, CI no SHA final, integração remota e
   verificação da versão instalada. Windows real permanece uma pendência explícita.

## Critérios de aceite

- [ ] O clique no card e a aba Bateria não geram `KeyError` nem erro de callback.
- [ ] PF indefinido continua reprovado, com motivo e orientação visíveis no texto.
- [ ] A consulta repetida não acumula itens nem modifica os registros históricos.
- [ ] Regressão falha no código anterior e passa com a correção.
- [ ] Suíte, doctor, operação no host e preservação registrados.

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
