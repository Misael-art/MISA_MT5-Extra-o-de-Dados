# Revisão crítica do plano de dados — revisão2

Revisão realizada em22/10/2026. Método: releitura do plano, confronto com
AGENTS.md, código/contratos instalados e contexto Git do host; revisão das
dependências e definição de testes verificáveis. Não houve execução da implantação,
nova campanha financeira ou alegação de novos testes de runtime.

Foram identificados18 pontos de ambiguidade/risco. As correções estão integradas
ao plano revisado e ao prompt, preservando a revisão1. Os gates documentais e o
grafo são conferidos por script; a aplicação de cada requisito continua sujeita
a testes durante a implementação.

| ID | Ponto encontrado | Correção | Como conferir |
|---|---|---|---|
| R01 | As fases podiam virar barreiras para o primeiro consumidor | Recortes BD08a/18a/19c1 e cadeia vertical explícita | Seção6 e grafo sem ciclos |
| R02 | Relações básicas podiam depender de pesquisa financeira | BD15a/b separa exatidão do mecanismo e utilidade financeira | E23/24 e seção13 |
| R03 | Catálogo podia terminar na contagem atual | Inventário integral e classesA/B/C sem exclusão silenciosa | E01/13 e seção10 |
| R04 | Cadastro por símbolo não assegurava isolamento | Identidade de base/fonte/instrumento e migração de namespace | E03/04/26 |
| R05 | Migração de chaves e legado estava genérica | Leitores/escritores, repetição, interrupção e rollback aditivo | E35 e seção12 |
| R06 | Tempo e alvos futuros poderiam ser confundidos | Contratos de observação/confirmação/disponibilidade e maturidade | E07/08/19/27 |
| R07 | Mesmo nome podia ser tratado como fórmula equivalente | Variantes e dependência opcional reconciliadas | E09/10/11/15 |
| R08 | Cache global podia invalidar tudo numa expansão | Assinatura granular com migração e preservação histórica | E18/19 e seção12 |
| R09 | Função pronta podia ser confundida com caminho integrado | Aceite da cadeia completa e valores conciliados | E28–34 |
| R10 | Indicador nativo pronto para API podia ser promessa offline falsa | Captura persistida identificada ou indisponibilidade explícita | E15 e seção13 |
| R11 | Expansão automática podia virar cálculo quadrático irrestrito | Seleção/paginação/orçamento e pares alcançados | E22/25/29 |
| R12 | Bancada compacta não delimitava os arquivos do usuário | Tetos separados e limpeza só de temporários próprios | E02/22 e seção16 |
| R13 | Root antigo/WIP e releases podiam ser confundidos com base Git | Contexto do host e checkout limpo correto no prompt | Pré-voo e comparação da composição |
| R14 | Instalador poderia trocar o atalho durante o preparo | Backup/conferência de efeitos colaterais e retorno | E35 e seção16 |
| R15 | Hashes novos podiam ser aprovados sem revisar capacidade | Requalificação dos seis perfis e fonte instalada exata | BD21/E35 |
| R16 | API pronta podia significar só documentação | Serviço executável e contrato testado, transporte externo posterior | E28/29/32 |
| R17 | Conclusão podia esconder regra indefinida como feed ausente | Estados de impedimento distintos e catálogo integral auditável | Seção17 e E36 |
| R18 | Uma contagem de testes podia virar alegação de fonte atual | Evidência com commit/teste/hash/escopo e baseline fresco | Seções14/15 e prompt |

## Segunda passagem

Conferir que há caminho inicial executável sem esperar todo o catálogo; todos
os nós alcançam o aceite final; cada caso tem dono; a inspeção do host não é
apresentada como validação de software; existem instruções para fonte ausente,
regra indefinida, teste inicial falho, mudança de namespace, recurso esgotado e
rollback. O manifesto da entrega registra o resultado dos checks documentais.

O plano é uma especificação de execução, não uma declaração de que as lacunas
foram eliminadas. A fotografia inicial permanece identificada e inalterada.
