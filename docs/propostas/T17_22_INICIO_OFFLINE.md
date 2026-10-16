# T17.22 — iniciar a análise da base local sem conectar o MT5

## Problema e objetivo

O lançador principal inicia a conexão automaticamente; ela pode abrir o
terminal configurado. Os smokes anteriores usaram o módulo instalado com
configuração privada, sem exercitar esse lançador na configuração real.
Acrescentar uma opção explícita `--offline` para estudar a base local sem
abrir/conectar o terminal. A opção vale para a sessão e não altera o INI.

## Passos, nesta ordem

1. Instalar ambiente próprio e dependências de desenvolvimento, mantendo HOME
   intacto e atalhos/configuração/cache da instalação em XDG privado. Concluído.
   Baseline `8bacadf`: 890 aprovados/9 ignorados em 810,86 s, saída 0, antes de
   alterações do projeto. Inspecionar startup, encerramento, conector, ações
   de conexão/abertura e testes existentes antes da implementação.
2. Acrescentar `--offline` ao ponto de entrada. Preservar a execução padrão;
   nessa sessão, dispensar a resolução do SDK na verificação de dependências,
   passar o modo ao conector e encerrar somente a conexão pertencente ao app.
   O `finally` atual resolve o SDK e chama shutdown incondicionalmente: o modo
   local precisa evitar esse efeito também ao sair. Não persistir novos campos.
3. O conector offline não resolve nem inicializa o SDK; símbolos e timeframes
   locais continuam disponíveis. Inicialização/encerramento/diagnóstico de erro
   não podem alcançar o SDK nessa sessão. Não modificar o singleton de outro
   conector ou desconectar uma sessão que não pertence a este modo.
4. Mostrar claramente o modo local no cabeçalho. Não agendar conexão automática;
   ações de abrir/verificar conexão/administração ficam indisponíveis e
   informam que é preciso reabrir sem a opção. Guardar também os métodos de
   ação para que chamadas diretas não abram terminal, ofereçam conta demo ou
   iniciem workers de conexão. Importação, inventário, análises e backtests
   locais continuam utilizando o mesmo banco e os mesmos componentes.
5. Testar parser, startup/saída e ausência de chamadas ao SDK/terminal, além do
   caminho padrão. Simular Windows/Linux, conferir estado/ações/teclado e
   disponibilidade das análises. Executar focados, suíte completa, doctor e
   revisão técnica/CI em PR próprio. Nenhuma operação em conta será executada.
6. Depois da integração individual, iniciar realmente `./run.sh --offline` da
   principal em display próprio. Fazer backup consistente antes de usar a base,
   preservar a configuração real, percorrer Minha base/Análise/Laboratório,
   capturar a interface e encerrar somente a própria janela pelo protocolo
   nativo, conferindo a saída do processo. Não encerrar o terminal do usuário.

## Aceite e limites

O usuário consegue investigar dados locais com indicação clara do modo, sem
conexão ou abertura automática de MT5. Testes usam provedores falsos e não
exigem terminal/conta. A ponte RPC que o lançador Linux já inicia é infraestrutura
separada; seu startup somente importa o SDK e não inicializa/login/envia ordens.
Não será usada como prova de conexão operacional ou autorização financeira.

Sem novo painel, estatística, banco, toolkit, dependência ou chave de configuração.
Nenhuma alteração de estratégia, risco, custos ou ordens. Tarefas de proveniência
da agregação e aviso parcial permanecem separadas. Windows com MT5 real continua
pendência manual; testes simulados e CI não serão apresentados como essa prova.
