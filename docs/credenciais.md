# Documentação: Gerenciamento de Credenciais

## Visão Geral
O sistema de gerenciamento de credenciais fornece um método seguro para armazenar e acessar credenciais sensíveis, como senhas e chaves de API, sem expô-las diretamente no código fonte.

## Funcionalidades

### Armazenamento Seguro
- Armazena credenciais em arquivos criptografados ou protegidos
- Utiliza variáveis de ambiente através de arquivo .env para maior segurança
- Impede que credenciais sejam incluídas em sistemas de controle de versão

### Gerenciamento de Credenciais do MT5
- Armazena login, senha e servidor para conexão com MetaTrader 5
- Permite salvar múltiplos perfis de conexão
- Facilita a troca entre diferentes contas e servidores

### Proteção de Dados
- As credenciais são carregadas na memória apenas quando necessário
- Implementa timeout para limpeza automática de credenciais da memória
- Fornece métodos seguros para obtenção de credenciais sem exposição direta

## Como Usar

### Configuração Inicial

1. Na primeira execução, o aplicativo solicitará suas credenciais do MT5:
   - Login (número da conta)
   - Senha
   - Servidor (opcional, será detectado automaticamente se não fornecido)

2. As credenciais serão salvas de forma segura em arquivos locais:
   - `.env` para variáveis de ambiente
   - `credentials.json` para versão criptografada (quando implementada)

### Uso no Aplicativo

1. O aplicativo carregará automaticamente as credenciais necessárias
2. Caso as credenciais não estejam disponíveis ou estejam inválidas:
   - Uma caixa de diálogo solicitará as informações necessárias
   - Opcionalmente, você pode marcar a opção "Salvar estas credenciais"

### Gerenciamento de Perfis (Quando Implementado)

1. Acesse o menu "Configurações" > "Gerenciar Credenciais"
2. Na janela de gerenciamento:
   - Adicione novos perfis de conexão
   - Edite perfis existentes
   - Remova perfis não utilizados
   - Defina o perfil padrão

## Notas de Segurança

- **NUNCA compartilhe** seus arquivos de credenciais (`.env`, `credentials.json`)
- Adicione estes arquivos ao `.gitignore` para evitar compartilhamento acidental
- Recomenda-se utilizar diferentes credenciais para ambientes de desenvolvimento e produção
- Em caso de suspeita de comprometimento, altere suas senhas imediatamente

## Solução de Problemas

### Falha na autenticação com o MT5
- Verifique se as credenciais estão corretas
- Confirme se o servidor está correto e acessível
- Tente reconectar manualmente com as mesmas credenciais no aplicativo MT5

### Arquivos de credenciais corrompidos
- Remova os arquivos `.env` e/ou `credentials.json`
- Execute o aplicativo novamente para recriar os arquivos com novas credenciais

### Credenciais não estão sendo salvas
- Verifique as permissões de escrita no diretório do aplicativo
- Confirme se o diretório não está protegido contra escrita 