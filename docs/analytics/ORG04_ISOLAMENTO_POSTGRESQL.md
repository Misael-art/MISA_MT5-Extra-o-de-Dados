# ORG-04 — isolamento dos testes PostgreSQL

Recorte de `80fd2f3`, sobre a base documental ORG-02 (`c390b04`). Não importa
ancestrais Analytics. A implementação de aplicação e as telas não mudam.

## Comportamento

As duas fixtures usam `tests/postgres_helpers.py`: schema `mt5x_test_<uuid>` por
contexto, `search_path` restrito ao schema criado e remoção somente desse schema
no encerramento. Opções preexistentes da URL permanecem. URL ausente é recusada
antes de conectar. Uma falha ao criar o schema não concede propriedade para removê-lo.

A inspeção da base atual encontrou dois caminhos adicionais que escapavam do
recorte antigo: o teste UTC abria a URL bruta, e o teste de serviços também criava
as tabelas de controle em `public`. Ambos passam a usar contexto isolado; o UTC
usa schema próprio para não misturar sua base de tempo com o teste padrão.
Isso aplica o requisito desta missão de isolar ambas as fixtures e preservar
`public`; não traz os imports de feature store da branch antiga.

## Aceites e reprodução

- Testes unitários sem servidor: `python -m pytest -q tests/test_postgres_helpers.py`.
- Testes reais: `python -m pytest -q tests/test_postgresql.py tests/test_database_url.py tests/test_trade_analytics_pg.py tests/test_postgres_helpers.py tests/test_postgres_isolation_pg.py`.
- Usar `MT5X_TEST_PG_URL` somente de contêiner/banco descartável exclusivo.
  Sem a variável, a integração continua explicitamente ignorada.
- `test_postgres_isolation_pg.py` é incluído pelo padrão `tests/test_*_pg.py` do CI.
- Sentinela em `public`, tabela homônima no schema isolado, limpeza em sucesso,
  exceção do teste, falha de construção, banco desconectado e falha de fechamento; colisão de nome
  preserva schema alheio; dois contextos simultâneos veem tabelas independentes.

Baseline Linux/Python 3.11.15/Xvfb, sem URL PostgreSQL: **837 aprovados, 9 ignorados
em 853,63 s**, saída 0, antes das mudanças. Instalador documentado e dependências
dev executados; HOME/XDG do instalador isolados para preservar atalhos do usuário.
Os resultados posteriores e checks no SHA exato acompanham o PR deste recorte.

Nenhuma ramificação por sistema operacional é introduzida. Windows real e
TimescaleDB não foram exercitados nesta tarefa; não se infere aprovação dessas
plataformas a partir do PostgreSQL 16 no Linux.

Primeira execução PostgreSQL: 22 aprovados e uma falha de interpolação no teste de
serviços (`%3D` na URL). A configuração desse teste foi alinhada à aplicação com
`ConfigParser(interpolation=None)`; repetição: **23 aprovados**, sentinela intacta,
nenhum schema de teste remanescente e contêiner removido, saída 0. Foi acrescentado
também o caso de banco desconectado; a bateria final no SHA do PR cobre esse caso.
