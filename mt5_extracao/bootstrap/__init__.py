"""
Configuração inicial e diagnóstico da aplicação MT5 Extração.

Este pacote usa SOMENTE a biblioteca padrão do Python para que possa ser
executado pelos instaladores logo após a criação do ambiente virtual, antes
mesmo de todas as dependências estarem disponíveis.

Uso:
    python -m mt5_extracao.bootstrap configure   # gera config/config.ini e .env
    python -m mt5_extracao.bootstrap doctor      # verifica o ambiente
    python -m mt5_extracao.bootstrap locate-mt5  # lista instalações do MT5
"""
