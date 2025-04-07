import logging
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime

log = logging.getLogger(__name__)

class ExternalDataSource(ABC):
    """
    Interface abstrata para fontes de dados históricos externas.
    Define o contrato que qualquer provedor de dados externo deve seguir.
    """

    @abstractmethod
    def is_configured(self) -> bool:
        """
        Verifica se a fonte de dados está corretamente configurada e pronta para uso.
        Por exemplo, se as chaves de API necessárias estão presentes.

        Returns:
            bool: True se configurada, False caso contrário.
        """
        pass

    @abstractmethod
    def get_historical_m1_data(self, symbol: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame | None:
        """
        Busca dados históricos M1 para um símbolo específico dentro de um intervalo de datas.

        Args:
            symbol (str): O símbolo do ativo (ex: "WINJ24", "PETR4").
            start_dt (datetime): Data e hora de início do período desejado.
            end_dt (datetime): Data e hora de fim do período desejado.

        Returns:
            pd.DataFrame | None: Um DataFrame Pandas contendo os dados OHLCV (colunas: 'time', 'open', 'high', 'low', 'close', 'real_volume')
                                 indexado por tempo (datetime) e ordenado ascendentemente.
                                 Retorna None se a busca falhar ou se a fonte não estiver configurada.
                                 Retorna um DataFrame vazio se não houver dados no período, mas a busca foi bem-sucedida.
        """
        pass

    def __repr__(self):
        return f"<{self.__class__.__name__}>"


class DummyExternalSource(ExternalDataSource):
    """
    Implementação "Dummy" da interface ExternalDataSource.
    Usada para testes e como placeholder quando nenhuma fonte real está configurada.
    """

    def is_configured(self) -> bool:
        """Sempre retorna True, pois não requer configuração."""
        log.debug("DummyExternalSource: Verificando configuração (sempre True).")
        return True

    def get_historical_m1_data(self, symbol: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame | None:
        """
        Simula a busca de dados M1, mas sempre retorna None.
        Loga uma mensagem indicando que foi chamado.
        """
        log.info(f"DummyExternalSource: Chamado para buscar dados M1 para {symbol} de {start_dt} a {end_dt}.")
        log.warning("DummyExternalSource: Esta é uma fonte de dados 'dummy' e não busca dados reais. Retornando None.")
        # Em um cenário de teste, poderia retornar um DataFrame vazio:
        # return pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'real_volume']).set_index('time')
        return None