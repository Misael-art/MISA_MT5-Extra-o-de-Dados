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

class CsvExternalSource(ExternalDataSource):
    """
    Fallback M1 a partir de arquivos CSV numa pasta ([FALLBACK] csv_dir).

    Um arquivo por símbolo, procurado nesta ordem: <símbolo>.csv, <símbolo>_M1.csv e o nome
    normalizado (WIN$N -> win_n.csv), sem diferenciar maiúsculas. Formatos aceitos:

    1. Colunas time,open,high,low,close[,tick_volume,spread,real_volume] (separador , ou ;)
    2. Exportação do MT5 ("Barras" -> Exportar): <DATE> <TIME> <OPEN> <HIGH> <LOW> <CLOSE>
       <TICKVOL> <VOL> <SPREAD>, separado por tabulação, datas como 2024.01.02

    Os horários devem estar no mesmo fuso das barras do MT5 (horário da corretora).
    """

    COLUMNS = ["time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume"]
    _MT5_NAMES = {"<date>": "date", "<time>": "clock", "<open>": "open", "<high>": "high", "<low>": "low",
                  "<close>": "close", "<tickvol>": "tick_volume", "<vol>": "real_volume", "<spread>": "spread"}

    def __init__(self, csv_dir):
        self.csv_dir = csv_dir
        self._cache = {}

    def is_configured(self) -> bool:
        import os
        ok = bool(self.csv_dir) and os.path.isdir(self.csv_dir)
        if not ok:
            log.warning(f"CsvExternalSource: pasta não encontrada: '{self.csv_dir}'. Ajuste [FALLBACK] csv_dir.")
        return ok

    @staticmethod
    def _normalize(text):
        text = "".join(c if c.isalnum() else "_" for c in str(text).lower())
        return "_".join(filter(None, text.split("_")))

    def find_file(self, symbol):
        import os
        if not self.is_configured():
            return None
        wanted = {f"{symbol}.csv".lower(), f"{symbol}_m1.csv".lower(),
                  f"{self._normalize(symbol)}.csv", f"{self._normalize(symbol)}_m1.csv"}
        for name in sorted(os.listdir(self.csv_dir)):
            if name.lower() in wanted:
                return os.path.join(self.csv_dir, name)
        return None

    @classmethod
    def read_csv(cls, path) -> pd.DataFrame:
        """Lê um CSV em qualquer dos formatos aceitos e devolve as colunas padrão, ordenadas por tempo."""
        raw = pd.read_csv(path, sep=None, engine="python")
        cols = {c: str(c).strip().lower() for c in raw.columns}
        raw = raw.rename(columns=cols)
        if "<date>" in raw.columns:
            raw = raw.rename(columns=cls._MT5_NAMES)
            clock = raw["clock"].astype(str) if "clock" in raw.columns else "00:00:00"
            raw["time"] = pd.to_datetime(raw["date"].astype(str).str.replace(".", "-", regex=False) + " " + clock)
        elif "time" in raw.columns:
            raw["time"] = pd.to_datetime(raw["time"])
        else:
            raise ValueError(f"{path}: coluna de horário não encontrada (esperado 'time' ou '<DATE>'/'<TIME>')")
        missing = [c for c in ("open", "high", "low", "close") if c not in raw.columns]
        if missing:
            raise ValueError(f"{path}: faltam as colunas {', '.join(missing)}")
        for col in ("tick_volume", "spread", "real_volume"):
            if col not in raw.columns:
                raw[col] = 0
        df = raw[cls.COLUMNS].dropna(subset=["time", "open", "high", "low", "close"])
        return df.drop_duplicates("time", keep="last").sort_values("time").reset_index(drop=True)

    def get_historical_m1_data(self, symbol: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame | None:
        path = self.find_file(symbol)
        if path is None:
            log.warning(f"CsvExternalSource: nenhum CSV para {symbol} em '{self.csv_dir}'.")
            return None
        if path not in self._cache:
            try:
                self._cache[path] = self.read_csv(path)
            except (ValueError, OSError, pd.errors.ParserError) as e:
                log.error(f"CsvExternalSource: não foi possível ler {path}: {e}")
                return None
        df = self._cache[path]
        part = df[(df["time"] >= pd.Timestamp(start_dt)) & (df["time"] <= pd.Timestamp(end_dt))]
        log.info(f"CsvExternalSource: {len(part)} barras M1 de {symbol} entre {start_dt} e {end_dt} ({path}).")
        return part.reset_index(drop=True)
