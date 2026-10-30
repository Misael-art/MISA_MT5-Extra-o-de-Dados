import pytest

from mt5_extracao import timeframes
from mt5_extracao.timeframes import Timeframe, parse


@pytest.mark.parametrize("value,expected", [
    (Timeframe.H1, Timeframe.H1), (1, Timeframe.M1), (16385, Timeframe.H1), (60, Timeframe.H1),
    (240, Timeframe.H4), (1440, Timeframe.D1), (43200, Timeframe.MN1),
    ("M1", Timeframe.M1), ("m15", Timeframe.M15), ("H4", Timeframe.H4), ("MN1", Timeframe.MN1),
    ("1min", Timeframe.M1), ("5 minutos", Timeframe.M5), ("1 hora", Timeframe.H1), ("1 mês", Timeframe.MN1),
    ("1 mes", Timeframe.MN1), ("diario", Timeframe.D1), ("TIMEFRAME_H1", Timeframe.H1), ("60", Timeframe.H1),
])
def test_parse(value, expected):
    assert parse(value) is expected


@pytest.mark.parametrize("value", ["xyz", 7, None, True, 3.5])
def test_parse_unknown(value):
    assert parse(value) is None


def test_labels_used_in_table_names_are_unchanged():
    # Esses nomes compõem os nomes das tabelas existentes: não podem mudar
    assert [tf.label for tf in timeframes.MAIN] == [
        "1 minuto", "5 minutos", "15 minutos", "30 minutos", "1 hora", "4 horas", "1 dia", "1 semana", "1 mês"]


def test_module_constants_and_minutes():
    assert timeframes.TIMEFRAME_H4 == 16388 and timeframes.TIMEFRAME_MN1 == 49153
    assert timeframes.MINUTES[16408] == 1440


def test_connector_timeframes_are_mt5_values_even_offline(connector):
    connector.is_initialized = False
    values = [v for _, v in connector.get_available_timeframes()]
    assert values == [tf.value for tf in timeframes.MAIN]
    assert 60 not in values  # antes, sem conexão, devolvia minutos (60) em vez de 16385


def test_connector_convert(connector):
    assert connector._convert_timeframe_to_mt5("1 hora") == 16385
    assert connector._convert_timeframe_to_mt5(240) == 16388
    assert connector._convert_timeframe_to_mt5("??") == 1


def test_ordering_uses_duration_and_preserves_labels_without_duplicate_aliases():
    values = ["H4", "1 hora", "M5", "H1", "1 minuto", "M1", "D1", "M15", "MN1", "W1", "M30"]
    before = values[:]
    assert timeframes.ordered_unique(iter(values)) == [
        "1 minuto", "M5", "M15", "M30", "1 hora", "H4", "D1", "W1", "MN1"]
    assert values == before


def test_ordering_keeps_unknowns_visible_without_reinterpreting_them():
    assert timeframes.ordered_unique(["M7", "D1", "custom", "M7", "H1"]) == ["H1", "D1", "M7", "custom"]
    assert timeframes.ordered_unique([]) == []
