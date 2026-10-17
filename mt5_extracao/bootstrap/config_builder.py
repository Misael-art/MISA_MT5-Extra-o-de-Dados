"""
Geração idempotente de config/config.ini, .env e config/default_symbols.json.

Regras:
- Valores já existentes no config.ini do usuário são PRESERVADOS; só chaves
  ausentes recebem o padrão. Overrides explícitos (ex.: --mt5-path) vencem.
- A senha nunca vai para o config.ini; credenciais ficam no .env (lido por
  mt5_extracao.security.CredentialManager), com permissão 600 no Linux.
- Toda escrita é atômica (arquivo temporário + os.replace).
"""
import configparser
import json
import os
import re
import tempfile
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Optional

from . import paths

# Seções/chaves lidas pelo app (app.py, mt5_connector.py) + ponte Linux.
DEFAULTS: "OrderedDict[str, OrderedDict[str, str]]" = OrderedDict([
    ("MT5", OrderedDict([
        ("path", ""),            # diretório com terminal64.exe no host
        ("windows_path", ""),    # mesmo diretório no formato C:\... (Wine)
        ("wine_prefix", ""),     # prefixo Wine (somente Linux)
        ("hidden", "false"),     # true = MT5 fora da tela (Linux: tela virtual Xvfb; Windows: minimizado)
        ("hidden_display", ":99"),  # número da tela virtual do modo oculto (Linux)
        ("manage_layout", "true"),  # ao iniciar um robô: janelas laterais fechadas e o gráfico dele maximizado
        ("layout_hide_navigation", "true"),  # partes da organização acima (também em Arquivo > Configurações):
        ("layout_hide_toolbox", "true"),     # Observação do Mercado e Navegador; Caixa de Ferramentas e Testador;
        ("layout_hide_toolbars", "true"),    # barras de ferramentas;
        ("layout_own_profile", "true"),      # gráfico do robô sozinho e maximizado, em perfil de gráficos próprio
        ("panel_layout", "auto"),            # disposição do painel do HADOUKEN: auto, compact, normal ou wide
        ("view_panel", "true"),              # "Abrir visão MT5" põe o painel no gráfico (sem bateria: só visualização)
        ("panel_robot", "hdk_hadouken_painel"),  # qual robô do catálogo é o painel
        ("panel_preset", ""),                # preset do painel na visão (vazio = padrão do código)
        ("panel_any_hours", "false"),        # true = o painel opera em qualquer horário (desliga a janela de horário dele)
        ("panel_demo_blocked", "false"),     # true = o painel manual abre bloqueado (PARAR TUDO) mesmo em conta demo
    ])),
    ("BRIDGE", OrderedDict([
        ("enabled", "false"),    # true no Linux: MT5 acessado via RPyC dentro do Wine
        ("host", paths.DEFAULT_BRIDGE_HOST),
        ("port", str(paths.DEFAULT_BRIDGE_PORT)),
        ("wine_python", ""),     # C:\Python311\python.exe dentro do prefixo
    ])),
    ("DATABASE", OrderedDict([
        ("type", "sqlite"),               # sqlite ou postgresql
        ("path", "database/mt5_data.db"),  # arquivo SQLite
        ("url", ""),                       # PostgreSQL: postgresql://usuario@host:5432/banco (senha: DB_PASSWORD no .env)
    ])),
    ("EXTRACTION", OrderedDict([
        ("chunk_days_m1", "30"),
        ("chunk_days_m5_m15", "90"),
        ("chunk_days_default", "365"),
    ])),
    ("FALLBACK", OrderedDict([
        ("external_source_m1_fallback_enabled", "False"),
        ("external_source_m1_type", "Dummy"),   # Dummy (teste) ou Csv
        ("csv_dir", ""),                         # pasta com <símbolo>.csv (tipo Csv)
    ])),
    ("APP", OrderedDict([
        ("session_start", "09:00"),  # início do pregão (relatório de qualidade: lacunas)
        ("session_end", "18:30"),    # fim do pregão
        ("time_basis", "broker"),    # broker = horário da corretora (como o MT5 devolve); utc = converte
        ("broker_utc_offset", "-3"), # fuso da corretora em horas (B3: -3), usado com time_basis = utc
        ("data_dir", ""),            # vazio = pasta do projeto; auto = pasta de dados do usuário; ou um caminho
        ("notifications", "true"),   # pop-up do sistema ao fim de cada ação (extração, teste, bateria, robô)
    ])),
    ("STRATEGY", OrderedDict([       # backtest e validação (mt5x backtest/validate/report)
        ("initial_equity", "100000"),
        ("risk_per_trade", "0.01"),      # 1% do capital por operação
        ("commission_per_lot", "0"),     # por lote e por lado, na moeda da conta
        ("slippage_points", "0"),
        ("max_drawdown", "0.20"),        # limite do drawdown p95 do Monte Carlo (Filtro C)
        ("min_profit_factor", "1.3"),
        ("min_trades", "100"),
    ])),
    ("SCREENER", OrderedDict([       # triagem de ativos (mt5x screen)
        ("max_spread_atr", "0.10"),
        ("min_liquidity_percentile", "30"),
        ("max_bad_bars", "0.01"),
        ("min_years_daily", "3"),
        ("min_years_intraday", "1"),
        ("spike_multiple", "3"),
        ("max_spike_share", "0.02"),
        ("max_correlation", "0.7"),
        ("portfolio", ""),               # símbolos da carteira, separados por vírgula
        ("events_file", ""),             # CSV time,symbol,impact (opcional)
        ("event_window_hours", "4"),
    ])),
    ("ROBOTS", OrderedDict([         # catálogo de robôs (mt5x robots; aba Robôs)
        ("library_dirs", ""),            # pastas com robôs MQL5 (Experts/, Include/, Presets/), separadas por ;
        ("metaeditor_path", ""),         # vazio = MetaEditor64.exe da instalação do MT5 ([MT5] path)
        ("tester_model", "4"),           # Strategy Tester: 4 ticks reais (padrão), 0 cada tick; 1 e 2 não valem na bateria
        ("max_real_risk_pct", "1.0"),    # teto de risco por operação (%) aceito pela bateria de segurança
        ("min_demo_days", "7"),          # período comprovado em demo antes de disponibilizar PLAY real
        ("allow_real", "false"),        # bloqueio global REAL, também conferido pela auditoria; não autoriza por si só
        ("max_account_risk_pct", "0"),   # mt5x portfolio: avisa se o risco da conta até o stop passar disso (%; 0 = desligado)
        ("account_exposure_groups", ""), # mt5x portfolio: grupos fixos; grupos separados por ; e padrões de símbolos por ,
        ("safety_lib_dir", ""),          # clone local da biblioteca mt5x-safety (vazio = não instalada)
        ("batch_max_cpu_pct", "75"),     # backtests pela interface: teto de uso de CPU (%) para rodar mais um em paralelo
        ("batch_max_ram_pct", "85"),     # idem, para a memória
        ("batch_max_workers", "0"),      # máximo de backtests simultâneos (0 = núcleos do processador menos um)
    ])),
    ("RESOURCES", OrderedDict([      # gestão de recursos (aba Recursos; mt5x resources). Automação só com opt-in
        ("auto_enabled", "false"),           # true = monitora e age sozinho no intervalo abaixo
        ("monitor_interval_minutes", "60"),
        ("alert_db_gb", "10"),               # avisa quando a base passar disso (0 = desligado)
        ("alert_disk_free_pct", "10"),       # avisa quando o disco livre cair abaixo disso (%)
        ("auto_vacuum", "false"),            # otimiza sozinho quando o espaço recuperável passar de vacuum_min_mb
        ("vacuum_min_mb", "200"),
        ("auto_clean_orphans", "false"),     # remove sozinho registros de controle de tabelas que não existem mais
        ("auto_archive", "false"),           # arquiva sozinho (reversível) as séries das regras abaixo
        ("auto_archive_rules", "obsoleta,vazia"),  # sem_lucro, sem_estudo, obsoleta, vazia
        ("obsolete_days", "180"),            # idade da última barra para a regra "obsoleta"
        ("archive_dir", ""),                 # vazio = pasta arquivo/ ao lado da base
        ("keep_snapshots_days", "365"),      # histórico de medições guardado
        ("auto_backup", "false"),            # cópia de segurança da base (SQLite) a cada backup_interval_hours
        ("backup_interval_hours", "24"),
        ("backup_keep", "5"),                # quantas cópias guardar (as mais velhas são apagadas)
        ("backup_dir", ""),                  # vazio = pasta copias/ ao lado da base
        ("auto_clean_files", "false"),       # apaga sozinho logs e exportações mais velhos que os dias abaixo
        ("logs_keep_days", "30"),            # 0 = nunca apaga logs (o log em uso nunca é apagado)
        ("exports_keep_days", "0"),          # 0 = nunca apaga exportações
        ("archive_format", "sqlite"),        # formato do arquivo: sqlite (abre em qualquer lugar) ou parquet (menor; pyarrow)
        ("auto_trim", "false"),              # apara sozinho (arquiva) o histórico mais velho que os dias abaixo
        ("retention_intraday_days", "0"),    # barras intradiárias (menores que 1 dia); 0 = guarda tudo
        ("retention_ticks_days", "0"),       # ticks e book; 0 = guarda tudo
        ("max_series_mb", "0"),              # avisa quando uma série passar disso (MB); 0 = desligado
        ("block_disk_free_pct", "0"),        # extração para de gravar com disco livre abaixo disso (%); 0 = nunca
        ("block_db_gb", "0"),                # extração para de gravar com a base acima disso (GB); 0 = nunca
        ("retention_overrides", ""),         # retenção por ativo: WIN$N:30, PETR4:0 (0 = guarda tudo desse ativo)
    ])),
    ("TRADE_ANALYTICS", OrderedDict([  # Laboratório (Fase 16; mt5x trades)
        ("event_window_before_min", "30"),   # operação "com notícia": evento até N min antes da entrada
        ("event_window_after_min", "30"),    # ... ou até N min depois
        ("event_symbols", ""),               # moeda -> ativos, ex.: BRL: WIN$N WDO$N; USD: WDO$N
        ("calendar_days_back", "1095"),      # exportação do calendário do MT5: dias de histórico
        ("calendar_days_forward", "30"),     # ... e dias à frente
        ("calendar_countries", ""),          # códigos de país (BR,US); vazio = todos
        ("calendar_min_importance", "0"),    # 0 todos, 1 baixa+, 2 média+, 3 só alta
        ("time_bucket_min", "30"),           # faixas de horário das operações (minutos)
        ("volatility_period", "14"),         # barras do retorno anterior à entrada
        ("min_trades_cell", "30"),           # menos operações que isso numa célula = "amostra pequena"
        ("wf_folds", "4"),                   # blocos do walk-forward que valida as regras (mínimo 2)
        ("holdout_fraction", "0.2"),         # fração final do histórico guardada intocada para o veredito
    ])),
])

DEFAULT_SYMBOLS = OrderedDict([
    ("indices_futuros_b3", ["WIN$N", "WIN$", "IND$N"]),
    ("dolar_futuro_b3", ["WDO$N", "WDO$", "DOL$N"]),
    ("acoes_b3", ["PETR4", "VALE3", "ITUB4", "BBDC4", "BBAS3", "ABEV3", "B3SA3", "WEGE3"]),
    ("forex", ["EURUSD", "GBPUSD", "USDJPY", "USDBRL"]),
])


def load_config(path: Path) -> configparser.ConfigParser:
    cfg = _new_parser()
    if Path(path).exists():
        cfg.read(path, encoding="utf-8")
    return cfg


def _new_parser() -> configparser.ConfigParser:
    # interpolation=None: senhas/caminhos com "%" não quebram o parser
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.optionxform = str  # preserva maiúsculas/minúsculas das chaves
    return cfg


def build_config(existing: Optional[configparser.ConfigParser] = None,
                 overrides: Optional[Dict[str, Dict[str, str]]] = None) -> configparser.ConfigParser:
    """
    Mescla: padrões < existente < overrides.
    Seções/chaves desconhecidas do arquivo existente são mantidas.
    """
    result = _new_parser()
    for section, keys in DEFAULTS.items():
        result[section] = dict(keys)
    if existing is not None:
        for section in existing.sections():
            if not result.has_section(section):
                result.add_section(section)
            for key, value in existing.items(section, raw=True):
                result[section][key] = value
    for section, keys in (overrides or {}).items():
        if not result.has_section(section):
            result.add_section(section)
        for key, value in keys.items():
            if value is not None:
                result[section][key] = str(value)
    return result


def atomic_write_text(path: Path, text: str, private: bool = False) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        if private and os.name != "nt":
            os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


def write_config(path: Path, cfg: configparser.ConfigParser) -> None:
    import io
    buf = io.StringIO()
    buf.write("# Gerado por: python -m mt5_extracao.bootstrap configure\n")
    buf.write("# Pode ser editado manualmente; valores existentes são preservados ao reconfigurar.\n\n")
    cfg.write(buf)
    atomic_write_text(path, buf.getvalue())


_SECTION_LINE = re.compile(r"^\s*\[(?P<name>[^\]]+)\]\s*$")


def set_config_value(path: Path, section: str, key: str, value: str) -> None:
    """
    Grava UMA chave no config.ini trocando só a linha dela (ou acrescentando-a ao fim da seção, ou a seção ao fim
    do arquivo). Comentários, ordem e as demais chaves ficam como estão: é o que a interface usa para salvar uma
    opção sem regravar o arquivo do usuário.
    """
    path = Path(path)
    if any(ch in str(value) for ch in "\r\n"):
        raise ValueError(f"Valor inválido para [{section}] {key}: não pode ter quebra de linha.")
    lines = path.read_text(encoding="utf-8").splitlines() if path.is_file() else []
    # sem diferenciar maiúsculas: o leitor padrão do Python não diferencia, e duas grafias da chave o fariam falhar
    key_line = re.compile(r"^\s*(" + re.escape(key) + r")\s*[=:]", re.IGNORECASE)
    start = end = None
    for index, line in enumerate(lines):
        match = _SECTION_LINE.match(line)
        if match and start is None and match.group("name").strip() == section:
            start = index
        elif match and start is not None:
            end = index
            break
    new_line = f"{key} = {value}"
    if start is None:
        lines += ([""] if lines and lines[-1].strip() else []) + [f"[{section}]", new_line]
    else:
        end = len(lines) if end is None else end
        found = next((i for i in range(start + 1, end) if key_line.match(lines[i])), None)
        if found is not None:
            lines[found] = f"{key_line.match(lines[found]).group(1)} = {value}"     # mantém a grafia do arquivo
        else:
            last = max((i for i in range(start, end) if lines[i].strip()), default=start)
            lines.insert(last + 1, new_line)
    atomic_write_text(path, "\n".join(lines) + "\n")


def read_env_file(path: Path) -> "OrderedDict[str, str]":
    values: "OrderedDict[str, str]" = OrderedDict()
    path = Path(path)
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = _unescape(value[1:-1])
        values[key.strip()] = value
    return values


def _unescape(value: str) -> str:
    """Desfaz \\\\ e \\' (mesmo formato aceito pelo python-dotenv)."""
    out = []
    i = 0
    while i < len(value):
        ch = value[i]
        if ch == "\\" and i + 1 < len(value) and value[i + 1] in ("\\", "'", '"'):
            out.append(value[i + 1])
            i += 2
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _quote_env(value: str) -> str:
    if value == "" or any(ch in value for ch in " #'\"=\\$"):
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return "'" + escaped + "'"
    return value


def write_env_credentials(path: Path, login: Optional[str], password: Optional[str],
                          server: Optional[str]) -> bool:
    """
    Grava/atualiza MT5_LOGIN, MT5_PASSWORD e MT5_SERVER no .env, preservando
    outras variáveis. Valores None não alteram a chave existente.
    Retorna True se algo foi escrito.
    """
    updates = {"MT5_LOGIN": login, "MT5_PASSWORD": password, "MT5_SERVER": server}
    updates = {k: v for k, v in updates.items() if v is not None}
    if not updates:
        return False
    current = read_env_file(path)
    current.update(updates)
    lines = ["# Credenciais do MetaTrader 5 - NÃO versione este arquivo"]
    lines += [f"{k}={_quote_env(v)}" for k, v in current.items()]
    atomic_write_text(path, "\n".join(lines) + "\n", private=True)
    return True


def ensure_symbols_file(path: Path) -> bool:
    """Cria default_symbols.json se não existir. Retorna True se criou."""
    path = Path(path)
    if path.exists():
        return False
    atomic_write_text(path, json.dumps(DEFAULT_SYMBOLS, indent=2, ensure_ascii=False) + "\n")
    return True


def ensure_work_dirs(root: Path = paths.PROJECT_ROOT) -> None:
    for name in paths.WORK_DIRS:
        (Path(root) / name).mkdir(parents=True, exist_ok=True)
