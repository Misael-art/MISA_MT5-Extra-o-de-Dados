"""Pré-checagem local e pacote para envio manual de robôs ao MQL5 Market.

O comando não envia nem publica nada. A aprovação do pré-check também não substitui
os testes e a validação oficial feitos pela área do vendedor do MQL5.com.
"""
import os
import re
import shutil
from pathlib import Path
from typing import List, Optional

from . import battery, catalog, mql, mt5_tools

_VERSION = re.compile(r"^\d{1,3}\.\d{3}$")
_FIXED_SYMBOL_CALL = re.compile(
    r"\b(?:SymbolSelect|CopyRates|CopyTicks|CopyTicksRange|SymbolInfo\w*|i[A-Z]\w*)\s*"
    r"\(\s*\"([A-Z0-9$._-]{3,24})\""
)
_FIXED_SYMBOL_COMPARE = re.compile(
    r"(?:\b_Symbol\b|\bSymbol\s*\(\s*\))\s*(?:==|!=)\s*\"([A-Z0-9$._-]{3,24})\""
)
_FIXED_SYMBOL_ASSIGN = re.compile(
    r"\b(?:symbol|ticker|asset|ativo)\w*\s*=\s*\"([A-Z0-9$._-]{3,24})\"", re.I
)
_FIXED_ACCOUNT = re.compile(
    r"\b(?:AccountInfoInteger\s*\(\s*ACCOUNT_LOGIN\s*\)\s*(?:==|!=)\s*\d{4,}|"
    r"ACCOUNT_LOGIN\s*(?:==|!=)\s*\d{4,}|"
    r"(?:==|!=)\s*\d{4,}[^\n;]*ACCOUNT_LOGIN|"
    r"(?:login|account(?:_number|_id)?)\s*=\s*\d{4,})\b", re.I
)
_FIXED_SERVER = re.compile(
    r"AccountInfoString\s*\(\s*ACCOUNT_SERVER\s*\)\s*(?:==|!=)\s*\"[^\"]+\"", re.I
)
_FIXED_PATH = re.compile(r"\"(?:[A-Za-z]:[\\/]|/|\\\\)[^\"]+\"")
_LOG_ERROR = re.compile(r"\b(error|failed|failure|not enough money|no money|cannot load|could not)\b", re.I)
_ZERO_ERRORS = re.compile(r"\b0\s+errors?\b|\berrors?\s*[:=]\s*0\b", re.I)


def _source_files(robot) -> List[Path]:
    return [Path(path) for path in getattr(robot, "files", [])
            if str(path).lower().endswith((".mq5", ".mqh")) and Path(path).is_file()]


def _main_source(robot) -> Optional[Path]:
    path = Path(getattr(robot, "path", "")) if getattr(robot, "path", "") else None
    if path is not None and path.suffix.lower() == ".mq5" and path.is_file():
        return path
    return next((p for p in _source_files(robot) if p.suffix.lower() == ".mq5"), None)


def _source_text(robot) -> str:
    parts = []
    for path in _source_files(robot):
        try:
            text = mql.read_text(path)
        except OSError:
            continue
        text = mql._BLOCK_COMMENT.sub("", text)
        text = re.sub(r"//[^\n]*", "", text)
        parts.append(text)
    return "\n".join(parts)


def _main_properties(robot):
    source = _main_source(robot)
    if source is None:
        return None, ""
    try:
        parsed = mql.parse_source(mql.read_text(source))
    except OSError:
        return None, ""
    return parsed, "\n".join(item.strip() for item in parsed.descriptions if item.strip())


def _item(item_id: str, name: str, status: str, detail: str, fix: str = "") -> battery.Item:
    return battery.Item(battery.STATIC, item_id, name, status, detail, fix)


def _compile_item(result) -> battery.Item:
    if result is None:
        return _item("compile", "Compila sem erros nem avisos", battery.FAIL,
                     "não há resultado de compilação registrado",
                     "Rode `mt5x robots compile` e repita o pré-check.")
    errors, warnings = getattr(result, "errors", None), getattr(result, "warnings", None)
    compiled = Path(getattr(result, "compiled", "")) if getattr(result, "compiled", "") else None
    if errors is None or warnings is None:
        return _item("compile", "Compila sem erros nem avisos", battery.FAIL,
                     getattr(result, "summary", "log de compilação incompleto"),
                     "Rode `mt5x robots compile` no MetaEditor e confira o log.")
    if errors:
        return _item("compile", "Compila sem erros nem avisos", battery.FAIL,
                     f"{errors} erro(s), {warnings} aviso(s)", "Corrija o fonte e compile de novo.")
    if warnings:
        return _item("compile", "Compila sem erros nem avisos", battery.FAIL,
                     f"0 erro(s), {warnings} aviso(s)", "Elimine os avisos e compile de novo.")
    if not getattr(result, "ok", False) or compiled is None or not compiled.is_file():
        return _item("compile", "Compila sem erros nem avisos", battery.FAIL,
                     getattr(result, "summary", "arquivo .ex5 não encontrado"),
                     "Rode `mt5x robots compile` e confira o resultado.")
    return _item("compile", "Compila sem erros nem avisos", battery.OK, "0 erros, 0 avisos")


def _static_source_items(robot) -> List[battery.Item]:
    parsed, description = _main_properties(robot)
    if parsed is None:
        return [
            _item("version", "Versão no formato XXX.YYY", battery.FAIL, "fonte .mq5 não encontrado",
                  "Importe o código-fonte completo do Expert Advisor."),
            _item("description", "Descrição do produto preenchida", battery.FAIL, "fonte .mq5 não encontrado",
                  "Adicione `#property description` ao Expert Advisor."),
        ]

    version = parsed.version
    items = [_item("version", "Versão no formato XXX.YYY", battery.OK if _VERSION.fullmatch(version) else battery.FAIL,
                   version or "`#property version` ausente",
                   "Use `#property version \"1.000\"` (três dígitos após o ponto)."
                   if not _VERSION.fullmatch(version) else "")]
    items.append(_item("description", "Descrição do produto preenchida", battery.OK if description else battery.FAIL,
                       description if description else "`#property description` ausente ou vazio",
                       "Preencha `#property description` em inglês." if not description else ""))
    page_flags = []
    if re.search(r"https?://|www\.", description, re.I):
        page_flags.append("link externo")
    if re.search(r"\b(profit|profits|guarantee|guaranteed|make money|lucro garantido)\b", description, re.I):
        page_flags.append("possível promessa de resultado")
    items.append(_item("presentation", "Descrição sem link externo ou promessa de lucro",
                       battery.FAIL if page_flags else battery.WARN,
                       ", ".join(page_flags) if page_flags else "a descrição exige revisão manual do vendedor",
                       "Remova links e promessas de resultado; revise também os textos da página do produto."
                       if page_flags else "Confirme manualmente a apresentação antes do envio."))
    items.append(_item("language", "Mensagens e entradas em inglês", battery.WARN,
                       "a revisão do idioma dos textos visíveis exige leitura humana",
                       "Revise nomes de entradas, mensagens e textos da página em inglês."))

    if getattr(robot, "dll_imports", []):
        dlls = ", ".join(robot.dll_imports)
        items.append(_item("dll", "Sem importação de DLL", battery.FAIL, dlls,
                           "Remova `#import` de DLL; o produto do Market deve ser autocontido."))
    else:
        items.append(_item("dll", "Sem importação de DLL", battery.OK, "nenhuma DLL importada"))

    source = _source_text(robot)
    fixed = []
    if _FIXED_SYMBOL_CALL.search(source) or _FIXED_SYMBOL_COMPARE.search(source) or _FIXED_SYMBOL_ASSIGN.search(source):
        fixed.append("símbolo")
    if _FIXED_ACCOUNT.search(source) or _FIXED_SERVER.search(source):
        fixed.append("conta ou servidor")
    if _FIXED_PATH.search(source):
        fixed.append("caminho absoluto")
    items.append(_item("fixed_environment", "Sem símbolo, conta ou caminho fixo", battery.FAIL if fixed else battery.OK,
                       ", ".join(fixed) if fixed else "nenhum padrão fixo encontrado no código",
                       "Use o símbolo do gráfico, parâmetros configuráveis e caminhos relativos." if fixed else ""))

    upper = source.upper()
    lot_ok = "SYMBOL_VOLUME_MIN" in upper and "SYMBOL_VOLUME_STEP" in upper
    items.append(_item("min_lot", "Respeita lote mínimo e passo", battery.OK if lot_ok else battery.WARN,
                       "o código consulta lote mínimo e passo" if lot_ok else "não encontrei a conferência de mínimo e passo",
                       "Confira `SYMBOL_VOLUME_MIN` e `SYMBOL_VOLUME_STEP` antes de enviar ordens." if not lot_ok else ""))
    margin_ok = any(token in upper for token in ("ORDERCALCMARGIN", "ACCOUNT_MARGIN_FREE", "TRADE_RETCODE_NO_MONEY"))
    items.append(_item("margin", "Trata margem insuficiente", battery.OK if margin_ok else battery.WARN,
                       "o código confere margem ou retcode de fundos insuficientes"
                       if margin_ok else "não encontrei tratamento explícito de margem insuficiente",
                       "Confira a margem antes de enviar ou trate `TRADE_RETCODE_NO_MONEY`." if not margin_ok else ""))
    return items


def _log_error_lines(path: Path) -> List[str]:
    try:
        text = mql.read_text(path)
    except OSError:
        return ["arquivo não pôde ser lido"]
    errors = []
    for line in text.splitlines():
        line = line.strip()
        if line and not _ZERO_ERRORS.search(line) and _LOG_ERROR.search(line):
            errors.append(line[:220])
    return errors


def _run_items(robot, runs) -> List[battery.Item]:
    try:
        fingerprint = catalog.fingerprint(robot)
    except (OSError, ValueError):
        fingerprint = ""
    tester_source = "strategy tester"
    current = []
    for run in runs or []:
        metrics = run.get("metrics") or {}
        if run.get("source") != tester_source:
            continue
        if run.get("robot_key") and run.get("robot_key") != robot.key:
            continue
        if fingerprint and metrics.get("source_fingerprint") != fingerprint:
            continue
        # Execuções anteriores à T10.11 não têm este campo. Só contam para a evidência
        # quando foram criadas com captura por execução habilitada.
        if "tester_log_captured" in metrics:
            current.append((run, metrics))

    if not current:
        return [battery.Item(battery.STATISTICAL, "tester_logs", "Logs sem erros em mais de um ativo e timeframe",
                             battery.FAIL, "não há backtests atuais com log isolado registrado",
                             "Rode `mt5x robots backtest` em pelo menos dois ativos e dois timeframes.")]

    valid, failures = [], []
    for run, metrics in current:
        path_text = metrics.get("tester_log") or ""
        if not metrics.get("tester_log_captured") or not path_text:
            failures.append(f"{run.get('symbol', '?')} {run.get('timeframe', '?')}: log não capturado")
            continue
        path = Path(path_text)
        if not path.is_file() or path.stat().st_size == 0:
            failures.append(f"{run.get('symbol', '?')} {run.get('timeframe', '?')}: log ausente ou vazio")
            continue
        errors = _log_error_lines(path)
        if errors:
            failures.append(f"{run.get('symbol', '?')} {run.get('timeframe', '?')}: " + errors[0])
        else:
            valid.append(run)

    symbols = {str(run.get("symbol", "")).strip() for run in valid if run.get("symbol")}
    timeframes = {str(run.get("timeframe", "")).strip() for run in valid if run.get("timeframe")}
    if failures:
        detail = "; ".join(failures[:3])
        return [battery.Item(battery.STATISTICAL, "tester_logs", "Logs sem erros em mais de um ativo e timeframe",
                             battery.FAIL, detail, "Corrija o erro ou repita o backtest e confira o log isolado.")]
    if len(symbols) < 2 or len(timeframes) < 2:
        return [battery.Item(battery.STATISTICAL, "tester_logs", "Logs sem erros em mais de um ativo e timeframe",
                             battery.FAIL,
                             f"{len(symbols)} ativo(s), {len(timeframes)} timeframe(s) com log válido",
                             "Rode o robô em pelo menos dois ativos e dois timeframes.")]
    return [battery.Item(battery.STATISTICAL, "tester_logs", "Logs sem erros em mais de um ativo e timeframe",
                         battery.OK,
                         f"{len(valid)} execução(ões), {len(symbols)} ativo(s), {len(timeframes)} timeframe(s)")]


def check(robot, compile_result, runs) -> List[battery.Item]:
    """Confere os requisitos locais do Market; devolve itens no mesmo formato da bateria."""
    if getattr(robot, "kind", None) != catalog.MQL5:
        return [_item("kind", "Expert Advisor MQL5", battery.FAIL,
                      "o item selecionado não é um Expert Advisor MQL5",
                      "Escolha um robô MQL5 com código-fonte.")]
    if not getattr(robot, "has_source", True):
        return [_item("source", "Código-fonte disponível", battery.FAIL, "só há um arquivo .ex5",
                      "Importe o arquivo .mq5 e seus includes para fazer a pré-checagem.")]
    return [_compile_item(compile_result), *_static_source_items(robot), *_run_items(robot, runs)]


def ready(items: List[battery.Item]) -> bool:
    """A pré-checagem permite montar o pacote quando não há falhas; avisos ficam visíveis."""
    return bool(items) and not any(item.status == battery.FAIL for item in items)


def _description(robot) -> str:
    _, text = _main_properties(robot)
    return text


def pack(robot, compile_result, destination, with_source: bool = False) -> List[Path]:
    """Monta uma pasta local para revisão manual; não envia o pacote ao MQL5.com."""
    compiled_text = getattr(compile_result, "compiled", "") if compile_result is not None else ""
    compiled = Path(compiled_text) if compiled_text else None
    if (not getattr(compile_result, "ok", False) or getattr(compile_result, "errors", None) != 0
            or getattr(compile_result, "warnings", None) != 0 or compiled is None or not compiled.is_file()):
        raise ValueError("O .ex5 sem erros e sem avisos não está disponível; rode `mt5x robots compile` primeiro.")
    description = _description(robot)
    if not description:
        raise ValueError("`#property description` está vazia; o pacote não foi criado.")
    target = Path(destination).expanduser()
    if target.exists() and (not target.is_dir() or any(target.iterdir())):
        raise FileExistsError(f"A pasta de pacote não está vazia: {target}")
    target.mkdir(parents=True, exist_ok=True)
    created = []
    ex5 = target / compiled.name
    shutil.copy2(compiled, ex5)
    created.append(ex5)
    description_file = target / "description.txt"
    description_file.write_text(description + "\n", encoding="utf-8")
    created.append(description_file)
    readme = target / "LEIA-ME.txt"
    readme.write_text(
        "Pacote local para revisão antes do envio manual ao MQL5 Market.\n\n"
        "Ainda depende do vendedor:\n"
        "- cadastrar e manter o perfil de vendedor;\n"
        "- preparar imagens e textos da página do produto;\n"
        "- definir o preço;\n"
        "- enviar o produto pela área do vendedor e acompanhar a validação oficial.\n\n"
        "Este pacote não promete aprovação nem resultado financeiro.\n",
        encoding="utf-8")
    created.append(readme)
    if with_source:
        sources = _source_files(robot)
        if not sources:
            raise ValueError("Nenhum fonte .mq5/.mqh está disponível para incluir.")
        common = Path(os.path.commonpath([str(path.resolve().parent) for path in sources]))
        source_root = target / "source"
        for path in sources:
            relative = path.resolve().relative_to(common)
            output = source_root / relative
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, output)
            created.append(output)
    return created


def last_compile_result(config, robot):
    """Lê a última compilação do MetaEditor se ela corresponde ao código instalado atual."""
    data = mt5_tools.data_dir(config)
    plan = mt5_tools.install_plan(robot, data)
    result = mt5_tools.CompileResult(False, source=str(plan[0].target))
    stale = [item.target.name for item in plan
             if item.source.suffix.lower() in (".mq5", ".mqh")
             and (not item.target.is_file() or item.target.read_bytes() != item.source.read_bytes())]
    if stale:
        result.summary = "O código instalado não corresponde ao fonte atual: " + ", ".join(stale[:4])
        return result
    source = plan[0].target
    compiled, log_path = source.with_suffix(".ex5"), source.with_suffix(".log")
    result.log_path = str(log_path)
    if not log_path.is_file():
        result.summary = "Log de compilação não encontrado; rode `mt5x robots compile`."
        return result
    errors, warnings, result.messages = mt5_tools.parse_compile_log(mql.read_text(log_path))
    result.errors, result.warnings = errors, warnings
    result.compiled = str(compiled) if compiled.is_file() else ""
    if errors is None or warnings is None:
        result.summary = "Log de compilação incompleto (sem a linha Result)."
    elif not compiled.is_file() or compiled.stat().st_mtime < source.stat().st_mtime:
        result.summary = "O .ex5 não existe ou é anterior ao fonte atual. Rode `mt5x robots compile`."
    else:
        result.ok = errors == 0
        result.summary = f"{errors} erro(s), {warnings} aviso(s)"
    return result
