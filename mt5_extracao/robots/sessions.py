"""Sessões portáteis próprias dos cards. O terminal do usuário e a ponte continuam abertos.

Sem envio de ordens Python ou encerramento global. O EA MQL5 continua responsável por
ordens e Safety; a instrumentação só publica posições do próprio magic/símbolo.
"""
import configparser
import json
import logging
import os
import shutil
import subprocess
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

from mt5_extracao.bootstrap import display, paths
from . import backtest, battery, catalog, contract, controls, live, mql, mt5_tools

log = logging.getLogger(__name__)


class Cancelled(RuntimeError):
    pass


def native_interrupted(directory):
    """Cancelamento explícito no Tester, sem confundir stop de uma posição com parada do teste."""
    import re
    pattern = re.compile(r"(?:testing|test|downloading)[^\r\n]{0,150}(?:canceled|cancelled|stopped by user|terminated by user)", re.I)
    tester = Path(directory) / "Tester"
    logs = list((tester / "logs").glob("*.log"))
    logs.extend(tester.glob("Agent-*/logs/*.log"))  # STOP visual é registrado pelo agente local no build 6272
    for path in logs:
        try:
            with path.open("rb") as handle:
                handle.seek(max(0, path.stat().st_size - 65536))
                tail = handle.read()
            # Logs diários UTF-16, cauda sem BOM; UTF-8 também ocorre em alguns builds.
            content = tail.decode("utf-16-le", errors="replace") if b"\x00" in tail[:100] else tail.decode("utf-8", errors="replace")
            if pattern.search(content):
                return True
        except OSError:
            continue
    return False


def owned_processes(config, names=(paths.MT5_TERMINAL_EXE,), *, strict=False, launchers=False):
    """Mesmo prefixo Wine não significa mesma instalação. Exigir também o executável exato."""
    expected_paths = [mt5_tools.install_dir(config) / name for name in names]
    prefix = mt5_tools.wine_prefix(config)
    expected_paths = {mt5_tools._windows_path(p, prefix).replace("\\", "/").rstrip("/").lower() for p in expected_paths}
    found = []
    import psutil
    expected_names = {name.lower() for name in names}
    launch_path = mt5_tools._windows_path(mt5_tools.install_dir(config) / "launch.bat", prefix).replace("\\", "/").lower()
    if launchers:
        expected_names.update(("cmd.exe", "wine", "wine64"))
    for process in psutil.process_iter():
        candidate = False
        try:
            executable = process.exe() if paths.is_windows() else process.cmdline()[0]
            candidate = str(executable).replace("\\", "/").rstrip("/").lower() in expected_paths
            if not candidate and launchers:
                candidate = any(str(arg).strip('"').replace("\\", "/").lower() == launch_path
                                for arg in process.cmdline())
            if not candidate:
                continue
            if not paths.is_windows():
                theirs = process.environ().get("WINEPREFIX") or str(Path.home() / ".wine")
                if Path(theirs).expanduser().resolve() != prefix.resolve():
                    continue
            found.append(process)
        except psutil.NoSuchProcess:
            continue
        except (OSError, IndexError, psutil.Error) as error:
            if strict:
                if not candidate:
                    try:
                        candidate = process.name().lower() in expected_names
                    except psutil.NoSuchProcess:
                        continue
                    except (OSError, psutil.Error):
                        candidate = True
                if candidate:
                    raise RuntimeError("Não foi possível conferir um processo nativo; mantenha a sessão e confira as permissões antes de liberar seus arquivos.") from error
    return found


def _copy_file(source, target, *, check=None):
    source, target = Path(source), Path(target)
    if not source.is_file():
        return
    if check is not None:
        check(path=target, reserve_bytes=source.stat().st_size)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    target.chmod(0o600)


def _private_config(config, directory):
    child = configparser.ConfigParser(interpolation=None)
    child.read_dict({s: dict(config[s]) for s in config.sections()})
    if not child.has_section("MT5"):
        child.add_section("MT5")
    child["MT5"]["path"] = str(directory / "terminal64.exe")
    # A configuração da cópia nunca aponta o compilador para a instância operacional.
    if child.has_option("ROBOTS", "metaeditor_path"):
        child["ROBOTS"]["metaeditor_path"] = ""
    return child


def prepare(config, robot, directory, symbol, *, require_source=True, check=None, _child=None):
    """Cópias privadas, sem links graváveis, senhas em INI ou argumentos de login."""
    if require_source and not robot.has_source:
        raise ValueError("Estes comandos precisam do fonte para conferir segurança e acompanhar o Expert.")
    from pathlib import PurePosixPath, PureWindowsPath
    symbol = str(symbol)
    components = (PurePosixPath(symbol), PureWindowsPath(symbol))
    if (not symbol or any(char in symbol for char in "\r\n\0") or
            any(part.is_absolute() or part.drive or ".." in part.parts for part in components)):
        raise ValueError("Símbolo inválido para preparar o perfil privado; escolha um ativo do catálogo.")
    directory = Path(directory)
    if check is not None:
        check(path=directory, reserve_bytes=1024)
    from functools import partial
    copy_file = partial(_copy_file, check=check)
    directory.mkdir(parents=True, exist_ok=False, mode=0o700)
    (directory / "relatorio.json").write_text(json.dumps({"artifact_owner": "mt5x-session", "artifact_version": 1,
        "session": directory.name, "state": "preparing"}, ensure_ascii=False), encoding="utf-8")
    source = mt5_tools.install_dir(config)
    data = mt5_tools.data_dir(config)
    for name in ("terminal64.exe", "metatester64.exe", "MetaEditor64.exe", "metaeditor64.exe"):
        copy_file(source / name, directory / name)
    for file in source.glob("*.dll"):
        copy_file(file, directory / file.name)
    # Cache nativo cifrado; nunca convertido em credenciais de texto.
    for folder in ("config", "Config"):
        for file in (data / folder).glob("*"):
            if file.is_file() and file.suffix.lower() in (".dat", ".srv"):
                copy_file(file, directory / folder / file.name)
        common_ini = data / folder / "common.ini"
        if common_ini.is_file():
            # Somente identidade do último login, nunca copiar opções de Expert ou senhas.
            native = configparser.ConfigParser(interpolation=None, strict=False)
            native.read_string(mql.read_text(common_ini))
            text = "[Common]\r\n" + "".join(f"{key}={native.get('Common', key)}\r\n"
                                              for key in ("Login", "Server") if native.has_option("Common", key))
            destination = directory / folder / "common.ini"
            destination.parent.mkdir(parents=True, exist_ok=True)
            if check is not None:
                check(path=destination, reserve_bytes=len(text.encode("utf-16")))
            destination.write_bytes(text.encode("utf-16"))
            destination.chmod(0o600)
    bases = next((p for p in data.iterdir() if p.is_dir() and p.name.lower() == "bases"), data / "Bases")
    for server in bases.glob("*"):
        if not server.is_dir():
            continue
        for folder in ("history", "ticks"):
            actual = next((p for p in server.iterdir() if p.is_dir() and p.name.lower() == folder), server / folder)
            selected = actual / symbol
            if selected.is_dir():
                shutil.copytree(selected, directory / bases.name / server.name / actual.name / symbol,
                                copy_function=copy_file)
        for file in server.glob("*"):
            if file.is_file():
                copy_file(file, directory / bases.name / server.name / file.name)
    target = mt5_tools.install_plan(robot, directory)
    for item in target:
        if not item.target.resolve().is_relative_to(directory.resolve()):
            raise ValueError("O plano de instalação alcança arquivos fora do perfil privado; revise o catálogo.")
        copy_file(item.source, item.target)
    # Includes do compilador são somente cópias, inclusive a biblioteca padrão.
    includes = data / "MQL5" / "Include"
    if includes.is_dir():
        shutil.copytree(includes, directory / "MQL5" / "Include", dirs_exist_ok=True,
                        copy_function=copy_file)
    child = _child if _child is not None else _private_config(config, directory)
    from . import safety_lib
    if check is not None:
        check(path=directory)
    safety_lib.ensure(child, robot, directory)
    # O catálogo resolve includes locais antes da biblioteca instalada. A cópia
    # avaliada precisa conservar exatamente esses bytes, inclusive WIP do dono.
    for item in target:
        if not item.target.resolve().is_relative_to(directory.resolve()):
            raise ValueError("O plano de instalação alcança arquivos fora do perfil privado; revise o catálogo.")
        copy_file(item.source, item.target)
    # Formato observado no visualizador nativo: panes secundários recolhidos,
    # barra de reprodução preservada. A pessoa pode reabri-los pelo menu View.
    visual = directory / "Tester" / "config" / "metatester.ini"
    visual.parent.mkdir(parents=True, exist_ok=True)
    # IsVisible isolado é ignorado pelo build nativo: preservar também o
    # grafo de docking, sem preferências, identidades ou caminhos do host.
    text = "[Visualization]\r\nchartmode=1\r\n" + (Path(__file__).parent / "templates" / "visual_tester.ini.tmpl").read_text(encoding="utf-8")
    if check is not None:
        check(path=visual, reserve_bytes=len(text.encode("utf-16")))
    visual.write_bytes(text.encode("utf-16"))
    return child


def telemetry_source(robot, filename, account=None, trades_filename=None, lease_filename=None, common_namespace=None,
                     input_receipt=None):
    """Preserva timers da estratégia; em conta, cria heartbeat próprio quando ela não usa timer."""
    import re
    source_graph = "\n".join(mql.read_text(Path(p)) for p in dict.fromkeys([robot.path] + robot.files)
                             if Path(p).suffix.lower() in (".mq5", ".mqh"))
    magic = battery.magic_number(robot, controls.effective_values(robot))
    magic_input = contract.input_name(robot, "magic")
    if not magic_input:
        magic_input = next((i.name for i in robot.inputs if "magic" in i.name.lower()), "")
    magic_expr = magic_input or (magic if str(magic).isdigit() else "0")
    events = []
    for name, args, call in (("OnTick", "", ""), ("OnTimer", "", ""),
                              ("OnTrade", "", ""), ("OnBookEvent", "const string &symbol", "symbol"),
                              ("OnTradeTransaction", "const MqlTradeTransaction &trans,const MqlTradeRequest &request,const MqlTradeResult &result",
                               "trans,request,result"),
                              ("OnChartEvent", "const int id,const long &lparam,const double &dparam,const string &sparam",
                               "id,lparam,dparam,sparam")):
        if re.search(r"\bvoid\s+" + name + r"\s*\(", source_graph):
            events.append((name, args, call))
    init = re.search(r"\b(int|void)\s+OnInit\s*\(", source_graph)
    if not events or not init:
        raise ValueError("Não foi possível instrumentar os eventos deste Expert; seu código original foi preservado.")
    definitions = "#define OnInit MT5X_OnInit\n" + "\n".join(f"#define {name} MT5X_{name}" for name, _, _ in events)
    undefs = "#undef OnInit\n" + "\n".join(f"#undef {name}" for name, _, _ in events)
    tester = bool(re.search(r"\bdouble\s+OnTester\s*\(", source_graph))
    deinit = bool(re.search(r"\bvoid\s+OnDeinit\s*\(", source_graph))
    own_timer = bool(account) and not re.search(r"\bEventSet(?:Millisecond)?Timer\s*\(", source_graph) and not any(name == "OnTimer" for name, _, _ in events)
    if deinit:
        definitions += "\n#define OnDeinit MT5X_OnDeinit"
        undefs += "\n#undef OnDeinit"
    if trades_filename and tester:
        definitions += "\n#define OnTester MT5X_OnTester"
        undefs += "\n#undef OnTester"
    common_code = ""
    if common_namespace:
        # Todas as trocas Common do código testado ficam numa pasta desta
        # passagem. O observador abaixo escreve só seus nomes UUID próprios.
        namespace = common_namespace.replace("\\", "\\\\")
        common_code = f'''string MT5X_CommonPath(const string name) {{
  if(StringFind(name,"..")>=0 || StringFind(name,":")>=0) return "{namespace}invalid_path";
  return "{namespace}"+name;
}}
int MT5X_CommonOpen(const string name,const int flags,const short delimiter=9,const uint codepage=CP_ACP) {{
  return FileOpen((flags&FILE_COMMON)!=0 ? MT5X_CommonPath(name) : name,flags,delimiter,codepage);
}}
bool MT5X_CommonExists(const string name,const int flags=0) {{
  return FileIsExist((flags&FILE_COMMON)!=0 ? MT5X_CommonPath(name) : name,flags);
}}
bool MT5X_CommonDelete(const string name,const int flags=0) {{
  return FileDelete((flags&FILE_COMMON)!=0 ? MT5X_CommonPath(name) : name,flags);
}}
bool MT5X_CommonCopy(const string src,const int common,const string dst,const int flags) {{
  return FileCopy((common&FILE_COMMON)!=0 ? MT5X_CommonPath(src) : src,common,
                  (flags&FILE_COMMON)!=0 ? MT5X_CommonPath(dst) : dst,flags);
}}
bool MT5X_CommonMove(const string src,const int common,const string dst,const int flags) {{
  return FileMove((common&FILE_COMMON)!=0 ? MT5X_CommonPath(src) : src,common,
                  (flags&FILE_COMMON)!=0 ? MT5X_CommonPath(dst) : dst,flags);
}}
bool MT5X_CommonFolderCreate(const string name,const int flags=0) {{ return FolderCreate((flags&FILE_COMMON)!=0 ? MT5X_CommonPath(name) : name,flags); }}
bool MT5X_CommonFolderDelete(const string name,const int flags=0) {{ return FolderDelete((flags&FILE_COMMON)!=0 ? MT5X_CommonPath(name) : name,flags); }}
bool MT5X_CommonFolderClean(const string name,const int flags=0) {{ return FolderClean((flags&FILE_COMMON)!=0 ? MT5X_CommonPath(name) : name,flags); }}
long MT5X_CommonProperty(const int handle,const ENUM_FILE_PROPERTY_INTEGER prop) {{ return FileGetInteger(handle,prop); }}
long MT5X_CommonProperty(const string name,const ENUM_FILE_PROPERTY_INTEGER prop,const bool common=false) {{
  return FileGetInteger(common ? MT5X_CommonPath(name) : name,prop,common);
}}
long MT5X_CommonFind(const string filter,string &name,const int flags=0) {{
  return FileFindFirst((flags&FILE_COMMON)!=0 ? MT5X_CommonPath(filter) : filter,name,flags);
}}
'''
        file_functions = {"FileOpen": "Open", "FileIsExist": "Exists", "FileDelete": "Delete", "FileCopy": "Copy",
                          "FileMove": "Move", "FolderCreate": "FolderCreate", "FolderDelete": "FolderDelete",
                          "FolderClean": "FolderClean", "FileGetInteger": "Property", "FileFindFirst": "Find"}
        definitions += "\n" + "\n".join(f"#define {name} MT5X_Common{suffix}" for name, suffix in file_functions.items())
        undefs += "\n" + "\n".join(f"#undef {name}" for name in file_functions)
    guard = "MQLInfoInteger(MQL_TESTER)"  # wrapper histórico nunca inicializa como Expert de conta
    lease_code = ""
    if account:
        # No código só há identidade não secreta; a conta e o tipo são conferidos a cada evento.
        server = str(account["server"]).replace("\\", "\\\\").replace('"', '\\"')
        mode = "ACCOUNT_TRADE_MODE_DEMO" if account["demo"] else "ACCOUNT_TRADE_MODE_REAL"
        guard = f'!MQLInfoInteger(MQL_TESTER) && AccountInfoInteger(ACCOUNT_LOGIN)=={int(account["login"])} && AccountInfoString(ACCOUNT_SERVER)=="{server}" && AccountInfoInteger(ACCOUNT_TRADE_MODE)=={mode}'
        if lease_filename:
            guard += " && MT5X_Lease()"
            lease_code = f'''bool MT5X_Lease() {{
  int f=FileOpen("{lease_filename}",FILE_READ|FILE_TXT|FILE_ANSI|FILE_COMMON|FILE_SHARE_READ|FILE_SHARE_WRITE);
  if(f==INVALID_HANDLE) return false;
  long stamp=StringToInteger(FileReadString(f)); FileClose(f);
  long age=(long)TimeGMT()-stamp;
  return stamp>0 && age>=-2 && age<=15;
}}
'''
    wrappers = "\n".join(f"void {name}({args}) {{ if(!({guard})) {{ ExpertRemove(); return; }} "
                           f"MT5X_{name}({call}); MT5X_Publish(); }}" for name, args, call in events)
    receipt_source = ""
    if input_receipt is not None:
        from . import native_inputs
        if account is not None:
            raise ValueError("Conferência de inputs disponível somente no Tester DEMO.")
        receipt_source = native_inputs.source(robot, **input_receipt)
        guard += " && AccountInfoInteger(ACCOUNT_TRADE_MODE)==ACCOUNT_TRADE_MODE_DEMO"
    timer_start = "EventSetTimer(1);" if own_timer else ""
    init_call = "int result=MT5X_OnInit();" if init.group(1) == "int" else "MT5X_OnInit(); int result=INIT_SUCCEEDED;"
    if input_receipt is not None:
        init_call += " if(result==INIT_SUCCEEDED && !MT5X_RecordInputs()) return INIT_FAILED;"
    init_call += f" if(result==INIT_SUCCEEDED) {{ {timer_start} MT5X_Chart(); MT5X_Publish(); }} return result;"
    wrappers += f"\nint OnInit() {{ if(!({guard})) return INIT_FAILED; {init_call} }}"
    if own_timer:
        wrappers += f"\nvoid OnTimer() {{ if(!({guard})) {{ ExpertRemove(); return; }} MT5X_Publish(); }}"
    if deinit or own_timer:
        wrappers += f"\nvoid OnDeinit(const int reason) {{ " + ("EventKillTimer(); " if own_timer else "") + (f"if({guard}) MT5X_OnDeinit(reason); " if deinit else "") + "}"
    observer = ""
    if trades_filename:
        observer = (Path(__file__).parent / "templates" / "session_export.mqh.tmpl").read_text(encoding="utf-8")
        wrappers += f'\ndouble OnTester() {{ double result={"MT5X_OnTester()" if tester else "0"}; MT5X_Export("{trades_filename}",{magic_expr}); return result; }}'
    lease_code += receipt_source
    # Atualização de estado limitada a ~4Hz de parede, sem importar o relógio histórico.
    return f'''{common_code}
{definitions}
#include "{Path(robot.path).name}"
{undefs}
{observer}
{lease_code}
ulong MT5X_Last=0;
void MT5X_Chart() {{
  if(MQLInfoInteger(MQL_TESTER) && !MQLInfoInteger(MQL_VISUAL_MODE)) return;
  ChartSetInteger(0,CHART_MODE,CHART_CANDLES); ChartSetInteger(0,CHART_SHOW_GRID,false);
  ChartSetInteger(0,CHART_COLOR_BACKGROUND,clrWhiteSmoke); ChartSetInteger(0,CHART_COLOR_FOREGROUND,clrDarkSlateGray);
  ChartSetInteger(0,CHART_COLOR_CHART_UP,clrSeaGreen); ChartSetInteger(0,CHART_COLOR_CHART_DOWN,clrIndianRed);
  ChartSetInteger(0,CHART_COLOR_CANDLE_BULL,clrSeaGreen); ChartSetInteger(0,CHART_COLOR_CANDLE_BEAR,clrIndianRed);
  ChartSetInteger(0,CHART_SHOW_TRADE_LEVELS,true); ChartSetInteger(0,CHART_AUTOSCROLL,true);
  ChartRedraw();
}}
void MT5X_Publish() {{
  ulong now=GetTickCount64(); if(now-MT5X_Last<250) return; MT5X_Last=now;
  int count=0; double profit=0;
  for(int i=PositionsTotal()-1;i>=0;i--) {{
    if(PositionGetTicket(i)>0 && PositionGetInteger(POSITION_MAGIC)=={magic_expr} && PositionGetString(POSITION_SYMBOL)==_Symbol) {{
      count++; profit+=PositionGetDouble(POSITION_PROFIT)+PositionGetDouble(POSITION_SWAP);
    }}
  }}
  int f=FileOpen("{filename}",FILE_WRITE|FILE_TXT|FILE_ANSI|FILE_COMMON|FILE_SHARE_READ);
  bool allowed=MQLInfoInteger(MQL_TESTER) || (MQLInfoInteger(MQL_TRADE_ALLOWED) && TerminalInfoInteger(TERMINAL_TRADE_ALLOWED));
  if(f!=INVALID_HANDLE) {{ FileWriteString(f,IntegerToString(count)+";"+DoubleToString(profit,8)+";"+IntegerToString(allowed)+";"+IntegerToString(TerminalInfoInteger(TERMINAL_CONNECTED))+";"+AccountInfoString(ACCOUNT_CURRENCY)); FileClose(f); }}
}}
{wrappers}
'''


def session_root(config):
    """Raiz dos perfis privados do executor; a retenção usa a mesma origem."""
    prefix = mt5_tools.wine_prefix(config)
    return prefix / "drive_c" / "mt5x_sessions" if prefix else paths.PROJECT_ROOT / "exports" / "robot_sessions"


class Session:
    """Uma execução, controlada fora da thread Tk; STOP nunca alcança outros terminais."""
    def __init__(self, db, config, robot, symbol, timeframe, preset=None, mode="backtest", start=None, end=None,
                 account=None, consent=None, root=None, native_options=None,
                 input_receipt_fields=None, expected_native_inputs=None):
        if mode not in ("play", "replay", "backtest"):
            raise ValueError("Modo de execução inválido.")
        self.db, self.config, self.robot = db, config, robot
        from mt5_extracao.resources.files import session_owner_key
        self.owner_store = session_owner_key(db)
        from . import native_inputs
        if (input_receipt_fields is not None or expected_native_inputs is not None) and mode == "play":
            raise ValueError("Conferência de inputs disponível somente no Tester DEMO.")
        if expected_native_inputs is not None and input_receipt_fields is None:
            raise ValueError("Informe os inputs públicos do perfil antes de conferir o preset.")
        self.input_fields = (native_inputs.fields_for(robot, input_receipt_fields)
                             if input_receipt_fields is not None else None)
        self.expected_inputs = (native_inputs.expected_values(expected_native_inputs, self.input_fields)
                                if self.input_fields is not None else None)
        self.native_input_receipt = None
        self.native_options, self.exception = native_options, None
        self.instrumented = False
        self.symbol, self.timeframe, self.preset, self.mode = symbol, timeframe, preset, mode
        self.start, self.end, self.account, self.consent = start, end, account, consent
        self.id = uuid.uuid4().hex
        self.directory = Path(root or session_root(config)) / self.id
        self.cancelled = threading.Event()
        self.state, self.error, self.record = "created", "", None
        self.process = None
        self.child_config = None
        self.started = time.time()
        self.finished = None
        self.telemetry_file = f"mt5x_session_{self.id}.txt"
        self.trades_file = f"mt5x_session_{self.id}_trades.csv"
        self.input_receipt_file = f"mt5x_session_{self.id}_inputs.csv"
        self.lease_file = f"mt5x_session_{self.id}_lease.txt"
        self.report = self.directory / "relatorio.json"
        self.observed_seconds = 0.0
        self.currency = str((account or {}).get("currency") or "")

    @property
    def active(self):
        return self.state in ("queued", "preparing", "running", "stopping")

    def cancel(self):
        self.cancelled.set()
        if self.active:
            self.state = "stopping"

    def _check_cancel(self, **kwargs):
        if self.native_options is not None and self.native_options.get("control") is not None:
            if kwargs.get("path") is not None:
                parent = Path(kwargs["path"])
                while not parent.exists() and parent.parent != parent:
                    parent = parent.parent
                kwargs["path"] = parent
            self.native_options["control"].check(**kwargs)
        if self.cancelled.is_set():
            raise Cancelled("Execução interrompida pelo usuário.")

    def _stop_owned(self):
        if self.mode == "play":
            try:
                self._write_lease("0")
            except (OSError, ValueError):
                log.exception("Não foi possível revogar a presença da sessão; encerrando seu terminal.")
        import psutil
        targets = []
        for proc in owned_processes(self.child_config, ("terminal64.exe", "MetaEditor64.exe", "metatester64.exe"), launchers=True):
            try:
                if proc.create_time() >= self.started - 2:
                    proc.terminate()
                    targets.append(proc)
            except psutil.NoSuchProcess:
                continue
        if self.process and self.process.poll() is None:
            self.process.terminate()
            targets.append(self.process)
        deadline = time.monotonic() + 15
        for proc in targets:
            try:
                proc.wait(timeout=max(.01, deadline - time.monotonic()))
            except (psutil.NoSuchProcess, ProcessLookupError):
                continue
            except (psutil.TimeoutExpired, subprocess.TimeoutExpired):
                log.warning("Um processo próprio ainda não encerrou; a sessão permanece protegida contra limpeza e nova execução.")

    def _write_lease(self, value=None):
        common = mt5_tools.common_files_dir(self.config)
        if common is None:
            raise ValueError("Pasta comum indisponível para acompanhar a sessão.")
        path = common / self.lease_file
        temp = path.with_suffix(".tmp")
        temp.write_text(value if value is not None else str(int(time.time())), encoding="ascii")
        os.replace(temp, path)

    def _run_command(self, command, cwd, prefix, timeout, compiling=False):
        self._check_cancel()
        env = dict(os.environ)
        if prefix:
            env.update(WINEPREFIX=str(prefix), WINEDEBUG="-all")
        hidden = compiling or self.mode == "backtest"
        if hidden and not paths.is_windows():
            env = display.child_env(self.child_config, {**env, display.ENV_SWITCH: "1"})
        # REPLAY e PLAY sempre seguem a tela do usuário, mesmo com hidden no INI.
        if paths.is_windows():
            args = command
            startup = display.windows_startup(hidden)
        else:
            bat = self.directory / "launch.bat"
            bat.write_bytes(("@echo off\r\n" + command.replace("%", "%%") + "\r\n").encode("cp1252"))
            args = ["wine", "cmd", "/c", paths.linux_to_wine_path(bat, prefix)]
            startup = None
        with (self.directory / ("compile-process.log" if compiling else "process.log")).open("wb") as output:
            if self.mode == "play" and not compiling:
                self._write_lease()
            self.process = subprocess.Popen(args, cwd=str(cwd), env=env, stdout=output, stderr=subprocess.STDOUT,
                                            **({"startupinfo": startup} if startup else {}))
            bounded = compiling or self.mode == "backtest"
            deadline = time.monotonic() + timeout if bounded and timeout is not None else None
            observed_at = time.monotonic()
            parent_seen, last_process_check = False, 0.0
            self.state = "preparing" if compiling else "running"
            while self.process.poll() is None:
                self._check_cancel()
                if self.cancelled.wait(.2):
                    self._stop_owned()
                    self.process.wait(timeout=15)
                    raise Cancelled("Execução interrompida pelo usuário.")
                if deadline is not None and time.monotonic() >= deadline:
                    self._stop_owned()
                    self.process.wait(timeout=15)
                    raise RuntimeError("A sessão excedeu o tempo previsto; o terminal desta sessão foi encerrado.")
                now = time.monotonic()
                if self.mode == "replay" and not compiling and now - last_process_check >= 1:
                    last_process_check = now
                    if native_interrupted(self.directory):
                        self._stop_owned()
                        self.process.wait(timeout=15)
                        raise Cancelled("O teste ou download de ticks foi interrompido no MT5. Não há resultado final aprovado.")
                    parents = owned_processes(self.child_config)
                    parent_seen = parent_seen or bool(parents)
                    # No Wine o cmd pode esperar o visualizador depois de o
                    # terminal já ter concluído e gravado o relatório. Encerrar
                    # somente o visualizador concluído entrega o relatório à UI.
                    if parent_seen and not parents:
                        html = list(self.directory.glob("mt5x_*.htm"))
                        common = mt5_tools.common_files_dir(self.config)
                        csv_ready = common is not None and (common / self.trades_file).is_file()
                        if html and csv_ready:
                            for viewer in owned_processes(self.child_config, ("metatester64.exe",)):
                                viewer.terminate()
                if self.mode == "play" and not compiling:
                    self._write_lease()
                status, _, _ = self.telemetry()
                if not compiling and self.mode == "play" and self.account.get("demo") and status[0] not in ("idle", "unknown", "disabled"):
                    self.observed_seconds += min(1, max(0, now - observed_at))
                observed_at = now
            if not compiling and self.mode != "play" and native_interrupted(self.directory):
                raise Cancelled("O teste ou download de ticks foi interrompido no MT5. Não há resultado final aprovado.")
            return self.process.returncode

    def telemetry(self):
        common = mt5_tools.common_files_dir(self.config)
        path = common / self.telemetry_file if common else None
        try:
            fields = path.read_text(encoding="cp1252").strip().split(";")
            count, profit = int(fields[0]), float(fields[1])
            if len(fields) > 4:
                self.currency = fields[4]
            fresh = self.active and time.time() - path.stat().st_mtime < 5
            if self.mode == "play" and fresh and (len(fields) < 4 or fields[2:4] != ["1", "1"]):
                return ("disabled", "Expert carregado · negociação ou conexão desligada"), count, profit
            return controls.activity_state(self.active, count, profit, fresh), count, profit
        except (OSError, ValueError, TypeError, AttributeError, IndexError):
            return controls.activity_state(self.active), None, None

    def _prepare_expert(self, *, allow_binary=False, compiled_override=None):
        """Preparação única dos executores existentes; o binário legado não ganha observador fictício."""
        self._check_cancel()
        if not allow_binary and not self.robot.has_source:
            raise ValueError("Estes comandos precisam do fonte para conferir segurança e acompanhar o Expert.")
        # Registrar a posse antes de copiar: uma preparação recusada pelo orçamento
        # ainda é nossa e pode comprovar que nenhum processo foi iniciado.
        self.child_config = _private_config(self.config, self.directory)
        if self.mode != "play":
            mt5_tools.register_native_context(self)
        self.child_config = prepare(self.config, self.robot, self.directory, self.symbol,
                                    require_source=not allow_binary, check=self._check_cancel,
                                    _child=self.child_config)
        self.child_config._mt5x_session = self
        self._check_cancel()
        if self.input_fields is not None and (not self.robot.has_source or compiled_override is not None):
            raise ValueError("Conferência dos inputs exige fonte instrumentável; remova o binário externo ou forneça a fonte revisada.")
        if not self.robot.has_source or compiled_override is not None:
            origin = Path(compiled_override) if compiled_override is not None else mt5_tools.install_plan(
                self.robot, mt5_tools.data_dir(self.config))[0].target.with_suffix(".ex5")
            if not origin.is_file():
                raise ValueError("Binário instalado indisponível; instale/compile antes de iniciar o teste isolado.")
            target = mt5_tools.install_plan(self.robot, self.directory)[0].target.with_suffix(".ex5")
            _copy_file(origin, target, check=self._check_cancel)
            self.instrumented = False
            return target
        plan = mt5_tools.install_plan(self.robot, self.directory)
        wrapper = plan[0].target.with_name("mt5x_session.mq5")
        wrapper.write_text(telemetry_source(self.robot, self.telemetry_file,
                                          self.account if self.mode == "play" else None,
                                          self.trades_file if self.mode != "play" else None,
                                          self.lease_file if self.mode == "play" else None,
                                          "mt5x_sessions\\" + self.id + "\\" if self.mode != "play" else None,
                                          input_receipt=({"filename": self.input_receipt_file, "session_id": self.id,
                                                          "fields": self.input_fields} if self.input_fields is not None else None)), encoding="utf-8")
        result = mt5_tools.compile_source(wrapper, mt5_tools.find_metaeditor(self.child_config),
                                          mt5_tools.wine_prefix(self.child_config),
                                          run=(self.native_options.get("run") if self.native_options is not None and self.native_options.get("run") is not None else
                                               lambda command, cwd, prefix, timeout: self._run_command(command, cwd, prefix, timeout, compiling=True)))
        if not result.ok:
            raise RuntimeError(result.summary)
        self.instrumented = True
        return wrapper

    def _verify_input_receipt(self, common):
        from . import native_inputs, store
        self._check_cancel()
        origin = common / self.input_receipt_file if common is not None else None
        if origin is None or not origin.is_file():
            raise ValueError("Recibo de inputs não encontrado; confira OnInit, o .set e os logs desta sessão.")
        target = self.directory / "native_inputs.csv"
        _copy_file(origin, target, check=self._check_cancel)
        receipt = native_inputs.read(target, session_id=self.id, symbol=self.symbol, timeframe=self.timeframe,
                                     fields=self.input_fields, expected=self.expected_inputs,
                                     complete=set(self.input_fields) == {item.name for item in self.robot.inputs})
        if self.record is None:
            raise ValueError("Execução nativa ausente; recibo não pode aprovar um resultado inexistente.")
        store.save_native_input_receipt(self.db, self.record["run_id"], receipt,
                                        artifact_directory=self.directory, robot_key=self.robot.key)
        self.record["native_inputs"] = receipt
        self.native_input_receipt = receipt

    def run(self, read_account=None):
        self.state = "preparing"
        native_lease = None
        try:
            self._check_cancel()
            if self.mode != "play":
                self.state = "queued"
                lease = mt5_tools.tester_lease(self._check_cancel)
                lease.__enter__()
                native_lease = lease
                self._check_cancel()
                self.state = "preparing"
            if self.mode == "play":
                if read_account is None:
                    raise ValueError("PLAY exige revalidação da conta conectada.")
                self.robot = catalog.get_robot(catalog.load_catalog(self.config, self.db), self.robot.key)
                current = read_account()
                context = controls.selection(self.robot, self.symbol, self.timeframe, self.preset)
                digest = controls.context_digest(self.robot, self.symbol, self.timeframe, self.preset, current, context["run"])
                if self.consent is None:
                    raise ValueError("Confirmação do PLAY ausente.")
                self.consent.validate(digest, bool(controls.loss_scenario(context["run"])), not current.get("demo", True))
                issues = controls.play_issues(self.config, self.robot, self.symbol, self.timeframe, self.preset, current, self.db)
                if issues:
                    raise ValueError("\n".join(issues))
                self.account = current
            wrapper = self._prepare_expert(allow_binary=self.native_options is not None,
                                           compiled_override=self.native_options.get("compiled_override") if self.native_options else None)
            # Usar o nome próprio: a recompilação automática do MT5 pode regenerar
            # o binário original, sem os observadores, ao abrir uma instalação nova.
            self._check_cancel()
            values = controls.native_values(self.robot, self.preset)
            export = contract.input_name(self.robot, "export_trades") or "InpExportTrades"
            if export in {i.name for i in self.robot.inputs}:
                values[export] = "false"   # não tocar CSV compartilhado de outra sessão
            if self.mode == "play":
                # Revalidar depois da preparação, que pode levar tempo.
                refreshed = catalog.get_robot(catalog.load_catalog(self.config, self.db), self.robot.key)
                current = read_account()
                digest = controls.context_digest(refreshed, self.symbol, self.timeframe, self.preset, current,
                                                 controls.selection(refreshed, self.symbol, self.timeframe, self.preset)["run"])
                self.consent.validate(digest, bool(controls.loss_scenario(context["run"])), not current.get("demo", True))
                issues = controls.play_issues(self.config, refreshed, self.symbol, self.timeframe, self.preset, current, self.db)
                if issues:
                    raise ValueError("\n".join(issues))
                if any(not item.target.is_file() or item.target.read_bytes() != item.source.read_bytes()
                       for item in mt5_tools.install_plan(refreshed, self.directory)
                       if item.source.suffix.lower() in (".mq5", ".mqh")):
                    raise ValueError("O código mudou durante a preparação. Revise e confirme o PLAY novamente.")
                preset = mt5_tools.write_tester_set(self.directory, "session", values)
                folder = self.directory / "MQL5" / "Presets"
                folder.mkdir(parents=True, exist_ok=True)
                shutil.copy2(self.directory / "MQL5" / "Profiles" / "Tester" / preset, folder / preset)
                ini = mt5_tools.startup_ini(mt5_tools.expert_name(wrapper, self.directory), self.symbol,
                                            self.timeframe, current["login"], current["server"], preset,
                                            protect_context=True)
                ini += live.organize_window(self.child_config, self.directory, self.symbol, self.timeframe)
                mt5_tools.start_terminal(self.child_config, self.directory, ini, "session",
                                        spawn=lambda command, cwd, prefix: self._run_command(command, cwd, prefix, None),
                                        is_running=lambda: bool(owned_processes(self.child_config)))
            else:
                import socket
                with socket.socket() as port_socket:
                    port_socket.bind(("127.0.0.1", 0))
                    tester_port = port_socket.getsockname()[1]
                common = mt5_tools.common_files_dir(self.config)
                if common is None and self.native_options is None:
                    raise ValueError("A pasta Common/Files não está disponível para o relatório da sessão.")
                options = self.native_options
                if options is None:
                    self.record = backtest.run_mql5(self.db, self.child_config, self.robot, self.symbol, self.timeframe,
                                                    self.start, self.end, self.preset, values, run=self._run_command,
                                                    model="4", visual=self.mode == "replay", read_csv=False,
                                                    export_trades=False,
                                                    tester_port=tester_port,
                                                    compiled_override=wrapper.with_suffix(".ex5"),
                                                    trades_csv_override=common / self.trades_file,
                                                    is_running=lambda: bool(owned_processes(self.child_config)))
                else:
                    call = dict(options)
                    call.update(config=self.child_config, timeframe=self.timeframe, start=self.start, end=self.end,
                                run=options.get("run") or self._run_command,
                                export_trades=False,
                                compiled_override=wrapper.with_suffix(".ex5"),
                                tester_port=options.get("tester_port") or tester_port,
                                is_running=lambda: bool(owned_processes(self.child_config)))
                    self.record = backtest.run_mql5(**call)
                if self.input_fields is not None:
                    self._verify_input_receipt(common)
            self.state = "completed"
        except Cancelled as error:
            if self.child_config is not None:
                self._stop_owned()
            self.exception = error if self.native_options is not None else None
            self.state, self.error = "cancelled", str(error)
        except Exception as error:
            self.exception = error if self.native_options is not None else None
            self.state, self.error = "failed", str(error) or type(error).__name__
            if self.child_config is not None:
                self._stop_owned()
            log.exception("Falha na sessão %s do robô %s", self.mode, self.robot.key)
        finally:
            self.finished = time.time()
            resources_released, resource_error = False, ""
            try:
                if self.child_config is not None:
                    if native_lease is not None:
                        resources_released = mt5_tools.release_native_context(self)
                    else:
                        resources_released = not owned_processes(self.child_config,
                            ("terminal64.exe", "MetaEditor64.exe", "metatester64.exe"), strict=True, launchers=True)
            except Exception as error:
                resource_error = str(error)
                log.exception("Recursos da sessão ainda não puderam ser liberados; preservando seus arquivos.")
            finally:
                if native_lease is not None:
                    native_lease.__exit__(None, None, None)
            if self.directory.exists():
                payload = {"artifact_owner": "mt5x-session", "artifact_version": 1,
                           "owner_store": self.owner_store,
                           "resources_released": resources_released, "resource_error": resource_error,
                           "wine_prefix": str(mt5_tools.wine_prefix(self.config) or ""),
                           "session": self.id, "mode": self.mode, "state": self.state, "error": self.error,
                           "robot": self.robot.key, "symbol": self.symbol, "timeframe": self.timeframe,
                           "preset": self.preset or "", "parameters": controls.effective_values(self.robot, self.preset),
                           "account": self.account if self.mode == "play" else None,
                           "started_at": datetime.fromtimestamp(self.started).isoformat(),
                           "finished_at": datetime.fromtimestamp(self.finished).isoformat(), "elapsed_seconds": self.finished - self.started,
                           "run": self.record, "limitations": ["Histórico não garante resultado futuro.",
                           "Custos e disponibilidade de ticks reais dependem do relatório/diário da corretora.",
                           "STOP desanexa o Expert ao encerrar só esta sessão; posições existentes não são liquidadas."]}
                if self.input_fields is not None:
                    payload["native_inputs"] = self.native_input_receipt
                self.report.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
                if self.mode == "play" and self.account and self.account.get("demo") and self.observed_seconds > 0:
                    from . import store
                    records = store.load_records(self.db)
                    meta = records.get(self.robot.key, {}).get("meta", {})
                    evidence = list(meta.get("demo_sessions", []))
                    evidence.append({"session": self.id, "verified_demo": True, "observed_seconds": self.observed_seconds,
                                     "identity": controls.context_digest(self.robot, self.symbol, self.timeframe, self.preset, {}, None)})
                    store.save_record(self.db, self.robot.key, self.robot.kind, self.robot.name, self.robot.path,
                                      self.robot.stage, dict(meta, demo_sessions=evidence))
                # Não conservar duplicatas do cache cifrado de login após encerrar.
                for folder in ("config", "Config"):
                    for file in (self.directory / folder).glob("*"):
                        if file.suffix.lower() in (".dat", ".srv"):
                            file.unlink()
        return self
