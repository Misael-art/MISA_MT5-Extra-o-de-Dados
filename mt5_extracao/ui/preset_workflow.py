"""T20.22 inicial: consumidor Tk do percurso S09; regras/finanças ficam nos serviços."""
from datetime import datetime,timedelta,timezone
from pathlib import Path
import tkinter as tk
from tkinter import ttk,filedialog,messagebox

from mt5_extracao.bootstrap import paths
from mt5_extracao.composition import Composition
from mt5_extracao.robots import store as robot_store
from mt5_extracao.presets import defaults,store,manual,execution
from mt5_extracao.presets.compiler import PresetIdentity,TestContext,compile_package
from mt5_extracao.presets.names import make_name
from .components import ScrollableContent,DetailsPanel
from .theme import fmt_num

FIELDS=(("fast","EMA curta (barras)"),("slow","EMA longa (barras)"),("warmup_bars","Aquecimento (barras)"),
        ("stop_points","SL fixo (pontos)"),("target_points","TP fixo (pontos)"))
INPUT_LABELS={"InpFastPeriod":"EMA curta","InpSlowPeriod":"EMA longa","InpWarmupBars":"Aquecimento",
 "InpStopPoints":"SL (pontos)","InpTargetPoints":"TP (pontos)","InpRiskPct":"Risco (%)",
 "InpMagic":"Magic","InpDeviationPoints":"Desvio (pontos)","InpPresetId":"ID do preset","InpConfigHash":"Hash da configuração"}


class NameDialog:
    def __init__(self,parent,robot,current,callback,*,rename=False):
        self.window=tk.Toplevel(parent);self.window.title("Renomear Manual" if rename else "Salvar como Manual")
        self.window.transient(parent);self.window.grab_set();self.window.geometry("650x240")
        self.robot,self.current,self.callback=robot,current,callback
        body=ttk.Frame(self.window,padding=16);body.pack(fill=tk.BOTH,expand=True)
        ttk.Label(body,text="Nome do preset",style="Heading.TLabel").pack(anchor="w")
        self.name=tk.StringVar(value=current.identity.label if rename else "")
        entry=ttk.Entry(body,textvariable=self.name);entry.pack(fill=tk.X,pady=8);entry.focus_set()
        self.preview=ttk.Label(body,wraplength=610,style="Muted.TLabel");self.preview.pack(fill=tk.X)
        self.save=ttk.Button(body,text="Salvar",style="Accent.TButton",command=self.confirm);self.save.pack(side=tk.RIGHT,pady=12)
        ttk.Button(body,text="Cancelar",command=self.window.destroy).pack(side=tk.LEFT,pady=12)
        self.name.trace_add("write",lambda *_:self.update());self.update()
    def update(self):
        try:
            identity=PresetIdentity("preview",category="Manual",label=self.name.get().strip())
            ctx=self.current.composition.to_dict()["context"]
            filename=make_name(self.robot.key.split(":",1)[-1],ctx["symbol"],ctx["timeframe"],identity,self.current.composition.config_hash).filename
            self.preview.configure(text=filename);self.save.state(["!disabled"])
        except ValueError as error:
            self.preview.configure(text=str(error));self.save.state(["disabled"])
    def confirm(self):
        name=self.name.get().strip();self.window.destroy();self.callback(name)


class PresetWorkflow:
    def __init__(self,controller,robot):
        self.controller,self.ui,self.robot=controller,controller.ui,robot
        self.db,self.config=self.ui.app.db_manager,self.ui.app.config
        self.symbol,self.timeframe,_=controller.chosen[robot.key]
        if not self.symbol:raise ValueError("Escolha Ativo / TF antes de abrir os presets.")
        self.window=tk.Toplevel(self.ui.root);self.window.title("S09 · Default e Manual")
        self.window.geometry("920x780");self.window.minsize(650,550);self.window.transient(self.ui.root)
        self.window.protocol("WM_DELETE_WINDOW",self.close)
        self.current=None;self.options={};self.pending=False;self.dirty=False;self.setting=False
        self.execution=None;self.last_evidence=None;self.name_dialog=None;self.artifacts={}
        self.status=tk.StringVar(value="Lendo Default e seleção…")
        content=ScrollableContent(self.window,self.ui.theme);content.pack(fill=tk.BOTH,expand=True)
        self.body=body=content.content
        ttk.Label(body,text="S09 · Default → Manual → backtest → replay",style="Title.TLabel",wraplength=850).pack(anchor="w",padx=12,pady=(12,4))
        ttk.Label(body,text=f"{self.symbol} · {self.timeframe} · variante mínima EMA com SL/TP fixos",style="Heading.TLabel",wraplength=850).pack(anchor="w",padx=12)
        ttk.Label(body,text="Histórico no Strategy Tester DEMO · execução ao vivo não faz parte deste percurso.",style="Info.TLabel",wraplength=850).pack(anchor="w",padx=12,pady=4)
        row=ttk.Frame(body,padding=12);row.pack(fill=tk.X);row.columnconfigure(0,weight=1)
        self.selected=tk.StringVar();self.combo=ttk.Combobox(row,textvariable=self.selected,state="readonly")
        self.combo.grid(row=0,column=0,sticky="ew");self.combo.bind("<<ComboboxSelected>>",lambda _e:self.choose())
        choices=ttk.Frame(row);choices.grid(row=1,column=0,sticky="ew",pady=(6,0))
        self.reopen=ttk.Button(choices,text="Reabrir",command=self.reload)
        self.clone=ttk.Button(choices,text="Salvar como Manual",command=self.clone_dialog)
        self.rename=ttk.Button(choices,text="Renomear",command=self.rename_dialog)
        self.flow(choices,(self.reopen,self.clone,self.rename))
        form=ttk.LabelFrame(body,text="Parâmetros da estratégia",padding=12);form.pack(fill=tk.X,padx=12,pady=4)
        self.fields={};self.entries=[]
        for index,(key,label) in enumerate(FIELDS):
            ttk.Label(form,text=label).grid(row=index//3*2,column=index%3,sticky="w",padx=(0,12))
            value=tk.StringVar();value.trace_add("write",lambda *_:self.mark_dirty())
            entry=ttk.Entry(form,textvariable=value,width=16);entry.grid(row=index//3*2+1,column=index%3,sticky="ew",padx=(0,12),pady=(2,8))
            self.fields[key]=value;self.entries.append(entry)
        for col in range(3):form.columnconfigure(col,weight=1)
        self.save=ttk.Button(form,text="Salvar revisão",command=self.save_revision);self.save.grid(row=3,column=2,sticky="e")
        self.protection=ttk.Label(body,text="",style="Muted.TLabel",wraplength=850);self.protection.pack(fill=tk.X,padx=12,pady=4)
        ctx=ttk.LabelFrame(body,text="Contexto do ensaio histórico",padding=12);ctx.pack(fill=tk.X,padx=12,pady=6)
        now=datetime.now();self.start=tk.StringVar(value=(now-timedelta(days=1)).date().isoformat());self.end=tk.StringVar(value=now.date().isoformat())
        self.deposit=tk.StringVar(value=str(self.config.getfloat("STRATEGY","initial_equity",fallback=100000)))
        self.context_entries=[]
        for index,(label,var) in enumerate((("De (AAAA-MM-DD)",self.start),("Até (AAAA-MM-DD)",self.end),("Capital simulado · moeda do Tester",self.deposit))):
            ttk.Label(ctx,text=label).grid(row=0,column=index,sticky="w",padx=(0,12));entry=ttk.Entry(ctx,textvariable=var,width=22)
            entry.grid(row=1,column=index,sticky="ew",padx=(0,12),pady=4);ctx.columnconfigure(index,weight=1);self.context_entries.append(entry)
            var.trace_add("write",lambda *_:self.context_changed())
        ttk.Label(ctx,text="Modelo: cada tick · cobertura efetiva de ticks reais vem do relatório, não desta escolha.",style="Muted.TLabel",wraplength=850).grid(row=2,column=0,columnspan=3,sticky="w")
        output=ttk.Frame(body,padding=12);output.pack(fill=tk.X);output.columnconfigure(0,weight=1)
        self.export_dir=tk.StringVar(value=str(paths.PROJECT_ROOT/"exports"/"presets"))
        ttk.Label(output,text="Pasta de exportação").grid(row=0,column=0,sticky="w")
        ttk.Entry(output,textvariable=self.export_dir).grid(row=1,column=0,sticky="ew")
        ttk.Button(output,text="Escolher…",command=self.browse).grid(row=1,column=1,padx=4)
        actions=ttk.Frame(body,padding=(12,4));actions.pack(fill=tk.X)
        self.export=ttk.Button(actions,text="Exportar pacote",command=self.export_package)
        self.import_button=ttk.Button(actions,text="Importar Manual…",command=self.import_package)
        self.backtest=ttk.Button(actions,text="Backtest nativo",style="Accent.TButton",command=lambda:self.start_native("backtest"))
        self.replay=ttk.Button(actions,text="Replay nativo",command=lambda:self.start_native("replay"))
        self.stop=ttk.Button(actions,text="Cancelar esta sessão",command=self.cancel)
        self.flow(actions,(self.export,self.import_button,self.backtest,self.replay,self.stop))
        ttk.Label(body,textvariable=self.status,wraplength=850,style="Info.TLabel").pack(fill=tk.X,padx=12,pady=8)
        summary=ttk.Frame(body,padding=(12,4));summary.pack(fill=tk.X);self.result=tk.StringVar(value="Sem conferência nativa neste contexto.")
        ttk.Label(summary,textvariable=self.result,wraplength=850,style="Heading.TLabel").pack(fill=tk.X)
        ttk.Button(summary,text="Consultar resultados salvos",command=self.consult).pack(anchor="w",pady=4)
        self.tables=ttk.Notebook(body);self.tables.pack(fill=tk.BOTH,expand=True,padx=12,pady=6)
        self.trades=self.tree("Negócios",("entrada","saída","lado","lotes","resultado","SL","TP"))
        self.inputs=self.tree("Inputs carregados",("input","esperado","lido no MT5"))
        self.signals=self.tree("Sinais · horário do servidor",("origem","confirmação","disponível","lado","entrada","motivo"))
        details=DetailsPanel(body,"Identidade e arquivo do preset",expanded=False,information=True);details.pack(fill=tk.X,padx=12,pady=6)
        self.details=ttk.Label(details.body,text="",wraplength=830,style="Muted.TLabel");self.details.pack(fill=tk.X)
        foot=ttk.Frame(body,padding=12);foot.pack(fill=tk.X)
        self.files=ttk.Button(foot,text="Arquivos da sessão",command=self.open_files);self.files.pack(side=tk.LEFT)
        self.csv=ttk.Button(foot,text="Abrir CSV nativo",command=self.open_csv);self.csv.pack(side=tk.LEFT,padx=4)
        ttk.Button(foot,text="Fechar",command=self.close).pack(side=tk.RIGHT)
        self.body.bind("<Configure>",self.wrap,add="+")
        self.reload()
    @staticmethod
    def flow(frame,buttons):
        previous=[None]
        def layout(event):
            width=max(button.winfo_reqwidth() for button in buttons)+8
            columns=max(1,min(3,event.width//width))
            if columns==previous[0]:return
            previous[0]=columns
            for col in range(len(buttons)):frame.columnconfigure(col,weight=int(col<columns),uniform="preset" if col<columns else "")
            for index,button in enumerate(buttons):button.grid(row=index//columns,column=index%columns,sticky="ew",padx=(0,6),pady=3)
        frame.bind("<Configure>",layout,add="+")
    def wrap(self,event):
        def walk(widget):
            for child in widget.winfo_children():
                if isinstance(child,ttk.Label) and int(child.cget("wraplength"))>0:child.configure(wraplength=max(120,event.width-48))
                walk(child)
        walk(self.body)
    def tree(self,label,columns):
        frame=ttk.Frame(self.tables);self.tables.add(frame,text=label)
        tree=ttk.Treeview(frame,columns=columns,show="headings",height=6)
        for col in columns:tree.heading(col,text=col);tree.column(col,width=115,minwidth=60,stretch=True)
        y=ttk.Scrollbar(frame,orient=tk.VERTICAL,command=tree.yview);x=ttk.Scrollbar(frame,orient=tk.HORIZONTAL,command=tree.xview)
        tree.configure(yscrollcommand=y.set,xscrollcommand=x.set);y.pack(side=tk.RIGHT,fill=tk.Y);x.pack(side=tk.BOTTOM,fill=tk.X);tree.pack(fill=tk.BOTH,expand=True)
        return tree
    def exists(self):return bool(self.window.winfo_exists())
    def _busy(self,value):
        self.pending=value;self.update_buttons()
    def update_buttons(self):
        active=bool(self.execution and self.execution.session.active)
        busy=self.pending or active;loaded=self.current is not None;is_manual=loaded and self.current.identity.category=="Manual"
        for button in (self.reopen,self.clone,self.export,self.import_button):button.state(["disabled" if busy or not loaded else "!disabled"])
        self.combo.configure(state="disabled" if busy or self.dirty else "readonly")
        self.reopen.state(["disabled" if busy else "!disabled"])
        self.clone.state(["disabled" if busy or not loaded or self.dirty else "!disabled"])
        self.export.state(["disabled" if busy or not loaded or self.dirty else "!disabled"])
        for entry in self.entries:entry.configure(state="normal" if is_manual and not busy else "disabled")
        for entry in self.context_entries:entry.configure(state="disabled" if busy else "normal")
        self.save.state(["!disabled" if is_manual and self.dirty and not busy else "disabled"])
        self.rename.state(["!disabled" if is_manual and not self.dirty and not busy else "disabled"])
        disabled=busy or not loaded or self.dirty or bool(getattr(self.ui.app,"offline",False))
        for button in (self.backtest,self.replay):button.state(["disabled" if disabled else "!disabled"])
        self.stop.state(["!disabled" if active else "disabled"])
        self.files.state(["!disabled" if self.artifacts.get("artifact_directory") else "disabled"])
        self.csv.state(["!disabled" if self.artifacts.get("trades_csv") else "disabled"])
    def work(self,action,done):
        if self.pending:return
        self._busy(True)
        def finished(value,error):
            if not self.exists():return
            self.pending=False
            if error:self.status.set(str(error));self.update_buttons();return
            done(value);self.update_buttons()
        self.ui.run_bg(action,finished)
    def _load(self,preferred=None):
        default=defaults.ensure_default(self.db,self.robot,symbol=self.symbol,timeframe=self.timeframe)
        items=[default]+manual.list_manual(self.db,self.robot.key,self.symbol,self.timeframe)
        chosen=store.selected_preset(self.db,self.robot.key,self.symbol,self.timeframe)
        wanted=preferred or (chosen.identity.id if chosen else default.identity.id)
        if chosen and all(item.identity.id!=chosen.identity.id for item in items):
            items.append(chosen)
        selected=next((item for item in items if item.identity.id==wanted),default)
        if chosen and chosen.identity.id==wanted and chosen.identity.revision!=selected.identity.revision:
            selected=manual.get_manual(self.db,wanted,chosen.identity.revision) if chosen.identity.category=="Manual" else chosen
            items=[item for item in items if item.identity.id!=wanted]+[selected]
        return items,selected
    def reload(self,preferred=None):
        self.work(lambda:self._load(preferred),self.loaded)
    def loaded(self,data):
        items,selected=data
        self.options={}
        for item in items:
            label=("Default · Padrão do código" if item.identity.category=="Default" else "Manual · "+item.identity.label)+f" · v{item.identity.revision}"
            if label in self.options:label+=" · fonte anterior"
            self.options[label]=item
        self.combo.configure(values=list(self.options));self.set_current(selected)
    def set_current(self,item):
        self.current=item;self.setting=True
        for key,_ in FIELDS:self.fields[key].set(str(item.composition.to_dict()["strategy"]["params"][key]))
        self.selected.set(next(label for label,value in self.options.items() if value.identity==item.identity))
        self.setting=False;self.dirty=False
        label="Default preservado · use Salvar como Manual para editar." if item.identity.category=="Default" else "Rascunho compilado · aguarda conferência nativa deste contexto."
        self.status.set(label)
        self.protection.configure(text="Risco por saldo: até 0,5% · uma posição · SL obrigatório · parcial/BE/trailing indisponíveis. Proteções não são editadas aqui.")
        self.details.configure(text=(f"Nome: {item.identity.label}\nRevisão: {item.identity.revision}\n"
                                    f"Arquivo: {item.name.filename}\nConfiguração: {item.composition.config_hash}\n"
                                    f"Origem Default: {item.identity.default_origin or item.identity.id}"))
        self.clear_result();self.update_buttons()
    def choose(self):
        if self.pending:return
        item=self.options[self.selected.get()]
        self.work(lambda:(store.select_preset(self.db,self.robot.key,self.symbol,self.timeframe,item.identity.id,item.identity.revision),item)[1],self.set_current)
    def mark_dirty(self):
        if self.setting or self.current is None:return
        self.dirty=True;self.status.set("Alterações não salvas · salve uma revisão antes de executar.");self.update_buttons();self.clear_result()
    def context_changed(self):
        if self.setting:return
        self.clear_result()
    def clear_result(self):
        self.last_evidence=None;self.artifacts={};self.result.set("Sem conferência nativa neste contexto.")
        for tree in (self.trades,self.inputs,self.signals):tree.delete(*tree.get_children())
    def clone_dialog(self):
        if self.current is None or self.pending or self.dirty:return
        self.name_dialog=NameDialog(self.window,self.robot,self.current,self.clone_named)
    def clone_named(self,name):
        source=self.current
        def action():
            item=manual.save_as_manual(self.db,self.robot,source.identity.id,name,source_revision=source.identity.revision)
            store.select_preset(self.db,self.robot.key,self.symbol,self.timeframe,item.identity.id,item.identity.revision)
            return self._load(item.identity.id)
        self.work(action,self.loaded)
    def rename_dialog(self):
        self.name_dialog=NameDialog(self.window,self.robot,self.current,self.rename_named,rename=True)
    def rename_named(self,name):
        current=self.current
        self.work(lambda:(manual.rename_manual(self.db,current.identity.id,name),self._load(current.identity.id))[1],self.loaded)
    def save_revision(self):
        try:
            data=self.current.composition.to_dict()
            for key,_ in FIELDS:data["strategy"]["params"][key]=int(self.fields[key].get().strip())
            comp=Composition(data);current=self.current
        except ValueError as error:self.status.set("Confira períodos/pontos inteiros. "+str(error));return
        def action():
            item=manual.save_revision(self.db,self.robot,current.identity.id,comp,expected_revision=current.identity.revision)
            store.select_preset(self.db,self.robot.key,self.symbol,self.timeframe,item.identity.id,item.identity.revision)
            return self._load(item.identity.id)
        self.work(action,self.loaded)
    def context(self):
        return TestContext(self.start.get().strip(),self.end.get().strip(),deposit=float(self.deposit.get().strip().replace(",",".")),model="0")
    def browse(self):
        folder=filedialog.askdirectory(parent=self.window,title="Pasta de exportação")
        if folder:self.export_dir.set(folder)
    def export_action(self,context):
        if self.dirty:raise ValueError("Salve a revisão antes de exportar ou executar.")
        preset_id,revision,directory=self.current.identity.id,self.current.identity.revision,self.export_dir.get()
        return lambda:manual.export_preset(self.db,self.robot,preset_id,directory,revision=revision,test_context=context)
    def export_package(self):
        try:context=self.context();action=self.export_action(context)
        except ValueError as error:self.status.set(str(error));return
        self.work(action,lambda value:self.status.set("Pacote exportado sem sobrescrever: "+str(value["set_path"])))
    def import_package(self):
        path=filedialog.askopenfilename(parent=self.window,title="Importar Manual",filetypes=[("Pacote Manual","*.manifest.json")])
        if not path:return
        def action():
            item=manual.import_manual(self.db,self.robot,path)
            store.select_preset(self.db,self.robot.key,self.symbol,self.timeframe,item.identity.id,item.identity.revision)
            return self._load(item.identity.id)
        self.work(action,self.loaded)
    def start_native(self,mode):
        if self.pending:return
        if getattr(self.ui.app,"offline",False):self.status.set("Reabra sem --offline para executar o MT5; edição/exportação continuam locais.");return
        if self.controller.chosen[self.robot.key][:2]!=(self.symbol,self.timeframe):self.status.set("Ativo/TF mudou no card; reabra o percurso no contexto selecionado.");return
        existing=self.controller.sessions.get(self.robot.key)
        if existing and existing.active:self.status.set("Já existe sessão própria em andamento; aguarde ou cancele-a.");return
        try:context=self.context();action=self.export_action(context)
        except ValueError as error:self.status.set(str(error));return
        def prepared(value):
            try:job=execution.PresetExecution(self.db,self.config,self.robot,value["manifest_path"],mode=mode)
            except ValueError as error:self.status.set(str(error));return
            self.execution=job;self.controller.sessions[self.robot.key]=job.session
            job.session.state="preparing";self.controller.panel.render();self.status.set("Preparando "+("replay Visual" if mode=="replay" else "backtest nativo")+" · sessão própria…")
            self.work(job.run,self.completed)
        self.work(action,prepared)
    def completed(self,job):
        if job.evidence:self.show_evidence(job.evidence,job.session.record["metrics"])
        else:self.result.set("Conferência pendente: "+(job.error or job.status));self.status.set(job.error or job.status)
        self.controller.panel.reload()
    def cancel(self):
        if self.execution:self.execution.cancel();self.status.set("Cancelando somente esta sessão…")
    def consult(self):
        if self.current is None or self.pending:return
        if self.dirty:self.status.set("Salve a revisão antes de consultar este contexto.");return
        try:
            context=self.context();current=self.current
            package=compile_package(current.composition,self.robot,current.identity,context,canonical_filename=True)
        except ValueError as error:self.status.set(str(error));return
        def action():
            rows=execution.load_evidence(self.db,execution_hash=package.execution_hash)
            if not rows:return None
            runs=robot_store.load_runs(self.db,run_id=rows[0]["run_id"])
            return rows[0],runs[0]["metrics"] if runs else {}
        self.work(action,lambda value:self.show_evidence(*value) if value else self.result.set("Sem evidência nativa salva para esta revisão/contexto."))
    def show_evidence(self,proof,artifacts=None):
        self.clear_result();self.last_evidence=proof;self.artifacts=artifacts or {}
        status=proof["status"];trades=proof["trades"]
        text=("Replay nativo conferido com o backtest" if proof["functional_cycle"] else
              "Somente contratos: nenhum negócio; aceite pendente" if status=="contract_only" else
              "Divergência nativa; aceite pendente" if status=="native_divergence" else
              "Backtest nativo conferido" if proof["mode"]=="backtest" else "Replay executado; falta referência do backtest")
        self.result.set(f"{text} · {trades} operações encerradas\n"
                        f"Resultado {fmt_num(proof['net_profit'])} {proof.get('currency') or ''} · CSV/livro/saldo conciliados.\n"
                        "Simulação Python é outro motor; execução ao vivo DEMO não foi realizada por este percurso.")
        self.status.set("Inputs realmente carregados e modo nativo conferidos. Histórico/revisão da corretora sem certificação; cobertura de ticks reais depende do relatório.")
        for row in proof["ledger"]:self.trades.insert("","end",values=(row["entry_time"],row["exit_time"],"Compra" if row["direction"]>0 else "Venda",row["volume"],row["pnl"],row["stop_price"],row["target_price"]))
        expected=self.current.manifest["native"]["expected_native_inputs"]
        for key,value in proof["inputs"]["values"].items():self.inputs.insert("","end",values=(INPUT_LABELS.get(key,key),expected.get(key,"—"),value))
        def clock(value):return datetime.fromtimestamp(value,timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for row in proof["signals"]:self.signals.insert("","end",values=(clock(row["event_at"]),clock(row["confirmed_at"]),clock(row["available_at"]),"Compra" if row["direction"]>0 else "Venda","Sim" if row["opened"] else "Recusada",row["reason"]))
        if proof.get("first_difference"):self.status.set("Primeira divergência: "+str(proof["first_difference"])+". Confira os arquivos; tolerância não foi ampliada.")
    def open_files(self):
        value=self.artifacts.get("artifact_directory")
        if value:self.ui._open_path(str(value))
    def open_csv(self):
        value=self.artifacts.get("trades_csv")
        if value:self.ui._open_path(str(value))
    def close(self):
        if self.pending or self.execution and self.execution.session.active:self.window.withdraw()
        else:self.window.destroy()
