"""T20.22 inicial: consumidor Tk do percurso S09; regras/finanças ficam nos serviços."""
from datetime import datetime,timedelta,timezone
from pathlib import Path
import tkinter as tk
from tkinter import ttk,filedialog,messagebox

from mt5_extracao.bootstrap import paths
from mt5_extracao.composition import Composition
from mt5_extracao.composition import native
from mt5_extracao.composition.partials import stages,preview_plan
from mt5_extracao.composition.sizing import preview_sizing,preview_fixed_volume,SizingPreviewUnavailable
from mt5_extracao.robots import store as robot_store
from mt5_extracao.presets import defaults,store,manual,execution
from mt5_extracao.presets.compiler import PresetIdentity,TestContext,compile_package
from mt5_extracao.presets.names import make_name
from .components import ScrollableContent,DetailsPanel
from .theme import fmt_num

def native_adapter_profile(robot):
    if robot.key=="mql5:hdk_pullback_nativo":
        from mt5_extracao.composition.pullback_native import require_robot
        return require_robot(robot)
    if robot.key=="mql5:hdk_stochema9":
        from mt5_extracao.composition.stoch_native import require_robot
        return require_robot(robot)
    return native._require_robot(robot)


FIELDS=(("fast","EMA curta (barras)"),("slow","EMA longa (barras)"),("warmup_bars","Aquecimento (barras)"),
        ("stop_points","SL fixo (pontos)"),("target_points","TP fixo (pontos)"))
INPUT_LABELS={"InpFastPeriod":"EMA curta","InpSlowPeriod":"EMA longa","InpWarmupBars":"Aquecimento",
 "InpStopPoints":"SL (pontos)","InpTargetPoints":"TP (pontos)","InpRiskPct":"Risco (%)",
 "InpMagic":"Magic","InpDeviationPoints":"Desvio (pontos)","InpPresetId":"ID do preset","InpConfigHash":"Hash da configuração",
 "InpLotMode":"Modo de lotes (0 fixo / 1 risco)","InpFixedLot":"Volume fixo solicitado (lotes)"}

INPUT_LABELS.update({"InpTimeMaxSeconds":"Prazo absoluto (s)","InpTimeProgressSeconds":"Prazo de progresso (s)",
 "InpTimeMinimumProfit":"Lucro mínimo (moeda)","InpTimeGainSeconds":"Prazo de ganho (s)","InpTimeLossSeconds":"Prazo de perda (s)",
 "InpTimeTieSeconds":"Prazo de empate (s)","InpTimeFlatBand":"Faixa de empate (moeda)","InpTimeMoneyDigits":"Casas da moeda", "InpTimeDonchianIdleSeconds":"Tempo sem renovação Donchian (s)",
 "InpTimeATRBaseSeconds":"Prazo base ATR (s)","InpTimeATRMinSeconds":"Mínimo ATR (s)",
 "InpTimeERMinSeconds":"Idade mínima ER (s)","InpTimeERMaximum":"ER abaixo de (fração)"})

INPUT_LABELS.update({"InpMagicNumber":"Magic", "InpDailyLossPct":"Perda máxima diária (%)",
 "InpWeeklyLossPct":"Perda máxima semanal (%)", "InpMaxDrawdownPct":"Drawdown máximo (%)",
 "InpKPeriod":"Período K", "InpSlowing":"Desaceleração K", "InpDPeriod":"Período D SMA", "InpEmaPeriod":"Período EMA",
 "InpUpperLevel":"Nível de compra", "InpLowerLevel":"Nível de venda", "InpStopOffsetPoints":"Offset EMA (pontos)",
 "InpExportTrades":"Exportação adicional do Expert"})

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
        self.pullback=robot.key=="mql5:hdk_pullback_nativo"
        self.stoch=robot.key=="mql5:hdk_stochema9"
        profile_level=native_adapter_profile(robot)[2]
        self.strategy_fields=(("trend","EMA de tendência (barras)"),("adx_min","ADX mínimo"),("rsi_period","RSI (barras)"),("rsi_pullback","RSI de recuo"),("swing_bars","Fundo/topo (barras)"),("target_r","Alvo em R")) if self.pullback else FIELDS
        if self.stoch:
            from mt5_extracao.composition.stoch_native import STRATEGY_FIELDS
            self.strategy_fields=STRATEGY_FIELDS
        self.profile_label="S10 · Estocástico/EMA" if self.stoch else "Pullback" if self.pullback else "S09"
        self.window=tk.Toplevel(self.ui.root);self.window.title(self.profile_label+" · Default e Manual")
        self.window.geometry("920x780");self.window.minsize(650,550);self.window.transient(self.ui.root)
        self.window.protocol("WM_DELETE_WINDOW",self.close)
        self.current=None;self.options={};self.pending=False;self.dirty=False;self.setting=False
        self.lots_enabled=profile_level>=2
        self.partials_enabled=profile_level>=3
        self.temporal_enabled=not self.pullback and profile_level>=4
        self.er_enabled=not self.pullback and profile_level>=5
        self.donchian_enabled=not self.pullback and profile_level>=6
        self.atr_enabled=not self.pullback and profile_level>=7
        self.session_available=not self.pullback and profile_level>=8
        self.cooldown_available=not self.pullback and profile_level>=9
        self.symbol_info=None;self.preview_error=False;self.source_compatible=True
        self.execution=None;self.last_evidence=None;self.name_dialog=None;self.artifacts={}
        self.status=tk.StringVar(value="Lendo Default e seleção…")
        content=ScrollableContent(self.window,self.ui.theme);content.pack(fill=tk.BOTH,expand=True)
        self.body=body=content.content
        ttk.Label(body,text=self.profile_label+" · Default → Manual → backtest → replay",style="Title.TLabel",wraplength=850).pack(anchor="w",padx=12,pady=(12,4))
        ttk.Label(body,text=f"{self.symbol} · {self.timeframe} · "+("K lento por razão de somas · D SMA · stop/Move Guard EMA · alvo desde o sinal" if self.stoch else "EMA/ADX/RSI · stop no swing · alvo em R · metade em 1R" if self.pullback else "variante mínima EMA com SL/TP fixos"),style="Heading.TLabel",wraplength=850).pack(anchor="w",padx=12)
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
        for index,(key,label) in enumerate(self.strategy_fields):
            ttk.Label(form,text=label,wraplength=180 if self.stoch else 0).grid(row=index//3*2,column=index%3,sticky="w",padx=(0,12))
            value=tk.StringVar();value.trace_add("write",lambda *_:self.mark_dirty())
            entry=ttk.Entry(form,textvariable=value,width=16);entry.grid(row=index//3*2+1,column=index%3,sticky="ew",padx=(0,12),pady=(2,8))
            self.fields[key]=value;self.entries.append(entry)
        for col in range(3):form.columnconfigure(col,weight=1)
        self.save=ttk.Button(form,text="Salvar revisão",command=self.save_revision);self.save.grid(row=5 if self.pullback or self.stoch else 3,column=2,sticky="e")
        if self.lots_enabled:
            lots=ttk.LabelFrame(body,text="Lotes · uma única entrada",padding=12);lots.pack(fill=tk.X,padx=12,pady=4)
            fields=ttk.Frame(lots);fields.pack(fill=tk.X)
            mode_field,fixed_field,risk_field=(ttk.Frame(fields) for _ in range(3))
            self.lot_mode=tk.StringVar(value="Por risco");self.fixed_volume=tk.StringVar(value="0.01");self.risk_pct=tk.StringVar(value="0.5")
            ttk.Label(mode_field,text="Modo de dimensionamento").pack(anchor="w")
            self.lot_combo=ttk.Combobox(mode_field,textvariable=self.lot_mode,values=("Por risco","Volume fixo"),state="readonly",width=20)
            self.lot_combo.pack(fill=tk.X)
            ttk.Label(fixed_field,text="Volume fixo solicitado (lotes)").pack(anchor="w")
            self.fixed_entry=ttk.Entry(fixed_field,textvariable=self.fixed_volume,width=16);self.fixed_entry.pack(fill=tk.X)
            ttk.Label(risk_field,text="Orçamento (% do saldo, até 0,5)").pack(anchor="w")
            risk_entry=ttk.Entry(risk_field,textvariable=self.risk_pct,width=16);risk_entry.pack(fill=tk.X)
            self.flow(fields,(mode_field,fixed_field,risk_field))
            self.entries.extend((self.fixed_entry,risk_entry))
            self.lot_preview=tk.StringVar(value="Aguardando especificação atual do símbolo…")
            ttk.Label(lots,textvariable=self.lot_preview,wraplength=850).pack(fill=tk.X,pady=(8,0))
            for variable in (self.lot_mode,self.fixed_volume,self.risk_pct):variable.trace_add("write",lambda *_:self.mark_dirty())
        if self.stoch:
            budget=ttk.LabelFrame(body,text="Orçamento por entrada",padding=12);budget.pack(fill=tk.X,padx=12,pady=4)
            self.risk_pct=tk.StringVar(value="0.5")
            ttk.Label(budget,text="Percentual do saldo · até 0,5% · dimensionamento por risco").pack(anchor="w")
            entry=ttk.Entry(budget,textvariable=self.risk_pct,width=16);entry.pack(anchor="w",pady=4)
            self.entries.append(entry);self.risk_pct.trace_add("write",lambda *_:self.mark_dirty())
        if self.partials_enabled:
            box=ttk.LabelFrame(body,text="Parciais · saldo protegido",padding=12);box.pack(fill=tk.X,padx=12,pady=4)
            self.partial_mode=tk.StringVar(value="Desabilitada");self.partial_count=tk.StringVar(value="1")
            self.partial_be=tk.BooleanVar(value=False);self.partial_fields={};self.partial_widgets=[]
            first=ttk.Frame(box);first.pack(fill=tk.X)
            for label,var,choices in (("Unidade da agenda",self.partial_mode,("Desabilitada","Fração (%)","Lotes")),("Etapas",self.partial_count,("1","2"))):
                field=ttk.Frame(first);field.pack(side=tk.LEFT,fill=tk.X,expand=True,padx=(0,8))
                ttk.Label(field,text=label).pack(anchor="w");widget=ttk.Combobox(field,textvariable=var,values=choices,state="readonly",width=20);widget.pack(fill=tk.X)
                self.partial_widgets.append(widget)
            self.partial_stage_widgets={}
            for i in (1,2):
                row=ttk.Frame(box);row.pack(fill=tk.X,pady=5)
                group=[]
                for key,label,default in ((f"trigger{i}_r",f"Etapa {i} · gatilho R",str(.5*i)),(f"quantity{i}","Quantidade na unidade acima","50"),(f"base{i}","Base da fração","Inicial" if i==1 else "Saldo")):
                    field=ttk.Frame(row);ttk.Label(field,text=label).pack(anchor="w")
                    var=tk.StringVar(value=default);self.partial_fields[key]=var
                    widget=ttk.Combobox(field,textvariable=var,values=("Inicial","Saldo"),state="readonly",width=16) if key.startswith("base") else ttk.Entry(field,textvariable=var,width=16)
                    widget.pack(fill=tk.X);group.append(field);self.partial_widgets.append(widget)
                    self.partial_stage_widgets.setdefault(i,[]).append(widget)
                    var.trace_add("write",lambda *_:self.mark_dirty())
                self.flow(row,group)
            self.partial_check=ttk.Checkbutton(box,text="Solicitar break-even somente após redução confirmada",variable=self.partial_be);self.partial_check.pack(anchor="w")
            self.partial_widgets.append(self.partial_check)
            self.partial_preview=tk.StringVar(value="Parciais desabilitadas.")
            ttk.Label(box,textvariable=self.partial_preview,wraplength=850).pack(fill=tk.X,pady=5)
            for var in (self.partial_mode,self.partial_count,self.partial_be):var.trace_add("write",lambda *_:self.mark_dirty())
        if self.temporal_enabled:
            from mt5_extracao.strategies.temporal import TemporalPolicy
            box=ttk.LabelFrame(body,text="Saídas temporais · SL físico permanece obrigatório",padding=12);box.pack(fill=tk.X,padx=12,pady=4)
            self.time_on=tk.BooleanVar(value=False);self.time_fields={};self.time_widgets=[]
            self.time_toggle=ttk.Checkbutton(box,text="Ativar prazos desde o primeiro preenchimento",variable=self.time_on);self.time_toggle.pack(anchor="w")
            ttk.Label(box,text="Segundos de relógio, incluindo gaps; zero desabilita cada prazo. Parciais não reiniciam a idade. PnL observado inclui custos já lançados nos fills; custo da saída será conferido no preenchimento.",wraplength=850).pack(fill=tk.X,pady=4)
            form=ttk.Frame(box);form.pack(fill=tk.X)
            labels={"max_seconds":"Limite absoluto (s)","progress_seconds":"Prazo do lucro mínimo (s)","minimum_profit":"Lucro mínimo (moeda do Tester)","gain_seconds":"Prazo em ganho (s)","loss_seconds":"Prazo em perda (s)","tie_seconds":"Prazo em empate (s)","flat_band":"Faixa de empate ± (moeda)","money_digits":"Casas da moeda no Tester"}
            for index,(key,value) in enumerate(TemporalPolicy().to_dict().items()):
                row,col=divmod(index,3);ttk.Label(form,text=labels[key]).grid(row=row*2,column=col,sticky="w",padx=(0,8))
                var=tk.StringVar(value=str(value));self.time_fields[key]=var
                widget=ttk.Entry(form,textvariable=var,width=22);widget.grid(row=row*2+1,column=col,sticky="ew",padx=(0,8),pady=(2,6));self.time_widgets.append(widget)
                var.trace_add("write",lambda *_:self.mark_dirty())
            for col in range(3):form.columnconfigure(col,weight=1)
            if self.er_enabled:
                self.er_on=tk.BooleanVar(value=False);self.er_fields={};self.er_widgets=[]
                self.er_toggle=ttk.Checkbutton(box,text="ER(20) de barras fechadas após idade mínima · empate mantém",variable=self.er_on);self.er_toggle.pack(anchor="w",pady=(6,0))
                er_form=ttk.Frame(box);er_form.pack(fill=tk.X)
                for key,label in (("er_min_seconds","Idade mínima desde o fill (s)"),("maximum_er","ER estritamente abaixo de (0..1)")):
                    field=ttk.Frame(er_form);field.pack(side=tk.LEFT,fill=tk.X,expand=True,padx=(0,8))
                    ttk.Label(field,text=label).pack(anchor="w");var=tk.StringVar(value="0");self.er_fields[key]=var
                    entry=ttk.Entry(field,textvariable=var);entry.pack(fill=tk.X);self.er_widgets.append(entry)
                    var.trace_add("write",lambda *_:self.mark_dirty())
                self.er_on.trace_add("write",lambda *_:self.mark_dirty())
            if self.donchian_enabled:
                self.dc_on=tk.BooleanVar(value=False);self.dc_seconds=tk.StringVar(value="30")
                self.dc_toggle=ttk.Checkbutton(box,text="Donchian anterior(20): close rompe, retorna e fica sem renovação",variable=self.dc_on);self.dc_toggle.pack(anchor="w",pady=(6,0))
                ttk.Label(box,text="Segundos desde o último close favorável fora do canal; empate não renova").pack(anchor="w")
                self.dc_entry=ttk.Entry(box,textvariable=self.dc_seconds);self.dc_entry.pack(fill=tk.X)
                self.dc_on.trace_add("write",lambda *_:self.mark_dirty());self.dc_seconds.trace_add("write",lambda *_:self.mark_dirty())

            if self.atr_enabled:
                self.atr_on=tk.BooleanVar(value=False);self.atr_fields={};self.atr_widgets=[]
                self.atr_toggle=ttk.Checkbutton(box,text="ATR(14) fechado: horizonte adaptativo com mínimo e teto absoluto",variable=self.atr_on);self.atr_toggle.pack(anchor="w",pady=(6,0))
                ttk.Label(box,text="Prazo base × ATR da entrada / ATR conhecido; conta desde o primeiro fill. Base congelada; sem ATR válido, SL e teto continuam.",wraplength=850).pack(fill=tk.X)
                for key,label,value in (("atr_base_seconds","Prazo base ATR (s)",300),("atr_min_seconds","Mínimo ATR (s)",60)):
                    ttk.Label(box,text=label).pack(anchor="w");var=tk.StringVar(value=str(value));self.atr_fields[key]=var
                    entry=ttk.Entry(box,textvariable=var);entry.pack(fill=tk.X);self.atr_widgets.append(entry)
                    var.trace_add("write",lambda *_:self.mark_dirty())
                self.atr_on.trace_add("write",lambda *_:self.mark_dirty())
            self.time_on.trace_add("write",lambda *_:self.mark_dirty())
        if self.session_available:
            self.build_session_fields(body)
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
        self.sizing=self.tree("Lotes e risco · eventos nativos",("origem do sinal","modo","solicitado","plano","preenchido","risco até SL","orçamento","margem requerida","margem livre","mín/passo/máx nativos")) if self.lots_enabled else None
        self.partials=self.tree("Parciais · confirmação e saldo",("origem","instante","etapa","estado","antes","solicitado","reduzido","saldo","SL antes","SL depois","BE confirmado","resultado da redução")) if self.partials_enabled else None
        self.deals=self.tree("Preenchimentos e custos",("instante servidor","posição","entrada/saída","lotes","preço","resultado bruto","comissão","swap","taxa")) if self.partials_enabled else None
        self.temporal=self.tree("Tempo · decisão e confirmação",("origem","instante","motivo","estado","idade (s)","PnL observado","antes","reduzido","saldo","resultado da saída","ER(20)","ER observado em")) if self.temporal_enabled else None
        self.channel=self.tree("Donchian · contexto conhecido",("instante conhecido","barra de origem","limite superior anterior","limite inferior anterior","close","renovou","rompimento observado","relógio desde")) if self.donchian_enabled else None
        self.atr_context=self.tree("ATR · horizonte conhecido",("instante conhecido","semente de origem","barra fechada","ATR base congelado","ATR atual","prazo (s)")) if self.atr_enabled else None
        self.session_events=self.tree("Sessão · entradas e cota",("origem","instante servidor","evento","dia de sessão","ciclos preenchidos","entrada permitida","motivo")) if self.session_available else None
        self.cooldown_events=self.tree("Espera · fechamento confirmado",("origem","instante conhecido","fechamento total","libera em","espera (s)","entrada permitida","motivo")) if self.cooldown_available else None
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
                if isinstance(child,ttk.Label) and int(child.cget("wraplength") or 0)>0:child.configure(wraplength=max(120,event.width-48))
                walk(child)
        walk(self.body)
    def build_session_fields(self,body):
        box=ttk.LabelFrame(body,text="Entradas · horários, dias e ciclos por sessão",padding=12);box.pack(fill=tk.X,padx=12,pady=4)
        self.session_on=tk.BooleanVar(value=False);self.session_fields={};self.session_widgets=[]
        self.session_toggle=ttk.Checkbutton(box,text="Ativar filtro de entradas e cota por sessão",variable=self.session_on);self.session_toggle.pack(anchor="w")
        ttk.Label(box,text="Horário do servidor da corretora, sem conversão UTC ou horário de verão. Dias e cota usam a sessão iniciada na âncora. Este filtro não encerra posições nem suspende SL, parciais ou saídas.",wraplength=850).pack(fill=tk.X,pady=4)
        form=ttk.Frame(box);form.pack(fill=tk.X)
        fields=(("start_minute","Permitir a partir de (HH:MM)","00:00"),("end_minute","Permitir até, exclusivo (HH:MM)","00:00"),
                ("exclude_start_minute","Excluir a partir de (HH:MM)","00:00"),("exclude_end_minute","Excluir até, exclusivo (HH:MM)","00:00"),
                ("anchor_minute","Início do dia de sessão (HH:MM)","00:00"),("max_cycles","Cota de ciclos preenchidos (0 sem cota)","0"))
        for index,(key,label,value) in enumerate(fields):
            row,col=divmod(index,2);ttk.Label(form,text=label).grid(row=row*2,column=col,sticky="w",padx=(0,8))
            var=tk.StringVar(value=value);self.session_fields[key]=var
            widget=ttk.Entry(form,textvariable=var,width=24);widget.grid(row=row*2+1,column=col,sticky="ew",padx=(0,8),pady=(2,6));self.session_widgets.append(widget)
            var.trace_add("write",lambda *_:self.mark_dirty())
        for col in range(2):form.columnconfigure(col,weight=1)
        days=ttk.Frame(box);days.pack(fill=tk.X);self.session_days=[]
        for label in ("Seg","Ter","Qua","Qui","Sex","Sáb","Dom"):
            var=tk.BooleanVar(value=True);self.session_days.append(var)
            widget=ttk.Checkbutton(days,text=label,variable=var);widget.pack(side=tk.LEFT);self.session_widgets.append(widget)
            var.trace_add("write",lambda *_:self.mark_dirty())
        ttk.Label(box,text="Janela com início=fim permite24h; exclusão com início=fim fica desativada. Janelas noturnas são aceitas. Uma posição conta no primeiro fill; redução e ordem sem preenchimento não criam outro ciclo.",wraplength=850).pack(fill=tk.X,pady=4)
        if self.cooldown_available:
            self.cooldown_seconds=tk.StringVar(value="0")
            ttk.Label(box,text="Espera após fechamento total confirmado (segundos; 0 desativa)").pack(anchor="w",pady=(8,0))
            self.cooldown_entry=ttk.Entry(box,textvariable=self.cooldown_seconds,width=24);self.cooldown_entry.pack(anchor="w")
            ttk.Label(box,text="Libera em fechamento conhecido + espera, inclusive. Parcial não inicia a espera. Histórico/identidade ausentes impedem só novas entradas; SL e gestão continuam ativos. S09 somente Tester.",wraplength=850).pack(fill=tk.X,pady=4)
            self.cooldown_seconds.trace_add("write",lambda *_:self.mark_dirty())
        self.session_on.trace_add("write",lambda *_:self.mark_dirty())

    def session_selection(self):
        extras=[]
        if self.cooldown_available:
            from mt5_extracao.strategies.cooldown import CooldownPolicy
            seconds=int(self.cooldown_seconds.get())
            if seconds!=0:extras=[{"mode":"cooldown_v1","params":{"seconds":CooldownPolicy(seconds).seconds}}]
        if not self.session_on.get():return extras
        from mt5_extracao.strategies.session_entry import SessionEntryPolicy
        params={"weekdays":sum((1<<i) for i,var in enumerate(self.session_days) if var.get())}
        for key,var in self.session_fields.items():
            value=var.get().strip()
            if key=="max_cycles":params[key]=int(value);continue
            if len(value)!=5 or value[2]!=":" or not value[:2].isdigit() or not value[3:].isdigit():
                raise ValueError("Horários de entrada/sessão exigem HH:MM entre00:00 e23:59.")
            hour,minute=map(int,value.split(":"))
            if not (0<=hour<24 and 0<=minute<60):raise ValueError("Horário inválido; use00:00 a23:59.")
            params[key]=hour*60+minute
        return [{"mode":"session_v1","params":SessionEntryPolicy(**params).to_dict()}]+extras

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
        editable=is_manual and not busy and self.source_compatible
        for entry in self.entries:entry.configure(state="normal" if editable else "disabled")
        if self.lots_enabled:
            self.lot_combo.configure(state="readonly" if editable else "disabled")
            self.fixed_entry.configure(state="normal" if editable and self.lot_mode.get()=="Volume fixo" else "disabled")
        if self.partials_enabled:
            for widget in self.partial_widgets:
                if isinstance(widget,ttk.Combobox):widget.configure(state="readonly" if editable and not self.pullback else "disabled")
                else:widget.state(["!disabled" if editable and not self.pullback else "disabled"])
            if self.partial_mode.get()=="Desabilitada":
                for widget in self.partial_widgets[1:]:widget.state(["disabled"])
            elif self.partial_count.get()=="1":
                for widget in self.partial_stage_widgets[2]:widget.state(["disabled"])
        if self.temporal_enabled:
            self.time_toggle.state(["!disabled" if editable else "disabled"])
            for widget in self.time_widgets:widget.configure(state="normal" if editable and self.time_on.get() else "disabled")
            if self.er_enabled:
                self.er_toggle.state(["!disabled" if editable and self.time_on.get() else "disabled"])
                for widget in self.er_widgets:widget.configure(state="normal" if editable and self.time_on.get() and self.er_on.get() else "disabled")
            if self.donchian_enabled:
                self.dc_toggle.state(["!disabled" if editable and self.time_on.get() else "disabled"])
                self.dc_entry.configure(state="normal" if editable and self.time_on.get() and self.dc_on.get() else "disabled")
            if self.atr_enabled:
                self.atr_toggle.state(["!disabled" if editable and self.time_on.get() else "disabled"])
                for widget in self.atr_widgets:widget.configure(state="normal" if editable and self.time_on.get() and self.atr_on.get() else "disabled")
        if self.session_available:
            self.session_toggle.state(["!disabled" if editable else "disabled"])
            for widget in self.session_widgets:widget.state(["!disabled" if editable and self.session_on.get() else "disabled"])
        if self.cooldown_available:self.cooldown_entry.state(["!disabled" if editable else "disabled"])
        for entry in self.context_entries:entry.configure(state="disabled" if busy or not self.source_compatible else "normal")
        self.save.state(["!disabled" if is_manual and self.dirty and not busy else "disabled"])
        self.rename.state(["!disabled" if is_manual and not self.dirty and not busy else "disabled"])
        if not self.source_compatible:
            for button in (self.clone,self.export,self.save):button.state(["disabled"])
        disabled=busy or not loaded or self.dirty or not self.source_compatible or self.preview_error or bool(getattr(self.ui.app,"offline",False))
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
        connector=getattr(self.ui.app,"mt5_connector",None)
        info=connector.get_symbol_info(self.symbol) if self.lots_enabled and connector is not None else None
        return items,selected,info
    def reload(self,preferred=None):
        self.work(lambda:self._load(preferred),self.loaded)
    def loaded(self,data):
        items,selected,*extra=data
        self.symbol_info=extra[0] if extra else None
        self.options={}
        for item in items:
            label=("Default · Padrão do código" if item.identity.category=="Default" else "Manual · "+item.identity.label)+f" · v{item.identity.revision}"
            if label in self.options:label+=" · fonte anterior"
            self.options[label]=item
        self.combo.configure(values=list(self.options));self.set_current(selected)
    def set_current(self,item):
        self.current=item;self.setting=True
        source=item.manifest["native"]["robot"]
        self.source_compatible=source["version"]==self.robot.version and source["fingerprint"]==(native_adapter_profile(self.robot))[0]
        for key,_ in self.strategy_fields:self.fields[key].set(str(item.composition.to_dict()["strategy"]["params"][key]))
        if self.lots_enabled:
            sizing=item.composition.to_dict()["sizing"]
            self.lot_mode.set("Volume fixo" if sizing["mode"]=="fixed_volume" else "Por risco")
            self.fixed_volume.set(str(sizing["params"].get("volume",.01)))
            self.risk_pct.set(str(sizing["params"]["risk_fraction"]*100))
        if self.stoch:self.risk_pct.set(str(item.composition.to_dict()["sizing"]["params"]["risk_fraction"]*100))
        if self.partials_enabled:
            plan=item.composition.to_dict()["partial_exit_plan"];items=stages(plan)
            self.partial_mode.set("Desabilitada" if not items else "Lotes" if items[0]["mode"]==1 else "Fração (%)")
            self.partial_count.set(str(len(items) or 1));self.partial_be.set(plan["params"].get("breakeven_after_confirmation",False))
            for i in (1,2):
                stage=items[i-1] if i<=len(items) else dict(trigger_r=.5*i,quantity=.5,mode=0,base=0 if i==1 else 1)
                self.partial_fields[f"trigger{i}_r"].set(str(stage["trigger_r"]))
                self.partial_fields[f"quantity{i}"].set(str(stage["quantity"]*(100 if stage["mode"]==0 else 1)))
                self.partial_fields[f"base{i}"].set("Inicial" if stage["base"]==0 else "Saldo")
        if self.temporal_enabled:
            from mt5_extracao.strategies.temporal import TemporalPolicy
            policy=item.composition.to_dict()["conditional_policy"];active=policy["mode"] in ("temporal_v1","temporal_er_v1","temporal_donchian_v1","temporal_atr_v1")
            self.time_on.set(active);values=policy["params"] if active else TemporalPolicy().to_dict()
            for key,var in self.time_fields.items():var.set(str(values[key]))
            if self.er_enabled:
                self.er_on.set(policy["mode"]=="temporal_er_v1" or (policy["mode"] in ("temporal_donchian_v1","temporal_atr_v1") and values.get("er_min_seconds",0)>0))
                for key,var in self.er_fields.items():var.set(str(values.get(key,0)))
            if self.donchian_enabled:
                self.dc_on.set(policy["mode"] in ("temporal_donchian_v1","temporal_atr_v1") and values.get("channel_idle_seconds",0)>0);self.dc_seconds.set(str(values.get("channel_idle_seconds",30)))
        if self.atr_enabled:
            self.atr_on.set(policy["mode"]=="temporal_atr_v1")
            for key,var in self.atr_fields.items():var.set(str(values.get(key,300 if key=="atr_base_seconds" else 60)))
        if self.session_available:
            from mt5_extracao.strategies.session_entry import SessionEntryPolicy,from_filters
            selection=from_filters(item.composition.to_dict()["filters"])
            self.session_on.set(selection is not None);params=(selection or SessionEntryPolicy()).to_dict()
            for key,var in self.session_fields.items():
                value=params[key];var.set(str(value) if key=="max_cycles" else f"{value//60:02d}:{value%60:02d}")
            for i,var in enumerate(self.session_days):var.set(bool(params["weekdays"] & (1<<i)))
        if self.cooldown_available:
            from mt5_extracao.strategies.cooldown import from_filters as cooldown_from_filters
            cooldown=cooldown_from_filters(item.composition.to_dict()["filters"])
            self.cooldown_seconds.set(str(cooldown.seconds if cooldown else 0))
        self.selected.set(next(label for label,value in self.options.items() if value.identity==item.identity))
        self.setting=False;self.dirty=False
        label="Default preservado · use Salvar como Manual para editar." if item.identity.category=="Default" else "Rascunho compilado · aguarda conferência nativa deste contexto."
        self.status.set(label)
        if not self.source_compatible:
            self.status.set("Fonte anterior preservada: parâmetros não serão migrados. Selecione o novo Default para criar outro Manual; consulta histórica continua disponível.")
        self.protection.configure(text="Risco por saldo: até 0,5% · uma posição · SL obrigatório · parcial/BE/trailing indisponíveis. Proteções não são editadas aqui.")
        self.details.configure(text=(f"Nome: {item.identity.label}\nRevisão: {item.identity.revision}\n"
                                    f"Arquivo: {item.name.filename}\nConfiguração: {item.composition.config_hash}\n"
                                    f"Origem Default: {item.identity.default_origin or item.identity.id}"))
        if self.partials_enabled:self.protection.configure(text="Risco até 0,5% · uma posição · SL obrigatório · parcial só após redução/histórico confirmados; trailing indisponível."+(" Pullback: metade em 1R; BE somente após confirmação." if self.pullback else ""))
        if self.stoch:self.protection.configure(text="Uma posição · SL obrigatório na EMA com offset · Move Guard só aperta em barra fechada. Alvo opcional desde o fechamento do sinal; 0 desliga. Parciais/BE indisponíveis. Tester DEMO; aceite operacional pendente.")
        self.clear_result();self.refresh_preview();self.update_buttons()
    def choose(self):
        if self.pending:return
        item=self.options[self.selected.get()]
        self.work(lambda:(store.select_preset(self.db,self.robot.key,self.symbol,self.timeframe,item.identity.id,item.identity.revision),item)[1],self.set_current)
    def mark_dirty(self):
        if self.setting or self.current is None:return
        self.dirty=True;self.status.set("Alterações não salvas · salve uma revisão antes de executar.");self.refresh_preview();self.update_buttons();self.clear_result()
    def context_changed(self):
        if self.setting:return
        self.clear_result();self.refresh_preview();self.update_buttons()
    def composition_from_fields(self):
        data=self.current.composition.to_dict()
        for key,_ in self.strategy_fields:
            raw=self.fields[key].get().strip()
            if self.stoch:raw=raw.replace(",",".")
            data["strategy"]["params"][key]=(float if key in ("adx_min","rsi_pullback","target_r","upper_level","lower_level") else int)(raw)
        if self.lots_enabled:
            params={"risk_fraction":float(self.risk_pct.get().replace(",","."))/100,"compound":True}
            mode="fixed_volume" if self.lot_mode.get()=="Volume fixo" else "risk_fraction"
            if mode=="fixed_volume":params["volume"]=float(self.fixed_volume.get().replace(",","."))
            data["sizing"]={"mode":mode,"params":params}
        if self.stoch:data["sizing"]={"mode":"risk_fraction","params":{"risk_fraction":float(self.risk_pct.get().replace(",","."))/100,"compound":True}}
        if self.partials_enabled and not self.pullback:
            mode=self.partial_mode.get()
            if mode=="Desabilitada":data["partial_exit_plan"]={"mode":"strategy_default","params":{}}
            else:
                params={"count":int(self.partial_count.get()),"breakeven_after_confirmation":self.partial_be.get()}
                for i in (1,2):
                    params[f"trigger{i}_r"]=float(self.partial_fields[f"trigger{i}_r"].get().replace(",","."))
                    params[f"quantity{i}"]=float(self.partial_fields[f"quantity{i}"].get().replace(",","."))/(100 if mode=="Fração (%)" else 1)
                    params[f"base{i}"]=0 if self.partial_fields[f"base{i}"].get()=="Inicial" else 1
                data["partial_exit_plan"]={"mode":"staged_fraction" if mode=="Fração (%)" else "staged_lots","params":params}
        if self.temporal_enabled:
            params={key:(float if key in ("minimum_profit","flat_band") else int)(var.get().replace(",",".")) for key,var in self.time_fields.items()} if self.time_on.get() else {}
            mode="temporal_v1" if self.time_on.get() else "disabled"
            if self.er_enabled and self.time_on.get() and self.er_on.get():
                params.update({key:(int if key=="er_min_seconds" else float)(var.get().replace(",",".")) for key,var in self.er_fields.items()})
                mode="temporal_er_v1"
            if self.donchian_enabled and self.time_on.get() and self.dc_on.get():
                params.update(er_min_seconds=params.get("er_min_seconds",0),maximum_er=params.get("maximum_er",0),channel_idle_seconds=int(self.dc_seconds.get()))
                mode="temporal_donchian_v1"
            if self.atr_enabled and self.time_on.get() and self.atr_on.get():
                params.update(er_min_seconds=params.get("er_min_seconds",0),maximum_er=params.get("maximum_er",0),channel_idle_seconds=params.get("channel_idle_seconds",0))
                params.update({key:int(var.get()) for key,var in self.atr_fields.items()});mode="temporal_atr_v1"
            data["conditional_policy"]={"mode":mode,"params":params}
        if self.session_available:data["filters"]=self.session_selection()
        return Composition(data)
    def refresh_preview(self):
        if not self.lots_enabled or self.current is None:return
        if not self.source_compatible:
            self.preview_error=True;self.lot_preview.set("Revisão de outra fonte: somente consulta; nenhuma conversão automática de lotes.");return
        try:
            data=self.composition_from_fields().to_dict()
            native.native_adapter(Composition(data),self.robot)
            context=self.context()
            if self.pullback:
                if data["sizing"]["mode"]=="fixed_volume":
                    volume=preview_fixed_volume(data["sizing"]["params"]["volume"],info=self.symbol_info)
                    preview_plan(data["partial_exit_plan"],volume=volume["effective"],minimum=volume["minimum"],maximum=volume["maximum"],step=volume["step"])
                self.partial_preview.set("Métade do volume inicial em 1R; saldo protegido; BE somente após confirmação. Agenda fixa neste perfil.")
                raise SizingPreviewUnavailable("SL depende do fundo/topo confirmado e da cotação de entrada; lote, parcial e risco são validados no Tester.")
            stop=data["protection"]["params"]["stop_points"] if data["protection"]["mode"]=="fixed_points" else data["strategy"]["params"]["stop_points"]
            value=preview_sizing(data["sizing"],info=self.symbol_info,stop_points=stop,deposit=context.deposit)
            requested="calculado por risco" if value["requested"] is None else str(value["requested"])
            self.lot_preview.set(f"Prévia no capital inicial: solicitado {requested} → {value['effective']} lotes. "
                f"Mín {value['minimum']} · passo {value['step']} · máx {value['maximum']}. Risco estimado {fmt_num(value['risk_money'])} / orçamento {fmt_num(value['risk_budget'])}. "
                "Especificação atual; margem e preenchimento serão conferidos no Tester. Não autoriza execução ao vivo.")
            if self.partials_enabled:
                schedule=preview_plan(data["partial_exit_plan"],volume=value['effective'],minimum=value['minimum'],maximum=value['maximum'],step=value['step'])
                self.partial_preview.set("Prévia: "+" → ".join(map(str,[value['effective']]+[r['remaining'] for r in schedule]))+" lotes. Gatilhos em R do SL inicial; confirmação e saldo serão conferidos no Tester." if schedule else "Parciais desabilitadas.")
            self.preview_error=False
        except SizingPreviewUnavailable as error:
            self.preview_error=False
            if self.partials_enabled:self.partial_preview.set("Agenda requer confirmação nativa de volume, histórico e proteção; prévia atual indisponível.")
            self.lot_preview.set("Prévia indisponível: "+str(error)+" O Tester deve confirmar especificação, risco e margem antes de qualquer entrada; não há aprovação antecipada.")
        except (ValueError,TypeError,OverflowError) as error:
            self.preview_error=True;self.lot_preview.set("Prévia indisponível/recusada: "+str(error))
            if self.partials_enabled:self.partial_preview.set(str(error))
    def clear_result(self):
        self.last_evidence=None;self.artifacts={};self.result.set("Sem conferência nativa neste contexto.")
        for tree in (self.trades,self.inputs,self.signals):tree.delete(*tree.get_children())
        if self.sizing is not None:self.sizing.delete(*self.sizing.get_children())
        if self.partials is not None:self.partials.delete(*self.partials.get_children())
        if self.deals is not None:self.deals.delete(*self.deals.get_children())
        if self.temporal is not None:self.temporal.delete(*self.temporal.get_children())
        if self.channel is not None:self.channel.delete(*self.channel.get_children())
        if self.atr_context is not None:self.atr_context.delete(*self.atr_context.get_children())
        if self.session_events is not None:self.session_events.delete(*self.session_events.get_children())
        if self.cooldown_events is not None:self.cooldown_events.delete(*self.cooldown_events.get_children())
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
            comp=self.composition_from_fields();current=self.current
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
        if not self.source_compatible or self.preview_error:
            self.status.set("Confira a fonte e a prévia de lotes antes de iniciar o Tester.");return
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
        if not self.source_compatible:
            current=self.current
            def previous_source():
                rows=[row for row in execution.load_evidence(self.db)
                      if row["preset"]["id"]==current.identity.id and row["preset"]["revision"]==current.identity.revision
                      and row["source"]==current.manifest["native"]["robot"]
                      and row["context"]==current.composition.to_dict()["context"]]
                if not rows:return None
                runs=robot_store.load_runs(self.db,run_id=rows[0]["run_id"])
                return rows[0],runs[0]["metrics"] if runs else {}
            def previous_loaded(value):
                if value:
                    self.show_evidence(*value)
                    self.status.set("Consulta da revisão/fonte anterior preservada; os campos de contexto da tela não filtram esta consulta. Confira o período original nos arquivos do resultado.")
                else:self.result.set("Sem resultado salvo para esta revisão/fonte anterior.")
            self.work(previous_source,previous_loaded);return
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
        if "partials" in proof:
            confirmed=sum(r["state"]=="confirmed" for r in proof["partials"])
            self.result.set(self.result.get()+f"\nParciais confirmadas: {confirmed}; saldo e custos por fill nas abas próprias." if confirmed else self.result.get()+"\nNenhuma parcial confirmada neste ensaio; sem aceite dessa capacidade.")
        if "temporal_events" in proof:
            count=sum(r["state"]=="confirmed" for r in proof["temporal_events"])
            self.result.set(self.result.get()+f"\nSaídas temporais confirmadas: {count}."+("" if count else " Sem aceite dessa capacidade neste ensaio."))
        if "channel_enabled" in proof:
            count=sum(r["state"]=="confirmed" and r["reason"]=="time_donchian" for r in proof.get("temporal_events",[]))
            self.result.set(self.result.get()+f"\nSaídas Donchian confirmadas: {count}."+(" Replay conciliado neste cenário." if proof.get("channel_acceptance") else " Sem aceite Donchian neste resultado."))
        if "atr_enabled" in proof:
            count=sum(r["state"]=="confirmed" and r["reason"]=="time_atr" for r in proof.get("temporal_events",[]))
            self.result.set(self.result.get()+f"\nSaídas ATR confirmadas: {count}."+(" Replay ATR conciliado neste cenário." if proof.get("atr_acceptance") else " Sem aceite ATR neste resultado."))
        if "efficiency_enabled" in proof:
            count=sum(r["state"]=="confirmed" and r["reason"]=="time_er" for r in proof.get("temporal_events",[]))
            self.result.set(self.result.get()+f"\nSaídas por ER confirmadas: {count}."+(" Replay de ER reconciliado neste cenário." if proof.get("efficiency_acceptance") else " Sem aceite de ER neste resultado."))
        if proof.get("session_enabled"):
            count=sum(not row["allowed"] for row in proof.get("session_events",[]) if row["event"]=="check")
            self.result.set(self.result.get()+f"\nRecusas pelo filtro de sessão: {count}."+(" Entradas/cota e replay conciliados neste cenário." if proof.get("session_acceptance") else " Sem aceite deste filtro neste resultado."))
        if proof.get("cooldown_enabled"):
            self.result.set(self.result.get()+"\nEspera após fechamento total: "+("recusa, nova entrada e replay conciliados neste cenário." if proof.get("cooldown_acceptance") else "sem aceite funcional neste resultado."))
        self.status.set("Inputs realmente carregados e modo nativo conferidos. Histórico/revisão da corretora sem certificação; cobertura de ticks reais depende do relatório.")
        for row in proof["ledger"]:self.trades.insert("","end",values=(row["entry_time"],row["exit_time"],"Compra" if row["direction"]>0 else "Venda",row["volume"],row["pnl"],row["stop_price"],row["target_price"]))
        expected=self.current.manifest["native"]["expected_native_inputs"]
        for key,value in proof["inputs"]["values"].items():self.inputs.insert("","end",values=(INPUT_LABELS.get(key,key),expected.get(key,"—"),value))
        def clock(value):return datetime.fromtimestamp(value,timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for row in proof["signals"]:self.signals.insert("","end",values=(clock(row["event_at"]),clock(row["confirmed_at"]),clock(row["available_at"]),"Compra" if row["direction"]>0 else "Venda","Sim" if row["opened"] else "Recusada",row["reason"]))
        if self.sizing is not None:
            for row in proof.get("sizing",[]):
                self.sizing.insert("","end",values=(clock(row["event_at"]),"Fixo" if row["mode"]=="fixed_volume" else "Por risco",
                    row["requested_volume"] if row["requested_volume"] is not None else "Calculado",row["planned_volume"],
                    row["filled_volume"] if row["filled_volume"] is not None else "Sem fill confirmado",row["risk_money"],row["risk_budget"],
                    row["margin_required"] if row["margin_known"] else "Indisponível",row["margin_free"] if row["margin_known"] else "Indisponível",
                    f"{row['volume_min']} / {row['volume_step']} / {row['volume_max']}" if row["spec_known"] else "Indisponível"))
        if self.partials is not None:
            for row in proof.get("partials",[]):
                self.partials.insert("","end",values=(clock(row['event_at']),clock(row['at']),row['stage'],row['state'],row['before'],row['requested'],row['filled'],row['remaining'],row['stop_before'],row['stop_after'],"Sim" if row['be_confirmed'] else "Não",row['net']))
        if self.deals is not None:
            for row in proof.get("deals",[]):
                self.deals.insert("","end",values=(clock(row['at_msc']/1000),row['position'],"Entrada" if row['entry']==0 else "Saída",row['volume'],row['price'],row['profit'],row['commission'],row['swap'],row['fee']))
        if self.temporal is not None:
            for row in proof.get("temporal_events",[]):
                self.temporal.insert("","end",values=(clock(row["event_at"]),clock(row["at_msc"]/1000),row["reason"],row["state"],row["age_ms"]/1000,row["net_observed"] if row["net_observed"] is not None else "Indisponível",row["before"],row["filled"],row["remaining"],row["cash"],row.get("er_value","—"),clock(row["er_observed_msc"]/1000) if row.get("er_observed_msc") else "—"))
        if self.channel is not None:
            for row in proof.get("channel_observations",[]):
                self.channel.insert("","end",values=(clock(row["at_msc"]/1000),clock(row["bar"]),row["upper"],row["lower"],row["close"],"Sim" if row["renewed"] else "Não","Sim" if row["seen"] else "Não",clock(row["anchor"]/1000)))
        if self.atr_context is not None:
            for row in proof.get("atr_observations",[]):
                self.atr_context.insert("","end",values=(clock(row["at_msc"]/1000),clock(row["seed"]),clock(row["times"][-1]),row["baseline"],row["current"],row["horizon"]))
        if self.session_events is not None:
            labels={"allowed":"Permitida","disabled":"Filtro desativado","weekday":"Dia excluído","outside_window":"Fora da janela","excluded_window":"Intervalo excluído","quota":"Cota preenchida","history_unavailable":"Histórico indisponível","clock_reversal":"Relógio regrediu","unsupported_history":"Histórico sem suporte"}
            for row in proof.get("session_events",[]):
                self.session_events.insert("","end",values=(clock(row["event_at"]),clock(row["at"]),"Preenchimento" if row["event"]=="fill" else "Entrada",clock(row["session_start"]),row["cycles"] if row["cycles"]>=0 else "Indisponível","Sim" if row["allowed"] else "Não",labels.get(row["reason"],row["reason"])))
        if proof.get("first_difference"):self.status.set("Primeira divergência: "+str(proof["first_difference"])+". Confira os arquivos; tolerância não foi ampliada.")
        if self.cooldown_events is not None:
            def milliseconds(at):return datetime.fromtimestamp(at/1000,timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            for row in proof.get("cooldown_events",[]):
                self.cooldown_events.insert("","end",values=(clock(row["event_at"]),milliseconds(row["at_msc"]),milliseconds(row["last_close_msc"]) if row["last_close_msc"] else "Sem encerramento anterior",milliseconds(row["last_close_msc"]+row["seconds"]*1000) if row["last_close_msc"] else "Já permitida",row["seconds"],"Sim" if row["allowed"] else "Não",row["reason"]))
    def open_files(self):
        value=self.artifacts.get("artifact_directory")
        if value:self.ui._open_path(str(value))
    def open_csv(self):
        value=self.artifacts.get("trades_csv")
        if value:self.ui._open_path(str(value))
    def close(self):
        if self.pending or self.execution and self.execution.session.active:self.window.withdraw()
        else:self.window.destroy()
