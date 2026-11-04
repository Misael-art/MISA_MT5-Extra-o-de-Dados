"""Janelas do par selecionado; estatísticas e publicação pertencem ao SDK."""
from datetime import datetime, timedelta, timezone
import threading
import tkinter as tk
from tkinter import ttk

from mt5_extracao import sdk
from mt5_extracao.ui.charts import Chart, style_axis
from mt5_extracao.ui.components import ContentPanel, DetailsPanel, ScrollableContent, TablePanel
from mt5_extracao.ui.relation_creator import RelationCreator
from mt5_extracao.ui.visual_tokens import SPACING


class RelationWindows:
    def __init__(self, parent, theme, *, db, relation, run_bg, write_guard_factory=None, on_saved=None):
        self.db, self.run_bg, self.on_saved = db, run_bg, on_saved
        self.write_guard_factory = write_guard_factory
        self.recipe = dict(relation["scope"]["settings"])
        self.manifest_id = relation["provenance"]["snapshot_manifest_id"]
        self.pending = self.close_after = False
        self._cancel = threading.Event()
        self._request = 0
        self.report = self.selected = self.saved_receipt = None
        self._wrap = []
        self.window = tk.Toplevel(parent)
        self.window.title("Relação por janela")
        owner = parent.winfo_toplevel();self.window.transient(owner)
        width = min(920, max(620, owner.winfo_width()-32), owner.winfo_screenwidth()-40)
        height = min(820, max(520, owner.winfo_height()-48), owner.winfo_screenheight()-80)
        x = max(0,min(owner.winfo_screenwidth()-width,owner.winfo_rootx()+(owner.winfo_width()-width)//2))
        y = max(0,min(owner.winfo_screenheight()-height,owner.winfo_rooty()+(owner.winfo_height()-height)//2))
        self.window.geometry(f"{width}x{height}+{x}+{y}");self.window.minsize(620,480)
        self.window.protocol("WM_DELETE_WINDOW",self.close)
        self.window.bind("<Escape>",lambda _e:(self.close(),"break")[-1])
        self.window.bind("<Configure>",self._resize,add="+")
        self.window.bind("<Destroy>",self._destroyed,add="+")
        self.viewport = ScrollableContent(self.window,theme);self.viewport.pack(fill=tk.BOTH,expand=True)
        body = ttk.Frame(self.viewport.content,style="Surface.TFrame",padding=SPACING["sm"]);body.pack(fill=tk.X)
        ttk.Label(body,text="Como a relação varia ao longo do tempo",font=theme.font_section).pack(anchor="w")
        description = " × ".join(f"{relation[s]['definition'].get('name') or relation[s]['feature_id']} · "
            f"{relation[s]['algorithm_version']} [{relation[s]['unit'] or 'unidade não declarada'}]" for s in ("left","right"))
        self._label(body,f"{relation['scope']['symbol']} / {relation['scope']['timeframe']} · {description}\n"
            f"Corte UTC {relation['provenance']['as_of_utc']} · {relation['relation']['method'].title()} · defasagem {relation['scope']['lag_bars']} nominal.")
        if relation['scope'].get('decision_window'):
            inherited=relation['scope']['decision_window']
            self._label(body,f"Recorte selecionado: UTC {inherited['start_inclusive_utc']} → {inherited['end_inclusive_utc']}.")
        self._label(body,"Comparação retrospectiva com corte fixo. Ambos os fechamentos ficam na janela; "
            "a defasagem não comprime lacunas nem atravessa sessões. Sem evidência causal ou financeira.")
        controls = ttk.Frame(body,style="Surface.TFrame");controls.pack(fill=tk.X,pady=SPACING["xs"])
        self.options,self.entries,self.titles = {},[],{}
        for index,(key,title,default) in enumerate((("window_bars","Janela:",100),("stride_bars","Avanço:",20),("max_windows","Máx. janelas:",32))):
            ttk.Label(controls,text=title).grid(row=0,column=index*2,sticky="w",padx=(0,SPACING["xs"]))
            variable=tk.StringVar(value=str(default));entry=ttk.Entry(controls,textvariable=variable,width=7)
            entry.grid(row=0,column=index*2+1,sticky="ew",padx=(0,SPACING["sm"]));controls.columnconfigure(index*2+1,weight=1)
            self.options[key]=variable;self.entries.append(entry);self.titles[key]=title
        self._label(body,"Janela e avanço contam barras observadas. Mínimos de amostra não são reduzidos automaticamente.")
        advanced=DetailsPanel(body,"Amostra e orçamento",expanded=False,information=True,theme=theme,padding=SPACING["xs"])
        advanced.pack(fill=tk.X,pady=SPACING["xs"]);advanced.body.columnconfigure(1,weight=1)
        fields=(("minimum_pairs","Mínimo de pares:",30),("minimum_sessions","Mínimo de sessões:",10),
            ("bootstrap_replicates","Réplicas:",999),("seed","Semente:",7),
            ("block_length_sessions","Sessões por bloco:",None),("tests_counted","Testes na família:",None),
            ("timeout_seconds","Tempo máximo (s):",120),("max_memory_mb","Memória máxima (MiB):",2048))
        for row,(key,title,default) in enumerate(fields):
            value=self.recipe.get(key,default);variable=tk.StringVar(value="" if value is None else str(value))
            entry=RelationCreator._entry(advanced.body,row,title,variable)
            self.options[key]=variable;self.entries.append(entry);self.titles[key]=title
        self._label(advanced.body,"Família vazia conta todas as janelas solicitadas, inclusive insuficientes. "
            "Uma família maior pode ser declarada; tentativas externas não são registradas aqui.",pack=False).grid(row=len(fields),column=0,columnspan=2,sticky="ew")
        summary=ContentPanel(body,"Janela selecionada",padding=SPACING["xs"]);summary.pack(fill=tk.X,pady=SPACING["xs"])
        self.effect=ttk.Label(summary.body,text="Calcule as janelas",font=theme.font_value,style="Surface.TLabel");self.effect.pack(anchor="w")
        self.scope=self._label(summary.body,"");self.sample=self._label(summary.body,"")
        self.inference=self._label(summary.body,"")
        self.chart=Chart(body,theme,height=2.1);self.chart.widget.pack(fill=tk.X,pady=SPACING["xs"])
        self.chart.message("Calcule para comparar os coeficientes; ausências não viram zero.")
        self.chart.canvas.mpl_connect("button_press_event",self._plot_selected)
        self.table=TablePanel(body,"Janelas completas · selecione para conferir a amostra",
            columns=("number","start","end","effect","pairs","excluded","state"),
            widths=(55,150,150,100,65,75,140),anchors=("center","w","w","center","center","center","w"),
            height=4,xscroll=True,padding=SPACING["xs"])
        self.table.pack(fill=tk.BOTH,expand=True)
        for key,title in zip(self.table.tree['columns'],("Janela","Início UTC","Fim UTC","Coeficiente","Pares","Excluídos","Estado")):
            self.table.tree.heading(key,text=title)
        self.table.tree.bind("<<TreeviewSelect>>",self._selected,add="+")
        self.population=self._label(body,"")
        footer=ttk.Frame(self.window,style="Surface.TFrame",padding=SPACING["xs"]);footer.pack(fill=tk.X)
        self.status=self._label(footer,"Escolha o tamanho e o avanço. O cálculo não salva automaticamente as janelas.")
        buttons=ttk.Frame(footer,style="Surface.TFrame");buttons.pack(fill=tk.X)
        self.calculate_button=ttk.Button(buttons,text="Calcular janelas",command=self.calculate);self.calculate_button.pack(side=tk.LEFT)
        self.save_button=ttk.Button(buttons,text="Salvar selecionada",command=self.save);self.save_button.pack(side=tk.LEFT,padx=SPACING["xs"])
        self.cancel_button=ttk.Button(buttons,text="Cancelar",command=self.cancel);self.cancel_button.pack(side=tk.LEFT)
        self.close_button=ttk.Button(buttons,text="Fechar",command=self.close);self.close_button.pack(side=tk.RIGHT)
        for variable in self.options.values():variable.trace_add("write",self._changed)
        self._controls();self.window.grab_set();self.entries[0].focus_set()

    def _label(self,parent,text,pack=True):
        label=ttk.Label(parent,text=text,style="Muted.TLabel",justify=tk.LEFT,wraplength=820)
        if pack:label.pack(fill=tk.X,pady=SPACING["2xs"])
        self._wrap.append(label);return label

    def _resize(self,event):
        if event.widget is self.window:
            for label in self._wrap:label.configure(wraplength=max(300,event.width-48))

    def alive(self):
        try:return bool(self.window.winfo_exists())
        except tk.TclError:return False

    def _destroyed(self,event):
        if event.widget is self.window:self._cancel.set()

    def _controls(self):
        for entry in self.entries:entry.state(["disabled"] if self.pending else ["!disabled"])
        self.calculate_button.state(["disabled"] if self.pending else ["!disabled"])
        self.save_button.state(["disabled"] if self.pending or self.selected is None else ["!disabled"])
        self.cancel_button.state(["!disabled"] if self.pending and not self._cancel.is_set() else ["disabled"])
        self.table.tree.configure(selectmode="none" if self.pending else "browse")

    def _changed(self,*_args):
        self._request+=1
        if self.pending:self.cancel();return
        self._clear_report();self.status.configure(text="Parâmetros alterados; calcule novamente.")

    def _clear_report(self):
        had_report=self.report is not None
        self.report=self.selected=None
        self.table.tree.delete(*self.table.tree.get_children())
        self.effect.configure(text="Calcule as janelas")
        for label in (self.scope,self.sample,self.inference,self.population):label.configure(text="")
        if had_report:self.chart.message("Calcule novamente com os parâmetros atuais.")
        self._controls()

    def _arguments(self):
        options=dict(self.recipe)
        for key,variable in self.options.items():
            raw=variable.get().strip()
            if not raw and key not in ("tests_counted","block_length_sessions"):
                raise ValueError("Informe "+self.titles[key].lower())
            try:options[key]=None if not raw else float(raw) if key in ("timeout_seconds","max_memory_mb") else int(raw)
            except ValueError as exc:raise ValueError(self.titles[key]+" informe um número válido.") from exc
        return options

    def calculate(self):
        if self.pending:return
        try:options=self._arguments()
        except ValueError as exc:self.status.configure(text=str(exc));return
        self._clear_report();request=self._request
        def work():return sdk.query_feature_relation_windows(self.db,self.manifest_id,**options,cancel_check=self._cancel.is_set)
        def success(report):
            if request!=self._request:self.status.configure(text="Parâmetros mudaram; calcule novamente.");return
            self.report=report
            for item in report['windows']:
                row=item['result'];relation=row['relation'];key='window-'+str(item['index'])
                self.table.tree.insert('',tk.END,iid=key,values=(item['index']+1,item['decision_start_utc'],item['decision_end_utc'],
                    self._number(relation['effect']),relation['n_pairs'],sum(row['population']['exclusions'].values()),relation['state']))
            p=report['population']
            self.population.configure(text=f"{p['closed_decisions']} fechamentos · {p['windows']} janelas · "
                f"{p['pair_evaluations']} pares avaliados (com repetição entre janelas) · "
                f"{p['decisions_not_in_any_window']} fechamentos fora das janelas. "
                f"Bonferroni: {report['correction']['tests_counted']} testes; tentativas externas não contabilizadas.")
            self._draw()
            self.status.configure(text="Selecione uma janela para conferir ou salvar o resultado original." if report['windows'] else
                "O recorte não contém uma janela completa. Ajuste o tamanho ou amplie a captura.")
            if report['windows']:self.table.tree.selection_set('window-0');self._selected(None)
        self._execute(work,success,"Calculando janelas — aguarde…")

    @staticmethod
    def _number(value):return "Indisponível" if value is None else format(value,"+.4f").replace('.',',')

    def _draw(self):
        import matplotlib.dates as dates
        self.chart.clear();self._ax=self.chart.figure.add_subplot(111)
        style_axis(self._ax,self.chart.theme)
        self._dates=[datetime.fromisoformat(w['decision_end_utc'].replace('Z','+00:00')) for w in self.report['windows']]
        values=[w['result']['relation']['effect'] for w in self.report['windows']]
        if not any(v is not None for v in values):self.chart.message("Nenhuma janela tem coeficiente disponível; confira amostra e motivos.");return
        self._ax.plot(self._dates,values,color=self.chart.theme.colors['accent'],marker='o',linewidth=1.2)
        self._ax.axhline(0,color=self.chart.theme.colors['border'],linewidth=.8)
        self._ax.set_ylim(-1.08,1.08);self._ax.set_ylabel('Coeficiente',color=self.chart.theme.colors['muted'])
        self._ax.set_xlabel('Fechamento da janela (UTC)',color=self.chart.theme.colors['muted'])
        self._ax.xaxis.set_major_locator(dates.AutoDateLocator(minticks=3,maxticks=5))
        self._ax.xaxis.set_major_formatter(dates.DateFormatter('%d/%m\n%H:%M',tz=timezone.utc))
        if len(self._dates)==1:self._ax.set_xlim(self._dates[0]-timedelta(days=1),self._dates[0]+timedelta(days=1))
        self.chart.draw()

    def _plot_selected(self,event):
        if self.pending or self.report is None or event.inaxes is not getattr(self,'_ax',None) or event.xdata is None:return
        import matplotlib.dates as dates
        index=min(range(len(self._dates)),key=lambda i:abs(dates.date2num(self._dates[i])-event.xdata))
        self.table.tree.selection_set('window-'+str(index));self.table.tree.see('window-'+str(index));self._selected(None)

    def _selected(self,_event):
        if self.pending or self.report is None:return
        selection=self.table.tree.selection()
        if not selection:return
        index=int(selection[0].split('-')[-1]);item=self.report['windows'][index];self.selected=item
        dto=item['result'];row,p=dto['relation'],dto['population']
        self.effect.configure(text=self._number(row['effect'])+' · '+row['method'].title())
        self.scope.configure(text=f"UTC {item['decision_start_utc']} → {item['decision_end_utc']} · ambos os fechamentos dentro.")
        self.sample.configure(text=f"Pares: {row['n_pairs']} · fontes distintas: {p['n_distinct_source_pairs']} · "
            f"sessões: {row['n_sessions']} · excluídos: {sum(p['exclusions'].values())} de {p['n_total']}. "
            f"Motivo: {row['reason'] or 'amostra disponível'}")
        def probability(value):return "não estimado" if value is None else format(value,".6g").replace('.',',')
        self.inference.configure(text=("Referência sintética · " if dto['synthetic_fixture'] else '')+
            f"IC: {self._number(row['effect_ci_low'])} a {self._number(row['effect_ci_high'])}; "
            f"p bruto: {probability(row['p_raw'])}; "
            f"p ajustado: {probability(row['p_adjusted'])}. Exploratório; sem utilidade financeira comprovada.")
        self._controls()

    def save(self):
        if self.pending or self.selected is None:return
        options=dict(self.selected['result']['scope']['settings'])
        limits=self.selected['result']['provenance']['limits']
        options.update(timeout_seconds=limits['timeout_seconds'],max_memory_mb=limits['max_memory_mb'])
        try:guard=self.write_guard_factory() if self.write_guard_factory is not None else None
        except Exception as exc:self.status.configure(text="Não foi possível obter a política de escrita: "+str(exc));return
        def work():return sdk.persist_feature_relation(self.db,self.manifest_id,**options,
            write_guard=guard,cancel_check=self._cancel.is_set)
        def success(receipt):
            self.saved_receipt=receipt
            try:
                if self.on_saved is not None:self.on_saved(receipt)
            except Exception as exc:
                self.status.configure(text=f"Janela salva ({receipt['analysis_id']}), mas não exibida: {exc}");return
            self.window.destroy()
        self._execute(work,success,"Salvando a janela selecionada — aguarde…",saving=True)

    def _execute(self,work,success,message,saving=False):
        self.pending=True;self.close_after=False;self._cancel.clear();self._controls();self.status.configure(text=message)
        def done(result,error):
            if not self.alive():return
            self.pending=False;self._controls()
            if error:
                self.status.configure(text=str(error))
                if self.close_after:self.window.destroy()
                return
            if self.close_after and not saving:self.window.destroy();return
            success(result)
        self.run_bg(work,done)

    def cancel(self):
        if self.pending:
            self._cancel.set();self.cancel_button.state(['disabled'])
            self.status.configure(text="Cancelando — aguarde a confirmação.")

    def close(self):
        if self.pending:self.close_after=True;self.cancel()
        else:self.window.destroy()
