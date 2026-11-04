"""Seleção de vínculos e leitura dos cruzamentos; matemática somente no SDK."""
import json
import threading
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, ttk

import pandas as pd

from mt5_extracao import sdk, timeframes
from mt5_extracao.analytics import datasets
from mt5_extracao.laboratory import inventory
from .series_observations import value_text

BASIS = {"Recebimento no candle": "received", "Publicação retrospectiva": "publication"}


class LaboratoryLinks:
    def __init__(self, panel):
        self.panel, self.ui = panel, panel.ui
        self.db = self.ui.app.db_manager
        self.window = tk.Toplevel(panel.frame)
        self.window.title("Laboratório · fontes, cruzamentos e candle")
        self.window.geometry("1080x800");self.window.minsize(650,480)
        self.window.protocol("WM_DELETE_WINDOW", self.close)
        self.pending = False;self.cancel_event = threading.Event();self.result = None
        self.rows = [];self.page = 0
        footer = ttk.Frame(self.window, padding=8);footer.pack(side=tk.BOTTOM, fill=tk.X)
        self.status = ttk.Label(footer, text="Lendo o inventário armazenado — aguarde…", wraplength=960)
        self.status.pack(fill=tk.X)
        self.progress = ttk.Progressbar(footer, mode="indeterminate");self.progress.pack(fill=tk.X)
        self.cancel_button=ttk.Button(footer,text="Cancelar cálculo",command=self.cancel_event.set)
        self.cancel_button.pack(side=tk.LEFT)
        ttk.Button(footer,text="Fechar",command=self.close).pack(side=tk.RIGHT)
        self.tabs=ttk.Notebook(self.window);self.tabs.pack(fill=tk.BOTH,expand=True,padx=8,pady=8)
        self.source_tab=ttk.Frame(self.tabs,padding=8);self.matrix_tab=ttk.Frame(self.tabs,padding=8);self.candle_tab=ttk.Frame(self.tabs,padding=8)
        for frame,label in [(self.source_tab,"Vincular fontes"),(self.matrix_tab,"Todos os cruzamentos"),(self.candle_tab,"Candle e cronologia")]:
            self.tabs.add(frame,text=label)
        self._sources();self._matrix();self._candle();self.load_inventory()

    def alive(self):
        return bool(self.window.winfo_exists())

    def _table(self,parent,columns,height=6):
        box=ttk.Frame(parent);box.pack(fill=tk.BOTH,expand=True,pady=4)
        tree=ttk.Treeview(box,columns=[c[0] for c in columns],show="headings",height=height)
        for name,title,width in columns:
            tree.heading(name,text=title);tree.column(name,width=width,minwidth=75,anchor='e' if name in ('effect','n','count') else 'w')
        tree.grid(row=0,column=0,sticky="nsew");box.rowconfigure(0,weight=1);box.columnconfigure(0,weight=1)
        sx=ttk.Scrollbar(box,orient=tk.HORIZONTAL,command=tree.xview);sx.grid(row=1,column=0,sticky="ew")
        sy=ttk.Scrollbar(box,orient=tk.VERTICAL,command=tree.yview);sy.grid(row=0,column=1,sticky="ns")
        tree.configure(xscrollcommand=sx.set,yscrollcommand=sy.set)
        return tree

    def _sources(self):
        from .components import ScrollableContent
        viewport=ScrollableContent(self.source_tab,self.ui.theme);viewport.pack(fill=tk.BOTH,expand=True)
        frame=viewport.content
        ttk.Label(frame,text="Vínculo explícito ao ativo selecionado. Abrir não captura nem classifica; só Congelar e analisar cria os snapshots.",wraplength=620).pack(anchor="w")
        self.query=tk.StringVar();ttk.Entry(frame,textvariable=self.query).pack(fill=tk.X,pady=4)
        self.query.trace_add("write",lambda *_:self.show_inventory())
        self.inventory=self._table(frame,[("name","Série / versão",380),("symbol","Objeto",150),("tf","TF",70),("source","Fonte",150)],height=6)
        actions=ttk.Frame(frame);actions.pack(fill=tk.X)
        ttk.Button(actions,text="← Página",command=lambda:self.change_page(-1)).pack(side=tk.LEFT)
        ttk.Button(actions,text="Página →",command=lambda:self.change_page(1)).pack(side=tk.LEFT)
        self.inventory_count=ttk.Label(actions);self.inventory_count.pack(side=tk.LEFT,padx=8)
        form=ttk.Frame(frame);form.pack(fill=tk.X,pady=8);form.columnconfigure(1,weight=1)
        self.relationship=tk.StringVar(value="Associação explícita de pesquisa")
        self.basis=tk.StringVar(value=next(k for k,v in BASIS.items() if v==self.panel.timeline_basis))
        self.start=tk.StringVar();self.end=tk.StringVar()
        for row,(label,variable) in enumerate([("Descrição objeto → ativo",self.relationship),("Início UTC da fonte (vazio = primeira observação)",self.start),("Fim UTC exclusivo (vazio = última janela)",self.end)]):
            ttk.Label(form,text=label).grid(row=row,column=0,sticky="w",padx=(0,8),pady=2)
            ttk.Entry(form,textvariable=variable).grid(row=row,column=1,sticky="ew")
        ttk.Label(form,text="Perspectiva da análise").grid(row=3,column=0,sticky="w")
        combo=ttk.Combobox(form,textvariable=self.basis,values=list(BASIS),state="readonly")
        combo.grid(row=3,column=1,sticky="ew");combo.bind("<<ComboboxSelected>>",self.set_basis)
        row=ttk.Frame(frame);row.pack(fill=tk.X)
        buttons=[ttk.Button(row,text="Vincular selecionadas ao ativo",command=self.add),
                 ttk.Button(row,text="Importar e vincular snapshot…",command=self.import_source),
                 ttk.Button(row,text="Retirar vínculo selecionado",command=self.remove)]
        def layout(event):
            cols=3 if event.width>900 else 2
            for i in range(3):row.columnconfigure(i,weight=1 if i<cols else 0)
            for i,button in enumerate(buttons):button.grid(row=i//cols,column=i%cols,sticky='ew',padx=2,pady=2)
        row.bind('<Configure>',layout)
        ttk.Button(frame,text="Coletar fonte econômica…",command=self.open_macro).pack(anchor="w",pady=6)
        ttk.Button(frame,text='Materializar Super ATR…',command=self.open_super_atr).pack(anchor='w',pady=6)
        self.linked=self._table(frame,[("target","Ativo",100),("source","Objeto / snapshot",230),("tf","TF",75),("count","Séries",80),("why","Associação explícita",340)],height=3)
        self.show_links()

    def open_super_atr(self):
        if self.pending:return
        if getattr(self,'super_atr_capture',None) is not None and self.super_atr_capture.alive():self.super_atr_capture.window.lift();return
        from .super_atr_capture import SuperAtrCapture
        self.super_atr_capture=SuperAtrCapture(self)

    def open_macro(self):
        if self.pending:return
        if getattr(self,"macro_capture",None) is not None and self.macro_capture.alive():
            self.macro_capture.window.lift();return
        from .macro_capture import MacroCapture
        def saved(result):
            self.last_macro_receipt=result;self.query.set(result['symbol']);self.load_inventory()
        self.macro_capture=MacroCapture(self.ui,self.window,db=self.db,on_busy=self._busy,on_saved=saved,cancel_event=self.cancel_event)

    def set_basis(self,*_):
        self.panel.timeline_basis=BASIS[self.basis.get()];self.panel._changed()
        self.status.configure(text="Perspectiva alterada para a próxima análise. O relatório atual continua congelado.")

    def load_inventory(self):
        self._busy(True,"Lendo inventário de fontes armazenadas — aguarde…")
        def done(entries,error):
            if not self.alive():return
            self._busy(False)
            if error:self.status.configure(text=f"Inventário indisponível: {error}");return
            self.rows=[dict(entry=e,series=s) for e in entries for s in e['series']]
            self.show_inventory();self.status.configure(text="Escolha as versões e vincule explicitamente ao ativo. Nenhuma relação econômica é inferida pelo nome.")
        self.ui.run_bg(lambda:inventory.entries(self.db),done)

    def show_inventory(self):
        terms=self.query.get().casefold().split()
        self.filtered=[r for r in self.rows if all(t in (r['entry']['name']+' '+r['entry']['feature_id']+' '+r['series']['symbol']+' '+r['series']['timeframe']).casefold() for t in terms)]
        self.page=max(0,min(self.page,max(0,(len(self.filtered)-1)//50)))
        self.inventory.delete(*self.inventory.get_children());self.visible=self.filtered[self.page*50:(self.page+1)*50]
        for i,r in enumerate(self.visible):
            e,s=r['entry'],r['series'];self.inventory.insert('',tk.END,iid=str(i),values=(e['name']+' · '+e['algorithm_version'],s['symbol'],s['timeframe'],e.get('source_id') or e.get('family')))
        self.inventory_count.configure(text=f"{len(self.filtered)} séries · página {self.page+1}")

    def change_page(self,step):
        self.page+=step;self.show_inventory()

    def target(self):
        symbol=self.panel.symbol.get()
        if not symbol:raise ValueError("Escolha o ativo do gráfico antes de vincular.")
        if self.panel.linked_selections and self.panel.linked_target!=symbol:
            raise ValueError("Os vínculos pertencem a outro ativo; retire-os antes de mudar a associação.")
        return symbol

    def add(self):
        try:
            target=self.target();groups={}
            if not self.relationship.get().strip():raise ValueError("Descreva a associação escolhida.")
            chosen=[self.visible[int(i)] for i in self.inventory.selection()]
            if not chosen:raise ValueError("Selecione uma ou mais versões no inventário.")
            for row in chosen:
                e,s=row['entry'],row['series'];groups.setdefault((s['symbol'],s['timeframe']),[]).append(row)
            for (symbol,tf),rows in groups.items():
                step=pd.Timedelta(minutes=timeframes.parse(tf).minutes)
                first=self.start.get().strip() or min(r['series']['first'] for r in rows)
                end=self.end.get().strip() or (max(pd.Timestamp(r['series']['last']) for r in rows)+step).isoformat()
                features=[[r['entry']['feature_id'],r['entry']['algorithm_version']] for r in rows]
                item=dict(symbol=symbol,timeframe=tf,start=first,end=end,features=features,relationship=self.relationship.get().strip())
                if item not in self.panel.linked_selections:self.panel.linked_selections.append(item)
            self.panel.linked_target=target;self.panel._changed();self.show_links()
            self.status.configure(text=f"Vínculo escolhido para {target}. Feche e use Congelar e analisar; nenhum dado foi coletado.")
        except (ValueError,AttributeError) as exc:self.status.configure(text=str(exc))

    def show_links(self):
        self.linked.delete(*self.linked.get_children())
        for i,item in enumerate(self.panel.linked_selections):
            self.linked.insert('',tk.END,iid=str(i),values=(self.panel.linked_target,item.get('symbol',item.get('manifest_id','')[:16]),item.get('timeframe','congelado'),len(item['features']),item['relationship']))

    def remove(self):
        for i in sorted(map(int,self.linked.selection()),reverse=True):self.panel.linked_selections.pop(i)
        self.panel._changed();self.show_links()

    def import_source(self):
        if self.pending:return
        try:target=self.target()
        except ValueError as exc:self.status.configure(text=str(exc));return
        path=filedialog.askopenfilename(parent=self.window,filetypes=[('Snapshot JSON','*.json')])
        if not path:return
        relationship=self.relationship.get().strip()
        if not relationship:self.status.configure(text="Descreva o vínculo antes de importar.");return
        self._busy(True,"Importando snapshot existente — aguarde; nenhuma coleta/classificação…")
        def work():
            if Path(path).stat().st_size>64*1024*1024:
                raise ValueError("Snapshot excede64MiB; exporte uma janela menor antes de importar.")
            snap=datasets.import_snapshot(self.db,path)
            return dict(manifest_id=snap.manifest_id,features=[[d['feature_id'],d['algorithm_version']] for d in snap.feature_definitions],relationship=relationship)
        def done(item,error):
            if not self.alive():return
            self._busy(False)
            if error:self.status.configure(text=f"Snapshot não vinculado: {error}");return
            self.panel.linked_selections.append(item);self.panel.linked_target=target;self.panel._changed();self.show_links()
            self.status.configure(text="Snapshot importado e vínculo escolhido. Use Congelar e analisar para incluir no relatório.")
        self.ui.run_bg(work,done)

    def _matrix(self):
        from .components import ScrollableContent
        self.matrix_viewport=ScrollableContent(self.matrix_tab,self.ui.theme)
        self.matrix_viewport.pack(fill=tk.BOTH,expand=True)
        frame=self.matrix_viewport.content
        row=ttk.Frame(frame);row.pack(fill=tk.X)
        ttk.Button(row,text="Calcular todos os pares da análise",command=self.calculate).pack(side=tk.LEFT)
        ttk.Button(row,text="Exportar relatório HTML…",command=self.export).pack(side=tk.LEFT,padx=4)
        self.lag=tk.StringVar(value='0');combo=ttk.Combobox(row,textvariable=self.lag,values=['0'],width=5,state='readonly')
        self.lag_box=combo
        combo.pack(side=tk.LEFT);combo.bind('<<ComboboxSelected>>',lambda *_:self.show_matrix())
        protocol_row=ttk.Frame(frame);protocol_row.pack(fill=tk.X,pady=6)
        ttk.Button(protocol_row,text="Registrar protocolo JSON…",command=self.register_protocol).pack(side=tk.LEFT)
        ttk.Button(protocol_row,text="Avaliar protocolo registrado…",command=self.evaluate_protocol).pack(side=tk.LEFT,padx=4)
        self.matrix=self._table(frame,[("left","Preditor / TF",300),("right","Resposta / TF",300),("effect","Spearman",110),("n","Pares",80),("state","Estado / motivo",230)],height=8)
        self.matrix.bind('<<TreeviewSelect>>',self.matrix_selected)
        self.matrix_page=0
        row=ttk.Frame(frame);row.pack(fill=tk.X)
        ttk.Button(row,text="← Pares",command=lambda:self.page_matrix(-1)).pack(side=tk.LEFT)
        ttk.Button(row,text="Pares →",command=lambda:self.page_matrix(1)).pack(side=tk.LEFT)
        self.matrix_count=ttk.Label(row);self.matrix_count.pack(side=tk.LEFT,padx=8)
        ttk.Label(frame,text="Selecione um par para ver a estabilidade em quatro blocos cronológicos.").pack(anchor='w')
        self.windows=self._table(frame,[("first","Início UTC",180),("last","Fim UTC",180),("effect","Spearman",110),("n","Pares",80),("reason","Motivo",220)],height=4)

    def _busy(self,on,message=None):
        self.pending=on
        if message:self.status.configure(text=message)
        if on:self.progress.pack(fill=tk.X);self.progress.start(12)
        else:self.progress.stop();self.progress.pack_forget()

    def register_protocol(self):
        if self.pending:return
        report=self.panel.report
        if report is None:self.status.configure(text='Congele uma análise antes de registrar.');return
        path=filedialog.askopenfilename(parent=self.window,filetypes=[('Protocolo JSON','*.json')])
        if not path:return
        def work():
            if Path(path).stat().st_size>1024*1024:raise ValueError('Protocolo excede1MiB.')
            return sdk.register_laboratory_protocol(self.db,report,json.loads(Path(path).read_text(encoding='utf-8')))
        self._busy(True,'Registrando família e janelas — aguarde…')
        def done(result,error):
            if not self.alive():return
            self._busy(False)
            if error:self.status.configure(text=f'Protocolo não registrado: {error}');return
            self.protocol_id=result['protocol_id'];self.status.configure(text='Protocolo registrado. Avaliar é uma ação separada; o relatório de origem já foi exposto.')
        self.ui.run_bg(work,done)

    def evaluate_protocol(self):
        if self.pending:return
        if self.panel.report is None or not getattr(self,'protocol_id',None):self.status.configure(text='Registre o protocolo JSON para esta análise primeiro.');return
        from tkinter import simpledialog
        key=simpledialog.askstring('Avaliação registrada','Chave desta avaliação (letras/números/hífen):',parent=self.window)
        if not key:return
        report=self.panel.report;self.cancel_event.clear();self._busy(True,'Avaliando família, cortes temporais e bootstrap — aguarde…')
        def done(result,error):
            if not self.alive():return
            self._busy(False)
            if error:self.status.configure(text=f'Avaliação não concluída: {error}. Acesso permanece no ledger.');return
            self.evaluation=result;self.status.configure(text=f"Avaliação salva no ledger: {result['tests_counted']} testes com Bonferroni. Exporte HTML para consultar. Sem qualificação de previsão ou reserva inédita.")
        from mt5_extracao.analytics import producer_jobs
        memory=min(8192,max(1024,int(producer_jobs.current_rss_bytes()/1024**2)+256))
        self.ui.run_bg(lambda:sdk.evaluate_laboratory_protocol(self.db,report,self.protocol_id,key=key,cancel_check=self.cancel_event.is_set,max_memory_mb=memory),done)

    def calculate(self):
        if self.pending:return
        report=self.panel.report
        if report is None:self.status.configure(text="Congele a análise no Laboratório antes de calcular os cruzamentos.");return
        self.cancel_event.clear();self._busy(True,"Calculando os pares da seleção — aguarde…")
        def done(result,error):
            if not self.alive():return
            self._busy(False)
            if error:self.status.configure(text=f"Cruzamentos indisponíveis: {error}");return
            self.result=result;self.result_report=report;self.matrix_page=0
            self.lag_box.configure(values=[str(m['lag_bars']) for m in result['matrices']]);self.show_matrix()
            self.status.configure(text="Todos os pares selecionados estão disponíveis por página. — não é correlação zero; veja o motivo.")
        self.ui.run_bg(lambda:sdk.laboratory_crossings(report,cancel_check=self.cancel_event.is_set),done)

    def show_matrix(self):
        if not self.result:return
        matrix=next(m for m in self.result['matrices'] if m['lag_bars']==int(self.lag.get()))
        names={c['id']:c['name']+' · '+c['timeframe'] for c in matrix['channels']}
        rows=[r for group in matrix['cells'] for r in group]
        self.matrix_page=max(0,min(self.matrix_page,max(0,(len(rows)-1)//100)))
        self.matrix.delete(*self.matrix.get_children())
        self.shown_pairs=rows[self.matrix_page*100:(self.matrix_page+1)*100]
        for i,r in enumerate(self.shown_pairs):
            self.matrix.insert('',tk.END,iid=str(i),values=(names[r['left']],names[r['right']],value_text(r['effect']),r['n_pairs'],r['reason_text'] or r['reason'] or 'Exploratória'))
        self.matrix_count.configure(text=f"{len(rows)} pares · página {self.matrix_page+1} · defasagem {self.lag.get()}")
        self.windows.delete(*self.windows.get_children())

    def matrix_selected(self,*_):
        selected=self.matrix.selection()
        if not selected or not self.result:return
        row=self.shown_pairs[int(selected[0])]
        timeline=next((t for t in self.result.get('timelines',[]) if t['left']==row['left'] and t['right']==row['right'] and t['lag_bars']==int(self.lag.get())),None)
        self.windows.delete(*self.windows.get_children())
        for w in (timeline or {}).get('windows',[]):
            self.windows.insert('',tk.END,values=(w['start'],w['end'],value_text(w['value']),w['n_pairs'],w['reason'] or 'Exploratória'))
        if timeline is None:self.status.configure(text=row['reason_text'] or row['reason'] or 'Sem estabilidade elegível neste par.')

    def page_matrix(self,step):
        self.matrix_page+=step;self.show_matrix()

    def export(self):
        if self.pending:return
        report=self.panel.report
        if report is None:self.status.configure(text="Congele uma análise antes de exportar.");return
        path=filedialog.asksaveasfilename(parent=self.window,defaultextension='.html',initialfile='cruzamentos-'+report['analysis_id'][:12]+'.html')
        if not path:return
        self.cancel_event.clear();self._busy(True,"Preparando relatório portátil — aguarde…")
        def done(result,error):
            if not self.alive():return
            self._busy(False);self.status.configure(text=f"Relatório não exportado: {error}" if error else f"Relatório salvo em {result['path']}. Abra no navegador; nenhuma conexão externa é necessária.")
        evaluation=getattr(self,'evaluation',None)
        if evaluation and evaluation['analysis_id']!=report['analysis_id']:evaluation=None
        execution=getattr(self,'execution',None)
        if execution and execution['analysis_id']!=report['analysis_id']:execution=None
        self.ui.run_bg(lambda:sdk.export_laboratory_report(report,path,cancel_check=self.cancel_event.is_set,evaluation=evaluation,execution=execution),done)

    def _candle(self):
        from .components import ScrollableContent
        viewport=ScrollableContent(self.candle_tab,self.ui.theme);viewport.pack(fill=tk.BOTH,expand=True);body=viewport.content
        row=ttk.Frame(body);row.pack(fill=tk.X)
        self.candle_index=tk.StringVar(value='0');ttk.Label(row,text="Índice do candle (desde0)").pack(side=tk.LEFT)
        ttk.Entry(row,textvariable=self.candle_index,width=8).pack(side=tk.LEFT,padx=4)
        ttk.Button(row,text="Inspecionar",command=self.show_candle).pack(side=tk.LEFT)
        ttk.Label(body,text="Duplo clique no gráfico ou seleção de recibo leva ao candle. Valores, recebimentos e execução são informações separadas.",wraplength=600).pack(anchor='w',pady=6)
        box=ttk.Frame(body);box.pack(fill=tk.BOTH,expand=True);box.rowconfigure(0,weight=1);box.columnconfigure(0,weight=1)
        self.candle_text=tk.Text(box,height=8,wrap='word',font=('TkDefaultFont',10),background=self.ui.theme.colors['field'],foreground=self.ui.theme.colors['ink'])
        self.candle_text.grid(row=0,column=0,sticky='nsew')
        scroll=ttk.Scrollbar(box,orient=tk.VERTICAL,command=self.candle_text.yview);scroll.grid(row=0,column=1,sticky='ns')
        self.candle_text.configure(yscrollcommand=scroll.set)
        self.candle_values=self._table(body,[('name','Série / TF',320),('value','Valor',120),('unit','Unidade',100),('available','Disponível / fechamento',210),('state','Estado',180)],height=5)
        execution_row=ttk.Frame(body);execution_row.pack(fill=tk.X,pady=6)
        self.execution_choice=tk.StringVar();self.execution_options={}
        ttk.Button(execution_row,text='Listar execuções',command=self.list_executions).grid(row=0,column=0,sticky='w')
        self.execution_box=ttk.Combobox(execution_row,textvariable=self.execution_choice,state='readonly');self.execution_box.grid(row=1,column=0,sticky='ew')
        ttk.Button(execution_row,text='Vincular execução selecionada',command=self.link_execution).grid(row=2,column=0,sticky='w')
        execution_row.columnconfigure(0,weight=1)
        self.execution_events=self._table(body,[('kind','Evento observado / modelo',180),('phase','Fase',90),('fill','Preço fill',110),('volume','Volume fill',110),('sl','SL inicial gravado',130),('tp','TP inicial gravado',130),('net','Resultado acumulado',140),('time','Disponibilidade do produtor',230)],height=4)
        ttk.Label(body,text='Cronologia das fontes · selecione uma linha para ir ao candle representado',wraplength=600).pack(anchor='w')
        self.events_page=0;self.events_analysis=None;self.event_rows=[]
        event_nav=ttk.Frame(body);event_nav.pack(fill=tk.X)
        ttk.Button(event_nav,text='← Recibos',command=lambda:self.page_events(-1)).pack(side=tk.LEFT)
        ttk.Button(event_nav,text='Recibos →',command=lambda:self.page_events(1)).pack(side=tk.LEFT)
        self.events_count=ttk.Label(event_nav);self.events_count.pack(side=tk.LEFT,padx=6)
        self.events_tree=self._table(body,[('name','Série',260),('time','Disponibilidade',210),('value','Valor',110),('revision','Revisão',80),('candle','Candle',80),('state','Estado',180)],height=6)
        self.events_tree.bind('<<TreeviewSelect>>',self.event_selected)
        from .components import DetailsPanel
        details=DetailsPanel(body,title='Recibo completo do candle',expanded=False);details.pack(fill=tk.BOTH,expand=True,pady=6)
        self.candle_receipt=tk.Text(details.body,height=7,wrap='word',font=('TkDefaultFont',9));self.candle_receipt.pack(fill=tk.BOTH,expand=True)

    def page_events(self,step):
        self.events_page=max(0,self.events_page+step);self.show_events()

    def show_events(self):
        report=self.panel.report
        if self.events_analysis is not report:
            self.events_analysis=report;self.events_page=0
            self.event_rows=[(c,e) for c in report['channels'] for e in c.get('receipt_events',[])]
        self.events_page=min(self.events_page,max(0,(len(self.event_rows)-1)//50))
        key=(id(report),self.events_page)
        if getattr(self,'_shown_event_page',None)==key:return
        self._shown_event_page=key;self.events_tree.delete(*self.events_tree.get_children())
        for i,(c,e) in enumerate(self.event_rows[self.events_page*50:(self.events_page+1)*50],self.events_page*50):
            key='publication_bar_index' if report['selection'].get('timeline_basis')=='publication' else 'receipt_bar_index'
            self.events_tree.insert('',tk.END,iid=str(i),values=(c['name'],e['available_at'],value_text(e['value']),e['revision'],e.get(key) if e.get(key) is not None else 'fora',e['state']))
        self.events_count.configure(text=f'{len(self.event_rows)} recibos · página{self.events_page+1}')

    def event_selected(self,*_):
        selected=self.events_tree.selection()
        if not selected:return
        _c,e=self.event_rows[int(selected[0])];key='publication_bar_index' if self.panel.report['selection'].get('timeline_basis')=='publication' else 'receipt_bar_index'
        index=e.get(key)
        if index is None:self.status.configure(text='Recibo fora do recorte ou sem candle alinhado; não foi projetado em outra barra.');return
        self.show_candle(index)

    def list_executions(self):
        if self.pending:return
        from mt5_extracao.robots import store
        self._busy(True,'Lendo metadados de execuções — aguarde…')
        def done(rows,error):
            if not self.alive():return
            self._busy(False)
            if error:self.status.configure(text=str(error));return
            self.execution_options={f"{r['robot_key']} · {r['symbol']} · {r['timeframe']} · {r['created_at']} · {r['run_id'][:8]}":r['run_id'] for r in rows}
            self.execution_box.configure(values=list(self.execution_options));self.execution_choice.set('')
            self.status.configure(text='Até50 execuções recentes. Escolha a mesma grade/revisão; vínculo incompatível será recusado.')
        self.ui.run_bg(lambda:store.run_index(self.db,limit=50),done)

    def link_execution(self):
        if self.pending or self.panel.report is None:return
        ident=self.execution_options.get(self.execution_choice.get())
        if not ident:self.status.configure(text='Liste e escolha uma execução gravada.');return
        report=self.panel.report;self._busy(True,'Conciliando recibo de execução — aguarde…')
        import time
        from mt5_extracao.analytics import producer_jobs,indicator_materializer
        self.cancel_event.clear();started=dict(started_monotonic=time.monotonic());memory=min(8192,max(1024,int(producer_jobs.current_rss_bytes()/1024**2)+256))
        def guard():
            if self.ui.app.db_manager is not self.db:raise ValueError('Base alterada durante a consulta de execução.')
            indicator_materializer._guard(started,None,self.cancel_event.is_set,120,memory)
        def done(result,error):
            if not self.alive():return
            self._busy(False)
            if error:self.status.configure(text=str(error));return
            if report is not self.panel.report:self.status.configure(text='Análise mudou; vínculo não aplicado.');return
            self.execution=result;self.show_candle();self.status.configure(text='Observações vinculadas à mesma revisão. Base do produtor declarada; sem aceite operacional.')
        self.ui.run_bg(lambda:sdk.link_laboratory_execution(self.db,report,ident,guard=guard),done)

    def show_candle(self,index=None):
        if self.panel.report is None:self.status.configure(text="Congele ou abra uma análise.");return
        try:
            if index is not None:self.candle_index.set(str(index))
            execution=getattr(self,'execution',None)
            if execution and execution['analysis_id']!=self.panel.report['analysis_id']:execution=None
            index=int(self.candle_index.get());context=sdk.laboratory_candle(self.panel.report,index,execution=execution)
            self.panel.select_candle(index)
            lines=[f"{context['symbol']} · {context['timeframe']} · fechamento {context['close_utc']}",
                   ' · '.join(f"{k}: {value_text(v)}" for k,v in context['price'].items()),
                   'Perspectiva: '+context['timeline_basis'],
                   'Sessão: '+str(context['market_session'] or 'sem calendário congelado')+' · '+context['calendar_basis'],
                   'Motivo do preço: não inferido de correlação.','']
            self.candle_values.delete(*self.candle_values.get_children())
            for c in context['channels']:self.candle_values.insert('',tk.END,values=(c['name']+' · '+c['timeframe'],value_text(c['raw_value']),c['unit'] or '—',c['source_end'] or '—',c['state'] or 'fechamento nominal'))
            lines.append('\nPeríodos econômicos de referência (não são causa do preço):')
            lines.extend(f"{p['value']} · {p['start']} → {p['end']} · recebido {p['available_at']}" for p in context.get('source_periods',[]))
            lines.append(f"Recebimentos neste candle: {len(context['received_events'])}")
            lines.append(f"Janelas de publicação encerradas neste candle: {len(context['publication_windows'])}")
            self.execution_events.delete(*self.execution_events.get_children())
            observed=context.get('execution')
            if observed:
                basis={'modeled_execution_order':'sequência do modelo'}.get(observed['observation_basis'],observed['observation_basis'])
                lines.append('Execução: '+observed['run_id']+' · '+str(basis))
                labels={'account_balance':'Saldo','floating_equity':'Patrimônio','performance_equity':'Patrimônio em cotas'}
                lines.append('Capital gravado: '+' · '.join(f'{labels.get(k,k)}: {value_text(v)}' for k,v in observed['capital'].items()))
                for event in observed['events']:
                    kind={'entry':'Entrada','exit':'Saída','mark':'Marcação','partial':'Parcial'}.get(event['kind'],event['kind'])
                    phase={'open':'Abertura','intrabar':'Intrabar','close':'Fechamento'}.get(event['phase'],event['phase'])
                    self.execution_events.insert('',tk.END,values=(kind,phase,value_text(event.get('fill_price')),value_text(event.get('fill_volume')),value_text(event.get('initial_stop_price')),value_text(event.get('initial_target_price')),value_text(event.get('net_pnl')),event['available_at_utc']))
            else:lines.append('Nenhuma execução vinculada à mesma revisão; intenções não são fills.')
            self.candle_receipt.configure(state='normal');self.candle_receipt.delete('1.0',tk.END);self.candle_receipt.insert('1.0',json.dumps(context,ensure_ascii=False,indent=2));self.candle_receipt.configure(state='disabled')
            self.show_events()
            self.candle_text.configure(state='normal');self.candle_text.delete('1.0',tk.END);self.candle_text.insert('1.0','\n'.join(lines));self.candle_text.configure(state='disabled')
        except (ValueError,KeyError) as exc:self.status.configure(text=f"Candle indisponível: {exc}")

    def close(self):
        if self.pending:
            self.cancel_event.set();self.status.configure(text="Aguarde o término/cancelamento antes de fechar.");return
        self.window.destroy()
