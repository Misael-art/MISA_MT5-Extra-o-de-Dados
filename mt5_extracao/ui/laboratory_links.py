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
        self.linked=self._table(frame,[("target","Ativo",100),("source","Objeto / snapshot",230),("tf","TF",75),("count","Séries",80),("why","Associação explícita",340)],height=3)
        self.show_links()

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
        self.ui.run_bg(lambda:sdk.export_laboratory_report(report,path,cancel_check=self.cancel_event.is_set,evaluation=evaluation),done)

    def _candle(self):
        row=ttk.Frame(self.candle_tab);row.pack(fill=tk.X)
        self.candle_index=tk.StringVar(value='0');ttk.Label(row,text="Índice do candle (desde0)").pack(side=tk.LEFT)
        ttk.Entry(row,textvariable=self.candle_index,width=8).pack(side=tk.LEFT,padx=4)
        ttk.Button(row,text="Inspecionar",command=self.show_candle).pack(side=tk.LEFT)
        ttk.Label(self.candle_tab,text="O duplo clique no gráfico seleciona o candle. Captura e disponibilidade são mostradas separadamente.",wraplength=950).pack(anchor='w',pady=6)
        box=ttk.Frame(self.candle_tab);box.pack(fill=tk.BOTH,expand=True);box.rowconfigure(0,weight=1);box.columnconfigure(0,weight=1)
        self.candle_text=tk.Text(box,wrap='word',font=('TkDefaultFont',10),background=self.ui.theme.colors['field'],foreground=self.ui.theme.colors['ink'])
        self.candle_text.grid(row=0,column=0,sticky='nsew')
        scroll=ttk.Scrollbar(box,orient=tk.VERTICAL,command=self.candle_text.yview);scroll.grid(row=0,column=1,sticky='ns')
        self.candle_text.configure(yscrollcommand=scroll.set)

    def show_candle(self,index=None):
        if self.panel.report is None:self.status.configure(text="Congele ou abra uma análise.");return
        try:
            if index is not None:self.candle_index.set(str(index))
            context=sdk.laboratory_candle(self.panel.report,int(self.candle_index.get()))
            lines=[f"{context['symbol']} · {context['timeframe']} · fechamento {context['close_utc']}",
                   ' · '.join(f"{k}: {value_text(v)}" for k,v in context['price'].items()),
                   'Perspectiva: '+context['timeline_basis'],
                   'Sessão: '+str(context['market_session'] or 'sem calendário congelado')+' · '+context['calendar_basis'],
                   'Motivo do preço: não inferido de correlação.','']
            for c in context['channels']:
                lines.extend([f"{c['name']} · {c['timeframe']}: {value_text(c['raw_value'])} {c['unit'] or ''}",
                              f"Observação {c['observation_time'] or '—'} · disponibilidade/fechamento {c['source_end'] or '—'} · {c['state'] or 'nominal'}"])
            lines.append('\nPeríodos econômicos de referência (não são causa do preço):')
            lines.extend(json.dumps(p,ensure_ascii=False,indent=2) for p in context.get('source_periods',[]))
            lines.append('\nRecebimentos neste candle:')
            lines.extend(json.dumps(e,ensure_ascii=False,indent=2) for e in context['received_events'])
            lines.append('\nJanelas de publicação encerradas neste candle:')
            lines.extend(json.dumps(e,ensure_ascii=False,indent=2) for e in context['publication_windows'])
            self.candle_text.configure(state='normal');self.candle_text.delete('1.0',tk.END);self.candle_text.insert('1.0','\n'.join(lines));self.candle_text.configure(state='disabled')
        except (ValueError,KeyError) as exc:self.status.configure(text=f"Candle indisponível: {exc}")

    def close(self):
        if self.pending:
            self.cancel_event.set();self.status.configure(text="Aguarde o término/cancelamento antes de fechar.");return
        self.window.destroy()
