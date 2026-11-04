"""Leitura de séries por janela dentro do Laboratório; dados/indicadores vêm do SDK."""
from datetime import datetime,timezone
import json
import threading
import tkinter as tk
from tkinter import filedialog,ttk

from mt5_extracao import sdk
from mt5_extracao.ui import charts
from mt5_extracao.ui.components import DetailsPanel,MetricCard,Notice,ScrollableContent


def value_text(value):
    if value is None:return '—'
    if isinstance(value,(int,float)):
        return format(value,',.0f' if float(value).is_integer() else ',.4f').replace(',','_').replace('.',',').replace('_','.')
    return str(value)


class SeriesObservations:
    def __init__(self,parent,ui):
        self.ui,self.db,self.theme=ui,ui.app.db_manager,ui.theme
        self.pending=False;self.report=None;self._cancel=threading.Event();self.close_after=False;self._creator=None
        self.window=tk.Toplevel(parent);self.window.title('Laboratório · séries por janela')
        self.window.transient(parent.winfo_toplevel());self.window.protocol('WM_DELETE_WINDOW',self.close)
        self.window.geometry(f"{min(1060,self.window.winfo_screenwidth()-60)}x{min(800,self.window.winfo_screenheight()-80)}")
        self.window.minsize(600,480)
        footer=ttk.Frame(self.window,padding=8);footer.pack(side=tk.BOTTOM,fill=tk.X)
        self.status=ttk.Label(footer,text='Lendo fontes armazenadas — aguarde…',wraplength=850)
        self.status.pack(fill=tk.X)
        self.progress=ttk.Progressbar(footer,mode='indeterminate');self.progress.pack(fill=tk.X,pady=4)
        self.cancel_button=ttk.Button(footer,text='Cancelar',command=self.cancel);self.cancel_button.pack(side=tk.LEFT)
        ttk.Button(footer,text='Fechar',command=self.close).pack(side=tk.RIGHT)
        scroll=ScrollableContent(self.window,self.theme);scroll.pack(fill=tk.BOTH,expand=True)
        body=scroll.content;body.columnconfigure(0,weight=1)
        Notice(body,'Valores por janela de publicação, vistos retrospectivamente no corte. '
            'Abrir esta tela não captura nem classifica. Lacunas não são zero.',theme=self.theme).pack(fill=tk.X,padx=8,pady=6)
        form=ttk.Frame(body,padding=8);form.pack(fill=tk.X);form.columnconfigure(1,weight=1)
        self.source=tk.StringVar();self.start=tk.StringVar();self.end=tk.StringVar()
        self.cut=tk.StringVar(value=datetime.now(timezone.utc).isoformat());self.mean=tk.StringVar(value='3')
        self.source_box=ttk.Combobox(form,textvariable=self.source,state='readonly');self.source_box.grid(row=0,column=1,sticky='ew')
        ttk.Label(form,text='Objeto / TF:').grid(row=0,column=0,sticky='w',padx=(0,8))
        self.source_box.bind('<<ComboboxSelected>>',self.choose_source)
        self.inputs=[self.source_box]
        for row,(label,var) in enumerate([('Início UTC (inclusivo):',self.start),('Fim UTC (exclusivo):',self.end),
                                        ('Corte de disponibilidade:',self.cut),('Média (janelas consecutivas):',self.mean)],1):
            ttk.Label(form,text=label).grid(row=row,column=0,sticky='w',padx=(0,8),pady=2)
            entry=ttk.Entry(form,textvariable=var);entry.grid(row=row,column=1,sticky='ew',pady=2);self.inputs.append(entry)
        selection=DetailsPanel(body,'Séries e versões · seleção múltipla',expanded=True,theme=self.theme)
        selection.pack(fill=tk.X,padx=8,pady=4)
        self.list=tk.Listbox(selection.body,selectmode=tk.MULTIPLE,exportselection=False,height=5)
        self.list.pack(fill=tk.X);self.inputs.append(self.list)
        horizontal=ttk.Scrollbar(selection.body,orient=tk.HORIZONTAL,command=self.list.xview)
        horizontal.pack(fill=tk.X);self.list.configure(xscrollcommand=horizontal.set)
        actions=ttk.Frame(body,padding=8);actions.pack(fill=tk.X)
        self.read_button=ttk.Button(actions,text='Congelar e ler',style='Accent.TButton',command=self.read)
        self.refresh_button=ttk.Button(actions,text='Atualizar fontes',command=self.reload)
        self.relation_button=ttk.Button(actions,text='Criar relação…',command=self.relation)
        self.export_button=ttk.Button(actions,text='Exportar snapshot…',command=self.export)
        self.import_button=ttk.Button(actions,text='Abrir snapshot…',command=self.open_snapshot)
        for index,button in enumerate([self.read_button,self.refresh_button,self.relation_button,self.export_button,self.import_button]):
            button.grid(row=index//2,column=index%2,sticky='ew',padx=3,pady=3)
        actions.columnconfigure((0,1),weight=1)
        cards=ttk.Frame(body);cards.pack(fill=tk.X,padx=8,pady=4);self.cards={}
        for i,(key,label) in enumerate([('quantity','Quantidade'),('positive','Positivo'),('negative','Negativo')]):
            card=MetricCard(cards,label,theme=self.theme);card.grid(row=0,column=i,sticky='nsew',padx=2)
            cards.columnconfigure(i,weight=1);self.cards[key]=card
        table_frame=ttk.Frame(body,padding=8);table_frame.pack(fill=tk.X)
        table_frame.columnconfigure(0,weight=1)
        self.table=ttk.Treeview(table_frame,columns=('name','source','value','state'),show='headings',height=4)
        for key,title,width in [('name','Série / versão',300),('source','Fonte',110),('value','Última janela',110),('state','Estado',150)]:
            self.table.heading(key,text=title);self.table.column(key,width=width,minwidth=90,anchor='e' if key=='value' else 'w')
        self.table.grid(row=0,column=0,sticky='ew');self.table.bind('<<TreeviewSelect>>',self.detail)
        sx=ttk.Scrollbar(table_frame,orient=tk.HORIZONTAL,command=self.table.xview);sx.grid(row=1,column=0,sticky='ew')
        sy=ttk.Scrollbar(table_frame,orient=tk.VERTICAL,command=self.table.yview);sy.grid(row=0,column=1,sticky='ns')
        self.table.configure(xscrollcommand=sx.set,yscrollcommand=sy.set)
        from mt5_extracao.ui.laboratory_series import AnalysisChart
        self.chart=AnalysisChart(body,self.theme,'Séries e médias · janelas UTC',self.draw)
        self.chart.frame.pack(fill=tk.BOTH,expand=True,padx=8,pady=4)
        details=DetailsPanel(body,'Fonte, versão, classificação e disponibilidade',expanded=False,theme=self.theme)
        details.pack(fill=tk.X,padx=8,pady=4)
        self.details=tk.Text(details.body,height=10,wrap='word',background=self.theme.colors['surface'],
            foreground=self.theme.colors['ink'],relief='flat');self.details.pack(fill=tk.X)
        self.window.bind('<Configure>',lambda e:self.status.configure(wraplength=max(300,self.window.winfo_width()-24)) if e.widget is self.window else None)
        self.reload()

    def alive(self):return bool(self.window.winfo_exists())

    def busy(self,value,message=None,*,cancellable=True):
        self.pending=value
        self._cancellable=cancellable
        if message:self.status.configure(text=message)
        if value:
            self.progress.pack(fill=tk.X,pady=4,before=self.cancel_button);self.progress.start(12)
        else:self.progress.stop();self.progress.pack_forget()
        for widget in [self.read_button,self.refresh_button,self.relation_button,self.export_button,self.import_button]:
            widget.state(['disabled'] if value else ['!disabled'])
        self.cancel_button.state(['!disabled'] if value and cancellable else ['disabled'])
        for widget in self.inputs:
            if isinstance(widget,ttk.Combobox):widget.configure(state='disabled' if value else 'readonly')
            else:widget.configure(state='disabled' if value else 'normal')
        if not value and self.report is None:
            self.relation_button.state(['disabled']);self.export_button.state(['disabled'])
        if not value and self.source.get() not in getattr(self,'sources',{}):self.read_button.state(['disabled'])

    def reload(self):
        if self.pending:return
        self._cancel.clear();self.busy(True,'Lendo fontes e versões — aguarde…')
        def work():return sdk.stored_series_sources(self.db),sdk.query_catalog(self.db)
        def done(result,error):
            if not self.alive():return
            self.busy(False)
            if self.close_after:self.window.destroy();return
            if error:self.status.configure(text='Leitura indisponível: '+str(error));return
            if self._cancel.is_set():self.status.configure(text='Leitura cancelada; seleção anterior preservada.');return
            sources,catalog=result
            if catalog['status']!='ok':self.status.configure(text=str(catalog['errors']));return
            self.sources={f"{s['symbol']} · {s['timeframe']}":s for s in sources}
            self.definitions=[d for d in catalog['definitions'] if d.get('registered_definition') is not None]
            self.source_box.configure(values=list(self.sources))
            if self.source.get() not in self.sources:self.source.set(next(iter(self.sources),''))
            self.choose_source()
            self.status.configure(text=('Escolha versões e congele a leitura. Nenhuma captura de internet será iniciada. '+getattr(self,'_window_notice','')) if sources else
                'Nenhuma série armazenada nesta base. Capture/ingira dados pelo fluxo explícito antes de consultar.')
        self.ui.run_bg(work,done)

    def choose_source(self,*_):
        source=self.sources.get(self.source.get())
        if source is None:return
        if not self.pending:self.read_button.state(['!disabled'])
        import pandas as pd
        from mt5_extracao import timeframes
        from mt5_extracao.laboratory.service import MAX_BARS
        first,end=pd.Timestamp(source['start']),pd.Timestamp(source['end'])
        bound=end-pd.Timedelta(minutes=MAX_BARS*timeframes.parse(source['timeframe']).minutes)
        self.start.set(max(first,bound).isoformat());self.end.set(end.isoformat())
        self._window_notice='Início limitado às últimas20 mil janelas nominais; ajuste o período para explorar outra parte.' if first<bound else ''
        if self._window_notice:self.status.configure(text=self._window_notice)
        self.choices=[d for d in self.definitions if d.get('timeframe') in (None,source['timeframe'])]
        self.list.delete(0,tk.END)
        preferred=next((d['algorithm_version'] for d in self.choices if d['feature_id'].startswith('mentions.')),None)
        for i,d in enumerate(self.choices):
            self.list.insert(tk.END,f"{d['name']} · {d['feature_id']} · {d['algorithm_version']}")
            if (preferred and d['algorithm_version']==preferred) or (preferred is None and i<3):self.list.selection_set(i)

    def read(self):
        if self.pending:return
        try:
            source=dict(self.sources[self.source.get()]);pairs=[(self.choices[i]['feature_id'],self.choices[i]['algorithm_version']) for i in self.list.curselection()]
            if not pairs:raise ValueError('Escolha ao menos uma série/versão.')
            if len(pairs)>6:raise ValueError('Escolha até6 séries por gráfico para manter valores e legendas legíveis.')
            period=int(self.mean.get());start,end,cut=self.start.get(),self.end.get(),self.cut.get()
        except (ValueError,KeyError) as exc:self.status.configure(text=str(exc));return
        from mt5_extracao.resources import guard
        check=guard.make(self.db,self.ui.app.config);self._cancel.clear();self.busy(True,'Congelando e preparando séries/médias — aguarde…')
        def work():
            frozen=sdk.capture_feature_snapshot(self.db,symbol=source['symbol'],timeframe=source['timeframe'],start=start,end=end,
                stored_features=pairs,write_guard=check,cancel_check=self._cancel.is_set)
            return sdk.query_stored_observations(self.db,frozen['manifest_id'],as_of=cut,mean_period=period,
                write_guard=check,cancel_check=self._cancel.is_set)
        def done(report,error):
            if not self.alive():return
            self.busy(False)
            if self.close_after:self.window.destroy();return
            if error:self.status.configure(text='Leitura indisponível: '+str(error));return
            if self._cancel.is_set():self.status.configure(text='Leitura cancelada; resultado anterior preservado.');return
            self.report=report;self.present();self.busy(False)
        self.ui.run_bg(work,done)

    def present(self):
        r=self.report;self.table.delete(*self.table.get_children())
        self.chart.set_heading(r['symbol']+' · '+r['timeframe']+' · séries congeladas por janela UTC')
        for i,c in enumerate(r['channels']):
            self.table.insert('',tk.END,iid=str(i),values=(c['name']+' · '+c['algorithm_version'][:14],c['source_id'],
                value_text(c['display_values'][-1]),{'VALID':'Disponível','UNAVAILABLE':'Indisponível',
                    'INVALIDATED':'Invalidado','NOT_AVAILABLE_AT_CUT':'Ausente no corte'}.get(c['qualities'][-1],c['qualities'][-1])))
        variants={c['algorithm_version'] for c in r['channels'] if c['feature_id'].startswith('mentions.')}
        for key,card in self.cards.items():
            matches=[c for c in r['channels'] if c['feature_id']=='mentions.'+key]
            usable=len(matches)==1 and len(variants)==1
            detail='Selecione uma população comum de Q/P/N; consulte as versões na tabela.'
            if usable:
                c=matches[0];meta=c['metadata'][-1];detail='Última janela · '+str(c['source_id'])
                if key!='quantity' and 'label_counts' in meta:
                    detail+='\n'+value_text(meta['label_counts'].get('abstained',0))+' abstenções fora de P/N'
                if meta.get('synthetic_fixture'):detail+=' · referência sintética'
                if meta.get('qualification')=='CONTROLLED_ENGLISH_BENCHMARK_ONLY':detail+=' · inglês restrito'
                elif meta.get('qualification')=='CONTROLLED_PORTUGUESE_BENCHMARK_ONLY':detail+=' · português restrito'
            card.set_value(value_text(matches[0]['values'][-1]) if usable else '—',detail)
        self.status.configure(text=f"{r['symbol']} {r['timeframe']} · {len(r['times'])} janelas · corte {r['as_of']} · "
            f"snapshot {r['manifest_id'][:12]}. Abstenção não é neutralidade; confira os motivos abaixo.")
        if r['channels']:self.table.selection_set('0');self.detail()
        self.chart.redraw()

    def detail(self,*_):
        if not self.report or not self.table.selection():return
        c=self.report['channels'][int(self.table.selection()[0])];m=c['metadata'][-1];capture=dict(m.get('capture',{}));capture.pop('roster',None)
        text=json.dumps(dict(series=c['feature_id'],version=c['algorithm_version'],source=c['source_id'],unit=c['unit'],
            scope=c['parameters'],last_window=self.report['times'][-1],value=c['display_values'][-1],quality=c['qualities'][-1],
            available_at=c['available_at'][-1],revision=c['revisions'][-1],synthetic_fixture=m.get('synthetic_fixture'),classification=m.get('classification'),
            label_counts=m.get('label_counts'),reason=m.get('reason'),qualification=m.get('qualification'),capture=capture,
            mean=dict(period=c['mean_period'],value=c['mean'][-1],available_at=c['mean_available_at'][-1],implementation=c['mean_implementation']),
            limitations=self.report['limitations']),ensure_ascii=False,indent=2)
        self.details.configure(state='normal');self.details.delete('1.0',tk.END);self.details.insert('1.0',text);self.details.configure(state='disabled')

    def draw(self,chart):
        if not self.report:chart.message('Congele uma seleção para visualizar valores e médias.');return
        import pandas as pd
        chart.clear();groups={}
        for c in self.report['channels']:groups.setdefault(c['unit'] or 'Unidade não declarada',[]).append(c)
        times=pd.to_datetime(self.report['times'],utc=True)
        from matplotlib.dates import AutoDateLocator,ConciseDateFormatter
        from matplotlib.ticker import FuncFormatter,MaxNLocator
        for i,(unit,channels) in enumerate(groups.items()):
            ax=chart.figure.add_subplot(len(groups),1,i+1)
            for c in channels:
                scope=c['parameters'].get('mentions',{})
                label=c['name'].split(' · ')[0]+' · '+str(c['source_id'])+' · '+str(scope.get('query_id',c['algorithm_version'][:8]))+' · '+c['algorithm_version'][-6:]
                line,=ax.plot(times,c['values'],marker='o',markersize=4,label=charts.plain(label))
                if any(v is not None for v in c['mean']):
                    ax.plot(times,c['mean'],linestyle='--',color=line.get_color(),label=f'Média {c["mean_period"]} · '+charts.plain(label))
            ax.set_ylabel(charts.plain(unit));locator=AutoDateLocator(minticks=2,maxticks=5)
            ax.yaxis.set_major_locator(MaxNLocator(nbins=4,integer=unit=='publication/window'))
            ax.yaxis.set_major_formatter(FuncFormatter(lambda value,_position:value_text(value)))
            ax.xaxis.set_major_locator(locator);ax.xaxis.set_major_formatter(ConciseDateFormatter(locator))
            charts.style_axis(ax,self.theme,'Janelas UTC · valores conhecidos no corte')
            ax.legend(fontsize=8,loc='best')
        chart.draw()

    def relation(self):
        if self.pending or not self.report:return
        if self._creator is not None and self._creator.alive():self._creator.window.lift();return
        from mt5_extracao.ui.relation_creator import RelationCreator
        from mt5_extracao.resources import guard
        r=self.report
        self._creator=RelationCreator(self.window,self.theme,db=self.db,run_bg=self.ui.run_bg,
            captures=[dict(manifest_id=r['manifest_id'],symbol=r['symbol'],timeframe=r['timeframe'])],as_of=r['as_of'],
            write_guard_factory=lambda:guard.make(self.db,self.ui.app.config),on_saved=lambda receipt:self.status.configure(
                text='Relação salva pelo cálculo canônico; consulte Relações e aderência. Sem grade de mercado o resultado pode ser insuficiente.'))

    def export(self):
        if self.pending or not self.report:return
        target=filedialog.asksaveasfilename(parent=self.window,defaultextension='.json',filetypes=[('Snapshot JSON','*.json')])
        if not target:return
        manifest=self.report['manifest_id'];self.busy(True,'Exportando blocos congelados — aguarde…',cancellable=False)
        def done(result,error):
            if not self.alive():return
            self.busy(False,'Exportação indisponível: '+str(error) if error else 'Snapshot exportado: '+str(result))
            if self.close_after:self.window.destroy()
        self.ui.run_bg(lambda:sdk.export_feature_snapshot(self.db,manifest,target),done)

    def open_snapshot(self):
        if self.pending:return
        source=filedialog.askopenfilename(parent=self.window,filetypes=[('Snapshot JSON','*.json')])
        if not source:return
        try:period=int(self.mean.get())
        except ValueError:self.status.configure(text='Informe um período inteiro para a média.');return
        cut=self.cut.get();self._cancel.clear();self.busy(True,'Abrindo blocos congelados — aguarde…',cancellable=False)
        def work():
            snapshot=sdk.import_feature_snapshot(self.db,source)
            return sdk.query_stored_observations(self.db,snapshot['manifest_id'],as_of=cut,mean_period=period)
        def done(report,error):
            if not self.alive():return
            self.busy(False)
            if self.close_after:self.window.destroy();return
            if error:self.status.configure(text='Snapshot indisponível: '+str(error));return
            self.source.set('Snapshot importado · '+report['symbol']+' · '+report['timeframe'])
            self.choices=[];self.list.delete(0,tk.END)
            for c in report['channels']:self.list.insert(tk.END,c['name']+' · '+c['algorithm_version'])
            self.report=report;self.present();self.busy(False)
        self.ui.run_bg(work,done)

    def cancel(self):
        if self.pending and self._cancellable:
            self._cancel.set();self.status.configure(text='Cancelando — aguarde a confirmação.');self.cancel_button.state(['disabled'])
        elif self.pending:self.status.configure(text='Concluindo a gravação antes de fechar — aguarde…')

    def close(self):
        if self.pending:self.close_after=True;self.cancel()
        else:
            if self._creator is not None and self._creator.alive():self._creator.close(cancel=True)
            self.chart.restore();self.window.destroy()
