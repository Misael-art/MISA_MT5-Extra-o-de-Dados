"""Resultados do lote revisado: consulta assíncrona, gráficos e detalhe sem execução."""
import json
import threading
import time
import tkinter as tk
from tkinter import ttk, filedialog

from mt5_extracao import sdk
from mt5_extracao.analytics import indicator_materializer, producer_jobs
from mt5_extracao.robots import store,matrix
from .components import ScrollableContent, TablePanel
from .charts import Chart, style_axis, diverging_cmap
from .theme import fmt_num

STATES = dict(completed='Concluído', pending='Pendente', running='Executando', failed='Falha',
              interrupted='Interrompido', excluded='Excluído', missing_result='Resultado ausente', inconsistent='Vínculo divergente')
METRICS = {'Retorno líquido (%)': ('return_pct', 100), 'DD patrimônio (%)': ('max_floating_drawdown_pct', 100),
           'Operações': ('trades', 1), 'Fator de lucro': ('profit_factor', 1)}


class ScopeComparisonDialog:
    def __init__(self, ui, batch_id, *, parent=None):
        self.ui, self.db, self.batch_id = ui, ui.app.db_manager, batch_id
        self.result, self.pending, self.curve_page = None, False, 0
        self._redraw_id = None
        self.cancel = threading.Event()
        self.parent_modal = parent
        self.window = tk.Toplevel(parent or ui.root); self.window.title('Preset entre tempos gráficos')
        if parent is not None:
            from .robot_controls import _modal_when_mapped
            self.window.transient(parent);_modal_when_mapped(self.window)
        self.window.geometry('1060x800'); self.window.minsize(600, 500); self.window.protocol('WM_DELETE_WINDOW', self.close)
        shell = ttk.Frame(self.window, padding=12); shell.pack(fill=tk.BOTH, expand=True)
        footer = ttk.Frame(shell); footer.pack(side=tk.BOTTOM, fill=tk.X)
        self.status = ttk.Label(footer, text='Lendo o lote revisado — aguarde…', wraplength=800)
        self.status.pack(fill=tk.X); self.status.bind('<Configure>', lambda e:self.status.configure(wraplength=max(100,e.width-4)))
        self.progress = ttk.Progressbar(footer, mode='indeterminate'); self.progress.pack(fill=tk.X)
        actions = ttk.Frame(footer); actions.pack(fill=tk.X, pady=6)
        self.refresh = ttk.Button(actions, text='Atualizar', command=self.load); self.refresh.pack(side=tk.LEFT)
        ttk.Button(actions, text='Exportar HTML…', command=self.export).pack(side=tk.LEFT, padx=6)
        ttk.Button(actions, text='Fechar', command=self.close).pack(side=tk.RIGHT)
        self.context = ttk.Label(shell, text='', wraplength=900, justify=tk.LEFT); self.context.pack(fill=tk.X, pady=(0,8))
        self.context.bind('<Configure>', lambda e:self.context.configure(wraplength=max(100,e.width-4)))
        filters = ttk.Frame(shell); filters.pack(fill=tk.X)
        self.asset, self.metric = tk.StringVar(), tk.StringVar(value=next(iter(METRICS)))
        self.asset_combo = ttk.Combobox(filters, textvariable=self.asset, state='readonly', width=18)
        self.asset_combo.pack(side=tk.LEFT)
        ttk.Combobox(filters, textvariable=self.metric, values=list(METRICS), state='readonly', width=24).pack(side=tk.LEFT, padx=6)
        self.tabs = ttk.Notebook(shell); self.tabs.pack(fill=tk.BOTH, expand=True, pady=6)
        self.views = []
        for title in ('Resultados', 'Ativos × TF', 'Capital', 'Detalhe'):
            view = ScrollableContent(self.tabs, ui.theme); self.tabs.add(view, text=title); self.views.append(view)
        self.summary_chart = Chart(self.views[0].content, ui.theme, height=3.3)
        self.summary_chart.widget.pack(fill=tk.X); self.summary_chart.widget.configure(height=300)
        self.table = TablePanel(self.views[0].content, 'Células do lote · selecione para inspecionar',
            ('Ativo','TF','Estado','Operações','Retorno %','DD patrimônio %','PF','Run'), (130,65,145,90,110,140,130,280), xscroll=True, height=8)
        self.table.pack(fill=tk.BOTH, expand=True)
        self.table.tree.bind('<<TreeviewSelect>>', self.inspect)
        self.table.tree.bind('<Return>', lambda e:self.inspect(reveal=True))
        self.table.tree.bind('<Double-1>', lambda e:self.inspect(reveal=True))
        self.matrix_chart = Chart(self.views[1].content, ui.theme, height=3.8)
        self.matrix_chart.widget.pack(fill=tk.X); self.matrix_chart.widget.configure(height=360)
        ttk.Label(self.views[1].content, text=f'—: métrica ausente. Hachura: menos de{matrix.MIN_TRADES} operações ou amostra desconhecida. Passe o cursor para valor, n e estado; selecione para o detalhe.', wraplength=500).pack(fill=tk.X)
        self.matrix_chart.canvas.mpl_connect('button_press_event', self.matrix_select)
        pages = ttk.Frame(self.views[2].content); pages.pack(fill=tk.X)
        ttk.Button(pages,text='← TFs',command=lambda:self.page(-1)).pack(side=tk.LEFT)
        ttk.Button(pages,text='TFs →',command=lambda:self.page(1)).pack(side=tk.LEFT)
        self.curve_caption = ttk.Label(pages,text=''); self.curve_caption.pack(side=tk.LEFT,padx=8)
        self.curves = Chart(self.views[2].content, ui.theme, height=5.3)
        self.curves.widget.pack(fill=tk.X); self.curves.widget.configure(height=500)
        from matplotlib.backends.backend_tkagg import NavigationToolbar2Tk
        self.toolbar = NavigationToolbar2Tk(self.curves.canvas, self.views[2].content, pack_toolbar=False)
        self.toolbar.pack(fill=tk.X)
        ttk.Label(self.views[2].content, text='Patrimônio em fração do capital inicial declarado (base 1). Relógio e janela gravados por TF; zoom/pan não recalculam as métricas.',wraplength=550).pack(fill=tk.X)
        self.detail = ttk.Label(self.views[3].content,text='Selecione uma célula na tabela ou na matriz.',wraplength=850,justify=tk.LEFT)
        self.detail.pack(fill=tk.X); self.detail.bind('<Configure>',lambda e:self.detail.configure(wraplength=max(100,e.width-4)))
        receipt_frame=ttk.LabelFrame(self.views[3].content,text='Revisão efetiva, custos e tentativas',padding=6);receipt_frame.pack(fill=tk.X,pady=8)
        self.receipt=ui.theme.classic(tk.Text(receipt_frame,height=8,wrap='word',state='disabled'))
        receipt_scroll=ttk.Scrollbar(receipt_frame,command=self.receipt.yview);receipt_scroll.pack(side=tk.RIGHT,fill=tk.Y)
        self.receipt.configure(yscrollcommand=receipt_scroll.set);self.receipt.pack(fill=tk.BOTH,expand=True)
        self.trades = TablePanel(self.views[3].content,'Operações gravadas · sem inferir entradas/fills ausentes',('Saída','PnL','Volume'),(300,150,150),height=8,xscroll=True)
        self.trades.pack(fill=tk.BOTH,expand=True)
        self.detail_token = 0
        self.asset.trace_add('write', lambda *_:self.draw(reset=True))
        self.metric.trace_add('write', lambda *_:self.draw())
        self.window.bind('<Destroy>', self.destroyed, add='+')
        self.theme_listener = self.schedule_draw
        ui.theme.on_change(self.theme_listener)
        self.tabs.bind('<<NotebookTabChanged>>', lambda e:self.schedule_draw())
        for chart in (self.summary_chart,self.matrix_chart,self.curves):
            chart.widget.bind('<Map>',lambda e:self.schedule_draw(),add='+')
        self.matrix_chart.widget.bind('<Configure>',self.matrix_resized,add='+')
        self.load()

    def alive(self):
        try:return bool(self.window.winfo_exists())
        except tk.TclError:return False

    def job(self, work, done):
        if self.pending:return
        self.pending=True; self.cancel.clear(); self.progress.pack(fill=tk.X); self.progress.start(12)
        self.refresh.state(['disabled']); started=dict(started_monotonic=time.monotonic())
        memory=min(8192,max(1024,int(producer_jobs.current_rss_bytes()/1024**2)+256))
        def guard():
            if self.ui.app.db_manager is not self.db:raise ValueError('Base alterada; reabra a comparação.')
            indicator_materializer._guard(started,None,self.cancel.is_set,120,memory)
        def finish(result,error):
            self.pending=False
            if not self.alive():return
            self.progress.stop(); self.progress.pack_forget(); self.refresh.state(['!disabled'])
            if error:self.status.configure(text=str(error));return
            if self.ui.app.db_manager is not self.db:self.status.configure(text='Base alterada; reabra a comparação.');return
            done(result)
        self.ui.run_bg(lambda:work(guard),finish)

    def load(self):
        def done(value):
            self.result=value; self.detail_token+=1
            identity=value['preset_identity']; coverage=value['coverage']
            from mt5_extracao.robots.mt5_tools import TESTER_MODELS
            model='Barras da base local' if value['model']=='python_bar_backtest' else TESTER_MODELS.get(value['model'],value['model'])
            costs='custos configurados na simulação' if value['cost_basis']=='python_settings' else 'custos declarados no relatório nativo'
            currencies={r['currency'] for r in value['rows'] if r['currency']}
            currency=next(iter(currencies)) if len(currencies)==1 else 'moeda não uniforme/declarada'
            self.context.configure(text=f"{value['robot_name']} · {identity.get('label') or identity['id']} · revisão {identity['revision'] or 'por conteúdo'}\n"
                f"{value['interval']['start'][:10]} → {value['interval']['end'][:10]} · capital {fmt_num(value['settings']['initial_equity'])} {currency}\n"
                f"{model} · {costs} · revisão {identity['content_hash'][:12]} (recibo completo no detalhe)")
            counts=' · '.join(f"{STATES.get(k,k)}: {v}" for k,v in value['counts'].items())
            differences='Sem resultados concluídos e comprovados neste lote.' if not value['rows'] else f"Comparação separada: {len(value['reasons'])} diferença(s); veja o recibo no detalhe."
            if value['configuration_compatible'] and not value['compatible']:differences='Configuração consistente; coberturas temporais diferentes. Veja o detalhe.'
            self.status.configure(text=counts+'\n'+('Recibos compatíveis no recorte efetivo gravado.' if value['compatible'] else differences)+
                ('\nInterseção das barras: '+coverage['start']+' → '+coverage['end']+'. Métricas não recalculadas.' if coverage else '')+' Sem qualificação financeira.')
            self.cells={str(i):c for i,c in enumerate(value['cells'])}; tree=self.table.tree; tree.delete(*tree.get_children())
            for ident,c in self.cells.items():
                s=(c['result'] or {}).get('summary',{})
                tree.insert('',tk.END,iid=ident,values=(c['symbol'],c['timeframe'],STATES.get(c['state'],c['state']),
                    fmt_num(s.get('trades')),self.number(s.get('return_pct'),100),self.number(s.get('max_floating_drawdown_pct'),100),fmt_num(s.get('profit_factor')) if s.get('profit_factor') is not None else 'Indefinido' if c['result'] else '—',c['run_id'] or '—'))
            self.asset_combo.configure(values=value['symbols'])
            self.detail.configure(text='Selecione uma célula para revisar janela, preset, custos e operações.');self.trades.tree.delete(*self.trades.tree.get_children())
            self.receipt.configure(state='normal');self.receipt.delete('1.0',tk.END);self.receipt.configure(state='disabled')
            if self.asset.get() not in value['symbols']:self.asset.set(value['symbols'][0])
            self.draw(reset=True)
        self.job(lambda guard:sdk.compare_backtest_scope(self.db,self.batch_id,guard=guard),done)

    @staticmethod
    def number(value,factor=1):return fmt_num(None if value is None else value*factor)

    def schedule_draw(self):
        # O Map do TkAgg pode ajustar DPI/foto depois do evento do notebook.
        if self.alive() and self._redraw_id is None:
            self._redraw_id=self.window.after_idle(self._mapped_draw)

    def _mapped_draw(self):
        self._redraw_id=None
        if self.alive():self.draw()

    def matrix_resized(self,event):
        if not self.result or self.tabs.index('current')!=1:return
        vertical=event.width*.65/len(self.result['timeframes'])<28*self.matrix_chart.figure.dpi/100
        if vertical!=getattr(self,'_vertical_samples',None):self.schedule_draw()

    def draw(self,reset=False):
        if not self.result or not self.alive():return
        import numpy as np
        import pandas as pd
        if reset:self.curve_page=0
        key,factor=METRICS[self.metric.get()]; rows=[c for c in self.result['cells'] if c['symbol']==self.asset.get()]
        present=[c for c in rows if c['result'] and c['result']['summary'].get(key) is not None]
        tab=self.tabs.index('current')
        if tab==0:
            self.summary_chart.clear();ax=self.summary_chart.figure.add_subplot(111)
            values=[c['result']['summary'][key]*factor for c in present]
            ax.bar(range(len(values)),values,color=[self.ui.theme.colors['series'][0 if v>=0 else 1] for v in values])
            ax.axhline(0,color=self.ui.theme.colors['axis'],linewidth=.8);ax.set_xticks(range(len(present)),[c['timeframe'] for c in present],rotation=45)
            style_axis(ax,self.ui.theme,self.asset.get()+' · '+self.metric.get())
            if not present:ax.text(.5,.5,'Sem métrica concluída nesta seleção',ha='center',transform=ax.transAxes,color=self.ui.theme.colors['ink2'])
            self.summary_chart.hover=[lambda e: self.bar_tip(e,present,values,ax)]
            self.summary_chart.draw()
        if tab==1:
            self.matrix_chart.clear();axm=self.matrix_chart.figure.add_subplot(111)
            symbols,tfs=self.result['symbols'],self.result['timeframes'];lookup={(c['symbol'],c['timeframe']):c for c in self.result['cells']}
            grid=np.full((len(symbols),len(tfs)),np.nan)
            for (s,tf),c in lookup.items():
                v=(c['result'] or {}).get('summary',{}).get(key)
                if v is not None:grid[symbols.index(s),tfs.index(tf)]=v*factor
            finite=grid[np.isfinite(grid)];limit=float(abs(finite).max()) if finite.size and np.any(finite) else 1
            cmap=diverging_cmap(self.ui.theme).with_extremes(bad=self.ui.theme.colors['grid'])
            plotted=axm.imshow(np.ma.masked_invalid(grid),aspect='auto',cmap=cmap,vmin=-limit,vmax=limit)
            for y,x in zip(*np.where(~np.isfinite(grid))):
                axm.text(x,y,'—',ha='center',va='center',color=self.ui.theme.colors['ink2'],fontsize=7)
            from matplotlib.patches import Rectangle
            # A matriz de21 TFs precisa manter n legível mesmo no viewport estreito.
            vertical_samples=self.matrix_chart.figure.bbox.width*.65/len(tfs)<28*self.matrix_chart.figure.dpi/100
            self._vertical_samples=vertical_samples
            for (symbol,tf),cell in lookup.items():
                if cell['result'] is None:continue
                x,y=tfs.index(tf),symbols.index(symbol);n=cell['result']['summary']['trades']
                if n is None or n<matrix.MIN_TRADES:
                    axm.add_patch(Rectangle((x-.5,y-.5),1,1,facecolor=self.ui.theme.colors['grid'],edgecolor=self.ui.theme.colors['ink2'],hatch='///',alpha=.55,linewidth=0))
                if len(symbols)<=8:
                    sample='—' if n is None else str(int(n)) if int(n)==n else str(n)
                    axm.text(x,y+.24 if not np.isfinite(grid[y,x]) else y,'n='+sample,ha='center',va='center',color=self.ui.theme.colors['ink'],fontsize=6,rotation=90 if vertical_samples else 0)
            axm.set_xticks(range(len(tfs)),tfs,rotation=90 if vertical_samples else 60);axm.set_yticks(range(len(symbols)),symbols)
            style_axis(axm,self.ui.theme,self.metric.get(),grid_axis=None)
            colorbar=self.matrix_chart.figure.colorbar(plotted,ax=axm,shrink=.8)
            colorbar.ax.tick_params(colors=self.ui.theme.colors['ink2'],labelsize=8)
            colorbar.outline.set_edgecolor(self.ui.theme.colors['axis'])
            def tip(e):
                if e.inaxes!=axm or e.xdata is None or e.ydata is None:return
                x,y=int(e.xdata+.5),int(e.ydata+.5)
                if not(0<=x<len(tfs) and 0<=y<len(symbols)):return
                c=lookup.get((symbols[y],tfs[x]));v=(c['result'] or {}).get('summary',{}).get(key) if c else None
                n=(c['result'] or {}).get('summary',{}).get('trades') if c else None
                return f"{symbols[y]} {tfs[x]} · {STATES.get(c['state'],c['state']) if c else 'Sem combinação'} · {self.number(v,factor)} · n={self.number(n)}"
            self.matrix_chart.hover=[tip];self._matrix_axis=axm;self.matrix_chart.draw()
        if tab==2:
            self.curves.clear();completed=[c for c in rows if c['result']];count=len(completed)
            selected=completed[self.curve_page*6:(self.curve_page+1)*6]
            self.curve_caption.configure(text=f"{self.asset.get()} · página {self.curve_page+1}/{max(1,(count+5)//6)}")
            columns=min(2,len(selected)) or 1;plot_rows=max(1,(len(selected)+columns-1)//columns);axes=[]
            for i,c in enumerate(selected):
                a=self.curves.figure.add_subplot(plot_rows,columns,i+1,sharey=axes[0] if axes else None);axes.append(a);r=c['result'];points=r['curves']['floating_equity'];initial=r['summary']['initial_equity']
                if points and initial is not None and initial>0:
                    a.plot(pd.to_datetime([p[0] for p in points]),[p[1]/initial for p in points],color=self.ui.theme.colors['series'][0]);a.axhline(1,color=self.ui.theme.colors['axis'],linewidth=.6);a.tick_params(axis='x',labelrotation=20)
                else:a.text(.5,.5,'Sem patrimônio/capital válido',ha='center',transform=a.transAxes,color=self.ui.theme.colors['ink2'],fontsize=8)
                style_axis(a,self.ui.theme,c['timeframe']+' · n='+str(r['summary']['trades']));a.tick_params(labelsize=7)
            if not selected:
                a=self.curves.figure.add_subplot(111);a.axis('off');a.text(.5,.5,'Sem curvas concluídas',ha='center',color=self.ui.theme.colors['ink2'])
            self.curves.draw();self.toolbar.update()

    def bar_tip(self,e,cells,values,axis):
        if e.inaxes!=axis or e.xdata is None:return
        i=int(round(e.xdata))
        if 0<=i<len(cells):return cells[i]['timeframe']+' · '+self.number(values[i])+' · n='+str(cells[i]['result']['summary']['trades'])

    def matrix_select(self,e):
        if not self.result or e.inaxes is not getattr(self,'_matrix_axis',None) or e.xdata is None or e.ydata is None:return
        x,y=int(e.xdata+.5),int(e.ydata+.5)
        if not(0<=x<len(self.result['timeframes']) and 0<=y<len(self.result['symbols'])):return
        symbol,tf=self.result['symbols'][y],self.result['timeframes'][x]
        ident=next((i for i,c in self.cells.items() if (c['symbol'],c['timeframe'])==(symbol,tf)),None)
        if ident is not None:self.table.tree.selection_set(ident);self.inspect(reveal=True)

    def page(self,step):
        if not self.result:return
        count=sum(c['symbol']==self.asset.get() and c['result'] is not None for c in self.result['cells'])
        self.curve_page=max(0,min(max(0,(count-1)//6),self.curve_page+step));self.draw()

    def inspect(self,event=None,reveal=False):
        chosen=self.table.tree.selection()
        if not chosen or not self.result:return
        cell=self.cells[chosen[0]];self.detail_token+=1;token=self.detail_token
        r=cell['result'];self.trades.tree.delete(*self.trades.tree.get_children())
        text=f"{cell['symbol']} {cell['timeframe']} · {STATES.get(cell['state'],cell['state'])}\n{cell['reason']}\n"+'\n'.join(cell['limitations'])
        if r:
            s=r['summary']
            text+=f"\nRun {r['run_id']} · moeda {r['currency'] or 'não declarada'}\nJanela {r['interval']['start']} → {r['interval']['end']} · {r['interval']['bars']} barras\nSnapshot {r['data_manifest_id']}\nOperações {fmt_num(s['trades'])} · lucro líquido {fmt_num(s['net_profit'])} · retorno {self.number(s['return_pct'],100)}%\nSaldo final {fmt_num(s['final_balance'])} · patrimônio final {fmt_num(s['final_equity'])}\nDD patrimônio {self.number(s['max_floating_drawdown_pct'],100)}% · PF {fmt_num(s['profit_factor'])}"
        if r:text+='\nPF: '+r['profit_factor_note']
        receipt=dict(preset=self.result['preset_identity'],parameters=(r or {}).get('effective_params'),composition=(r or {}).get('composition'),execution=(r or {}).get('context'),attempts=cell['history'],comparison_reasons=self.result['reasons'])
        self.receipt.configure(state='normal');self.receipt.delete('1.0',tk.END);self.receipt.insert('1.0',json.dumps(receipt,ensure_ascii=False,indent=2));self.receipt.configure(state='disabled')
        self.detail.configure(text=text)
        if reveal:self.tabs.select(3)
        if r:
            def done(found,error):
                if not self.alive() or token!=self.detail_token or self.ui.app.db_manager is not self.db:return
                if error:self.detail.configure(text=text+'\n'+str(error));return
                for t in (found[0].get('trades') or []) if found else []:
                    self.trades.tree.insert('',tk.END,values=tuple(str(v) for v in t[:3]))
            self.ui.run_bg(lambda:store.load_runs(self.db,run_id=r['run_id']),done)

    def export(self):
        if self.pending or not self.result:return
        path=filedialog.asksaveasfilename(parent=self.window,defaultextension='.html',filetypes=[('HTML local','*.html')])
        if path:self.job(lambda guard:(guard(),sdk.export_laboratory_runs(self.result,path))[1],lambda _:self.status.configure(text='HTML local exportado com estados, curvas e recibos da comparação.'))

    def close(self):
        self.cancel.set();self.window.destroy()

    def destroyed(self,event):
        if event.widget==self.window:
            self.cancel.set();self.detail_token+=1
            if self._redraw_id is not None:
                self.window.after_cancel(self._redraw_id);self._redraw_id=None
            if self.theme_listener in self.ui.theme._listeners:self.ui.theme._listeners.remove(self.theme_listener)
            for view in self.views:
                if view._apply_theme in self.ui.theme._listeners:self.ui.theme._listeners.remove(view._apply_theme)
            if self.parent_modal is not None:
                try:
                    if self.parent_modal.winfo_exists():self.parent_modal.grab_set()
                except tk.TclError:pass
