"""Seleção explícita de execuções existentes; cálculo financeiro pertence ao motor."""
import json
import threading
import time
import tkinter as tk
from tkinter import ttk,filedialog

from mt5_extracao import sdk
from mt5_extracao.analytics import indicator_materializer,producer_jobs
from mt5_extracao.robots import store
from .components import ScrollableContent,TablePanel
from .series_observations import value_text


class FinancialComparison:
    def __init__(self,panel):
        self.panel,self.ui=panel,panel.ui;self.db=panel._db();self.offset=0;self.selected={};self.pending=False;self.result=None;self.cancel_event=threading.Event()
        self.window=tk.Toplevel(panel.frame);self.window.title('Comparar execuções gravadas');self.window.geometry('1100x800');self.window.minsize(650,500);self.window.protocol('WM_DELETE_WINDOW',self.close)
        bottom=ttk.Frame(self.window,padding=8);bottom.pack(side=tk.BOTTOM,fill=tk.X)
        self.status=ttk.Label(bottom,text='Lendo metadados — aguarde…',wraplength=900);self.status.pack(fill=tk.X)
        bottom.bind('<Configure>',lambda e:self.status.configure(wraplength=max(200,e.width-20)))
        self.progress=ttk.Progressbar(bottom,mode='indeterminate');self.progress.pack(fill=tk.X)
        ttk.Button(bottom,text='Cancelar / fechar',command=self.close).pack(anchor='e')
        scroll=ScrollableContent(self.window,self.ui.theme);scroll.pack(fill=tk.BOTH,expand=True);body=scroll.content
        ttk.Label(body,text='Escolha2 a8 execuções existentes. Abrir e comparar não executam estratégias.',wraplength=600).pack(anchor='w',pady=6)
        self.index=TablePanel(body,'Execuções disponíveis',('Run','Robô','Ativo','TF','De','Até'),(230,170,110,70,160,160),selectmode='extended',xscroll=True,height=5);self.index.pack(fill=tk.BOTH,expand=True)
        navigation=ttk.Frame(body);navigation.pack(fill=tk.X)
        ttk.Button(navigation,text='← Página',command=lambda:self.page(-1)).pack(side=tk.LEFT)
        ttk.Button(navigation,text='Página →',command=lambda:self.page(1)).pack(side=tk.LEFT)
        ttk.Button(navigation,text='Adicionar selecionadas',command=self.add).pack(side=tk.LEFT,padx=6)
        self.chosen=TablePanel(body,'Seleção desta comparação',('Run','Ativo','TF'),(330,180,90),selectmode='extended',xscroll=True,height=3);self.chosen.pack(fill=tk.BOTH,expand=True,pady=6)
        ttk.Button(body,text='Retirar da seleção',command=self.remove).pack(anchor='w')
        self.mode=tk.StringVar(value='Estratégias / presets no mesmo ativo')
        ttk.Combobox(body,textvariable=self.mode,values=['Estratégias / presets no mesmo ativo','Mesmo preset entre ativos'],state='readonly').pack(fill=tk.X,pady=6)
        actions=ttk.Frame(body);actions.pack(fill=tk.X)
        ttk.Button(actions,text='Comparar recibos',command=self.compare).grid(row=0,column=0,sticky='ew')
        ttk.Button(actions,text='Exportar HTML…',command=lambda:self.export('html')).grid(row=0,column=1,sticky='ew')
        ttk.Button(actions,text='Exportar JSON…',command=lambda:self.export('json')).grid(row=1,column=0,sticky='ew')
        for i in (0,1):actions.columnconfigure(i,weight=1)
        self.results=TablePanel(body,'Resultados gravados',('Run / ativo','Moeda','Operações','Lucro líquido','Saldo final','Patrimônio final','DD encerradas','DD patrimônio'),(280,90,90,120,120,140,130,130),xscroll=True,height=6);self.results.pack(fill=tk.BOTH,expand=True,pady=6)
        self.mode.trace_add('write',lambda *_:self.invalidate())
        self.load()

    def alive(self):return bool(self.window.winfo_exists())

    def job(self,work,done,message):
        if self.pending:return
        self.pending=True;self.cancel_event.clear();self.progress.pack(fill=tk.X);self.progress.start(12);self.status.configure(text=message)
        def finished(result,error):
            if not self.alive():return
            self.pending=False;self.progress.stop();self.progress.pack_forget()
            if error:self.status.configure(text=str(error));return
            if self.ui.app.db_manager is not self.db:self.status.configure(text='Base alterada; reabra a comparação no contexto atual.');return
            done(result)
        self.ui.run_bg(work,finished)

    def load(self):
        def done(rows):
            self.rows={r['run_id']:r for r in rows};self.index.tree.delete(*self.index.tree.get_children())
            for r in rows:self.index.tree.insert('',tk.END,iid=r['run_id'],values=tuple(r[k] for k in ('run_id','robot_key','symbol','timeframe','data_start','data_end')))
            self.status.configure(text=f'Página{self.offset//50+1} · {len(rows)} execuções. Selecione e adicione para comparar.')
        self.job(lambda:store.run_index(self.db,limit=50,offset=self.offset),done,'Lendo página de metadados — aguarde…')

    def page(self,step):
        if self.pending:return
        self.offset=max(0,self.offset+step*50);self.load()

    def add(self):
        if self.pending:return
        for ident in self.index.tree.selection():self.selected[ident]=self.rows[ident]
        self.show_selected()

    def remove(self):
        if self.pending:return
        for ident in self.chosen.tree.selection():self.selected.pop(ident,None)
        self.show_selected()

    def show_selected(self):
        self.invalidate()
        self.chosen.tree.delete(*self.chosen.tree.get_children())
        for ident,r in self.selected.items():self.chosen.tree.insert('',tk.END,iid=ident,values=(ident,r['symbol'],r['timeframe']))
        self.status.configure(text=f'{len(self.selected)} execuções escolhidas; comparação admite2 a8.')

    def invalidate(self):
        self.result=None;self.results.tree.delete(*self.results.tree.get_children())

    def compare(self):
        if self.pending:return
        selected=list(self.selected);mode='same_preset' if self.mode.get()=='Mesmo preset entre ativos' else 'same_asset'
        started=dict(started_monotonic=time.monotonic());memory=min(8192,max(1024,int(producer_jobs.current_rss_bytes()/1024**2)+256))
        def guard():
            if self.ui.app.db_manager is not self.db:raise ValueError('Base alterada durante a comparação.')
            indicator_materializer._guard(started,None,self.cancel_event.is_set,120,memory)
        def done(result):
            self.result=result;tree=self.results.tree;tree.delete(*tree.get_children())
            def percent(v):return value_text(None if v is None else v*100)+('%' if v is not None else '')
            for r in result['rows']:
                s=r['summary'];tree.insert('',tk.END,values=(r['run_id']+' · '+r['symbol'],r['currency'] or 'não declarada',value_text(s['trades']),value_text(s['net_profit']),value_text(s['final_balance']),value_text(s['final_equity']),percent(s['max_drawdown_pct']),percent(s['max_floating_drawdown_pct'])))
            self.status.configure(text=('Recibos compatíveis. ' if result['compatible'] else 'Diferenças: '+'; '.join(result['reasons'])+'. ')+'Sem qualificação financeira; HTML inclui curvas e contexto completo.')
        self.job(lambda:sdk.compare_laboratory_runs(self.db,selected,mode=mode,guard=guard),done,'Comparando execuções gravadas — aguarde…')

    def export(self,kind):
        if self.pending or self.result is None:return
        path=filedialog.asksaveasfilename(parent=self.window,defaultextension='.'+kind,initialfile='comparacao-execucoes.'+kind)
        if not path:return
        result=self.result
        def work():
            if kind=='html':return sdk.export_laboratory_runs(result,path)
            with open(path,'x',encoding='utf-8') as out:json.dump(result,out,ensure_ascii=False,allow_nan=False)
            return dict(path=path)
        self.job(work,lambda r:self.status.configure(text='Comparação salva em '+r['path']),'Exportando comparação congelada — aguarde…')

    def close(self):
        if self.pending:self.cancel_event.set();self.status.configure(text='Cancelamento solicitado; aguarde a tarefa terminar.');return
        self.window.destroy()
