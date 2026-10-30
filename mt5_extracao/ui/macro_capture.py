"""Ação explícita de coleta econômica; o cliente delega dados e snapshots ao SDK."""
from datetime import datetime,timezone
import threading
import time
import tkinter as tk
from tkinter import ttk

from mt5_extracao import sdk
from mt5_extracao.analytics import producer_jobs
from mt5_extracao.resources import guard
from .components import ScrollableContent


class MacroCapture:
    def __init__(self,ui,parent,*,db,on_busy,on_saved,cancel_event=None):
        self.ui,self.db=ui,db;self.on_busy,self.on_saved=on_busy,on_saved
        self.cancel_event=cancel_event or threading.Event();self.pending=False;self.close_after=False
        self.window=tk.Toplevel(parent);self.window.title('Coletar fonte econômica');self.window.geometry('640x420');self.window.minsize(540,360)
        self.window.protocol('WM_DELETE_WINDOW',self.close)
        viewport=ScrollableContent(self.window,ui.theme);viewport.pack(fill=tk.BOTH,expand=True)
        body=ttk.Frame(viewport.content,padding=12);body.pack(fill=tk.BOTH,expand=True);body.columnconfigure(1,weight=1)
        ttk.Label(body,text='Consulta pública explícita. Datas são períodos da fonte; disponibilidade é o recebimento local. Fases históricas permanecem retrospectivas.',wraplength=580).grid(row=0,column=0,columnspan=2,sticky='ew',pady=8)
        self.sources={item['name']:item for item in sdk.macro_source_capabilities()}
        today=datetime.now(timezone.utc).date()
        self.source=tk.StringVar(value=next(iter(self.sources)));self.start=tk.StringVar(value=f'{today.year}-01-01');self.end=tk.StringVar(value=today.isoformat())
        self.memory=tk.StringVar(value=str(min(8192,max(1024,int(producer_jobs.current_rss_bytes()/1024**2)+256))))
        ttk.Label(body,text='Fonte / convenção').grid(row=1,column=0,sticky='w')
        ttk.Combobox(body,textvariable=self.source,values=list(self.sources),state='readonly').grid(row=1,column=1,sticky='ew')
        for row,(label,value) in enumerate([('De (YYYY-MM-DD)',self.start),('Até exclusivo (YYYY-MM-DD)',self.end),('Teto RSS do processo (MiB)',self.memory)],2):
            ttk.Label(body,text=label).grid(row=row,column=0,sticky='w',pady=4)
            ttk.Entry(body,textvariable=value).grid(row=row,column=1,sticky='ew')
        self.status=ttk.Label(body,text='A fonte será congelada para vínculo posterior ao ativo. Nenhuma relação econômica é presumida.',wraplength=580)
        self.status.grid(row=5,column=0,columnspan=2,sticky='ew',pady=10)
        self.progress=ttk.Progressbar(body,mode='indeterminate');self.progress.grid(row=6,column=0,columnspan=2,sticky='ew');self.progress.grid_remove()
        self.submit_button=ttk.Button(body,text='Coletar e congelar fonte',command=self.submit);self.submit_button.grid(row=7,column=0,sticky='w',pady=8)
        ttk.Button(body,text='Cancelar / fechar',command=self.close).grid(row=7,column=1,sticky='e')

    def alive(self):
        return bool(self.window.winfo_exists())

    def close(self):
        if self.pending:
            self.close_after=True;self.cancel_event.set();self.status.configure(text='Cancelamento solicitado; fatos já confirmados são preservados.')
        else:self.window.destroy()

    def submit(self):
        if self.pending:return
        try:
            source=self.sources[self.source.get()]['id'];start,end=self.start.get().strip(),self.end.get().strip();memory=float(self.memory.get())
            if self.ui.app.db_manager is not self.db:raise ValueError('Base alterada; reabra a coleta no contexto atual.')
        except (ValueError,KeyError) as exc:self.status.configure(text=str(exc));return
        self.cancel_event.clear();self.pending=True;self.submit_button.state(['disabled']);self.progress.grid();self.progress.start(12)
        self.on_busy(True,'Coletando e congelando fonte econômica — aguarde…');self.status.configure(text='Coletando observações e congelando a fonte — aguarde…')
        def cancelled():return self.cancel_event.is_set() or self.ui.app.db_manager is not self.db
        def work():
            began=time.monotonic()
            result=sdk.collect_macro_observations(self.db,source_id=source,start=start,end=end,cancel_check=cancelled,
                timeout_seconds=120,max_memory_mb=memory,write_guard=guard.make(self.db,self.ui.app.config))
            if result['observations']:
                result['snapshot']=sdk.capture_feature_snapshot(self.db,symbol=result['symbol'],timeframe='D1',
                    start=start+'T00:00:00Z',end=end+'T00:00:00Z',stored_features=result['features'],
                    cancel_check=cancelled,timeout_seconds=max(.001,120-(time.monotonic()-began)),max_memory_mb=memory)
            return result
        def done(result,error):
            if not self.alive():return
            self.pending=False;self.progress.stop();self.progress.grid_remove();self.submit_button.state(['!disabled']);self.on_busy(False)
            if error:self.status.configure(text=f'Coleta não concluída: {error}. Fatos já confirmados foram preservados.')
            elif self.ui.app.db_manager is not self.db:self.status.configure(text='Base alterada; recibo não aplicado ao novo contexto.')
            elif result['observations']:
                self.on_saved(result);self.window.destroy();return
            else:self.status.configure(text='Nenhuma observação nessa janela; não foi inventado zero ou snapshot vazio.')
            if self.close_after:self.window.destroy()
        self.ui.run_bg(work,done)
