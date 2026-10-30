"""Materialização explícita do header Super ATR; cálculo/store pertencem a Dados."""
from pathlib import Path
import time
import tkinter as tk
from tkinter import ttk,filedialog

from mt5_extracao import sdk
from mt5_extracao.analytics import indicator_materializer,producer_jobs
from mt5_extracao.laboratory import service
from mt5_extracao.resources import guard as resource_guard
from .components import ScrollableContent


class SuperAtrCapture:
    def __init__(self,owner):
        self.owner,self.ui=owner,owner.ui;self.db=owner.db;panel=owner.panel
        self.pending=False;self.close_after=False;self.cancel_event=owner.cancel_event
        self.window=tk.Toplevel(owner.window);self.window.title('Materializar Super ATR');self.window.geometry('700x620');self.window.minsize(580,460);self.window.protocol('WM_DELETE_WINDOW',self.close)
        viewport=ScrollableContent(self.window,self.ui.theme);viewport.pack(fill=tk.BOTH,expand=True)
        body=ttk.Frame(viewport.content,padding=12);body.pack(fill=tk.BOTH,expand=True);body.columnconfigure(1,weight=1)
        ttk.Label(body,text='Calcula o header HADOUKEN fixado sobre uma janela congelada. A primeira barra fixa a semente; a disponibilidade é o recebimento local. Para ver a curva histórica, escolha Publicação retrospectiva ao vincular.',wraplength=620).grid(row=0,column=0,columnspan=2,sticky='ew',pady=8)
        symbol=panel.symbol.get() or (panel.report or {}).get('symbol','')
        native=[r['timeframe'] for r in getattr(panel,'_inventory',[]) if r['symbol']==symbol and not r.get('synthetic')]
        self.symbol=tk.StringVar(value=symbol);self.timeframe=tk.StringVar(value=native[0] if native else '')
        config=self.ui.app.config
        roots=[Path(p.strip()).expanduser() for p in config.get('ROBOTS','library_dirs',fallback='').split(';') if p.strip()]
        self.framework=tk.StringVar(value=str(next((p for p in roots if (p/'Include/Framework/HADOUKEN/Indicators/HDK_SuperATR.mqh').is_file()),'')))
        self.start=tk.StringVar(value=panel.start.get());self.end=tk.StringVar(value=panel.end.get())
        self.period=tk.StringVar(value='20');self.mean=tk.StringVar(value='0');self.deviation=tk.StringVar(value='1')
        self.memory=tk.StringVar(value=str(min(8192,max(1024,int(producer_jobs.current_rss_bytes()/1024**2)+256))))
        for i,(label,var) in enumerate([('Ativo selecionado',self.symbol),('Timeframe nativo',self.timeframe),('Raiz canônica HADOUKEN',self.framework),
            ('Início broker / semente (vazio=primeira)',self.start),('Fim broker (vazio=última)',self.end),('Período',self.period),
            ('Média (0=SMA,1=EMA)',self.mean),('Desvio',self.deviation),('Teto RSS do processo (MiB)',self.memory)],1):
            ttk.Label(body,text=label).grid(row=i,column=0,sticky='w',pady=4)
            if var is self.timeframe:widget=ttk.Combobox(body,textvariable=var,values=native,state='readonly')
            elif var is self.mean:widget=ttk.Combobox(body,textvariable=var,values=['0','1'],state='readonly')
            else:widget=ttk.Entry(body,textvariable=var,state='readonly' if var is self.symbol else 'normal')
            widget.grid(row=i,column=1,sticky='ew')
        ttk.Button(body,text='Escolher HADOUKEN existente…',command=self.choose_framework).grid(row=10,column=1,sticky='e')
        self.status=ttk.Label(body,text='Grava linha, ATR, lado e virada no store existente; nenhuma conta/ordem é acessada.',wraplength=620);self.status.grid(row=11,column=0,columnspan=2,sticky='ew',pady=8)
        self.progress=ttk.Progressbar(body,mode='indeterminate');self.progress.grid(row=12,column=0,columnspan=2,sticky='ew');self.progress.grid_remove()
        self.submit_button=ttk.Button(body,text='Congelar, materializar e preparar fonte',command=self.submit);self.submit_button.grid(row=13,column=0,sticky='w',pady=8)
        ttk.Button(body,text='Cancelar / fechar',command=self.close).grid(row=13,column=1,sticky='e')

    def alive(self):return bool(self.window.winfo_exists())

    def choose_framework(self):
        chosen=filedialog.askdirectory(parent=self.window,title='Raiz canônica HADOUKEN existente')
        if chosen:self.framework.set(chosen)

    def close(self):
        if self.pending:self.close_after=True;self.cancel_event.set();self.status.configure(text='Cancelamento solicitado; aguarde a etapa do compilador. Snapshots/fatos já confirmados são preservados.')
        else:self.window.destroy()

    def submit(self):
        if self.pending or self.owner.pending:return
        try:
            symbol,tf,root=self.symbol.get(),self.timeframe.get(),self.framework.get().strip()
            if not symbol or not tf or not root:raise ValueError('Escolha ativo, timeframe e raiz HADOUKEN existentes.')
            period,mean,deviation,memory=int(self.period.get()),int(self.mean.get()),float(self.deviation.get()),float(self.memory.get())
            start,end=self.start.get().strip() or None,self.end.get().strip() or None
        except ValueError as exc:self.status.configure(text=str(exc));return
        revision=self.owner.panel._selection_revision;self.pending=True;self.cancel_event.clear();self.submit_button.state(['disabled']);self.progress.grid();self.progress.start(12)
        self.owner._busy(True,'Congelando e materializando Super ATR — aguarde…');self.status.configure(text='Preparando fonte, compilando o adaptador e materializando — aguarde…')
        def cancelled():return self.cancel_event.is_set() or self.ui.app.db_manager is not self.db or revision!=self.owner.panel._selection_revision
        def work():
            started=dict(started_monotonic=time.monotonic())
            write=resource_guard.make(self.db,self.ui.app.config)
            def guard():indicator_materializer._guard(started,write,cancelled,120,memory)
            snapshot=service.capture_selection(self.db,symbol=symbol,timeframes_selected=[tf],start=start,end=end,guard=guard)[0]
            result=sdk.materialize_super_atr_snapshot(self.db,snapshot.manifest_id,framework_root=root,period=period,mean=mean,deviation=deviation,
                write_guard=write,cancel_check=cancelled,timeout_seconds=max(.001,120-(time.monotonic()-started['started_monotonic'])),max_memory_mb=memory)
            result['snapshot']=sdk.capture_feature_snapshot(self.db,symbol=symbol,timeframe=tf,start=result['window_start_utc'],end=result['window_end_utc'],stored_features=result['features'],
                cancel_check=cancelled,timeout_seconds=max(.001,120-(time.monotonic()-started['started_monotonic'])),max_memory_mb=memory)
            return result
        def done(result,error):
            if not self.alive():return
            self.pending=False;self.progress.stop();self.progress.grid_remove();self.submit_button.state(['!disabled']);self.owner._busy(False)
            if error:self.status.configure(text=f'Materialização não concluída: {error}. Fatos já confirmados permanecem preservados.')
            else:
                self.owner.last_super_atr_receipt=result;self.owner.query.set('super');self.owner.load_inventory();self.window.destroy();return
            if self.close_after:self.window.destroy()
        self.ui.run_bg(work,done)
