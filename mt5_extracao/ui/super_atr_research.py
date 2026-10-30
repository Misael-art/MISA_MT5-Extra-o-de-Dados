"""Criação explícita do candidato; compile/check/Session usam os controles existentes."""
from pathlib import Path
import threading
import tkinter as tk
from tkinter import ttk,filedialog
from mt5_extracao import sdk,robots
from .components import ScrollableContent


class SuperAtrResearch:
    def __init__(self,panel):
        self.panel,self.ui=panel,panel.ui;self.pending=False;self.cancel=threading.Event();self.close_after=False
        self.db=self.ui.app.db_manager
        self.window=tk.Toplevel(panel.frame);self.window.title('Super ATR · candidato de pesquisa');self.window.geometry('720x620');self.window.minsize(580,460);self.window.protocol('WM_DELETE_WINDOW',self.close)
        viewport=ScrollableContent(self.window,self.ui.theme);viewport.pack(fill=tk.BOTH,expand=True)
        body=ttk.Frame(viewport.content,padding=12);body.pack(fill=tk.BOTH,expand=True);body.columnconfigure(1,weight=1)
        ttk.Label(body,text='Gera um Expert pelo host HADOUKEN existente, somente Tester+DEMO. Entradas vêm desligadas. Nova virada no TF base + confirmação das últimas barras fechadas; stop na linha e saída oposta. Período20/SMA/desvio1 por TF são hipóteses de pesquisa.',wraplength=650).grid(row=0,column=0,columnspan=2,sticky='ew',pady=8)
        roots=[Path(p.strip()).expanduser() for p in self.ui.app.config.get('ROBOTS','library_dirs',fallback='').split(';') if p.strip()]
        self.name=tk.StringVar(value='SuperAtrPesquisa');self.folder=tk.StringVar();self.framework=tk.StringVar(value=str(next((p for p in roots if (p/'Include/Framework/HADOUKEN/Indicators/HDK_SuperATR.mqh').is_file()),'')))
        self.seed=tk.StringVar();self.base=tk.StringVar(value='M5');self.confirm=tk.StringVar(value='M15');self.second=tk.StringVar(value='desligado')
        for i,(label,var) in enumerate([('Nome',self.name),('Pasta de artefatos Experts/Include/Presets',self.folder),('Raiz canônica HADOUKEN',self.framework),('Primeira barra broker YYYY-MM-DDTHH:MM',self.seed),('TF base',self.base),('Primeiro confirmador',self.confirm),('Segundo confirmador (opcional)',self.second)],1):
            ttk.Label(body,text=label).grid(row=i,column=0,sticky='w',pady=5)
            if var in (self.base,self.confirm,self.second):
                choices=['M1','M2','M3','M4','M5','M6','M10','M12','M15','M20','M30','H1','H2','H3','H4','H6','H8','H12']
                if var is self.second:choices=['desligado']+choices
                widget=ttk.Combobox(body,textvariable=var,values=choices,state='readonly')
            else:widget=ttk.Entry(body,textvariable=var)
            widget.grid(row=i,column=1,sticky='ew')
        ttk.Button(body,text='Escolher pasta de artefatos…',command=lambda:self.choose(self.folder)).grid(row=8,column=0,sticky='w')
        ttk.Button(body,text='Escolher HADOUKEN existente…',command=lambda:self.choose(self.framework)).grid(row=8,column=1,sticky='e')
        self.status=ttk.Label(body,text='A semente deve existir e coincidir com a abertura em todos os TFs. Não compila/inicia MT5 nem habilita perfis ou ordens.',wraplength=650);self.status.grid(row=9,column=0,columnspan=2,sticky='ew',pady=12)
        self.progress=ttk.Progressbar(body,mode='indeterminate');self.progress.grid(row=10,column=0,columnspan=2,sticky='ew');self.progress.grid_remove()
        self.submit_button=ttk.Button(body,text='Gerar pacote de pesquisa',command=self.submit);self.submit_button.grid(row=11,column=0,sticky='w',pady=8)
        ttk.Button(body,text='Cancelar / fechar',command=self.close).grid(row=11,column=1,sticky='e')

    def alive(self):return bool(self.window.winfo_exists())

    def choose(self,var):
        selected=filedialog.askdirectory(parent=self.window)
        if selected:var.set(selected)

    def close(self):
        if self.pending:self.close_after=True;self.cancel.set();self.status.configure(text='Cancelamento solicitado — aguarde a conferência da composição.')
        else:self.window.destroy()

    def submit(self):
        if self.pending:return
        values=dict(name=self.name.get().strip(),folder=self.folder.get().strip(),framework_root=self.framework.get().strip(),seed_start=self.seed.get().strip(),base_timeframe=self.base.get(),confirm_timeframe=self.confirm.get(),second_timeframe=None if self.second.get()=='desligado' else self.second.get())
        if not values['folder'] or not values['framework_root']:self.status.configure(text='Escolha as pastas de artefatos e HADOUKEN existentes.');return
        self.pending=True;self.cancel.clear();self.submit_button.state(['disabled']);self.progress.grid();self.progress.start(12);self.status.configure(text='Conferindo composição e preparando fontes/preset — aguarde…')
        def cancelled():return self.cancel.is_set() or self.db is not self.ui.app.db_manager
        def work():
            files=sdk.create_super_atr_research_robot(**values,robots=self.panel.robots,config=self.ui.app.config,cancel_check=cancelled)
            if not cancelled():robots.import_library(self.db,values['folder'])
            return files
        def done(files,error):
            if not self.alive():return
            self.pending=False;self.progress.stop();self.progress.grid_remove();self.submit_button.state(['!disabled'])
            if error:self.status.configure(text=f'Pacote não concluído: {error}')
            else:
                self.created_files=files;self.status.configure(text=f'Pacote criado: {files[0]}. Entradas desligadas. Use os controles oficiais de compilar/verificar; aceite Tester/DEMO permanece pendente.');self.panel.reload()
            if self.close_after:self.window.destroy()
        self.ui.run_bg(work,done)
