"""Prova explícita Linux/C++ do corpo MQL real e header original fixado.

Execute: PYTHONPATH=. .venv/bin/python tests/super_atr_research_probe.py --framework RAIZ
Não é MT5/Tester nem simulação de fills; a plataforma de séries/conta é controlada.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from mt5_extracao.analytics import super_atr
from mt5_extracao.robots import scaffold

PLATFORM=r'''
#include <cassert>
#include <string>
#include <map>
#include <cstdio>
#include <cstdarg>
#include <ctime>
using datetime=long;using string=std::string;
enum ENUM_TIMEFRAMES{PERIOD_M1=1,PERIOD_M2=2,PERIOD_M3=3,PERIOD_M4=4,PERIOD_M5=5,PERIOD_M6=6,
 PERIOD_M10=10,PERIOD_M12=12,PERIOD_M15=15,PERIOD_M20=20,PERIOD_M30=30,PERIOD_H1=60,PERIOD_H2=120,
 PERIOD_H3=180,PERIOD_H4=240,PERIOD_H6=360,PERIOD_H8=480,PERIOD_H12=720};
enum{MQL_TESTER,ACCOUNT_TRADE_MODE,ACCOUNT_TRADE_MODE_DEMO=10,SERIES_SYNCHRONIZED=20,
 INIT_SUCCEEDED=0,INIT_FAILED=1,INIT_PARAMETERS_INCORRECT=2,TIME_DATE=1,TIME_MINUTES=2};
const string _Symbol="CONTROLLED_QA";ENUM_TIMEFRAMES _Period=PERIOD_M5;
datetime now=1704067200+400*300,decision=now,g_last_bar=0;
bool tester=false,synchronized=true;int account=ACCOUNT_TRADE_MODE_DEMO,initializations=0,ticks=0;
double g_guard_long=0,g_guard_short=0;
struct MqlRates{datetime time;double open,high,low,close;};
struct HDKBars{int n=400;};
struct HDKExitRules{double target_r=0,partial_r=0,partial_fraction=0;bool breakeven_after_partial=false;
 double trailing_atr=0;int max_bars=0;bool eod_exit=false;};
struct MqlDateTime{int year,mon,day,hour,min,sec,day_of_week,day_of_year;};
void TimeToStruct(datetime t,MqlDateTime &out){time_t v=t;std::tm g=*std::gmtime(&v);
 out={g.tm_year+1900,g.tm_mon+1,g.tm_mday,g.tm_hour,g.tm_min,g.tm_sec,g.tm_wday,g.tm_yday};}
string TimeToString(datetime t,int){return std::to_string(t);}
std::vector<string> decisions_log;
template<class T>T fmt_arg(T value){return value;}
inline const char *fmt_arg(const string &value){return value.c_str();}
template<class... A>void PrintFormat(const char *format,A... args){char buffer[512];
 std::snprintf(buffer,sizeof buffer,format,fmt_arg(args)...);decisions_log.push_back(buffer);}
struct HDKSignal{bool entry_long=false,entry_short=false,exit_long=false,exit_short=false;
 double stop_long=0,stop_short=0,target_long=0,target_short=0;};
std::map<int,std::vector<MqlRates>> history;
template<class T>void ArraySetAsSeries(T&,bool){}
template<class T>void ZeroMemory(T& x){x=T{};}
long MQLInfoInteger(int){return tester;}long AccountInfoInteger(int){return account;}
bool SeriesInfoInteger(string,int,int){return synchronized;}
int PeriodSeconds(int tf){return tf*60;}datetime TimeCurrent(){return now;}
datetime iTime(string,int,int){return decision;}
int Bars(string,int tf,datetime first,datetime last){int n=0;for(auto r:history[tf])n+=r.time>=first && r.time<=last;return n;}
int CopyRates(string,int tf,datetime first,datetime last,std::vector<MqlRates>& out){out.clear();for(auto r:history[tf])if(r.time>=first && r.time<=last)out.push_back(r);return int(out.size());}
void Print(string){}int StrategyInit(string){initializations++;return INIT_SUCCEEDED;}
void StrategyTick(){ticks++;}double StrategyTester(){return 0;}
'''
CASES=r'''
void make_history(){history.clear();for(int tf:{5,15})for(int i=0;i<500;i++){
 double c=100+8*std::sin(double(i*tf)/35);datetime t=InpSeedStart+i*tf*60;
 history[tf].push_back({t,c,c+1,c-1,c});}}
int main(){
 // Gate REAL/contest antes de StrategyInit e em cada tick; Tester explícito aceita.
 account=99;assert(OnInit()==INIT_FAILED && initializations==0);OnTick();assert(ticks==0);
 tester=true;assert(OnInit()==INIT_SUCCEEDED && initializations==1);tester=false;account=ACCOUNT_TRADE_MODE_DEMO;
 assert(OnInit()==INIT_SUCCEEDED);HDKBars bars;HDKSignal skipped;StrategySignal(bars,skipped);assert(!skipped.entry_long && !skipped.entry_short);
 InpBasePeriod=3;InpConfirmPeriod=3;make_history();
 // Confirmação incompleta não entra no cálculo; mudar TODO o futuro mantém saída exata.
 datetime cut=InpSeedStart+302*300;MT5XObservedPrefix prefix;int side,previous,run;double level,atr,close_last;
 assert(MT5XReadCurve(PERIOD_M15,3,0,1,cut,prefix,side,previous,level,atr,close_last,run));
 int expected_side=side;double expected_level=level;
 for(auto &r:history[15])if(r.time+900>cut){r.high=500;r.low=1;r.close=490;}
 assert(MT5XReadCurve(PERIOD_M15,3,0,1,cut,prefix,side,previous,level,atr,close_last,run));assert(side==expected_side && level==expected_level);
 // Prefixo observado revisado trava; não tolera diferença de preço.
 history[15][0].close+=.001;assert(!MT5XReadCurve(PERIOD_M15,3,0,1,cut,prefix,side,previous,level,atr,close_last,run));assert(g_revision_block);
 make_history();g_revision_block=false;MT5XObservedPrefix stale;
 history[15].erase(history[15].begin()+99);assert(!MT5XReadCurve(PERIOD_M15,3,0,1,cut,stale,side,previous,level,atr,close_last,run));
 make_history();history[15].erase(history[15].begin());MT5XObservedPrefix missing;
 assert(!MT5XReadCurve(PERIOD_M15,3,0,1,cut,missing,side,previous,level,atr,close_last,run));
 make_history();synchronized=false;assert(!MT5XReadCurve(PERIOD_M15,3,0,1,cut,missing,side,previous,level,atr,close_last,run));synchronized=true;
 // Corpo real de StrategySignal: eventos novos, confirmação fechada e consumo único.
 make_history();for(auto &r:history[15]){r.high=101;r.low=99;r.close=100;}
 g_base_prefix=MT5XObservedPrefix();g_confirm_prefix=MT5XObservedPrefix();g_processed_decision=0;
 InpResearchOrders=true;int entries=0,exits=0;
 for(int i=300;i<450;i++){
  now=decision=InpSeedStart+i*300;HDKSignal first;StrategySignal(bars,first);
  entries+=first.entry_long || first.entry_short;exits+=first.exit_long || first.exit_short;
  if(first.entry_long)assert(first.stop_long>0 && first.stop_short==0);
  if(first.entry_short)assert(first.stop_short>0 && first.stop_long==0);
  HDKSignal duplicate;StrategySignal(bars,duplicate);assert(!duplicate.entry_long && !duplicate.entry_short && !duplicate.exit_long && !duplicate.exit_short);
 }
 assert(entries>0 && exits>entries);
 // Confirmação controlada do lado vendido, sem alterar a regra do candidato.
 make_history();for(int i=0;i<int(history[15].size());i++){double c=i<5?100:90;auto &r=history[15][i];r.high=c+1;r.low=c-1;r.close=c;}
 g_base_prefix=MT5XObservedPrefix();g_confirm_prefix=MT5XObservedPrefix();g_processed_decision=0;int shorts=0;
 for(int i=300;i<450;i++){now=decision=InpSeedStart+i*300;HDKSignal signal;StrategySignal(bars,signal);shorts+=signal.entry_short;assert(!signal.entry_long);}
 assert(shorts>0);
 // Armamento desligado nunca gera entrada; saída oposta não depende da confirmação.
 make_history();g_base_prefix=MT5XObservedPrefix();g_confirm_prefix=MT5XObservedPrefix();g_processed_decision=0;InpResearchOrders=false;
 int protected_exits=0;for(int i=300;i<450;i++){now=decision=InpSeedStart+i*300;HDKSignal signal;StrategySignal(bars,signal);assert(!signal.entry_long && !signal.entry_short);protected_exits+=signal.exit_long || signal.exit_short;}
 assert(protected_exits>0);
 // SA-01 (variante 0) não liga alvo/fim do dia nas regras do host.
 HDKExitRules sa01;StrategyRules(sa01);assert(sa01.target_r==0 && !sa01.eod_exit);
 // ---- SA-REF-01 (variante 1): mesmas leituras causais, regras G19.15 congeladas ----
 InpVariant=1;HDKExitRules reference;StrategyRules(reference);assert(reference.target_r==2 && reference.eod_exit);
 assert(OnInit()==INIT_SUCCEEDED);
 InpRefMinBars=0;assert(OnInit()==INIT_PARAMETERS_INCORRECT);InpRefMinBars=100;
 InpRefStopATR=0;assert(OnInit()==INIT_PARAMETERS_INCORRECT);InpRefStopATR=2;
 // Base com viradas; confirmador inexistente não importa na variante 1.
 make_history();history[15].clear();
 g_base_prefix=MT5XObservedPrefix();g_processed_decision=0;g_revision_block=false;InpResearchOrders=true;decisions_log.clear();
 int ref_entries=0,ref_exits=0,ineligible=0;
 for(int i=110;i<450;i++){
  now=decision=InpSeedStart+i*300;HDKSignal signal;StrategySignal(bars,signal);
  ref_entries+=signal.entry_long || signal.entry_short;ref_exits+=signal.exit_long || signal.exit_short;
  if(signal.entry_long || signal.entry_short){
   MT5XObservedPrefix check;int s2,p2,r2;double l2,a2,c2;
   assert(MT5XReadCurve(PERIOD_M5,InpBasePeriod,InpBaseMean,InpBaseDeviation,decision,check,s2,p2,l2,a2,c2,r2));
   assert(s2!=p2 && r2>=InpRefMinBars);                                   // só virada estrita e elegível
   double expected=s2>0?c2-2*a2:c2+2*a2;
   if(signal.entry_long)assert(s2>0 && signal.stop_long==expected && signal.stop_short==0);
   if(signal.entry_short)assert(s2<0 && signal.stop_short==expected && signal.stop_long==0);
  }
  HDKSignal duplicate;StrategySignal(bars,duplicate);assert(!duplicate.entry_long && !duplicate.entry_short);
 }
 for(auto &line:decisions_log)ineligible+=line.find("eligible=0")!=string::npos;
 assert(ref_entries>0 && ref_exits==0);                                     // sem saída por virada
 assert(int(decisions_log.size())>=ref_entries && decisions_log[0].find("SAREF_DECISION")==0);
 // Idade: barras do mesmo dia; antes de InpRefMinBars a virada é registrada e recusada.
 InpRefMinBars=100000;g_base_prefix=MT5XObservedPrefix();g_processed_decision=0;decisions_log.clear();
 int blocked=0;for(int i=110;i<450;i++){now=decision=InpSeedStart+i*300;HDKSignal signal;StrategySignal(bars,signal);blocked+=signal.entry_long || signal.entry_short;}
 assert(blocked==0 && !decisions_log.empty());InpRefMinBars=100;
 // Desarmada: registra decisões, nunca entra.
 InpResearchOrders=false;g_base_prefix=MT5XObservedPrefix();g_processed_decision=0;decisions_log.clear();
 int disarmed=0;for(int i=110;i<450;i++){now=decision=InpSeedStart+i*300;HDKSignal signal;StrategySignal(bars,signal);disarmed+=signal.entry_long || signal.entry_short;}
 assert(disarmed==0 && !decisions_log.empty() && decisions_log[0].find("orders=0")!=string::npos);
 // Mais de 20.000 barras desde a semente: recusa com motivo explícito, nunca silêncio.
 {make_history();auto &m5=history[5];datetime last=m5.back().time;
  for(int i=1;i<=20000;i++){double c=100+std::sin(i/40.0);m5.push_back({last+i*300,c,c+1,c-1,c});}
  InpResearchOrders=true;g_base_prefix=MT5XObservedPrefix();g_processed_decision=0;decisions_log.clear();
  now=decision=m5.back().time+300;HDKSignal signal;StrategySignal(bars,signal);
  assert(!signal.entry_long && !signal.entry_short && !decisions_log.empty());
  assert(decisions_log.back().find("SAREF_BLOCKED")==0 && decisions_log.back().find("bars_since_seed_above_20000")!=string::npos);
  make_history();}
 // Gate REAL também na variante 1.
 tester=false;account=99;initializations=0;assert(OnInit()==INIT_FAILED && initializations==0);account=ACCOUNT_TRADE_MODE_DEMO;
 std::cout<<"{\"real_mql_body_and_header\":true,\"future_isolation\":true,\"stale_missing_revision_sync_refused\":true,\"deduplicated\":true,\"entries\":"<<entries<<",\"exits\":"<<exits<<",\"sa_ref_01_entries\":"<<ref_entries<<",\"sa_ref_01_opposite_flip_exits\":"<<ref_exits<<",\"sa_ref_01_ineligible_logged\":"<<ineligible<<",\"mt5_qualified\":false}\n";
}
'''


def vectorize(source):
    source=re.sub(r'const (double|MqlRates) &(\w+)\[\]',r'const std::vector<\1> &\2',source)
    return re.sub(r'\b(double|int|datetime|MqlRates) (\w+\[\](?:,\w+\[\])*)',lambda m:'std::vector<'+m[1]+'> '+m[2].replace('[]',''),source)


def main(framework):
    artifact,_=super_atr.source(framework);root=Path(framework)
    spec=importlib.util.spec_from_file_location('pinned_adapter',root/super_atr.ADAPTER);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    original=(root/super_atr.HEADER).read_text(encoding='utf-8-sig')
    consumer=scaffold.render('SuperAtrResearch.mq5.tmpl',NAME='NativeProbe',MAGIC=654321,SEED='2024.01.01 00:00',BASE='M5',CONFIRM='M15',SECOND_ENABLED='false',SECOND='H1')
    consumer=re.sub(r'(?m)^#(?:property|include).*\n','',consumer)
    consumer=re.sub(r'(?m)^input group.*\n','',consumer).replace('input ','')
    consumer=consumer.replace("D'2024.01.01 00:00'",'1704067200')
    code=module.SHIM+vectorize(original)+PLATFORM+vectorize(consumer)+CASES
    compiler=shutil.which('g++') or shutil.which('clang++')
    if not compiler:raise RuntimeError('Use o compilador C++ existente; nenhuma instalação automática.')
    with tempfile.TemporaryDirectory(prefix='mt5x-super-atr-probe-') as tmp:
        source,binary=Path(tmp)/'probe.cpp',Path(tmp)/'probe';source.write_text(code)
        result=subprocess.run([compiler,'-std=c++17','-O1',str(source),'-o',str(binary)],capture_output=True,text=True,timeout=45)
        if result.returncode:raise RuntimeError(result.stderr)
        result=subprocess.run([str(binary)],capture_output=True,text=True,timeout=20)
        if result.returncode:raise RuntimeError(result.stderr)
        receipt=json.loads(result.stdout);receipt['framework_commit']=artifact['framework_commit'];print(json.dumps(receipt))


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--framework',required=True);main(parser.parse_args().framework)
