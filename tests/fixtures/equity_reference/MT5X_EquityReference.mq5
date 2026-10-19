// T20.3: oráculo de saldo/equity do Strategy Tester. Não é um robô operacional.
#property strict
#property version "1.00"
#include <Trade/Trade.mqh>

input string InpOutput="mt5x_equity_reference.csv";
input long InpMagicNumber=203003;
input double InpVolume=0.01;

CTrade trade;
int output=INVALID_HANDLE, bars_seen=0;
datetime bar_time=0, entry_bar=0, exit_bar=0, entry_time=0, exit_time=0;
double op=0, hi=0, lo=0, last_bid=0, last_ask=0, balance=0, equity=0, qa_position_volume=0;
double entry_price=0, exit_price=0, initial_stop=0;

void Snapshot()
{
   balance=AccountInfoDouble(ACCOUNT_BALANCE);
   equity=AccountInfoDouble(ACCOUNT_EQUITY);
   qa_position_volume=PositionSelect(_Symbol) ? PositionGetDouble(POSITION_VOLUME) : 0;
}

void Flush()
{
   if(output==INVALID_HANDLE || bar_time==0) return;
   FileWrite(output,(long)bar_time,DoubleToString(op,10),DoubleToString(hi,10),DoubleToString(lo,10),
      DoubleToString(last_bid,10),DoubleToString((last_ask-last_bid)/_Point,4),
      DoubleToString(balance,8),DoubleToString(equity,8),DoubleToString(qa_position_volume,8),
      (long)entry_bar,(long)exit_bar,(long)entry_time,(long)exit_time,
      DoubleToString(entry_price,10),DoubleToString(exit_price,10),DoubleToString(initial_stop,10),
      DoubleToString(SymbolInfoDouble(_Symbol,SYMBOL_TRADE_TICK_SIZE),10),
      DoubleToString(SymbolInfoDouble(_Symbol,SYMBOL_TRADE_TICK_VALUE),10),DoubleToString(_Point,10),
      AccountInfoString(ACCOUNT_CURRENCY));
   FileFlush(output);
}

int OnInit()
{
   // Barreira independente de parâmetros, configuração e autorização da conta.
   if(!MQLInfoInteger(MQL_TESTER)) return INIT_FAILED;
   if(_Period!=PERIOD_H1 || PositionsTotal()!=0 || InpVolume<=0) return INIT_PARAMETERS_INCORRECT;
   output=FileOpen(InpOutput,FILE_WRITE|FILE_CSV|FILE_ANSI|FILE_COMMON,';');
   if(output==INVALID_HANDLE) return INIT_FAILED;
   FileWrite(output,"bar_time","open","high","low","close","spread_close","balance","equity","volume",
      "entry_bar","exit_bar","entry_time","exit_time","entry_price","exit_price","initial_stop",
      "tick_size","tick_value","point","currency");
   trade.SetExpertMagicNumber(InpMagicNumber);
   trade.SetTypeFillingBySymbol(_Symbol);
   return INIT_SUCCEEDED;
}

void OnTick()
{
   if(!MQLInfoInteger(MQL_TESTER)) return;
   MqlTick tick;
   if(!SymbolInfoTick(_Symbol,tick)) return;
   datetime current=iTime(_Symbol,PERIOD_H1,0);
   if(current!=bar_time)
   {
      Flush();
      bar_time=current;
      bars_seen++;
      op=hi=lo=tick.bid;
      if(bars_seen==2)
      {
         initial_stop=NormalizeDouble(tick.ask-0.1,_Digits);
         if(!trade.Buy(InpVolume,_Symbol,0,initial_stop,0,"T20.3 Tester only") || !PositionSelect(_Symbol))
         {
            Print("T20.3: entrada rejeitada, retcode=",trade.ResultRetcode());
            TesterStop(); return;
         }
         entry_bar=current;
         entry_time=(datetime)PositionGetInteger(POSITION_TIME);
         entry_price=PositionGetDouble(POSITION_PRICE_OPEN);
      }
      if(bars_seen==6)
      {
         if(!trade.PositionClose(_Symbol) || PositionSelect(_Symbol))
         {
            Print("T20.3: saída rejeitada, retcode=",trade.ResultRetcode());
            TesterStop(); return;
         }
         exit_bar=current;
         exit_time=TimeCurrent();
         exit_price=trade.ResultPrice();
      }
   }
   hi=MathMax(hi,tick.bid); lo=MathMin(lo,tick.bid);
   last_bid=tick.bid; last_ask=tick.ask;
   Snapshot();
}

double OnTester()
{
   Flush();
   return entry_price>0 && exit_price>0 ? 1.0 : 0.0;
}

void OnDeinit(const int reason)
{
   if(output!=INVALID_HANDLE) FileClose(output);
}
