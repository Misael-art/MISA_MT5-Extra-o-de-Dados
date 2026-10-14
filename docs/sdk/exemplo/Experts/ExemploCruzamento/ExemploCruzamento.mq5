//+------------------------------------------------------------------+
//| ExemploCruzamento.mq5                                                     |
//| Criado com o mt5x (mt5x robots new). Toda ordem passa pela       |
//| biblioteca de segurança mt5x-safety 1.2.0.                  |
//|                                                                  |
//| O que você escreve: a função Signal(), mais abaixo.              |
//| O que já vem pronto: risco, stop obrigatório, limites de perda,  |
//| parada de emergência e exportação das operações.                 |
//+------------------------------------------------------------------+
#property version   "1.00"
#property description "ExemploCruzamento: robô criado com o mt5x."
#property description "Toda ordem passa pela biblioteca de segurança mt5x-safety."

#include <MT5X/Safety/HDK_SafetyTypes.mqh>
#include <MT5X/Safety/HDK_Logger.mqh>
#include <MT5X/Safety/HDK_RiskGuard.mqh>
#include <MT5X/Safety/HDK_SafeExecutor.mqh>

input group "1. Identificação"
input long   InpMagicNumber     = 175355;  // Magic number (exclusivo deste robô)

input group "2. Risco"
input double InpRiskPct         = 0.5;   // Risco por trade (%)
input double InpDailyLossPct    = 2.0;   // Perda máxima diária (%)
input double InpWeeklyLossPct   = 4.0;   // Perda máxima semanal (%)
input double InpMaxDrawdownPct  = 10.0;  // Drawdown máximo desde o pico (%)

input group "3. Stop e alvo"
input int    InpAtrPeriod       = 14;    // Período do ATR
input double InpStopAtrMult     = 1.5;   // Stop (x ATR)
input double InpTakeProfitR     = 2;   // Alvo em múltiplos do risco (0 = sem alvo)

input group "4. Execução"
input int    InpDeviationPoints = 20;    // Desvio máximo de preço (pontos)
input bool   InpExportTrades    = false; // Exportar operações no fim do teste

//--- MT5X:PARAMETROS:INICIO (parâmetros da sua estratégia)
input group "5. Estratégia"
input int    InpRapidaPeriod = 10;   // Período de rapida (média simples)
input int    InpLentaPeriod = 40;   // Período de lenta (média simples)
//--- MT5X:PARAMETROS:FIM

CHDKLogger       g_log;
CHDKRiskGuard    g_risk;
CHDKSafeExecutor g_exec;
int              g_atr      = INVALID_HANDLE;
datetime         g_last_bar = 0;

//--- MT5X:GLOBAIS:INICIO (handles de indicadores da sua estratégia)
int g_rapida = INVALID_HANDLE;
int g_lenta = INVALID_HANDLE;
//--- MT5X:GLOBAIS:FIM

//+------------------------------------------------------------------+
//| Preparação dos indicadores da estratégia. Devolva false se algum |
//| não puder ser criado.                                            |
//+------------------------------------------------------------------+
bool SignalInit()
{
//--- MT5X:INIT:INICIO
   g_rapida = iMA(_Symbol, PERIOD_CURRENT, InpRapidaPeriod, 0, MODE_SMA, PRICE_CLOSE);
   g_lenta = iMA(_Symbol, PERIOD_CURRENT, InpLentaPeriod, 0, MODE_SMA, PRICE_CLOSE);
   return (g_rapida != INVALID_HANDLE && g_lenta != INVALID_HANDLE);
//--- MT5X:INIT:FIM
}

//+------------------------------------------------------------------+
//| SINAL: +1 para comprar, -1 para vender, 0 para não fazer nada.   |
//| É chamada uma vez por barra, com a barra anterior já fechada     |
//| (use o índice 1, nunca o 0). Sem sinal, o robô não opera.        |
//+------------------------------------------------------------------+
int Signal()
{
//--- MT5X:SINAL:INICIO
   double v_rapida[2];
   if(CopyBuffer(g_rapida, 0, 1, 2, v_rapida) != 2) return 0;
   double v_lenta[2];
   if(CopyBuffer(g_lenta, 0, 1, 2, v_lenta) != 2) return 0;
   bool buy = (v_rapida[1] > v_lenta[1] && v_rapida[0] <= v_lenta[0]);
   bool sell = (v_rapida[1] < v_lenta[1] && v_rapida[0] >= v_lenta[0]);
   if(buy && !sell) return 1;
   if(sell && !buy) return -1;
   return 0;
//--- MT5X:SINAL:FIM
}

//+------------------------------------------------------------------+
int OnInit()
{
   g_log.Init(InpMagicNumber, true, HDK_NOTIFY_CRITICAL, false, false, false, "", "", false);

   HDKRiskSettings cfg;
   ZeroMemory(cfg);
   cfg.lot_mode             = HDK_LOT_RISK_BALANCE;
   cfg.risk_pct             = InpRiskPct;
   cfg.risk_pct_max         = InpRiskPct;
   cfg.daily_loss_pct       = InpDailyLossPct;
   cfg.weekly_loss_pct      = InpWeeklyLossPct;
   cfg.max_dd_pct           = InpMaxDrawdownPct;
   cfg.max_open_risk_pct    = InpRiskPct;
   cfg.max_positions        = 1;
   cfg.max_correlated       = 1;
   cfg.block_averaging      = true;
   cfg.close_on_daily_limit = true;
   cfg.close_on_dd_limit    = true;
   cfg.margin_buffer_pct    = 20.0;

   if(!g_risk.Init(InpMagicNumber, _Symbol, cfg, "", GetPointer(g_log))) return(INIT_FAILED);
   g_exec.Init(InpMagicNumber, InpDeviationPoints, 3, cfg.margin_buffer_pct, GetPointer(g_log));

   g_atr = iATR(_Symbol, PERIOD_CURRENT, InpAtrPeriod);
   if(g_atr == INVALID_HANDLE) return(INIT_FAILED);
   if(!SignalInit()) return(INIT_FAILED);
   return(INIT_SUCCEEDED);
}

void OnDeinit(const int reason)
{
   if(g_atr != INVALID_HANDLE) IndicatorRelease(g_atr);
}

//--- Distância do stop em relação ao preço de entrada (0 = sem dados ou o mercado abriu além do stop).
//    O stop é um NÍVEL fixado no fechamento da barra do sinal: fechamento -/+ ATR x multiplicador. A distância
//    usada para o lote e para o alvo é a real, do preço de entrada até esse nível. É a mesma regra do
//    backtester Python do mt5x; assim os dois lados arriscam o mesmo e saem no mesmo preço.
double StopDistance(const ENUM_ORDER_TYPE type, const double price)
{
   double atr[1];
   if(CopyBuffer(g_atr, 0, 1, 1, atr) != 1) return 0;
   double reference = iClose(_Symbol, PERIOD_CURRENT, 1);
   if(reference <= 0 || atr[0] <= 0) return 0;
   double level = (type == ORDER_TYPE_BUY) ? reference - atr[0] * InpStopAtrMult : reference + atr[0] * InpStopAtrMult;
   double distance = (type == ORDER_TYPE_BUY) ? price - level : level - price;
   return (distance > 0) ? distance : 0;
}

//--- Este robô já tem posição neste ativo?
bool HasPosition()
{
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      if(PositionGetInteger(POSITION_MAGIC) == InpMagicNumber && PositionGetString(POSITION_SYMBOL) == _Symbol)
         return true;
   }
   return false;
}

//+------------------------------------------------------------------+
//| Ponto único de entrada: toda ordem nova passa por aqui.          |
//+------------------------------------------------------------------+
bool TryEnter(const ENUM_ORDER_TYPE type)
{
   string reason = g_risk.BlockReason();
   string side   = (type == ORDER_TYPE_BUY) ? "COMPRA" : "VENDA";
   if(reason == "" && g_risk.Paused()) reason = "pausado";
   if(reason == "") g_exec.TradingAllowed(_Symbol, reason);

   MqlTick tick;
   if(reason == "" && !SymbolInfoTick(_Symbol, tick)) reason = "sem cotação";

   double price   = (type == ORDER_TYPE_BUY) ? tick.ask : tick.bid;
   double sl_dist = (reason == "") ? StopDistance(type, price) : 0;
   if(reason == "" && sl_dist <= 0) reason = "stop indisponível (ATR sem dados ou o mercado abriu além do stop)";

   double lot = 0, risk_money = 0;
   if(reason == "")
   {
      double sl = (type == ORDER_TYPE_BUY) ? price - sl_dist : price + sl_dist;
      if(g_risk.CalcLot(_Symbol, type, price, sl, lot, risk_money, reason))
         g_risk.CheckEntry(_Symbol, type, risk_money, reason);
   }
   if(reason != "")
   {
      g_log.Event(HDK_LOG_WARNING, side + " recusada: " + reason, "deny:" + reason, 60);
      return false;
   }
   double tp_dist = (InpTakeProfitR > 0) ? sl_dist * InpTakeProfitR : 0;
   ulong  ticket  = 0;
   return g_exec.Open(_Symbol, type, lot, sl_dist, tp_dist, "ExemploCruzamento", ticket);
}

//+------------------------------------------------------------------+
void OnTick()
{
   ENUM_HDK_BLOCK_EVENT ev = g_risk.Update();
   if(ev != HDK_BLOCK_NONE)
   {
      bool close = (ev == HDK_BLOCK_KILL) || (ev == HDK_BLOCK_DD && g_risk.CloseOnDDLimit()) ||
                   ((ev == HDK_BLOCK_DAILY || ev == HDK_BLOCK_WEEKLY) && g_risk.CloseOnDailyLimit());
      g_log.Event(HDK_LOG_CRITICAL, "BLOQUEADO: " + g_risk.BlockReason());
      int failed = 0;
      if(close) g_exec.CloseAll(g_risk.BlockReason(), failed);
   }

   datetime bar = iTime(_Symbol, PERIOD_CURRENT, 0);
   if(bar == g_last_bar) return;
   g_last_bar = bar;
   if(HasPosition()) return;

   int signal = Signal();
   if(signal > 0)      TryEnter(ORDER_TYPE_BUY);
   else if(signal < 0) TryEnter(ORDER_TYPE_SELL);
}

//+------------------------------------------------------------------+
//| Fim do teste: exporta as operações fechadas (contrato do mt5x).  |
//+------------------------------------------------------------------+
double OnTester()
{
   if(!InpExportTrades || !HistorySelect(0, TimeCurrent() + 1)) return(0);
   string name = "HDK_trades_" + IntegerToString(InpMagicNumber) + ".csv";
   int h = FileOpen(name, FILE_WRITE | FILE_CSV | FILE_ANSI | FILE_COMMON, ';');
   if(h == INVALID_HANDLE) return(0);
   FileWrite(h, "time", "symbol", "type", "volume", "result");
   for(int i = 0; i < HistoryDealsTotal(); i++)
   {
      ulong deal = HistoryDealGetTicket(i);
      if(deal == 0 || HistoryDealGetInteger(deal, DEAL_MAGIC) != InpMagicNumber) continue;
      long entry = HistoryDealGetInteger(deal, DEAL_ENTRY);
      if(entry != DEAL_ENTRY_OUT && entry != DEAL_ENTRY_INOUT && entry != DEAL_ENTRY_OUT_BY) continue;
      double res = HistoryDealGetDouble(deal, DEAL_PROFIT) + HistoryDealGetDouble(deal, DEAL_SWAP) +
                   HistoryDealGetDouble(deal, DEAL_COMMISSION);
      FileWrite(h, TimeToString((datetime)HistoryDealGetInteger(deal, DEAL_TIME)), HistoryDealGetString(deal, DEAL_SYMBOL),
                (HistoryDealGetInteger(deal, DEAL_TYPE) == DEAL_TYPE_SELL ? "close_buy" : "close_sell"),
                DoubleToString(HistoryDealGetDouble(deal, DEAL_VOLUME), 2), DoubleToString(res, 2));
   }
   FileClose(h);
   return(0);
}
