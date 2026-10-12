//+------------------------------------------------------------------+
//| Robô de exemplo para os testes do catálogo (não é para operar)   |
//+------------------------------------------------------------------+
#property copyright "Testes"
#property version   "1.20"
#property description "Robô de exemplo com gestão de risco."
#property description "Segunda linha da descrição."

#include <Demo\Types.mqh>
#include <Trade\Trade.mqh>

/* input double InpIgnored = 9.0; // dentro de comentário de bloco */
// input double InpAlsoIgnored = 9.0;

input group "1. Risco"
input ENUM_DEMO_PROFILE InpProfile      = DEMO_PROFILE_MODERATE; // Perfil de risco
input double            InpRiskPct      = 1.0;   // Risco por trade (%) [Personalizado]
input double            InpDailyLossPct = 3.0;   // Perda máxima diária (%)
input double            InpMaxDrawdownPct = 15.0; // Drawdown máximo desde o pico (%)

input group "2. Stop"
input double            InpStopAtrMult  = 1.5;   // Stop (x ATR)
input bool              InpBlockAveraging = true; // Proibir preço médio (sem martingale/grid)
sinput long             InpMagicNumber  = 4242;  // Magic number
input string            InpStartTime    = "09:15"; // Início (HH:MM)

int OnInit() { return(INIT_SUCCEEDED); }
void OnTick() { }
