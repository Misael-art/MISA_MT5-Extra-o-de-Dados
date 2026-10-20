// QA de inputs públicos. Sem negócios; não constitui aceite operacional.
#property strict
#property version "1.00"
input int InpPeriod=20;           // barras
input double Risk=0.5;         // percentual apenas conferido; QA não envia ordens
input bool Enabled=false;     // habilitação declarada
input string PresetId="default"; // identidade pública portátil
int OnInit() {
  if(!MQLInfoInteger(MQL_TESTER) || AccountInfoInteger(ACCOUNT_TRADE_MODE)!=ACCOUNT_TRADE_MODE_DEMO)
    return INIT_FAILED;
  return INIT_SUCCEEDED;
}
void OnTick() {}
