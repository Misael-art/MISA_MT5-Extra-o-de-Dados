#property version "0.10"
input double InpLots           = 0.10; // Lote inicial
input double InpLotMultiplier  = 2.0;  // Multiplicador do lote após perda
input int    InpStopLoss       = 0;    // Stop (pontos; 0 = sem stop)
#import "helper.dll"
int Helper(int x);
#import
void OnTick() { }
