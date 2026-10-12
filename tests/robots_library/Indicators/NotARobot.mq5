#property indicator_chart_window
input int InpPeriod = 14; // Período
int OnInit() { return(0); }
int OnCalculate(const int rates_total, const int prev_calculated, const int begin, const double &price[]) { return(rates_total); }
