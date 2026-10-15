//+------------------------------------------------------------------+
//| MT5X_CalendarExport.mq5                                          |
//| Laboratório do mt5x (Fase 16, T16.3a/T16.3c): exporta o calendário|
//| econômico do próprio MetaTrader 5 para Common\Files.             |
//|                                                                  |
//| Só lê: não abre gráfico, não envia ordem, não usa DLL.           |
//| Saída: Common\Files\MT5X_calendar_<servidor>.csv (separador ;).  |
//| Pedido opcional: Common\Files\MT5X_calendar_request.txt com      |
//| linhas chave=valor (days_back, days_forward, countries,          |
//| min_importance), gravado pelo `mt5x trades calendar export`.     |
//+------------------------------------------------------------------+
#property copyright "mt5x"
#property version   "1.00"
#property strict
#property script_show_inputs false

input int    InpDaysBack      = 1095;   // dias de histórico
input int    InpDaysForward   = 30;     // dias à frente (agenda)
input string InpCountries     = "";     // códigos de país separados por vírgula (vazio = todos), ex.: BR,US
input int    InpMinImportance = 0;      // 0 = todos, 1 = baixa+, 2 = média+, 3 = só alta

const string REQUEST_FILE = "MT5X_calendar_request.txt";

int    g_days_back, g_days_forward, g_min_importance;
string g_countries;

//--- lê o pedido do mt5x (se existir), sobrepondo os parâmetros
void ReadRequest()
  {
   g_days_back = InpDaysBack;
   g_days_forward = InpDaysForward;
   g_countries = InpCountries;
   g_min_importance = InpMinImportance;
   if(!FileIsExist(REQUEST_FILE, FILE_COMMON))
      return;
   int h = FileOpen(REQUEST_FILE, FILE_READ | FILE_TXT | FILE_COMMON | FILE_ANSI);
   if(h == INVALID_HANDLE)
      return;
   while(!FileIsEnding(h))
     {
      string line = FileReadString(h);
      StringTrimLeft(line);
      StringTrimRight(line);
      int eq = StringFind(line, "=");
      if(eq <= 0)
         continue;
      string key = StringSubstr(line, 0, eq), value = StringSubstr(line, eq + 1);
      if(key == "days_back")
         g_days_back = (int)StringToInteger(value);
      else if(key == "days_forward")
         g_days_forward = (int)StringToInteger(value);
      else if(key == "countries")
         g_countries = value;
      else if(key == "min_importance")
         g_min_importance = (int)StringToInteger(value);
     }
   FileClose(h);
  }

//--- país do evento está no filtro?
bool CountryWanted(const string code)
  {
   if(g_countries == "")
      return true;
   string parts[];
   int n = StringSplit(g_countries, ',', parts);
   for(int i = 0; i < n; i++)
     {
      string p = parts[i];
      StringTrimLeft(p);
      StringTrimRight(p);
      if(StringCompare(p, code, false) == 0)
         return true;
     }
   return false;
  }

string Num(const bool has, const double value)
  {
   return has ? DoubleToString(value, 6) : "";
  }

string IsoUtc(const datetime value)
  {
   string result = TimeToString(value, TIME_DATE | TIME_SECONDS);
   StringReplace(result, ".", "-");
   StringReplace(result, " ", "T");
   return result + "Z";
  }

string Clean(string text)
  {
   StringReplace(text, ";", ",");
   StringReplace(text, "\r", " ");
   StringReplace(text, "\n", " ");
   return text;
  }

void OnStart()
  {
   if(MQLInfoInteger(MQL_TESTER))
     {
      Print("MT5X_CalendarExport: não exportado: o Strategy Tester usa horário simulado. "
            "Execute o script no terminal conectado.");
      return;
     }
   ReadRequest();
   string server = AccountInfoString(ACCOUNT_SERVER);
   StringReplace(server, " ", "_");
   string final_name = "MT5X_calendar_" + server + ".csv";
   string temp_name = final_name + ".parcial";

   datetime now = TimeTradeServer();
   datetime from = now - (datetime)g_days_back * 86400;
   datetime to = now + (datetime)g_days_forward * 86400;

   int h = FileOpen(temp_name, FILE_WRITE | FILE_TXT | FILE_COMMON | FILE_ANSI);
   if(h == INVALID_HANDLE)
     {
      Print("MT5X_CalendarExport: não foi possível criar ", temp_name, " (erro ", GetLastError(), ")");
      return;
     }
   // diferença servidor x UTC no momento da exportação, para converter os horários depois
   FileWriteString(h, "# server_utc_offset_sec=" + IntegerToString((long)(TimeTradeServer() - TimeGMT())) + "\r\n");
   FileWriteString(h, "time;event_id;name;country;currency;importance;actual;forecast;previous;observed_at_utc\r\n");

   int written = 0;
   // em blocos de 30 dias: históricos longos não cabem numa chamada só em todas as versões do terminal
   for(datetime start = from; start < to; start += 30 * 86400)
     {
      datetime stop = MathMin(start + 30 * 86400, to);
      MqlCalendarValue values[];
      ResetLastError();
      int n = CalendarValueHistory(values, start, stop);
      if(n < 0)
        {
         Print("MT5X_CalendarExport: CalendarValueHistory falhou (erro ", GetLastError(),
               "). O terminal precisa estar conectado à corretora.");
         continue;
        }
      // Marca quando este terminal recebeu os valores da API; não é um recibo declarado pela fonte.
      datetime observed_at_utc = TimeGMT();
      for(int i = 0; i < n; i++)
        {
         MqlCalendarEvent event;
         MqlCalendarCountry country;
         if(!CalendarEventById(values[i].event_id, event))
            continue;
         if(!CalendarCountryById(event.country_id, country))
            continue;
         if(!CountryWanted(country.code) || (int)event.importance < g_min_importance)
            continue;
         string line = TimeToString(values[i].time, TIME_DATE | TIME_MINUTES) + ";" +
                       IntegerToString((long)values[i].event_id) + ";" +
                       Clean(event.name) + ";" + country.code + ";" + country.currency + ";" +
                       IntegerToString((int)event.importance) + ";" +
                       Num(values[i].HasActualValue(), values[i].GetActualValue()) + ";" +
                       Num(values[i].HasForecastValue(), values[i].GetForecastValue()) + ";" +
                       Num(values[i].HasPreviousValue(), values[i].GetPreviousValue()) + ";" +
                       IsoUtc(observed_at_utc) + "\r\n";
         FileWriteString(h, line);
         written++;
        }
     }
   FileClose(h);
   // troca atômica: quem lê nunca vê um arquivo pela metade
   if(!FileMove(temp_name, FILE_COMMON, final_name, FILE_COMMON | FILE_REWRITE))
      Print("MT5X_CalendarExport: não foi possível renomear para ", final_name, " (erro ", GetLastError(), ")");
   else
      Print("MT5X_CalendarExport: ", written, " evento(s) gravado(s) em Common\\Files\\", final_name);
  }
//+------------------------------------------------------------------+
