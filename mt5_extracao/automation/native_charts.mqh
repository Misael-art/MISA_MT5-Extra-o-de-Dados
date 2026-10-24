// Apresentação por protocolo local. Não contém envio/alteração/fechamento de ordens.
// Incluído SOMENTE quando a sessão pede explicitamente chart_control.
// MT5 pode devolver aliases de handles do próprio EA. Não invalidar esses IDs
// com IndicatorRelease: até64 criações, liberadas ao encerrar a instância privada.
long MT5X_AgentCharts[];
int MT5X_AgentHandles[],MT5X_AgentWindows[],MT5X_AgentBuffers[],MT5X_AgentPeriods[],MT5X_AgentMeans[];
double MT5X_AgentDeviations[];
long MT5X_AgentIndicatorCharts[];
string MT5X_AgentNames[];
string MT5X_AgentCatalogIds[];
string MT5X_AgentLast="",MT5X_AgentReply="",MT5X_AgentPending="",MT5X_AgentImage="";
int MT5X_AgentPendingIndicator=-1,MT5X_AgentCreations=0;
string MT5X_AgentPendingSymbol="";
ENUM_TIMEFRAMES MT5X_AgentPendingTimeframe=PERIOD_CURRENT;

string MT5X_AgentEscape(string value)
{
   StringReplace(value,"\\","\\\\");StringReplace(value,"\"","\\\"");
   StringReplace(value,"\r","\\r");StringReplace(value,"\n","\\n");StringReplace(value,"\t","\\t");
   return value;
}
string MT5X_AgentQuote(const string value){return "\""+MT5X_AgentEscape(value)+"\"";}
bool MT5X_AgentId(const string value)
{
   if(StringLen(value)!=32)return false;
   for(int i=0;i<32;i++)if(StringFind("0123456789abcdef",StringSubstr(value,i,1))<0)return false;
   return true;
}
bool MT5X_AgentOwn(const long chart)
{
   if(chart<=0)return false;
   for(int i=0;i<ArraySize(MT5X_AgentCharts);i++)if(MT5X_AgentCharts[i]==chart && ChartSymbol(chart)!="")return true;
   return false;
}
void MT5X_AgentRespond(const string id,const bool ok,const string data,const string error="",const bool preserve=false)
{
   string body="{\"id\":"+MT5X_AgentQuote(id)+",\"ok\":"+(ok?"true":"false")+
      ",\"data\":"+data+",\"error\":"+MT5X_AgentQuote(error)+"}";
   string base="mt5x_control_@SESSION@";
   int f=FileOpen(base+".response.tmp",FILE_WRITE|FILE_TXT|FILE_ANSI|FILE_COMMON,0,CP_UTF8);
   if(f==INVALID_HANDLE)return;
   FileWriteString(f,body);FileFlush(f);FileClose(f);
   if(!FileMove(base+".response.tmp",FILE_COMMON,base+".response.json",FILE_COMMON|FILE_REWRITE))return;
   MT5X_AgentLast=id;MT5X_AgentReply=body;
   if(!preserve)FileDelete(base+"."+id+".working",FILE_COMMON);
}
string MT5X_AgentParameters(const int handle)
{
   ENUM_INDICATOR type;MqlParam params[];int count=IndicatorParameters(handle,type,params);
   if(count<0)return "null";
   string data="[";
   for(int i=0;i<count;i++)
   {
      if(i>0)data+=",";
      if(params[i].type==TYPE_STRING)data+="{\"type\":\"string\",\"value\":"+MT5X_AgentQuote(params[i].string_value)+"}";
      else if(params[i].type==TYPE_DOUBLE || params[i].type==TYPE_FLOAT)
         data+="{\"type\":\"number\",\"value\":"+(MathIsValidNumber(params[i].double_value)?DoubleToString(params[i].double_value,16):"null")+"}";
      else data+="{\"type\":\"integer\",\"value\":"+IntegerToString(params[i].integer_value)+"}";
   }
   return data+"]";
}
int MT5X_AgentSuperInputs(const int at)
{
   ENUM_INDICATOR type;MqlParam params[];int count=IndicatorParameters(MT5X_AgentHandles[at],type,params);
   if(count<0)return -1;
   int numbers=0,integers=0;
   for(int i=0;i<count;i++)
   {
      if(params[i].type==TYPE_STRING)continue; // nome do asset e headings, não são parâmetros matemáticos
      if(params[i].type==TYPE_DOUBLE || params[i].type==TYPE_FLOAT)
      {
         if(numbers>=10 || params[i].double_value!=(numbers+1)*MT5X_AgentDeviations[at])return false;
         numbers++;
      }
      else
      {
         long expected=integers==20?1:(integers%2==0?MT5X_AgentPeriods[at]:MT5X_AgentMeans[at]);
         if(integers>20 || params[i].integer_value!=expected)return false;
         integers++;
      }
   }
   return numbers==10 && integers==21;
}
void MT5X_AgentProgress(const string id,const string data)
{
   string base="mt5x_control_@SESSION@";
   int f=FileOpen(base+".progress.tmp",FILE_WRITE|FILE_TXT|FILE_ANSI|FILE_COMMON,0,CP_UTF8);
   if(f==INVALID_HANDLE)return;
   FileWriteString(f,"{\"id\":"+MT5X_AgentQuote(id)+",\"data\":"+data+"}");FileFlush(f);FileClose(f);
   FileMove(base+".progress.tmp",FILE_COMMON,base+".progress.json",FILE_COMMON|FILE_REWRITE);
}
string MT5X_AgentState(const long chart)
{
   string list="[";bool first=true;
   for(int i=0;i<ArraySize(MT5X_AgentHandles);i++)
   {
      if(MT5X_AgentHandles[i]==INVALID_HANDLE || MT5X_AgentIndicatorCharts[i]!=chart)continue;
      if(!first)list+=",";first=false;
      list+="{\"indicator\":"+MT5X_AgentQuote(IntegerToString(i+1))+",\"name\":"+
         MT5X_AgentQuote(MT5X_AgentNames[i])+",\"window\":"+IntegerToString(MT5X_AgentWindows[i])+
         ",\"catalog_id\":"+MT5X_AgentQuote(MT5X_AgentCatalogIds[i])+
         ",\"calculated\":"+IntegerToString(BarsCalculated(MT5X_AgentHandles[i]))+"}";
   }
   return "{\"chart\":"+MT5X_AgentQuote(IntegerToString(chart))+",\"symbol\":"+MT5X_AgentQuote(ChartSymbol(chart))+
      ",\"timeframe\":"+MT5X_AgentQuote(EnumToString(ChartPeriod(chart)))+
      ",\"width\":"+IntegerToString(ChartGetInteger(chart,CHART_WIDTH_IN_PIXELS))+
      ",\"height\":"+IntegerToString(ChartGetInteger(chart,CHART_HEIGHT_IN_PIXELS,0))+
      ",\"bars\":"+IntegerToString(Bars(ChartSymbol(chart),ChartPeriod(chart)))+
      ",\"indicators\":"+list+"]}";
}
bool MT5X_AgentStyle(const long chart,const string style)
{
   if(style!="light" && style!="dark" && style!="light_gray")return false;
#ifdef MT5X_PANEL_PRESENTATION_API
   if(chart==ChartID())return MT5X_PanelPresentation("theme",style=="light_gray"?"15":style=="dark"?"0":"7");
#endif
   // Um Expert com renderizador próprio pode sobrescrever propriedades. Sem API,
   // não afirmar aplicação de estilo no gráfico principal desse Expert.
   if(chart==ChartID())return false;
   color background=style=="dark"?C'17,24,39':style=="light_gray"?C'235,235,235':clrWhiteSmoke;
   color foreground=style=="dark"?clrGainsboro:clrDarkSlateGray;
   bool ok=ChartSetInteger(chart,CHART_MODE,CHART_CANDLES) && ChartSetInteger(chart,CHART_SHOW_GRID,false);
   ok=ChartSetInteger(chart,CHART_COLOR_BACKGROUND,background) && ok;
   ok=ChartSetInteger(chart,CHART_COLOR_FOREGROUND,foreground) && ok;
   ChartSetInteger(chart,CHART_COLOR_CANDLE_BULL,clrSeaGreen);ChartSetInteger(chart,CHART_COLOR_CANDLE_BEAR,clrIndianRed);
   ChartSetInteger(chart,CHART_AUTOSCROLL,true);ChartRedraw(chart);return ok;
}
// @STUDY_HELPERS@

bool MT5X_AgentLayoutSettling(const string symbol,const ENUM_TIMEFRAMES tf)
{
#ifdef HDK_LAYOUT_SEED_FILE
   // O host ainda pode fechar o auxiliar inicial com este mesmo símbolo/TF.
   // Nunca alterar o marcador nem a rotina de layout do host de outra frente.
   return !MQLInfoInteger(MQL_TESTER) && symbol==_Symbol && tf==_Period &&
      FileIsExist(HDK_LAYOUT_SEED_FILE,FILE_COMMON);
#else
   return false;
#endif
}

void MT5X_AgentOpen(const string id,const string symbol,const ENUM_TIMEFRAMES tf)
{
   int slot=-1,active=0;
   for(int i=0;i<ArraySize(MT5X_AgentCharts);i++)
      if(MT5X_AgentOwn(MT5X_AgentCharts[i]))active++;else if(slot<0)slot=i;
   if(StringLen(symbol)>64 || symbol=="" || PeriodSeconds(tf)<=0 || active>=16 || !SymbolSelect(symbol,true))
   {MT5X_AgentRespond(id,false,"{}","ativo/TF ou limite de16 gráficos inválido");return;}
   long created=ChartOpen(symbol,tf);
   if(created<=0){MT5X_AgentRespond(id,false,"{}","ChartOpen recusado");return;}
   if(slot<0){slot=ArraySize(MT5X_AgentCharts);ArrayResize(MT5X_AgentCharts,slot+1);}MT5X_AgentCharts[slot]=created;
   MT5X_AgentRespond(id,true,MT5X_AgentState(created));
}

void MT5X_AgentPoll()
{
   if(ArraySize(MT5X_AgentCharts)==0)
   {
      ArrayResize(MT5X_AgentCharts,1);MT5X_AgentCharts[0]=ChartID();
      string base="mt5x_control_@SESSION@";
      int ready=FileOpen(base+".ready.tmp",FILE_WRITE|FILE_TXT|FILE_ANSI|FILE_COMMON,0,CP_UTF8);
      if(ready!=INVALID_HANDLE)
      {
         FileWriteString(ready,"{\"session\":\"@SESSION@\",\"chart\":"+MT5X_AgentQuote(IntegerToString(ChartID()))+",\"ready\":true}");
         FileFlush(ready);FileClose(ready);FileMove(base+".ready.tmp",FILE_COMMON,base+".ready.json",FILE_COMMON|FILE_REWRITE);
      }
   }
   if(MT5X_AgentPending!="")
   {
      if(MT5X_AgentPendingSymbol!="")
      {
         if(MT5X_AgentLayoutSettling(MT5X_AgentPendingSymbol,MT5X_AgentPendingTimeframe))
         {MT5X_AgentProgress(MT5X_AgentPending,"{\"stage\":\"initial_layout\",\"chart_created\":false}");return;}
         string id=MT5X_AgentPending,symbol=MT5X_AgentPendingSymbol;
         ENUM_TIMEFRAMES tf=MT5X_AgentPendingTimeframe;
         MT5X_AgentPending="";MT5X_AgentPendingSymbol="";
         MT5X_AgentOpen(id,symbol,tf);return;
      }
      if(MT5X_AgentPendingIndicator>=0)
      {
         int at=MT5X_AgentPendingIndicator,handle=MT5X_AgentHandles[at];double trigger[1];
         long chart=MT5X_AgentIndicatorCharts[at];ENUM_TIMEFRAMES tf=ChartPeriod(chart);MqlRates history[];
         // ChartOpen não comprova histórico pronto. Solicitar a série real sem fabricar candles.
         int history_count=CopyRates(ChartSymbol(chart),tf,0,512,history);
         int copied=CopyBuffer(handle,0,1,1,trigger),calculated=BarsCalculated(handle);
         MT5X_AgentProgress(MT5X_AgentPending,"{\"stage\":\"indicator_calculation\",\"calculated\":"+IntegerToString(calculated)+
            ",\"copied\":"+IntegerToString(copied)+",\"history\":"+IntegerToString(history_count)+
            ",\"synchronized\":"+(SeriesInfoInteger(ChartSymbol(chart),tf,SERIES_SYNCHRONIZED)?"true":"false")+"}");
         if(calculated<=0)return;
         int window=MT5X_AgentWindows[at],matches=0;string actual="";
         for(int i=0;i<ChartIndicatorsTotal(chart,window);i++)
         {
            string name=ChartIndicatorName(chart,window,i);int other=ChartIndicatorGet(chart,window,name);
            if(other==handle){matches++;actual=name;}
         }
         MT5X_AgentProgress(MT5X_AgentPending,"{\"stage\":\"indicator_identity\",\"calculated\":"+IntegerToString(calculated)+
            ",\"matches\":"+IntegerToString(matches)+",\"name\":"+MT5X_AgentQuote(actual)+"}");
         if(matches!=1)return;
         bool super_atr=MT5X_AgentCatalogIds[at]=="" && MT5X_AgentBuffers[at]==10;
         int binding=super_atr?MT5X_AgentSuperInputs(at):-1;
         if(super_atr && (binding==0 || (binding<0 && !MQLInfoInteger(MQL_TESTER))))
         {
            MT5X_AgentRespond(MT5X_AgentPending,false,"{\"parameters\":"+MT5X_AgentParameters(handle)+"}","inputs efetivos do SuperATR divergem; apresentação não qualificada");
            MT5X_AgentPending="";MT5X_AgentPendingIndicator=-1;return;
         }
         MT5X_AgentNames[at]=actual;bool available=true;double value[1];string values="[";
         for(int buffer=0;buffer<MT5X_AgentBuffers[at];buffer++)
         {
            int count=CopyBuffer(handle,buffer,1,1,value);
            bool valid=count==1 && MathIsValidNumber(value[0]) && value[0]!=EMPTY_VALUE;
            if(!valid)available=false;
            if(buffer>0)values+=",";
            values+="{\"index\":"+IntegerToString(buffer)+",\"copied\":"+IntegerToString(count)+
               ",\"value\":"+(valid?DoubleToString(value[0],16):"null")+"}";
         }
         values+="]";
         ChartRedraw(chart);
         string parameters=MT5X_AgentParameters(handle);
         string status=binding==1?"verified":MQLInfoInteger(MQL_TESTER)?"unavailable_in_tester":
            MT5X_AgentCatalogIds[at]!="" && parameters!="null"?"observed_catalog_defaults":"not_checked_for_builtin";
         MT5X_AgentRespond(MT5X_AgentPending,true,"{\"indicator\":"+MT5X_AgentQuote(IntegerToString(at+1))+
            ",\"name\":"+MT5X_AgentQuote(actual)+",\"window\":"+IntegerToString(window)+",\"calculated\":"+IntegerToString(calculated)+",\"value_available\":"+(available?"true":"false")+",\"inputs_verified\":"+(binding==1?"true":"false")+",\"parameters_status\":"+MT5X_AgentQuote(status)+",\"parameters\":"+parameters+
            ",\"catalog_id\":"+MT5X_AgentQuote(MT5X_AgentCatalogIds[at])+",\"buffer_values\":"+values+",\"buffer_shift\":1}");
         MT5X_AgentPending="";MT5X_AgentPendingIndicator=-1;return;
      }
      if(FileIsExist(MT5X_AgentImage))
      {
         string folder="mt5x_control_@SESSION@";FolderCreate(folder,FILE_COMMON);
         bool copied=FileCopy(MT5X_AgentImage,0,folder+"\\"+MT5X_AgentImage,FILE_COMMON|FILE_REWRITE);
         MT5X_AgentRespond(MT5X_AgentPending,copied,"{\"file\":"+MT5X_AgentQuote(MT5X_AgentImage)+"}",copied?"":"cópia da captura ao canal comum recusada");
         MT5X_AgentPending="";MT5X_AgentImage="";
      }
      return;
   }
   string base="mt5x_control_@SESSION@",request=base+".request.tsv";
   if(!FileIsExist(request,FILE_COMMON))return;
   int f=FileOpen(request,FILE_READ|FILE_CSV|FILE_ANSI|FILE_COMMON,'\t',CP_UTF8);
   if(f==INVALID_HANDLE)return;
   string v[9];int n=0;
   while(!FileIsEnding(f) && n<9){v[n]=FileReadString(f);n++;}
   bool extra=!FileIsEnding(f);FileClose(f);
   string id=v[0];if(!MT5X_AgentId(id))return;
   if(n!=9 || extra){FileDelete(request,FILE_COMMON);MT5X_AgentRespond(id,false,"{}","payload inválido");return;}
   if(id==MT5X_AgentLast){FileDelete(request,FILE_COMMON);return;}
   string working=base+"."+id+".working";
   if(FileIsExist(working,FILE_COMMON))
   {FileDelete(request,FILE_COMMON);MT5X_AgentRespond(id,false,"{}","pedido anterior incerto; não repetir",true);return;}
   if(!FileMove(request,FILE_COMMON,working,FILE_COMMON))return;
   string method=v[1];long chart=StringToInteger(v[2]);if(chart==0)chart=ChartID();
   if(method=="chart.list")
   {
      string data="{\"charts\":[";bool first=true;
      for(int i=0;i<ArraySize(MT5X_AgentCharts);i++)
      {
         long c=MT5X_AgentCharts[i];if(!MT5X_AgentOwn(c))continue;
         if(!first)data+=",";first=false;data+=MT5X_AgentState(c);
      }
      MT5X_AgentRespond(id,true,data+"]}");return;
   }
   if(method=="chart.open")
   {
      ENUM_TIMEFRAMES tf=(ENUM_TIMEFRAMES)StringToInteger(v[4]);
      if(MT5X_AgentLayoutSettling(v[3],tf))
      {MT5X_AgentPending=id;MT5X_AgentPendingSymbol=v[3];MT5X_AgentPendingTimeframe=tf;return;}
      MT5X_AgentOpen(id,v[3],tf);return;
   }
   if(!MT5X_AgentOwn(chart)){MT5X_AgentRespond(id,false,"{}","gráfico não pertence ao controlador");return;}
   if(method=="study.catalog")
   {MT5X_AgentRespond(id,true,MT5X_AgentStudyCatalog());return;}
   if(method=="chart.select")
   {
      bool ok=ChartSetInteger(chart,CHART_BRING_TO_TOP,true);ChartRedraw(chart);
      MT5X_AgentRespond(id,ok,MT5X_AgentState(chart),ok?"":"seleção recusada");return;
   }
   if(method=="chart.close")
   {
      if(chart==ChartID()){MT5X_AgentRespond(id,false,"{}","use session.stop para o gráfico principal");return;}
      bool ok=ChartClose(chart);
      if(ok)
      {
         for(int i=0;i<ArraySize(MT5X_AgentCharts);i++)if(MT5X_AgentCharts[i]==chart)MT5X_AgentCharts[i]=0;
         for(int i=0;i<ArraySize(MT5X_AgentHandles);i++)if(MT5X_AgentIndicatorCharts[i]==chart && MT5X_AgentHandles[i]!=INVALID_HANDLE)
         {MT5X_AgentHandles[i]=INVALID_HANDLE;}
      }
      MT5X_AgentRespond(id,ok,"{}",ok?"":"ChartClose recusado");return;
   }
   if(method=="chart.style")
   {bool ok=MT5X_AgentStyle(chart,v[3]);MT5X_AgentRespond(id,ok,"{}",ok?"":"estilo indisponível neste host");return;}
   if(method=="chart.snapshot")
   {
      int w=(int)StringToInteger(v[3]),h=(int)StringToInteger(v[4]);
      if(w<200 || w>8192 || h<200 || h>8192){MT5X_AgentRespond(id,false,"{}","dimensões inválidas");return;}
      string file="mt5xa_"+id+".png";ChartRedraw(chart);
      if(!ChartScreenShot(chart,file,w,h,ALIGN_RIGHT)){MT5X_AgentRespond(id,false,"{}","captura recusada");return;}
      MT5X_AgentPending=id;MT5X_AgentImage=file;return;
   }
   if(method=="panel.set" || method=="panel.get")
   {
      if(chart!=ChartID()){MT5X_AgentRespond(id,false,"{}","painel não está neste gráfico");return;}
#ifdef MT5X_PANEL_PRESENTATION_API
      bool ok=method=="panel.get" || MT5X_PanelPresentation(v[3],v[4]);
      MT5X_AgentRespond(id,ok,MT5X_PanelPresentationState(),ok?"":"campo visual inválido");
#else
      MT5X_AgentRespond(id,false,"{}","Expert sem API de apresentação");
#endif
      return;
   }
   if(method=="indicator.add" || method=="study.indicator.add")
   {
      bool study=method=="study.indicator.add";
      int study_index=study?MT5X_AgentStudyIndex(v[3]):-1;
#ifdef HDK_STUDY_CATALOG_MQH
      if(study && (study_index<0 || study_index>=HDK_STUDY_NATIVE_COUNT))
      {MT5X_AgentRespond(id,false,"{}","catalog_id ausente no catálogo desta closure");return;}
#else
      if(study){MT5X_AgentRespond(id,false,"{}","Expert sem catálogo nativo de Estudo na closure");return;}
#endif
      int period=(int)StringToInteger(v[4]),mean=(int)StringToInteger(v[5]);double deviation=StringToDouble(v[6]);
      if(!study && (period<2 || period>500 || mean<0 || mean>1 || !MathIsValidNumber(deviation) || deviation<=0 || deviation>100))
      {MT5X_AgentRespond(id,false,"{}","parâmetros do indicador inválidos");return;}
      int active=0;
      for(int i=0;i<ArraySize(MT5X_AgentHandles);i++)
         if(MT5X_AgentHandles[i]!=INVALID_HANDLE)active++;
      if(active>=64 || MT5X_AgentCreations>=64){MT5X_AgentRespond(id,false,"{}","limite de64 criações por sessão atingido");return;}
      MT5X_AgentCreations++;
      string symbol=ChartSymbol(chart);ENUM_TIMEFRAMES tf=ChartPeriod(chart);int handle=INVALID_HANDLE,window=0,buffers=1;
      if(study)handle=MT5X_AgentStudyCreate(study_index,chart,window,buffers);
      else if(v[3]=="sma" || v[3]=="ema")handle=iMA(symbol,tf,period,0,v[3]=="ema"?MODE_EMA:MODE_SMA,PRICE_CLOSE);
      else if(v[3]=="atr"){handle=iATR(symbol,tf,period);window=(int)ChartGetInteger(chart,CHART_WINDOWS_TOTAL);}
      else if(v[3]=="rsi"){handle=iRSI(symbol,tf,period,PRICE_CLOSE);window=(int)ChartGetInteger(chart,CHART_WINDOWS_TOTAL);}
      else if(v[3]=="adx"){handle=iADX(symbol,tf,period);window=(int)ChartGetInteger(chart,CHART_WINDOWS_TOTAL);}
#ifdef MT5X_AGENT_SUPER_ATR
      else if(v[3]=="super_atr_10")
      {
         if(period==20 && mean==0 && deviation==1.0)
            handle=iCustom(symbol,tf,"HADOUKEN\\Study\\HDK_SuperATR"); // mesmo percurso do catálogo de Estudo
         else
         {
            // Em indicadores, input group também ocupa slot na chamada do MT5.
            MqlParam inputs[43]={};inputs[0].type=TYPE_STRING;inputs[0].string_value="HADOUKEN\\Study\\HDK_SuperATR";
            for(int i=0;i<10;i++)
            {
               int p=1+4*i;inputs[p].type=TYPE_STRING;inputs[p].string_value="Super ATR "+IntegerToString(i+1);
               inputs[p+1].type=TYPE_DOUBLE;inputs[p+1].double_value=(i+1)*deviation;
               inputs[p+2].type=TYPE_INT;inputs[p+2].integer_value=period;
               inputs[p+3].type=TYPE_INT;inputs[p+3].integer_value=mean;
            }
            inputs[41].type=TYPE_STRING;inputs[41].string_value="Inicialização da janela";
            inputs[42].type=TYPE_INT;inputs[42].integer_value=1;
            handle=IndicatorCreate(symbol,tf,IND_CUSTOM,43,inputs);
         }
      }
#endif
      int before=(int)MathMax(0,ChartIndicatorsTotal(chart,window));
      if(handle==INVALID_HANDLE || !ChartIndicatorAdd(chart,window,handle) || ChartIndicatorsTotal(chart,window)!=before+1)
      {MT5X_AgentRespond(id,false,"{}","indicador/ativo ausente ou ChartIndicatorAdd recusado");return;}
      // IDs não são reutilizados: referência antiga nunca remove indicador posterior.
      int at=ArraySize(MT5X_AgentHandles);ArrayResize(MT5X_AgentHandles,at+1);ArrayResize(MT5X_AgentWindows,at+1);ArrayResize(MT5X_AgentBuffers,at+1);
      ArrayResize(MT5X_AgentPeriods,at+1);ArrayResize(MT5X_AgentMeans,at+1);ArrayResize(MT5X_AgentDeviations,at+1);
      ArrayResize(MT5X_AgentIndicatorCharts,at+1);ArrayResize(MT5X_AgentNames,at+1);ArrayResize(MT5X_AgentCatalogIds,at+1);
      MT5X_AgentPeriods[at]=period;MT5X_AgentMeans[at]=mean;MT5X_AgentDeviations[at]=deviation;
      MT5X_AgentBuffers[at]=study?buffers:v[3]=="super_atr_10"?10:1;
      MT5X_AgentCatalogIds[at]=study?v[3]:"";
      MT5X_AgentHandles[at]=handle;MT5X_AgentWindows[at]=window;MT5X_AgentIndicatorCharts[at]=chart;
      MT5X_AgentNames[at]="";MT5X_AgentPending=id;MT5X_AgentPendingIndicator=at;ChartRedraw(chart);return;
   }
   if(method=="indicator.remove")
   {
      int at=(int)StringToInteger(v[3])-1;
      if(at<0 || at>=ArraySize(MT5X_AgentHandles) || MT5X_AgentHandles[at]==INVALID_HANDLE || MT5X_AgentIndicatorCharts[at]!=chart)
      {MT5X_AgentRespond(id,false,"{}","indicador não pertence ao controlador/gráfico");return;}
      int found=-1,matches=0;
      for(int w=0;w<(int)ChartGetInteger(chart,CHART_WINDOWS_TOTAL);w++)
         for(int i=0;i<ChartIndicatorsTotal(chart,w);i++)if(ChartIndicatorName(chart,w,i)==MT5X_AgentNames[at])
         {
            matches++;int h=ChartIndicatorGet(chart,w,MT5X_AgentNames[at]);
            if(h==MT5X_AgentHandles[at])found=w;
         }
      if(matches!=1 || found<0){MT5X_AgentRespond(id,false,"{}","identidade gráfica ambígua/alterada; não remover");return;}
      bool ok=ChartIndicatorDelete(chart,found,MT5X_AgentNames[at]);
      if(ok){MT5X_AgentHandles[at]=INVALID_HANDLE;}
      MT5X_AgentRespond(id,ok,"{}",ok?"":"ChartIndicatorDelete recusado");return;
   }
   MT5X_AgentRespond(id,false,"{}","método não suportado");
}
