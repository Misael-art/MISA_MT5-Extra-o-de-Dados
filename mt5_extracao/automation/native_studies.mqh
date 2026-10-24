// Projeção do catálogo da closure do Expert. Nenhuma fórmula/default duplicado.
int MT5X_AgentStudyIndex(const string id)
{
   if(StringFind(id,"native:")!=0)return -1;
   string number=StringSubstr(id,7);int length=StringLen(number);
   if(length<1 || length>3 || (length>1 && StringSubstr(number,0,1)=="0"))return -1;
   for(int i=0;i<length;i++)if(StringFind("0123456789",StringSubstr(number,i,1))<0)return -1;
   return (int)StringToInteger(number);
}

string MT5X_AgentStudyCatalog()
{
   string data="{\"catalog_sha256\":"+MT5X_AgentQuote(MT5X_AGENT_STUDY_HASH)+
      ",\"parameter_mode\":\"catalog_defaults\",\"source\":\"HDK_StudyCatalog.mqh in Expert closure\"";
#ifdef HDK_STUDY_CATALOG_MQH
   data+=",\"available\":true,\"entries\":[";
   for(int i=0;i<HDK_STUDY_NATIVE_COUNT;i++)
   {
      if(i>0)data+=",";
      string scale=HDKStudyNativeScale(i);
      data+="{\"catalog_id\":"+MT5X_AgentQuote("native:"+IntegerToString(i))+
         ",\"name\":"+MT5X_AgentQuote(HDKStudyNativeName(i))+
         ",\"scale\":"+MT5X_AgentQuote(scale)+
         ",\"buffers\":"+IntegerToString(HDKStudyNativeBuffers(i))+
         ",\"warmup\":"+IntegerToString(HDKStudyNativeWarmup(i))+
         ",\"placement\":"+MT5X_AgentQuote(scale=="price"?"main":"separate")+"}";
   }
   return data+"]}";
#else
   return data+",\"available\":false,\"entries\":[],\"reason\":\"Expert sem catalogo nativo de Estudo na closure\"}";
#endif
}

int MT5X_AgentStudyCreate(const int index,const long chart,int &window,int &buffers)
{
   window=-1;buffers=0;
#ifdef HDK_STUDY_CATALOG_MQH
   if(index<0 || index>=HDK_STUDY_NATIVE_COUNT)return INVALID_HANDLE;
   window=HDKStudyNativeScale(index)=="price"?0:(int)ChartGetInteger(chart,CHART_WINDOWS_TOTAL);
   buffers=HDKStudyNativeBuffers(index);
   // Participa da posse existente; não soltar um alias de outro componente.
   // Até64 aquisições; os recursos cessam com o processo privado da sessão.
   return HDKStudyNativeAcquire(index,ChartSymbol(chart),ChartPeriod(chart));
#else
   return INVALID_HANDLE;
#endif
}
