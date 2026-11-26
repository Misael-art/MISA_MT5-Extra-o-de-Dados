"""Roteia uma revisão de gestão pré-vinculada no mesmo passe do Tester."""
from decimal import Decimal

from ..laboratory import ultra_market_preset


def management_code(index, rules):
    return "".join(f'   U{index}_g_rules.{key}='
                   f'{str(value).lower() if type(value) is bool else format(Decimal(str(value)), "f")};\n'
                   for key, value in sorted(rules.items()))


def validate(initial, successor, window, switch_at):
    successor = ultra_market_preset.read(successor)
    if initial["schema"] != ultra_market_preset.PORTABLE or successor["schema"] != ultra_market_preset.PORTABLE:
        raise ValueError("successor.schema: a troca requer duas políticas portáteis.")
    if (successor["parent_sha256"] != initial["preset_sha256"] or successor["version"] != initial["version"] + 1):
        raise ValueError("successor.parent_sha256: esperado o sucessor direto da política ativa.")
    if type(switch_at) is not int or not window["start_msc"] < switch_at < window["last_msc"]:
        raise ValueError("switch_at_msc: deve estar dentro da janela executável.")
    if successor["research"] != initial["research"] or successor["parameters"] != initial["parameters"]:
        raise ValueError("successor.parameters/research: este controle troca apenas regras de gestão.")
    left, right = initial["profile"], successor["profile"]
    if {k: v for k, v in left.items() if k != "modules"} != {k: v for k, v in right.items() if k != "modules"}:
        raise ValueError("successor.profile: preserve o mercado, o motor e o orçamento agregado.")
    a, b = left["modules"], right["modules"]
    ignored = {"owner_rules", "preset_sha256", "inputs_sha256"}
    if len(a) != len(b) or any(
            {k: v for k, v in old.items() if k not in ignored} != {k: v for k, v in new.items() if k not in ignored}
            or old.get("owner_rules", {}).get("risk") != new.get("owner_rules", {}).get("risk")
            for old, new in zip(a, b)):
        raise ValueError("successor.profile.modules: preserve seleção, prioridades, dados e limites locais.")
    if a == b:
        raise ValueError("successor.owner_rules.management: a revisão deve alterar a gestão.")
    return successor


def executor_extension(source):
    anchor = "   bool BindPortfolio(CHDKPortfolioRiskGuard *portfolio,string &reason)"
    if source.count(anchor) != 1:
        raise ValueError("O executor fixado não expõe o ponto de vínculo da revisão.")
    return source.replace(anchor, '''   bool RestorePortfolio(CHDKPortfolioRiskGuard *portfolio,string &reason)
   {
      if(!MQLInfoInteger(MQL_TESTER) || !IdentityUnchanged() || portfolio==NULL ||
         CheckPointer(portfolio)==POINTER_INVALID || !portfolio.BoundTo(m_magic) ||
         m_max_retries!=1 || m_capture_entry || !m_entry_recorded || !m_management_recorded)
      {reason="estado local possui ação incerta ou vínculo inválido";return false;}
      m_portfolio=portfolio;
      m_next_intent="";m_next_event=0;m_entry_reservation=0;
      m_next_management="";m_management_action=0;m_management_state=HDK_RESERVATION_PREPARED;
      HDKExecutionResultReset(0,m_close_result);
      return true;
   }

''' + anchor)


def fragments(successor, switch_at, restore_at):
    helpers = f'''
const long UltraSwitchAt={switch_at};
const long UltraRestoreAt={restore_at or 0};
const int UltraNextVersion={successor["version"]};
const string UltraNextPreset="{successor["preset_sha256"]}";

ulong UltraIncumbent(const long magic)
{{
   for(int i=0;i<PositionsTotal();i++)
      if(PositionGetTicket(i)>0 && PositionGetInteger(POSITION_MAGIC)==magic && PositionGetString(POSITION_SYMBOL)==_Symbol)
         return (ulong)PositionGetInteger(POSITION_IDENTIFIER);
   return 0;
}}

bool UltraWorking(const long magic)
{{
   for(int i=0;i<OrdersTotal();i++)
      if(OrderGetTicket(i)>0 && OrderGetInteger(ORDER_MAGIC)==magic && OrderGetString(ORDER_SYMBOL)==_Symbol) return true;
   return false;
}}

string UltraOwnerSnapshot(const long magic)
{{
   string result="";
   for(int i=0;i<PositionsTotal();i++)
      if(PositionGetTicket(i)>0 && PositionGetInteger(POSITION_MAGIC)==magic && PositionGetString(POSITION_SYMBOL)==_Symbol)
         result+=StringFormat("P:%I64d:%I64d:%.8f:%.*f:%.*f,",PositionGetInteger(POSITION_IDENTIFIER),
            PositionGetInteger(POSITION_TICKET),PositionGetDouble(POSITION_VOLUME),_Digits,PositionGetDouble(POSITION_SL),
            _Digits,PositionGetDouble(POSITION_TP));
   for(int i=0;i<OrdersTotal();i++)
      if(OrderGetTicket(i)>0 && OrderGetInteger(ORDER_MAGIC)==magic && OrderGetString(ORDER_SYMBOL)==_Symbol)
         result+=StringFormat("O:%I64d:%.8f,",OrderGetInteger(ORDER_TICKET),OrderGetDouble(ORDER_VOLUME_CURRENT));
   return result;
}}
'''
    declarations, init, ticks = [], [], []
    for i, slot in enumerate(successor["profile"]["modules"]):
        magic = slot["magic"]
        declarations.append(f'CHDKPortfolioRiskGuard next_{i}; bool adopted_{i}=false, held_{i}=false, restored_{i}=false;')
        init.append(f'''   {{
      string bind_reason;
      if(!next_{i}.BindContext({magic},UltraBinding,"{slot["key"]}","mt5x-ultra-eurusd",
          "{slot["preset_sha256"]}",UltraNextVersion,UltraClosure,UltraBinarySha,"{slot["inputs_sha256"]}",bind_reason))
      {{Print(UltraPrefix,"DENIED|successor|",bind_reason);return INIT_FAILED;}}
   }}''')
        management = management_code(i, slot.get("owner_rules", {}).get("management", {}))
        ticks.append(f'''   if(UltraRestoreAt>0 && ultra_msc>=UltraRestoreAt && !restored_{i})
   {{
      string restore_reason,before_name,before_sha,after_name,after_sha;
      CHDKPortfolioRiskGuard *current=adopted_{i}?GetPointer(next_{i}):GetPointer(owner_{i});
      string before=UltraOwnerSnapshot({magic});
      if(!UltraReconcile(restore_reason) || !current.AuditJournal(before_name,before_sha,restore_reason) ||
         !U{i}_g_exec.RestorePortfolio(current,restore_reason) || !UltraReconcile(restore_reason) ||
         !current.AuditJournal(after_name,after_sha,restore_reason) || before_name!=after_name ||
         before_sha!=after_sha || before!=UltraOwnerSnapshot({magic}))
      {{Print(UltraPrefix,"DENIED|restore|",restore_reason);ultra_failed=true;return;}}
      restored_{i}=true;
      Print(UltraPrefix,"RECOVER|",{magic},"|",ultra_msc,"|",before_sha,"|",before);
   }}
   if(ultra_msc>=UltraSwitchAt && !adopted_{i})
   {{
      ulong incumbent=UltraIncumbent({magic});
      if(incumbent>0 || UltraWorking({magic}) || U{i}_g_entry_uncertain)
      {{
         if(!held_{i})
            Print(UltraPrefix,"ROTATE|",{magic},"|",ultra_msc,"|held|",incumbent,"|",UltraPresetSha,"|",UltraPresetVersion);
         held_{i}=true;
      }}
      else
      {{
         string switch_reason;
         if(UltraReconcile(switch_reason))
         {{
            if(!U{i}_g_exec.RestorePortfolio(GetPointer(next_{i}),switch_reason))
            {{Print(UltraPrefix,"DENIED|rotation|",switch_reason);ultra_failed=true;return;}}
            ZeroMemory(U{i}_g_rules);
            U{i}_StrategyRules(U{i}_g_rules);
{management}            adopted_{i}=true;
            Print(UltraPrefix,"ROTATE|",{magic},"|",ultra_msc,"|adopted|0|",UltraNextPreset,"|",UltraNextVersion);
         }}
      }}
   }}
   {{
      bool uncertain=U{i}_g_entry_uncertain;
      bool draining=ultra_msc>=UltraSwitchAt && !adopted_{i};
      if(draining) U{i}_g_entry_uncertain=true;
      U{i}_OnTick();
      if(draining) U{i}_g_entry_uncertain=uncertain;
   }}''')
    return helpers, declarations, init, ticks
