"""Journal público sintético; sem conta, MT5 ou fixtures privadas."""
import hashlib

import pytest

from mt5_extracao.robots import native_journal as journal

ROOT="HDKPR2."+"a"*32
CONTEXT="b"*64
POSITION=2**53+7
NAME=ROOT+"_RESERVATIONS.state"


def sha(value):return hashlib.sha256(value.encode("ascii")).hexdigest()


def words(row,field,value):
    row[field+"_h"]=value >> 32;row[field+"_l"]=value & (2**32-1)


def digest(row,field,value):
    for index in range(8):row[field+("a" if index < 4 else "b")+str(index%4)]=int(value[index*8:index*8+8],16)


def document(rows):
    # Ordem nativa publicada: prioridade alfabética, caixa somente no desempate.
    keys=sorted(rows,key=lambda key:(key.casefold(),tuple(character.isupper() for character in key)))
    payload=f"HDK_RESERVATIONS_3\n{'c'*64}\n{len(rows)}\n"+"".join(f"{key}|{float(rows[key]):.16e}\n" for key in keys)
    return (payload+"checksum="+sha(payload)+"\n").encode("ascii")


def fixture(direction=1, action_stop=None, intent=None):
    symbol=sha("EURUSD")
    entry=dict(R=20,E=1120,V=2,S=6,Q=2,P=100,L=90,T=140,K=0,A=1,C=1000,G=900,N=0,F=0,Y=0,J=200,H=0,b=20,u=0,r=0)
    if direction < 0:entry.update(L=110,T=60,K=1)
    for field,value in {"M":12,"O":77,"D":78,"v":5,"z":POSITION}.items():words(entry,field,value)
    for i in range(4):entry[f"X{i}"]=int(symbol[i*8:i*8+8],16)
    payload=f"REQ1|{symbol}|12|5|"+"|".join(f"{float(entry[f]):.16e}" for f in "Q P L T K A C G N F Y J H b".split())
    for field,value in {"B":CONTEXT,"I":"d"*64,"x":symbol,"W":sha(payload)}.items():digest(entry,field,value)
    action=dict(V=1,S=6,K=2,C=1200,N=1200,U=2,Q=0,P=0,L=102,T=140,H=0,j=0,y=0,h=0,l=0,r=10009,a=1,f=0,p=0,g=1,e=3,u=0,n=2,i=55,c=0)
    if direction < 0:action.update(L=98,T=60)
    if action_stop is not None:action["L"]=action_stop
    for field,value in {"E":1,"O":POSITION+1,"Z":POSITION,"Y":0,"D":0,"v":0}.items():words(action,field,value)
    intent=intent or f"trailing-{POSITION}-1100";intent_hash=sha(f"HDK_MANAGEMENT_INTENT1|{CONTEXT}|{len(intent)}:{intent}")
    payload=f"HDK_MANAGEMENT1|{CONTEXT}|{intent_hash}|{symbol}|1|{POSITION+1}|{POSITION}|0|"+"|".join(f"{float(action[f]):.16e}" for f in "V K C U Q P L T H j y h l".split())
    for field,value in {"B":CONTEXT,"I":intent_hash,"x":symbol,"W":sha(payload)}.items():digest(action,field,value)
    return {ROOT+"_SEQ":2,**{ROOT+"_R_1."+key:value for key,value in entry.items()},**{ROOT+"_A_2."+key:value for key,value in action.items()}}


def read(rows):return journal.read(document(rows),filename=NAME,context_hash=CONTEXT,symbol="EURUSD",magic=12)


def test_exact_ticket_words_request_and_effect_survive_binary64_limit():
    result=read(fixture())
    assert result["all_settled"] and not result["operational_acceptance"]
    assert result["entries"][0]["position"] == POSITION
    assert result["actions"][0]["target"] == POSITION+1
    assert result["actions"][0]["request_id"] == 55
    assert result["actions"][0]["confirmed"] == 0 and result["actions"][0]["effect"] == 3


@pytest.mark.parametrize("fault",["context","symbol","magic","immutable_entry","immutable_action","word","field","unknown","sequence","risk","uncertain_with_effect","response_unknown","bad_retcode"])
def test_corruption_cannot_become_a_confirmed_native_result(fault):
    rows=fixture();e=ROOT+"_R_1.";a=ROOT+"_A_2."
    if fault=="context":rows[a+"Ba0"]+=1
    elif fault=="symbol":rows[e+"xa0"]+=1
    elif fault=="magic":rows[e+"M_l"]=99
    elif fault=="immutable_entry":rows[e+"P"]+=1
    elif fault=="immutable_action":rows[a+"L"]+=1
    elif fault=="word":rows[a+"Z_l"]=2**32
    elif fault=="field":del rows[a+"N"]
    elif fault=="unknown":rows[a+"bad"]=0
    elif fault=="sequence":rows[ROOT+"_SEQ"]=1
    elif fault=="risk":rows[e+"R"]=0
    elif fault=="uncertain_with_effect":rows[a+"S"]=4
    elif fault=="response_unknown":rows[a+"g"]=0
    elif fault=="bad_retcode":rows[a+"r"]=10010
    with pytest.raises(ValueError):read(rows)


def test_pending_intention_is_preserved_without_operational_acceptance():
    rows=fixture();a=ROOT+"_A_2."
    rows[a+"S"]=2
    for field in ("N","r","a","g","e","u","n","i"):rows[a+field]=0
    result=read(rows)
    assert not result["all_settled"] and result["actions"][0]["state"] == 2


def test_stop_limit_protection_is_measured_from_limit_price_not_trigger():
    rows=fixture();prefix=ROOT+"_R_1."
    for key,value in dict(K=6,A=5,P=100,H=99,L=98).items():rows[prefix+key]=value
    symbol=sha("EURUSD")
    payload=f"REQ1|{symbol}|12|5|"+"|".join(f"{float(rows[prefix+f]):.16e}" for f in "Q P L T K A C G N F Y J H b".split())
    replacement={};digest(replacement,"W",sha(payload));rows.update({prefix+key:value for key,value in replacement.items()})
    assert read(rows)["entries"][0]["initial_stop"] == 98
    rows[prefix+"L"]=99.5
    payload=f"REQ1|{symbol}|12|5|"+"|".join(f"{float(rows[prefix+f]):.16e}" for f in "Q P L T K A C G N F Y J H b".split())
    replacement={};digest(replacement,"W",sha(payload));rows.update({prefix+key:value for key,value in replacement.items()})
    with pytest.raises(ValueError):read(rows)


@pytest.mark.parametrize("fault",["checksum","truncated","canonical","duplicate","order","oversize","encoding"])
def test_envelope_failure_is_explicit(fault):
    data=document(fixture())
    if fault=="checksum":data=data.replace(b"checksum=",b"checksum=0")
    elif fault=="truncated":data=data[:-1]
    elif fault=="canonical":data=data.replace(b"1.2000000000000000e+03",b"1200",1)
    elif fault=="duplicate":data=data.replace(b"\nchecksum=",b"\n"+data.splitlines()[3]+b"\nchecksum=")
    elif fault=="order":
        lines=data.splitlines(keepends=True);lines[3],lines[4]=lines[4],lines[3];data=b"".join(lines)
    elif fault=="oversize":data=b"x"*8388609
    elif fault=="encoding":data=b"\xff"+data
    with pytest.raises(ValueError):journal.read(data,filename=NAME,context_hash=CONTEXT,symbol="EURUSD",magic=12)


def test_capture_requires_unique_final_digest_and_preserves_an_immutable_copy(tmp_path):
    common=tmp_path/"Common";directory=tmp_path/"session";sid="a"*32
    source=common/"mt5x_sessions"/sid/NAME;source.parent.mkdir(parents=True)
    data=document(fixture());source.write_bytes(data)
    log=directory/"Tester/Agent-QA/logs/test.log";log.parent.mkdir(parents=True)
    marker=f"HDK_JOURNAL at=2000 reconciled=1 file={NAME} sha256={hashlib.sha256(data).hexdigest()} context={CONTEXT}\n"
    log.write_text(marker)
    binding=dict(context_hash=CONTEXT,session_id=sid,symbol="EURUSD")
    result=journal.capture(common,directory,binding,{"InpMagic":12})
    assert result["reconciled"] and (directory/"native_portfolio.state").read_bytes() == data
    log.write_text(marker+marker)
    with pytest.raises(ValueError):journal.capture(common,directory,binding,{"InpMagic":12})
    log.write_text(marker);source.write_bytes(data+b"\n")
    with pytest.raises(ValueError,match="mudou"):journal.capture(common,directory,binding,{"InpMagic":12})
