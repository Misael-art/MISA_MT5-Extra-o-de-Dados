"""Avaliação explícita de objeto em português, com abstenção conservadora.

Léxico/regras próprios; não traduz VADER inglês nem infere sentimento de preço.
Versão distinta e opt-in. A qualificação é publicada separadamente por domínio.
"""
import hashlib
from pathlib import Path
import re
import unicodedata

from . import mention_classifier,mentions

CLASSIFIER_ID='object-evaluation-pt'
POSITIVE={'bom','boa','bons','boas','otimo','otima','excelente','excelentes','incrivel','confiavel',
          'util','eficiente','rapido','rapida','maravilhoso','maravilhosa','brabo','braba','top','show','adorei'}
NEGATIVE={'ruim','ruins','pessimo','pessima','terrivel','horrivel','lixo','bugado','bugada','inutil',
          'lento','lenta','inconfiavel','problematico','problematica','decepcionante'}
AMBIGUOUS=re.compile(r'\?|["“”]|/s\b|\bsqn\b|\bso que nao\b|\baham\b|\bk{2,}\b|\bsarcas|\bironi|🤡|\b(?:dizem|disseram|segundo|boato|supostamente|cupom|desconto|compre agora|melhor que|pior que)\b')
PRICE=re.compile(r'\b(?:preco|cotacao|acoes|acao|bullish|bearish|valorizacao|desvalorizacao|disparou|subiu|caiu|comprar|vender)\b|\d\s*%')
SUBJECT=re.compile(r'^\s*TARGET\s+(?:e|eh|esta|parece|funciona|tem|foi|sao|era)\b')
EVALUATION=re.compile(r'^\s*(?:(?:eu|nos)\s+)?(?:(?:realmente|muito)\s+)?(nao recomendo|recomendo|adoro|amo|curto|gosto|detesto|odeio)\s+(?:(?:de|da|do|a|o)\s+)?TARGET\b')


def _normalize(text):
    return ''.join(c for c in unicodedata.normalize('NFKD',text.casefold()) if not unicodedata.combining(c))


def descriptor():
    # Reutiliza o detector já instalado/qualificado; a identidade inglesa permanece intacta.
    origin=mention_classifier.descriptor()
    return dict(id=CLASSIFIER_ID,version='1+'+mentions._hash(dict(
        code_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),language_detector=origin)),languages=['pt'])


def classify_document(body,aliases,*,language=None):
    if not isinstance(body,str) or len(body.encode())>64*1024:raise ValueError('Texto precisa ser string até64KiB.')
    aliases=mentions._labels(aliases,'aliases')
    if not aliases:raise ValueError('Informe aliases explícitos do objeto.')
    def result(label,reason,detected=None):return dict(label=label,reason=reason,language=detected)
    detected=language
    if language not in (None,'pt','pt-BR','pt-PT'):return result('abstained','UNSUPPORTED_LANGUAGE',language)
    if detected is None:
        _,factory,_=mention_classifier._engine()
        try:
            detector=factory.create();detector.append(body);probabilities=detector.get_probabilities()
        except Exception as exc:
            from langdetect.lang_detect_exception import LangDetectException
            if not isinstance(exc,LangDetectException):raise
            return result('abstained','LANGUAGE_UNKNOWN')
        if not probabilities or probabilities[0].lang!='pt' or probabilities[0].prob<.90:
            return result('abstained','LANGUAGE_NOT_CONFIRMED',probabilities[0].lang if probabilities else None)
    detected='pt';body=_normalize(body)
    if AMBIGUOUS.search(body):return result('abstained','AMBIGUOUS_CONTEXT',detected)
    if PRICE.search(body):return result('abstained','PRICE_DIRECTION_NOT_OBJECT_EVALUATION',detected)
    pattern=re.compile(r'(?<!\w)(?:'+'|'.join(re.escape(_normalize(a)) for a in sorted(aliases,key=len,reverse=True))+r')(?!\w)')
    targeted=pattern.sub('TARGET',body)
    if 'TARGET' not in targeted:return result('abstained','TARGET_NOT_FOUND',detected)
    polarities=[]
    clauses=re.split(r'[.!;\n]+|\b(?:mas|porem|entretanto)\b',targeted)
    for clause in clauses:
        words=set(re.findall(r'\w+',clause));positive=bool(words&POSITIVE);negative=bool(words&NEGATIVE)
        if 'TARGET' not in clause:
            if positive or negative:return result('abstained','UNRESOLVED_OTHER_CLAUSE',detected)
            continue
        evaluation=EVALUATION.search(clause)
        if not SUBJECT.search(clause) and not evaluation:return result('abstained','TARGET_SYNTAX_NOT_RESOLVED',detected)
        tail=re.sub(r'^\s*(?:e|eh|esta|parece|funciona|tem|foi|sao|era)\b','',clause.split('TARGET',1)[1])
        if re.search(r'\b(?:e|porque|enquanto|embora)\s+(?!ele\b|ela\b|seu\b|sua\b)\w+\s+(?:e|esta|parece)\b',tail):
            return result('abstained','MULTIPLE_SUBJECTS',detected)
        if evaluation:
            negative|=evaluation.group(1) in ('nao recomendo','detesto','odeio')
            positive|=evaluation.group(1) in ('recomendo','adoro','amo','curto','gosto')
        if re.search(r'\b(?:nao|nunca|jamais|nem)\b',clause) and not (evaluation and evaluation.group(1)=='nao recomendo'):
            return result('abstained','NEGATED_POLARITY_UNRESOLVED',detected)
        polarities.append('mixed' if positive and negative else 'positive' if positive else 'negative' if negative else 'neutral')
    labels=set(polarities)
    label='mixed' if 'mixed' in labels or {'positive','negative'}<=labels else 'positive' if 'positive' in labels else 'negative' if 'negative' in labels else 'neutral'
    return result(label,'EXPLICIT_OBJECT_EVALUATION' if label!='neutral' else 'NO_POLARITY_IN_SUPPORTED_CLAUSE',detected)
