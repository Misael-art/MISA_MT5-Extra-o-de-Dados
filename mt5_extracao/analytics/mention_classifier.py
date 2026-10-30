"""Avaliação conservadora de objeto em inglês; execução explícita, sem rede.

VADER MIT (C.J. Hutto); langdetect Apache-2.0 (Michal Danilak).
O escore intermediário não é medida publicada. Fora do domínio: abstenção.
"""
from functools import lru_cache
from importlib.metadata import version
from pathlib import Path
import hashlib
import re
import unicodedata

from mt5_extracao.analytics import mentions

CLASSIFIER_ID = 'object-vader-en'
AMBIGUOUS = re.compile(r'\?|["“”]|/s\b|\bsarcas|\byeah right\b|\b(?:said|says|say|according|reportedly|allegedly|rumou?r|discount|coupon|promo|buy now|better than|worse than|compared to)\b', re.I)
PRICE = re.compile(r'\b(?:stock|share price|price|bullish|bearish|rally|rise|rises|fall|falls|surge|gained|percent|going (?:up|down))\b|\d\s*%', re.I)
SUBJECT = r'^\s*TARGET\s+(?:is|was|are|were|has|have|works|seems|looks|feels)\b'
EVALUATION = r'^\s*(?:i|we)\s+(?:(?:really|absolutely|truly)\s+)?(?:love|like|admire|recommend|hate|dislike|regret using)\s+(?:the\s+)?TARGET\b'


@lru_cache(maxsize=1)
def _engine():
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        from langdetect.detector_factory import DetectorFactory, PROFILES_DIRECTORY
    except ImportError as exc:
        raise RuntimeError('Classificador ausente; execute o instalador oficial para instalar VADER/langdetect. P/N continuam indisponíveis.') from exc
    if version('vaderSentiment')!='3.3.2' or version('langdetect')!='1.0.9':
        raise RuntimeError('Versão do classificador difere da qualificada; reinstale as dependências oficiais.')
    analyzer=SentimentIntensityAnalyzer();factory=DetectorFactory();factory.seed=0;factory.load_profile(PROFILES_DIRECTORY)
    paths=[Path(__file__),Path(__import__('vaderSentiment.vaderSentiment',fromlist=['x']).__file__),
           Path(__import__('vaderSentiment.vaderSentiment',fromlist=['x']).__file__).with_name('vader_lexicon.txt'),
           Path(__import__('vaderSentiment.vaderSentiment',fromlist=['x']).__file__).with_name('emoji_utf8_lexicon.txt')]
    import langdetect
    paths+=sorted(Path(langdetect.__file__).parent.rglob('*.py'))+sorted(Path(PROFILES_DIRECTORY).iterdir())
    artifacts={p.name+':'+str(i):hashlib.sha256(p.read_bytes()).hexdigest() for i,p in enumerate(paths) if p.is_file()}
    return analyzer,factory,mentions._hash(artifacts)


def descriptor():
    """Identidade de código, léxico, detector e regras; não captura/classifica texto."""
    return dict(id=CLASSIFIER_ID,version='1+'+_engine()[2],languages=['en'])


def classify_document(body, aliases, *, language=None):
    """Um rótulo exclusivo por objeto; não reutiliza o sentimento global do texto."""
    analyzer,factory,_=_engine()
    if not isinstance(body,str) or len(body.encode())>64*1024:
        raise ValueError('Texto da classificação precisa ser string até64KiB.')
    aliases=mentions._labels(aliases,'aliases')
    if not aliases:raise ValueError('Classificação exige aliases explícitos do objeto.')
    def result(label,reason,detected=None):return dict(label=label,reason=reason,language=detected)
    body=unicodedata.normalize('NFKC',body)
    if language is not None and language!='en':return result('abstained','UNSUPPORTED_LANGUAGE',language)
    # Um idioma declarado serve aos textos curtos; sem declaração exige detector
    # determinístico e confiança alta. Não traduz nem presume português.
    detected=language
    if detected is None:
        try:
            detector=factory.create();detector.append(body);probabilities=detector.get_probabilities()
        except Exception as exc:
            from langdetect.lang_detect_exception import LangDetectException
            if not isinstance(exc,LangDetectException):raise
            return result('abstained','LANGUAGE_UNKNOWN')
        if not probabilities or probabilities[0].lang!='en' or probabilities[0].prob<.90:
            return result('abstained','LANGUAGE_NOT_CONFIRMED',probabilities[0].lang if probabilities else None)
        detected='en'
    if AMBIGUOUS.search(body):return result('abstained','AMBIGUOUS_CONTEXT',detected)
    if PRICE.search(body):return result('abstained','PRICE_DIRECTION_NOT_OBJECT_EVALUATION',detected)
    pattern=re.compile(r'(?<!\w)(?:'+'|'.join(re.escape(a) for a in sorted(aliases,key=len,reverse=True))+r')(?!\w)',re.I)
    targeted=pattern.sub('TARGET',body)
    if 'TARGET' not in targeted:return result('abstained','TARGET_NOT_FOUND',detected)
    clauses=re.split(r'[.!;\n]+|\bbut\b|\bhowever\b|\bwhereas\b',targeted,flags=re.I)
    polarities=[]
    for clause in clauses:
        if 'TARGET' not in clause:continue
        if not (re.search(SUBJECT,clause,re.I) or re.search(EVALUATION,clause,re.I)):
            return result('abstained','TARGET_SYNTAX_NOT_RESOLVED',detected)
        # Após a primeira oração, "and X is..." pode mudar de sujeito. Sem
        # parser sintático qualificado, evita atribuição da avaliação de outro.
        if re.search(r'\b(?:and|because|while|although)\s+(?!it\b|its\b)\w+\s+(?:is|was|are|were)\b',clause,re.I):
            return result('abstained','MULTIPLE_SUBJECTS',detected)
        scores=analyzer.polarity_scores(clause.replace('TARGET','object'));score=scores['compound']
        polarities.append('mixed' if scores['pos']>0 and scores['neg']>0 else 'positive' if score>=.25 else 'negative' if score<=-.25 else 'neutral')
    labels=set(polarities)
    label='mixed' if 'mixed' in labels or {'positive','negative'}<=labels else 'positive' if 'positive' in labels else 'negative' if 'negative' in labels else 'neutral'
    return result(label,'EXPLICIT_OBJECT_EVALUATION' if label!='neutral' else 'NO_POLARITY_IN_SUPPORTED_CLAUSE',detected)
