"""CLI de captura: rede só na ação explícita; seleção é JSON tipado."""
import json

from mt5_extracao import cli
from tests.test_mentions_contract import db
from tests.test_mention_sources import hn_selection,transport,Response


def arguments(db,tmp_path):
    config=tmp_path/'qa.ini';config.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\ntime_basis=broker\n',encoding='utf-8')
    selection=tmp_path/'seleção.json';selection.write_text(json.dumps(hn_selection()),encoding='utf-8-sig')
    return ['--config',str(config),'--json','analytics','features','mentions-collect','--selection',str(selection),
            '--start','2025-01-01T10:00Z','--end','2025-01-01T10:01Z']


def test_cli_collects_only_when_explicitly_requested(db,tmp_path,capsys,monkeypatch):
    session=transport(monkeypatch)
    assert cli.main(arguments(db,tmp_path))==0
    result=json.loads(capsys.readouterr().out)['data']['capture']
    assert result['receipt']['quantity']==2 and result['receipt']['source_verified']
    assert result['acquisition']['requests']==len(session.calls)==3


def test_cli_failed_api_is_error_and_fresh_panel_is_explicit(db,tmp_path,capsys,monkeypatch):
    transport(monkeypatch,lambda url:Response(None,403))
    command=arguments(db,tmp_path);assert cli.main(command)==1
    assert not json.loads(capsys.readouterr().out)['ok']
    session=transport(monkeypatch)
    assert cli.main(command+['--fresh-panel'])==0
    assert not json.loads(capsys.readouterr().out)['data']['capture']['acquisition']['resumed_panel']
    assert any(u.endswith('newstories.json') for u,_ in session.calls)
