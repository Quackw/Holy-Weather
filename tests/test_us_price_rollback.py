"""Pinned main/installed-US compatibility, without any runtime installation."""
import ast
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
from test_us_price_flow import us_market

ROOT=Path(__file__).resolve().parents[1]
INSTALLED='34b19c622de135533cd157593817f4e7fc677aba'
MAIN='667f6e01fe1dbdbc20165f2d35096b27c6d4d79f'

def tree(target,ref):
    for path in subprocess.check_output(['git','ls-tree','-r','--name-only',ref,'--','src'],cwd=ROOT,text=True).splitlines():
        p=target/path;p.parent.mkdir(parents=True,exist_ok=True)
        p.write_bytes(subprocess.check_output(['git','show',ref+':'+path],cwd=ROOT))
    return target/'src'

def scoped_installed(target):
    """Apply only this PR's US changes, retaining installed #40/#41 functions."""
    source=tree(target,INSTALLED)
    for name in ('core.py','storage.py'):
        (source/'holy_weather'/name).write_text((ROOT/'src/holy_weather'/name).read_text())
    path=source/'holy_weather/catalogue.py';code=path.read_text()
    current=(ROOT/'src/holy_weather/catalogue.py').read_text()
    nodes=ast.parse(current)
    def text(node):return ''.join(current.splitlines(keepends=True)[node.lineno-1:node.end_lineno])
    cls=next(n for n in nodes.body if isinstance(n,ast.ClassDef) and n.name=='CatalogueStore')
    usage=text(next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='usage'))
    helper=text(next(n for n in nodes.body if isinstance(n,ast.ClassDef) and n.name=='USPriceRecovery'))
    code=code.replace('import time\n','import time\nimport os\nimport stat\n',1)
    code=code.replace('class CatalogueStore(Store):\n','class CatalogueStore(Store):\n'+usage+'\n\n',1)
    code=code.replace('def observe_catalogue(',helper+'\n\n\ndef observe_catalogue(',1)
    anchor='                        subscribed=min(16,len(tokens));live_tokens=set(tokens[:16]);next_subscription=monotonic()+1\n'
    assert code.count(anchor)==1
    code=code.replace(anchor,anchor+"                        baseline_recovery=USPriceRecovery(monotonic()) if cfg.region=='us' else None\n")
    anchor='                                live_tokens.difference_update(closed)\n'
    assert code.count(anchor)==1
    code=code.replace(anchor,anchor+"                                if baseline_recovery:\n                                    requests=baseline_recovery.tick(store,ws,live_tokens,at,monotonic())\n                                    telemetry['price_baseline_resubscriptions']=telemetry.get('price_baseline_resubscriptions',0)+requests\n")
    assert 'price_recovery=False' in code and 'handoff=price_recovery' in code
    path.write_text(code)
    return source

@pytest.mark.parametrize('base',[MAIN,INSTALLED],ids=['main','installed-us'])
def test_current_data_rollback_after_recovery_and_new_records(gamma,tmp_path,base,record_property):
    _,m,r=us_market(gamma,tmp_path)
    (tmp_path/'binding.json').write_text(json.dumps([m,r]))
    old=tree(tmp_path/'old',base)
    new=scoped_installed(tmp_path/'new') if base==INSTALLED else ROOT/'src'
    records=[]
    for phase,source in [('old',old),('new',new),('rollback',old)]:
        env=dict(os.environ,PYTHONPATH=str(source)+os.pathsep+str(ROOT/'tests'),PYTHONDONTWRITEBYTECODE='1')
        p=subprocess.run([sys.executable,'-B',str(ROOT/'tests/us_flow_probe.py'),str(tmp_path),phase],
                         cwd=ROOT,env=env,capture_output=True,text=True,timeout=30)
        assert p.returncode==0,p.stdout+p.stderr
        records.append(json.loads(p.stdout))
    assert 'SENT' in records[-1]['queue'] and 'UNKNOWN' in records[-1]['queue'] and 'PENDING' in records[-1]['queue']
    record_property('current_data_rollback',records)

def test_installed_us_stack_keeps_lamp_and_scoped_flow_cases(tmp_path,record_property):
    source=scoped_installed(tmp_path/'installed')
    env=dict(os.environ,PYTHONPATH=str(source),PYTHONDONTWRITEBYTECODE='1')
    result=subprocess.run([sys.executable,'-B','-m','pytest','-q',str(ROOT/'tests/test_us_price_flow.py'),
        str(ROOT/'tests/test_lamp_send_window.py'),'-k','not installed_price_stack',
        '-o','faulthandler_timeout=0','--ci-stack-seconds=0'],cwd=ROOT,env=env,
        capture_output=True,text=True,timeout=90)
    assert result.returncode==0,result.stdout+result.stderr
    record_property('installed_us_base',INSTALLED)
    record_property('installed_us_cases',result.stdout.strip().splitlines()[-1])
