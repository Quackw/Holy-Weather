"""Same finite recovery experiment on installed US and scoped candidate."""
import ast
import json
import os
from pathlib import Path
import subprocess
import sys
import pytest

from test_network_quota_wait import DAY

ROOT=Path(__file__).resolve().parents[1]
INSTALLED='34b19c622de135533cd157593817f4e7fc677aba'
PARENT='667f6e01fe1dbdbc20165f2d35096b27c6d4d79f'

def saved(ref,name):return subprocess.check_output(['git','show',ref+':'+name],cwd=ROOT,text=True)
def tree(target):
    for name in subprocess.check_output(['git','ls-tree','-r','--name-only',INSTALLED,'--','src'],cwd=ROOT,text=True).splitlines():
        p=target/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(saved(INSTALLED,name))
    return target/'src'

def candidate(target):
    source=tree(target)
    for name in ('lifecycle.py','service.py'):
        path='src/holy_weather/'+name
        assert saved(INSTALLED,path)==saved(PARENT,path)
        (target/path).write_text((ROOT/path).read_text())
    path='src/holy_weather/recovery.py'
    def function(text):
        node=next(n for n in ast.parse(text).body if isinstance(n,ast.FunctionDef) and n.name=='supervise')
        return node,''.join(text.splitlines(keepends=True)[node.lineno-1:node.end_lineno])
    old=(target/path).read_text();node,oldbody=function(old)
    assert oldbody==function(saved(PARENT,path))[1]
    newbody=function((ROOT/path).read_text())[1]
    lines=old.splitlines(keepends=True)
    patched=''.join(lines[:node.lineno-1])+newbody+''.join(lines[node.end_lineno:])
    assert "{'price_recovery':True} if plan.continuous and cfg.us_lamp_enabled" in patched
    (target/path).write_text(patched)
    return source

def test_real_worker_quote_noaa_and_receipts_before_after(gamma,tmp_path,record_property,capsys):
    results=[]
    for label,source in [('before',tree(tmp_path/'old')),('after',candidate(tmp_path/'new'))]:
        base=tmp_path/label;base.mkdir(mode=0o700);(base/'gamma.json').write_text(json.dumps(gamma))
        env=dict(os.environ,PYTHONPATH=str(source)+os.pathsep+str(ROOT/'tests'),PYTHONDONTWRITEBYTECODE='1')
        p=subprocess.run([sys.executable,'-B',str(ROOT/'tests/network_quota_probe.py'),'controller',str(base)],
                         cwd=ROOT,env=env,capture_output=True,text=True,timeout=35)
        assert p.returncode==0,p.stdout+p.stderr
        value=json.loads((base/'RESULT.json').read_text());results.append(value)
    before,after=results
    assert before['launches']==0 and before['terminal']=='RESTART_BUDGET_EXHAUSTED'
    assert after['launches']==1 and after['launch_ms']==[after['permitted_ms']]
    assert after['charges_before']==before['charges_before']
    assert len(after['charges_after'])==len(before['charges_after'])+1
    assert after['duplicate_sends']==0 and after['old_receipts_preserved']==3
    assert after['child']['noaa_source_blocks']==10 and after['forecast_after_ms']>after['forecast_before_ms']
    if sys.platform=='linux':assert len(after['child']['writer_pids'])==1
    with capsys.disabled():print('NETWORK_QUOTA_PROOF '+json.dumps(results,sort_keys=True))
    record_property('network_quota_before_after',results)

@pytest.mark.parametrize('point',['quota','paid_backoff'])
def test_real_controller_exit_and_reentry_do_not_duplicate_credit(gamma,tmp_path,point,record_property,capsys):
    source=candidate(tmp_path/'source');base=tmp_path/'run';base.mkdir(mode=0o700)
    (base/'gamma.json').write_text(json.dumps(gamma))
    env=dict(os.environ,PYTHONPATH=str(source)+os.pathsep+str(ROOT/'tests'),PYTHONDONTWRITEBYTECODE='1')
    args=[sys.executable,'-B',str(ROOT/'tests/network_quota_probe.py'),'controller',str(base)]
    first=subprocess.run(args+[point],cwd=ROOT,env=env,capture_output=True,text=True,timeout=35)
    assert first.returncode==91,first.stdout+first.stderr
    before=json.loads((base/'control/state.json').read_text());baseline=json.loads((base/'baseline.json').read_text())
    assert not (base/'launch-once.json').exists()
    assert len(before['restarts'])==6+(point=='paid_backoff')
    deadline=before.get('next_ms',before['network_quota_wait']['resume_not_before_ms'])
    second=subprocess.run(args,cwd=ROOT,env=env,capture_output=True,text=True,timeout=35)
    assert second.returncode==0,second.stdout+second.stderr
    after=json.loads((base/'RESULT.json').read_text())
    assert after['launches']==1 and after['launch_ms']==[deadline]==[after['permitted_ms']]
    assert after['charges_after']==baseline['debts']+[baseline['t0']+120000]
    assert after['duplicate_sends']==0 and after['old_receipts_preserved']==3
    assert after['added_price_events']==1 and after['forecast_after_ms']>after['forecast_before_ms']
    if sys.platform=='linux':assert len(after['child']['writer_pids'])==1
    with capsys.disabled():print('NETWORK_QUOTA_REENTRY '+json.dumps(dict(point=point,before=before['network_quota_wait'],result=after),sort_keys=True))
    record_property('network_quota_real_reentry',after)
