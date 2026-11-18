"""Atomic control-file replacement can race the other process's size inventory."""
import json,os
from pathlib import Path
import pytest
from holy_weather import recovery as r
from holy_weather.core import canonical

NONCE='a'*32


def fixture(root,name,body):
    path=root/name;path.write_text(canonical(body));path.chmod(0o600);return path


def replacing(root,temporary,monkeypatch):
    original=Path.stat;replaced=[]
    def stat(path,*a,**kw):
        if path==temporary and not replaced:
            os.replace(temporary,root/temporary.name[1:-4]);replaced.append(1)
        return original(path,*a,**kw)
    monkeypatch.setattr(Path,'stat',stat)
    return replaced


@pytest.mark.parametrize('other',[NONCE+'.owner.json',NONCE+'.progress.json',NONCE+'.result.json','state.json'])
def test_atomic_other_writer_replace_does_not_abort_control_write(tmp_path,monkeypatch,other):
    root=tmp_path/'control';root.mkdir(mode=0o700)
    target=NONCE+'.progress.json' if other=='state.json' else 'state.json'
    fixture(root,target,{'version':'previous'})
    fixture(root,other,{'version':'old other'})
    temp=fixture(root,'.'+other+'.tmp',{'version':'new other'})
    changed=replacing(root,temp,monkeypatch)
    r.write(root,target,{'version':'new target'})
    assert changed and json.loads((root/target).read_text())=={'version':'new target'}
    assert json.loads((root/other).read_text())=={'version':'new other'}
    assert not temp.exists()


def test_replacement_snapshot_still_enforces_control_budget(tmp_path,monkeypatch):
    root=tmp_path/'control';root.mkdir(mode=0o700);fixture(root,'state.json',{'old':1})
    other=NONCE+'.progress.json';fixture(root,other,{'old':1})
    temp=fixture(root,'.'+other+'.tmp',{'large':'x'*200})
    replacing(root,temp,monkeypatch);monkeypatch.setattr(r,'CONTROL_BYTES',128)
    with pytest.raises(r.RecoveryStop,match='RECOVERY_CONTROL_BUDGET'):r.write(root,'state.json',{'new':1})
    assert json.loads((root/'state.json').read_text())=={'old':1}
    assert not (root/'.state.json.tmp').exists()


@pytest.mark.parametrize('name',['plan.json','.unexpected.json.tmp'])
def test_unknown_or_permanent_disappearance_remains_fatal(tmp_path,monkeypatch,name):
    root=tmp_path/'control';root.mkdir(mode=0o700);fixture(root,'state.json',{'old':1})
    vanished=fixture(root,name,{'must':'remain'});original=Path.stat;removed=[]
    def stat(path,*a,**kw):
        if path==vanished and not removed:vanished.unlink();removed.append(1)
        return original(path,*a,**kw)
    monkeypatch.setattr(Path,'stat',stat)
    with pytest.raises(FileNotFoundError):r.write(root,'state.json',{'new':1})
    assert json.loads((root/'state.json').read_text())=={'old':1}


def test_disappeared_known_temp_without_destination_is_not_assumed_complete(tmp_path,monkeypatch):
    root=tmp_path/'control';root.mkdir(mode=0o700);fixture(root,'state.json',{'old':1})
    vanished=fixture(root,'.'+NONCE+'.owner.json.tmp',{'owner':'unpublished'})
    original=Path.stat;removed=[]
    def stat(path,*a,**kw):
        if path==vanished and not removed:vanished.unlink();removed.append(1)
        return original(path,*a,**kw)
    monkeypatch.setattr(Path,'stat',stat)
    with pytest.raises(FileNotFoundError):r.write(root,'state.json',{'new':1})
    assert json.loads((root/'state.json').read_text())=={'old':1}


def test_control_inventory_retries_are_bounded_reads(tmp_path,monkeypatch):
    root=tmp_path/'control';root.mkdir(mode=0o700);fixture(root,'state.json',{'old':1})
    other=NONCE+'.progress.json';temp=root/('.'+other+'.tmp')
    original_iter=Path.iterdir;original_stat=Path.stat;counts=[0]
    def iterdir(path):
        if path==root:
            fixture(root,temp.name,{'churn':counts[0]});counts[0]+=1
        return original_iter(path)
    def stat(path,*a,**kw):
        if path==temp:os.replace(temp,root/other)
        return original_stat(path,*a,**kw)
    monkeypatch.setattr(Path,'iterdir',iterdir);monkeypatch.setattr(Path,'stat',stat)
    with pytest.raises(r.RecoveryStop,match='RECOVERY_CONTROL_INVENTORY_UNSTABLE'):r.write(root,'state.json',{'new':1})
    assert counts[0]==5 and json.loads((root/'state.json').read_text())=={'old':1}


def test_real_supervision_continues_after_atomic_progress_replace(gamma,tmp_path,monkeypatch):
    from test_price_recovery import seed,Sim
    plan,market=seed(gamma,tmp_path);root=tmp_path/'control';race=[];original=Path.stat
    class RaceSim(Sim):
        def spawn(self,root,nonce,token):
            child=super().spawn(root,nonce,token)
            temp=fixture(root,'.'+nonce+'.progress.json.tmp',json.loads((root/(nonce+'.progress.json')).read_text()))
            race.append(temp)
            return child
    def stat(path,*a,**kw):
        if race and path==race[0]:
            temp=race.pop();os.replace(temp,root/temp.name[1:-4])
        return original(path,*a,**kw)
    monkeypatch.setattr(Path,'stat',stat)
    sim=RaceSim(['COMPLETED_WINDOW'])
    value=sim.run(plan,root)
    assert value['status']=='DEADLINE' and len(sim.calls)==1 and not value['restarts']
    assert value['attempts'][0]['state']=='COMPLETED_WINDOW' and not race
