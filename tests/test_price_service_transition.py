"""Synthetic credentials/delivery only; exact prior source read offline from Git."""
from dataclasses import replace
from pathlib import Path
import importlib.util,json,os,sqlite3,subprocess,sys
from unittest.mock import patch
import pytest
from holy_weather import service,lifecycle as l,recovery as r,transition as t
from holy_weather.catalogue import CatalogueStore,Router
from holy_weather.core import canonical,sha
from holy_weather.history import fingerprint,digest
from holy_weather.alert_queue import preview,claim,finish
from test_price_lifecycle import OWNER,rolling,prepared,identity
from test_price_recovery import seed
from test_catalogue import AT,feed


@pytest.mark.parametrize('manager,env,reason',[
    ('running',{'SERVICE_RESULT':'success'},'MANUAL_STOP'),
    ('unknown',{},'MANUAL_STOP'),
    ('stopping',{'SERVICE_RESULT':'success'},'HOST_SHUTDOWN'),
    ('degraded',{'SERVICE_RESULT':'signal','EXIT_STATUS':'SEGV'},'CONTROLLER_FAILURE'),
    ('running',{'SERVICE_RESULT':'watchdog','EXIT_CODE':'killed','EXIT_STATUS':'ABRT'},'CONTROLLER_FAILURE'),
    ('running',{'SERVICE_RESULT':'exit-code','EXIT_CODE':'exited','EXIT_STATUS':'78'},'FATAL_SERVICE_EXIT')])
def test_systemd_stop_context_is_durable_and_no_signals(gamma,tmp_path,identity,manager,env,reason):
    plan,_=seed(gamma,tmp_path);root,state=prepared(tmp_path,rolling(plan),active=False,status='STOPPED')
    request=service.record_stop(root,environment=env,manager_state=manager)
    assert request['reason']==reason
    again=service.record_stop(root,environment={'SERVICE_RESULT':'exit-code','EXIT_CODE':'exited','EXIT_STATUS':'78'},manager_state='running')
    assert again==request  # No failed start may overwrite a previous manual stop.
    current=dict(OWNER,boot='next-boot')
    with patch.object(l,'own_identity',return_value=current):
        accepted=l.reconcile(root,state,AT+1,r.write,lambda:r.preflight(rolling(plan)))
    assert accepted==(reason=='HOST_SHUTDOWN')
    if accepted:
        after=l.compact_control(root,rolling(plan),state,r.write,AT+1)
        assert not (root/l.STOP_REQUEST).exists() and after['audit_tip']
    else:assert (root/l.STOP_REQUEST).exists()


def test_credential_explicit_private_bounded_no_env_fallback(tmp_path,monkeypatch,capsys):
    token='123456:'+'x'*24;root=tmp_path/'credentials';root.mkdir(mode=0o700);p=root/'telegram';p.write_text(token+'\n');p.chmod(0o400)
    assert service.credential(str(root))==token
    p.chmod(0o444)
    with pytest.raises(r.RecoveryStop,match='PERMISSIONS'):service.credential(str(root))
    p.unlink();p.symlink_to(tmp_path/'elsewhere')
    with pytest.raises(OSError):service.credential(str(root))
    monkeypatch.setenv('TELEGRAM_BOT_TOKEN',token)
    with pytest.raises(r.RecoveryStop,match='DIRECTORY_REQUIRED'):service.credential(None)
    monkeypatch.setattr(service,'run',lambda *a:(_ for _ in ()).throw(RuntimeError(token)))
    assert service.main(['run','--plan','synthetic','--control-dir','synthetic'])==78
    assert token not in capsys.readouterr().out


def test_templates_keep_limits_and_real_secure_boot_credential(tmp_path):
    path=Path(__file__).resolve().parents[1]/'deploy/render_price.py'
    spec=importlib.util.spec_from_file_location('render_price',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    out=module.render('/opt/holy-weather/releases/'+'a'*40,tmp_path/'rendered')
    unit=(out/'holy-weather-price@.service').read_text()
    assert '@RELEASE@' not in unit and 'RestartPreventExitStatus=78' in unit
    assert 'SendSIGKILL=no' in unit and 'LoadCredentialEncrypted=telegram:' in unit
    assert 'ExecStop=' in unit and 'KillMode=control-group' in unit
    for region in ('europe','us'):
        plan=r.Plan.load(json.loads((out/(region+'.json')).read_text()))
        assert plan.continuous and plan.session_day is None and plan.deadline_ms is None
        assert plan.config.quote_age_seconds==90 and plan.config.storage_bytes==256*1024**2
        assert not plan.config.weather_context and not plan.config.lamp_context
        assert plan.config.price_notification_filter
        assert (out/(region+'.json')).stat().st_mode&0o077==0


@pytest.mark.parametrize('new_stop,expected',[(False,78),(True,0)])
def test_service_distinguishes_latched_stop_from_current_sigterm(gamma,tmp_path,monkeypatch,new_stop,expected):
    plan,_=seed(gamma,tmp_path);path=tmp_path/'plan.json';path.write_text(canonical(replace(rolling(plan),deadline_ms=None).value()))
    monkeypatch.setattr(service.signal,'signal',lambda *a:None)
    def supervise(*args,**kwargs):
        if new_stop:kwargs['stop'].set()
        return dict(status='STOPPED',attempts=[])
    monkeypatch.setattr(service,'supervise',supervise)
    assert service.run(path,tmp_path/'control')==expected


def immutable_rows(s):
    return {name:list(s.c.execute('select * from '+name+' order by 1,2')) for name in
            ('inputs','samples','events','event_inputs','state','latest','market')}


@pytest.mark.parametrize('base',['81d00eed40ee179481822001f693a544d0fa2d29',
                                '9adf061e412f73246cb7d83dffb30ead68792e24',
                                '2dcbae183784e7d49a736da18a2508d125d7750e'])
def test_current_data_forward_and_old_code_rollback_after_new_rows(gamma,tmp_path,identity,base):
    finite,m=seed(gamma,tmp_path);root,state=prepared(tmp_path,finite,active=False,status='STOPPED')
    r.write(root,'plan.json',dict(plan=finite.value(),identity=state['identity'],code=state['identity']['code']))
    with CatalogueStore(finite.config) as s:
        s.connection_started(AT);router=Router(s);feed(router,m,'0.49',AT+1);feed(router,m,'0.71',AT+2)
        key=s.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        p=preview(s,key);assert claim(s,p,AT+3);finish(s,p,{'status':'SENT','message_id':7},AT+4)
        s.set_meta('alert_delivery_v1:uncertain',{'status':'UNKNOWN','track':'PRICE_ALERT','claimed_ms':AT+4})
        rows=immutable_rows(s);receipt=s.c.execute('select value from meta where key=?',(key,)).fetchone()[0]
        fired=dict(s.c.execute("select key,value from meta where key like 'price_alert_v1:%'"))
    dead=[dict(OWNER,pid=99)];plan=rolling(finite)
    result=t.rebind(plan,root,state['identity'],dead,tmp_path/'forward-backup',at=AT+5)
    b=sqlite3.connect(tmp_path/'forward-backup/monitor.sqlite3');assert b.execute('pragma integrity_check').fetchone()[0]=='ok';b.close()
    assert digest(tmp_path/'forward-backup/monitor.sqlite3')==result['backup_sha256']
    with CatalogueStore(plan.config,session=False) as s:
        assert immutable_rows(s)==rows and dict(s.c.execute("select key,value from meta where key like 'price_alert_v1:%'"))==fired
        assert s.c.execute('select value from meta where key=?',(key,)).fetchone()[0]==receipt
        assert r.prepare_resume(s,result['binding'],AT+6)  # UNKNOWN remains disabling, never resent.
        s.connection_started(AT+7);router=Router(s)
        feed(router,m,'0.75',AT+8);feed(router,m,'0.81',AT+9)
        rows=immutable_rows(s)
        assert len(rows['events'])>1
    # Extract exact prior source locally; no GitHub/network/production access.
    old=tmp_path/'old';old.mkdir()
    names=subprocess.check_output(['git','ls-tree','-r','--name-only',base,'src/holy_weather'],text=True).splitlines()
    for name in names:
        if name.endswith('.py'):
            dst=old/Path(name).relative_to('src');dst.parent.mkdir(parents=True,exist_ok=True)
            dst.write_bytes(subprocess.check_output(['git','show',base+':'+name]))
    env=dict(os.environ,PYTHONPATH=str(old),PYTHONDONTWRITEBYTECODE='1')
    oldcode=subprocess.check_output([sys.executable,'-B','-c','from holy_weather.recovery import code_identity;print(code_identity())'],env=env,text=True).strip()
    # Finite rollback has an explicitly bounded date/deadline and archives off:
    # do not reinstall the old archive defect or pretend it supports rolling.
    fallback=replace(finite,archive_dir=None)
    rolled=t.rebind(fallback,root,result['binding'],dead,tmp_path/'rollback-backup',at=AT+10,target_code=oldcode)
    script="""import io,json,os,sys
from holy_weather import recovery as r
from holy_weather.recovery import Plan,preflight,prepare_resume
from holy_weather.catalogue import CatalogueStore
from holy_weather.core import canonical
v=json.load(open(sys.argv[1]));plan=Plan.load(v['plan']);assert preflight(plan)==v['identity']
ticks=[int(sys.argv[2])]
r.time.time_ns=lambda:ticks[0]*1000000
r.observe_catalogue=lambda cfg,**kwargs:{'state':'COMPLETED_WINDOW'}
class Child:
 pid=os.getpid();stdin=io.StringIO();done=False
 def poll(self):
  if not self.done:
   r.worker(__import__('pathlib').Path(sys.argv[1]).parent,self.nonce,None)
   self.done=True;ticks[0]=plan.deadline_ms+1
  return 0
def spawn(root,nonce,token):
 child=Child();child.nonce=nonce;return child
assert r.supervise(plan,__import__('pathlib').Path(sys.argv[1]).parent,clock=lambda:ticks[0],_spawn=spawn)['status']=='DEADLINE'
with CatalogueStore(plan.config,session=False) as s:
 assert prepare_resume(s,v['identity'],int(sys.argv[2]))
 assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
 assert not s.c.execute('pragma foreign_key_check').fetchall()
 assert s.c.execute('pragma user_version').fetchone()[0]==1
 print(canonical({'events':[tuple(r) for r in s.c.execute('select * from events order by id')],'thresholds':[tuple(r) for r in s.c.execute("select key,value from meta where key like 'price_alert_v1:%' order by key")]}))
"""
    output=json.loads(subprocess.check_output([sys.executable,'-B','-c',script,str(root/'plan.json'),str(AT+11)],env=env,text=True))
    assert len(output['events'])==len(rows['events'])
    with CatalogueStore(fallback.config,session=False) as s:
        assert immutable_rows(s)==rows
        assert s.c.execute('select value from meta where key=?',(key,)).fetchone()[0]==receipt
        assert json.loads(s.c.execute("select value from meta where key='alert_delivery_v1:uncertain'").fetchone()[0])['status']=='UNKNOWN'
    assert rolled['binding']['code']==oldcode
    assert json.loads((tmp_path/'rollback-backup/MANIFEST.json').read_text())['source']==result['binding']


def test_transition_unknown_write_latches_and_active_writer_refuses(gamma,tmp_path,identity,monkeypatch):
    plan,_=seed(gamma,tmp_path);root,state=prepared(tmp_path,plan,active=False,status='STOPPED')
    r.write(root,'plan.json',dict(plan=plan.value(),identity=state['identity'],code=state['identity']['code']))
    with CatalogueStore(plan.config):
        with pytest.raises(BlockingIOError):t.rebind(rolling(plan),root,state['identity'],[dict(OWNER,pid=99)],tmp_path/'refused')
    original=t.write
    def fail(root,name,value):
        if name=='plan.json':raise OSError('synthetic failure after commit')
        original(root,name,value)
    monkeypatch.setattr(t,'write',fail)
    with pytest.raises(OSError):t.rebind(rolling(plan),root,state['identity'],[dict(OWNER,pid=99)],tmp_path/'partial')
    assert (root/'transition.json').exists()
    with pytest.raises(r.RecoveryStop,match='UNFINISHED_TRANSITION'):r.supervise(rolling(plan),root)
    assert (tmp_path/'partial/monitor.sqlite3').exists()


def test_moving_active_data_is_refused_before_any_new_path(gamma,tmp_path):
    plan,_=seed(gamma,tmp_path);destination=tmp_path/'stable';destination.mkdir(mode=0o700)
    with CatalogueStore(plan.config,session=False):
        with pytest.raises(BlockingIOError):t.move_closed(plan.config.data_dir,destination/'data','data')
    assert not (destination/'data').exists() and not (destination/'data').is_symlink()


def test_manual_resume_requires_explicit_closed_stop_and_preserves_debt(gamma,tmp_path,identity):
    plan,_=seed(gamma,tmp_path);plan=rolling(plan);root,state=prepared(tmp_path,plan,active=False,status='STOPPED')
    state['restarts']=[AT-1000];r.write(root,'state.json',state)
    r.write(root,'plan.json',dict(plan=plan.value(),identity=state['identity'],code=state['identity']['code']))
    service.record_stop(root,environment={'SERVICE_RESULT':'success'},manager_state='running')
    with pytest.raises(r.RecoveryStop,match='ALREADY_BOUND'):
        t.rebind(plan,root,state['identity'],[dict(OWNER,pid=99)],tmp_path/'not-authorized')
    result=t.rebind(plan,root,state['identity'],[dict(OWNER,pid=99)],tmp_path/'explicit-resume',resume_manual=True)
    assert result['binding']==state['identity']
    after=json.loads((root/'state.json').read_text())
    assert after['status']=='PREPARING' and after['restarts']==[AT-1000]
    assert not (root/l.STOP_REQUEST).exists()
    with CatalogueStore(plan.config,session=False) as s:
        raw=json.loads(s.c.execute('select value from meta where key=?',(result['audit_key'],)).fetchone()[0])
        assert json.loads(raw['files'][l.STOP_REQUEST])['reason']=='MANUAL_STOP'


@pytest.mark.parametrize('failure',['UNKNOWN_EXIT','FATAL','CRASH'])
def test_shutdown_or_manual_ack_never_overrides_last_child_failure(gamma,tmp_path,identity,failure):
    plan,_=seed(gamma,tmp_path);plan=rolling(plan);root,state=prepared(tmp_path,plan,active=False,status='STOPPED')
    state['attempts']=[dict(nonce='a'*32,state=failure,finished_ms=AT,error='StorageStop' if failure=='FATAL' else None)]
    r.write(root,'state.json',state)
    r.write(root,'plan.json',dict(plan=plan.value(),identity=state['identity'],code=state['identity']['code']))
    service.record_stop(root,environment={'SERVICE_RESULT':'success'},manager_state='running')
    with pytest.raises(r.RecoveryStop,match='CLEAN_MANUAL_STOP_REQUIRED'):
        t.rebind(plan,root,state['identity'],[dict(OWNER,pid=99)],tmp_path/'refused',resume_manual=True)
    assert not (tmp_path/'refused').exists()
    request=json.loads((root/l.STOP_REQUEST).read_text());request['reason']='HOST_SHUTDOWN';r.write(root,l.STOP_REQUEST,request)
    with patch.object(l,'own_identity',return_value=dict(OWNER,boot='next-boot')):
        assert not l.reconcile(root,state,AT+1,r.write,lambda:pytest.fail('fatal recovery'))


@pytest.mark.skipif(sys.platform!='linux',reason='Linux renameat2 atomic directory exchange in CI')
def test_stable_move_keeps_lock_inode_and_rejects_old_paths(gamma,tmp_path):
    plan,_=seed(gamma,tmp_path);source=Path(plan.config.data_dir);inode=(source/'.writer.lock').stat().st_ino
    before=digest(source/'monitor.sqlite3');stable=tmp_path/'stable';stable.mkdir(mode=0o700)
    result=t.move_closed(source,stable/'data','data')
    assert source.is_symlink() and (stable/'data/.writer.lock').stat().st_ino==inode
    assert digest(stable/'data/monitor.sqlite3')==before
    with pytest.raises(Exception,match='SYMLINK|PRIVATE'):CatalogueStore(plan.config,session=False)
    with CatalogueStore(replace(plan.config,data_dir=str(stable/'data')),session=False) as s:
        assert s.c.execute('pragma integrity_check').fetchone()[0]=='ok'
    assert result['destination']==str(stable/'data')
