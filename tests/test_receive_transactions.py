"""Durability, failure and frame-loss regressions for bounded EU receive slices."""
from contextlib import closing,nullcontext
from dataclasses import replace
import json,os,sqlite3,subprocess,sys
from pathlib import Path
import pytest
from holy_weather import catalogue,history
from holy_weather.storage import StorageStop,Store
from holy_weather.alert_queue import preview,claim,finish
from holy_weather.telegram import Sender
from test_catalogue import AT,setup,feed


def current_counts(s):
    with closing(sqlite3.connect(s.db_path.as_uri()+'?mode=ro',uri=True))as c:
        c.execute('pragma query_only=on')
        return c.execute('select count(*)from samples').fetchone()[0],c.execute("select count(*)from events where type='PRICE_ALERT'").fetchone()[0]


def init(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,1);s=catalogue.CatalogueStore(cfg,session=False)
    s.bind(*markets[0]);s.connection_started(AT)
    return s,markets[0][0],catalogue.Router(s)


def test_bounded_receive_matches_all_tables_and_commits_less(gamma,tmp_path):
    cfg,markets=setup(gamma,tmp_path,2);proof=[];commits=[]
    for batched in(False,True):
        with catalogue.CatalogueStore(replace(cfg,data_dir=str(tmp_path/str(batched))),session=False)as s:
            for m,receipt in markets:s.bind(m,receipt)
            s.connection_started(AT);r=catalogue.Router(s);sql=[];s.c.set_trace_callback(sql.append)
            for start in range(0,240,8):
                with s.receive_batch()if batched else nullcontext():
                    for n in range(start,start+8):
                        feed(r,markets[n%2][0],('0.49','0.56','0.71','0.81')[(n//2)%4],AT+n+1)
                if start==120:s.gap('CONTROL',AT+start+8);s.connection_started(AT+start+8)
            s.c.set_trace_callback(None);history.check(s.c);proof.append(history.fingerprint(s.c));commits.append(sum(x.lower()=='commit'for x in sql))
    assert proof[0]==proof[1]and commits[1]<commits[0]


def test_receive_slice_flushes_before_external_side_effect(gamma,tmp_path):
    s,m,r=init(gamma,tmp_path);seen=[]
    def transport(token,card):
        with closing(sqlite3.connect(s.db_path.as_uri()+'?mode=ro',uri=True))as c:
            c.execute('pragma query_only=on')
            statuses=[json.loads(raw)['status']for raw, in c.execute("select value from meta where key like 'alert_delivery_v1:%'")]
            assert statuses==['UNKNOWN']
        seen.append(card);return{'status':'SENT','message_id':41}
    sender=Sender('synthetic',transport)
    with s:
        with s.receive_batch():
            feed(r,m,'0.49',AT+1);feed(r,m,'0.81',AT+2)
            assert current_counts(s)==(0,0)
            sender.tick(s,AT+3);sender.thread.join(1);assert not sender.thread.is_alive()
            sender.tick(s,AT+4)
        sender.close(s);assert len(seen)==1
        assert json.loads(s.c.execute("select value from meta where key like 'alert_delivery_v1:%'").fetchone()[0])['status']=='SENT'
        sender=Sender('synthetic',lambda *a:pytest.fail('SENT repeated'));sender.tick(s,AT+5);sender.close(s)


def test_failed_input_savepoint_keeps_previous_input_and_no_partial_cross(gamma,tmp_path):
    s,m,r=init(gamma,tmp_path)
    with s:
        feed(r,m,'0.49',AT+1)
        s.c.execute("CREATE TEMP TRIGGER fail BEFORE INSERT ON meta WHEN NEW.key LIKE 'alert_delivery_v1:%' BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with pytest.raises(sqlite3.IntegrityError),s.receive_batch():
            feed(r,m,'0.48',AT+2);feed(r,m,'0.81',AT+3)
        assert current_counts(s)==(2,0)
        assert not s.c.execute("select 1 from meta where key like 'alert_delivery_v1:%'").fetchone()
        s.c.execute('DROP TRIGGER fail');feed(r,m,'0.81',AT+4)
        assert current_counts(s)==(3,1)


def test_commit_failure_is_not_retried_and_original_records_remain(gamma,tmp_path):
    s,m,r=init(gamma,tmp_path);original=s.c;calls=[]
    class FailingCommit:
        def __getattr__(self,k):return getattr(original,k)
        def execute(self,sql,*a):
            if sql.lower()=='commit':calls.append(sql);raise sqlite3.OperationalError('fixture commit failure')
            return original.execute(sql,*a)
    with s:
        before=current_counts(s);s.c=FailingCommit()
        with pytest.raises(sqlite3.OperationalError,match='fixture commit'),s.receive_batch():feed(r,m,'0.49',AT+1)
        assert len(calls)==1 and not original.in_transaction and current_counts(s)==before
        s.c=original


@pytest.mark.parametrize('bad',['directory','symlink','budget'])
def test_receive_guard_refusal_rolls_back_only_uncommitted_input(gamma,tmp_path,bad):
    s,m,r=init(gamma,tmp_path)
    with s:
        before=current_counts(s)
        with pytest.raises(StorageStop),s.receive_batch():
            feed(r,m,'0.49',AT+1)
            if bad=='directory':(s.root/'unexpected').mkdir()
            elif bad=='symlink':(s.root/'unexpected').symlink_to(s.db_path)
            else:
                with(s.root/'padding').open('wb')as f:f.truncate(s.cfg.storage_bytes-s.reserve)
            feed(r,m,'0.48',AT+2)
        assert current_counts(s)==before and not s.c.in_transaction


@pytest.mark.parametrize('large',['raw','state'])
def test_large_input_keeps_single_input_durability(gamma,tmp_path,large):
    s,m,r=init(gamma,tmp_path)
    with s:
        feed(r,m,'0.49',AT+1)
        if large=='state':
            row=s.c.execute('select token,payload from state').fetchone();v=json.loads(row['payload']);v['padding']='x'*70000
            with s.transaction():s.c.execute('update state set payload=?where token=?',(json.dumps(v),row['token']))
        with s.receive_batch():
            raw=dict(event_type='best_bid_ask',market=m['condition_id'],asset_id=m['tokens']['YES'],timestamp=str(AT+2),best_bid='0.48',best_ask='0.48')
            if large=='raw':raw['padding']='x'*20000
            r.feed(json.dumps(raw),AT+2);assert current_counts(s)==(2,0)
        assert not s.c.in_transaction


def test_non_receive_write_and_nested_batch_have_explicit_barriers(gamma,tmp_path):
    s,m,r=init(gamma,tmp_path)
    with s:
        with s.receive_batch():
            feed(r,m,'0.49',AT+1);assert current_counts(s)[0]==0
            s.set_meta('fixture_barrier',True);assert current_counts(s)[0]==1
            with pytest.raises(StorageStop,match='NESTED_RECEIVE'): 
                with s.receive_batch():pass
        assert s.c.execute('pragma synchronous').fetchone()[0]==2


def test_non_catalogue_receive_scope_keeps_original_commit(cfg):
    with Store(cfg,session=False)as s:
        with s.receive_batch():
            with s.transaction():s.c.execute("insert into meta values('synthetic','true')")
            assert not s.c.in_transaction


@pytest.mark.parametrize('stop_after',[None,10])
def test_receiver_drains_batches_without_losing_ninth_frame(gamma,tmp_path,stop_after):
    cfg,markets=setup(gamma,tmp_path,1);m,receipt=markets[0];n=[0];mono=[0.0];now=[AT];timeouts=[]
    class Stop:
        def is_set(self):return stop_after is not None and n[0]>=stop_after
        def wait(self,seconds):mono[0]+=seconds;return self.is_set()
    class WS:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,raw):pass
        def recv(self,timeout):
            timeouts.append(timeout)
            if n[0]>=17:mono[0]+=1;raise TimeoutError()
            n[0]+=1;mono[0]+=.0001;now[0]=AT+n[0]
            return json.dumps(dict(event_type='best_bid_ask',market=m['condition_id'],asset_id=m['tokens']['YES'],timestamp=str(now[0]),best_bid='0.48'if n[0]%2 else'0.49',best_ask='0.50'))
    result=catalogue.observe_catalogue(replace(cfg,duration_seconds=2),connect=lambda *a,**k:WS(),discover=lambda city:([m]if city=='london'else[],receipt,[]),clock=lambda:now[0],monotonic=lambda:mono[0],stop=Stop())
    count=stop_after or 17
    assert n[0]==count and result['transport']['snapshot']['frames']==count
    assert result['counters']['router']['routed_messages']==count and 0 in timeouts


def test_fresh_repair_trace_preserves_prior_one_shot_and_refuses_partial(gamma,tmp_path):
    from types import SimpleNamespace
    from holy_weather import ws_diagnostic as d
    s,m,r=init(gamma,tmp_path)
    with s:
        (s.root/d.ARM).write_bytes(d.ARM_CONTENT);(s.root/d.ARM).chmod(0o600)
        prior=b'prior immutable one-shot evidence';(s.root/d.OUTPUT).write_bytes(prior)
        assert d.armed(s)is None
        capture=d.armed(s,receive_repair=True);assert capture is not None
        capture.add('SAMPLE',1,0,0,0,None,None,None)
        result=capture.persist(s,SimpleNamespace(sent=SimpleNamespace(code=1011),rcvd=None))
        assert result['status']=='SAVED'and result['file']==d.REPAIR_OUTPUT
        assert (s.root/d.OUTPUT).read_bytes()==prior
        assert (s.root/d.REPAIR_OUTPUT).stat().st_size<=d.MAX_FILE
        assert d.armed(s,receive_repair=True)is None
    cfg,_=setup(gamma,tmp_path/'partial')
    with catalogue.CatalogueStore(cfg,session=False)as s:
        (s.root/d.ARM).write_bytes(d.ARM_CONTENT);(s.root/d.ARM).chmod(0o600)
        (s.root/d.PENDING).write_bytes(b'uncertain original evidence')
        assert d.armed(s,receive_repair=True)is None


@pytest.mark.parametrize('phase',['before_commit','after_commit','after_claim'])
def test_crash_preserves_old_sent_and_never_repeats_claim(gamma,tmp_path,phase):
    expected={'before_commit':None,'after_commit':'PENDING','after_claim':'UNKNOWN'}[phase]
    s,m,r=init(gamma,tmp_path)
    with s:
        feed(r,m,'0.49',AT+1);feed(r,m,'0.81',AT+2)
        old_key=s.c.execute("select key from meta where key like 'alert_delivery_v1:%'").fetchone()[0]
        card=preview(s,old_key);assert claim(s,card,AT+3);finish(s,card,{'status':'SENT','message_id':77},AT+4)
        old_raw=s.c.execute('select value from meta where key=?',(old_key,)).fetchone()[0]
        data_dir=str(s.root)
    (tmp_path/'crash_fixture.json').write_text(json.dumps({'market':m,'data_dir':data_dir,'at':AT+100}))
    env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',PYTHONPATH=str(Path(__file__).resolve().parents[1]/'src'))
    child=subprocess.run([sys.executable,'-B',str(Path(__file__).with_name('receive_crash_probe.py')),str(tmp_path),phase],env=env,capture_output=True,text=True,timeout=30)
    assert child.returncode==71,child.stdout+child.stderr
    with catalogue.CatalogueStore(replace(catalogue.CatalogueConfig(),data_dir=data_dir),session=False)as store:
        history.check(store.c)
        assert store.c.execute('select value from meta where key=?',(old_key,)).fetchone()[0]==old_raw
        extra=[json.loads(raw)['status']for key,raw in store.c.execute("select key,value from meta where key like 'alert_delivery_v1:%'")if key!=old_key]
        assert extra==([]if expected is None else[expected])
        if phase=='after_claim':
            sender=Sender('synthetic',lambda *a:pytest.fail('UNKNOWN replayed'))
            sender.tick(store,AT+200);sender.close(store)
        store.connection_started(AT+300);router=catalogue.Router(store)
        feed(router,m,'0.49',AT+301);feed(router,m,'0.81',AT+302)
        assert store.c.execute("select count(*)from events where type='PRICE_ALERT'").fetchone()[0]==(1 if phase=='before_commit'else 2)


def test_empty_queue_scan_flushes_pending_inputs_without_an_empty_write(gamma,tmp_path):
    from holy_weather.alert_queue import advance_waits
    s,m,r=init(gamma,tmp_path)
    with s:
        with s.receive_batch():
            feed(r,m,'0.49',AT+1);feed(r,m,'0.81',AT+2)
            sql=[];s.c.set_trace_callback(sql.append)
            advance_waits(s,AT+3)
            s.c.set_trace_callback(None)
            assert not s.c.in_transaction
            assert not any(x.lower().startswith('begin')for x in sql)
            assert any(x.lower()=='commit'for x in sql)
            with sqlite3.connect(s.db_path.as_uri()+'?mode=ro',uri=True)as other:
                assert other.execute("select count(*)from events where type='PRICE_ALERT'").fetchone()[0]==1
                assert other.execute("select count(*)from meta where key like 'alert_delivery_v1:%'").fetchone()[0]==1
