"""Opt-in transport reused from PR 5; no implicit sends, no uncertain retries."""
import argparse,json,os,re,queue,threading
from pathlib import Path
from html import unescape
from urllib.request import Request,urlopen
from urllib.error import HTTPError
from .core import Config,sha
from .storage import Store
from .weather import millis
from .alert_queue import TARGET,PREFIX,preview,claim,finish

HTTP_TIMEOUT_SECONDS=5
# Fits the controller's 30s termination grace and 60s STOPPING watchdog.
CLOSE_WAIT_SECONDS=HTTP_TIMEOUT_SECONDS+1

def post(token,preview,*,fetch=urlopen,clock=millis):
    """No logs of URL/token/response description; no retry, even on timeout/429."""
    if not preview['test'] and not preview['event_ms']<=clock()<=preview['expires_ms']:
        return {'status':'REJECTED','reason':'QUOTE_EXPIRED_BEFORE_HTTP'}
    try:
        body=json.dumps(dict(chat_id=TARGET,text=preview['html'],parse_mode='HTML',link_preview_options={'is_disabled':True},disable_notification=preview['test'])).encode()
        request=Request('https://api.telegram.org/bot'+token+'/sendMessage',data=body,headers={'Content-Type':'application/json'},method='POST')
        with fetch(request,timeout=HTTP_TIMEOUT_SECONDS)as r:
            raw=r.read(65537)
        if len(raw)>65536:return {'status':'UNKNOWN','reason':'RESPONSE_BOUND'}
        data=json.loads(raw)
        if data.get('ok') is not True:return {'status':'REJECTED','reason':'API_REJECTED'}
        m=data.get('result',{});chat=m.get('chat',{})
        if type(m.get('message_id')) is not int or m['message_id']<=0 or chat.get('username','').lower()!=TARGET[1:] or chat.get('type')!='channel' or type(chat.get('id')) is not int or type(m.get('date')) is not int or m['date']<=0:
            return {'status':'UNKNOWN','reason':'ACK_UNVERIFIED'}
        plain=unescape(re.sub(r'<[^>]+>','',preview['html']))
        link=unescape(re.search(r'<a href="([^"]+)">',preview['html'])[1])
        if m.get('text')!=plain or not any(e.get('type')=='text_link' and e.get('url')==link for e in m.get('entities',[])):
            return {'status':'UNKNOWN','reason':'ACK_CONTENT_UNVERIFIED'}
        return {'status':'SENT','message_id':m['message_id'],'chat_id':chat.get('id'),'telegram_date':m.get('date'),
                'response_text_sha256':sha(plain.encode()),'bucket_link_verified':True}
    except HTTPError as exc:
        return {'status':'UNKNOWN','reason':'HTTP_'+str(exc.code)}
    except Exception as exc:
        return {'status':'UNKNOWN','reason':type(exc).__name__}


def token_from_env():
    token=os.environ.get('HOLY_WEATHER_TELEGRAM_TOKEN','')
    if not re.fullmatch(r'\d+:[A-Za-z0-9_-]{20,}',token):raise ValueError('TELEGRAM_TOKEN_NOT_CONFIGURED')
    return token


class Sender:
    def __init__(self,token,transport=post,*,minimum_interval_ms=0):
        if type(minimum_interval_ms)is not int or not 0<=minimum_interval_ms<=60000:raise ValueError("SEND_INTERVAL_BOUND")
        self.minimum_interval_ms=minimum_interval_ms;self.next_send_ms=0
        self.token=token;self.transport=transport;self.thread=None;self.results=queue.Queue(maxsize=1);self.stopped=False
        self.closed=False;self.inflight=False;self.disabled_reason=None

    def restore_pacing(self,store):
        """Restore the remaining interval from the last durable claim in this store."""
        last=store.c.execute("select max(json_extract(value,'$.claimed_ms')) from meta where key like ?",
                             (PREFIX+'%',)).fetchone()[0]
        if last is not None:self.next_send_ms=max(self.next_send_ms,last+self.minimum_interval_ms)

    def status(self):
        return dict(telegram_enabled=self.disabled_reason is None,telegram_disabled_reason=self.disabled_reason)
    def _collect(self,store):
        try:
            p,r,completed=self.results.get_nowait()
        except queue.Empty:pass
        else:
            finish(store,p,r,completed)
            self.inflight=False
            if r['status']!='SENT':
                self.stopped=True
                self.disabled_reason='SEND_RESULT_UNKNOWN' if r['status']=='UNKNOWN' else 'SEND_RESULT_NOT_SENT'
    def tick(self,store,at):
        if self.closed:return
        from .alert_queue import advance_waits
        advance_waits(store,at)
        self._collect(store)
        if self.stopped or (self.thread and self.thread.is_alive()) or at<self.next_send_ms:return
        row=store.c.execute(f"select key from {store.active_delivery_source} where key like ? and json_extract(value,'$.status')='PENDING' and coalesce(json_extract(value,'$.not_before_ms'),0)<=? order by coalesce(json_extract(value,'$.priority'),0) desc,json_extract(value,'$.event_ms'),key limit 1",(PREFIX+'%',at)).fetchone()
        if not row:return
        p=preview(store,row[0])
        if not claim(store,p,at):return
        self.inflight=True
        self.next_send_ms=at+self.minimum_interval_ms
        def send():
            try:r=self.transport(self.token,p)
            except Exception as e:r={'status':'UNKNOWN','reason':type(e).__name__}
            self.results.put_nowait((p,r,millis()))
        self.thread=threading.Thread(target=send,daemon=True);self.thread.start()
    def close(self,store):
        if self.closed:return self.status()
        self.closed=True;self.stopped=True  # No new claims, including a later tick.
        if self.thread and self.inflight:self.thread.join(CLOSE_WAIT_SECONDS)
        self._collect(store)  # The SQLite writer still owns Store here.
        if self.inflight:self.disabled_reason='SEND_RESULT_UNKNOWN_AT_CLOSE'
        return self.status()


def main():
    a=argparse.ArgumentParser();a.add_argument('command',choices=['preview','send']);a.add_argument('--config',required=True)
    a.add_argument('--key');a.add_argument('--test',action='store_true');a.add_argument('--preview-file');a.add_argument('--preview-sha')
    args=a.parse_args()
    with Store(Config.load(args.config),session=False)as store:
        if args.command=='preview':result=preview(store,args.key,test=args.test)
        else:
            if not args.preview_file or not args.preview_sha:raise ValueError('EXACT_PREVIEW_REQUIRED')
            path=Path(args.preview_file)
            if path.stat().st_size>16384:raise ValueError('PREVIEW_BOUND')
            p=json.loads(path.read_text())
            if p.get('preview_sha256')!=args.preview_sha:raise ValueError('PREVIEW_SHA')
            token=token_from_env()
            result=finish(store,p,post(token,p),millis()) if claim(store,p,millis()) else {'status':'NOT_SENT_NOT_PENDING_OR_EXPIRED'}
        print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':main()
