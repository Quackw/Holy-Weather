"""Explicit, Europe-only, one-shot WebSocket evidence. No SQLite or network I/O.

The receiver records scalars without waiting for this recorder's lock. A Python
sampler owns no database and never reads message bodies, locals or credentials.
Only the catalogue owner persists one bounded file, after a recorded 1011.
"""
from collections import OrderedDict, deque
import array
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import struct
import sys
import termios
import threading
import time

from websockets.frames import Frame, DATA_OPCODES, PONG, CLOSE

ARM = '.ws-diagnostic-v1.arm'
ARM_CONTENT = b'HOLY_WEATHER_EUROPE_WS_DIAGNOSTIC_V1\n'
OUTPUT = 'ws-diagnostic-v1.jsonl'
PENDING = '.ws-diagnostic-v1.pending'
REPAIR_OUTPUT = 'ws-diagnostic-receive-repair.jsonl'
REPAIR_PENDING = '.ws-diagnostic-receive-repair.pending'
WINDOW_NS = 60_000_000_000
MAX_FILE = 1024 * 1024
MIN_FREE = 16 * 1024**3  # Existing accepted installation's disk reserve.
RECORD = struct.Struct('<QII6q')
CAPACITY = 8192  # A preallocated 512 KiB ring, not an unbounded Python queue.
KINDS = {name: n for n, name in enumerate((
    'CONNECTION', 'PING_QUEUED', 'PING_FLUSHED', 'PONG_PARSED', 'PONG_PROCESSED',
    'READ_PAUSED', 'READ_RESUMED', 'SAMPLE', 'PENDING_PING', 'PHASE_INTERVAL', 'PHASE_CURRENT',
    'RECEIVER_INTERVAL', 'MAIN_INTERVAL', 'CLIENT_FAIL', 'REMOTE_CLOSE'), 1)}
PHASES = {name: n for n, name in enumerate((
    'CONNECT', 'HOUSEKEEPING', 'MARKETS', 'DELIVERY', 'STATS', 'APP_PING',
    'RECV', 'ROUTER', 'CLOSE'), 1)}


def optional(function):
    """Diagnostics may miss evidence; they must not change a protocol outcome."""
    def call(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception:
            owner = getattr(args[0], 'owner', args[0])
            owner.faults = getattr(owner, 'faults', 0)+1
            return None
    return call


def readable(sock):
    try:
        value = array.array('i', [0])
        fcntl.ioctl(sock.fileno(), termios.FIONREAD, value, True)
        return value[0] if value[0] >= 0 else -1
    except Exception:
        return -1


def stacks(roles):
    """Strong frame/code references under the GIL; no native timed stack walk."""
    frames = sys._current_frames()
    result = {}
    try:
        for role, ident in roles.items():
            frame = frames.get(ident)
            lines = []
            for _ in range(16):
                if frame is None:
                    break
                code = frame.f_code
                # Metadata only. Never format a frame or access f_locals.
                lines.append([Path(code.co_filename).name[:96], code.co_name[:96], frame.f_lineno])
                frame = frame.f_back
            result[role] = lines
    finally:
        frames.clear()
    return result


class Capture:
    def __init__(self, *, clock=time.monotonic_ns, receive_repair=False):
        self.output = REPAIR_OUTPUT if receive_repair else OUTPUT
        self.pending = REPAIR_PENDING if receive_repair else PENDING
        self.clock = clock
        self.buffer = bytearray(CAPACITY * RECORD.size)
        self.lock = threading.Lock()
        self.start = self.count = self.sequence = 0
        self.attempt = 0
        self.dropped_busy = self.dropped_capacity = self.retired_time = 0
        self.faults = 0
        self.finished = False
        self.main_ident = threading.get_ident()
        self.current = (0, clock())
        self.bins = [[0, 0, 0] for _ in range(len(PHASES) + 1)]
        self.main_counts = (0, 0, 0)
        self.sessions = []
        self.stack_samples = deque(maxlen=3)
        self.wall_anchor_ms = time.time_ns() // 1_000_000
        self.mono_anchor_ns = clock()
        self.result = {'status': 'ARMED'}

    @optional
    def add(self, kind, connection, *values):
        if self.finished:
            return
        if not self.lock.acquire(False):
            self.dropped_busy += 1
            return
        try:
            if self.finished:return
            now = self.clock()
            cutoff = now - WINDOW_NS
            while self.count and RECORD.unpack_from(self.buffer, self.start * RECORD.size)[0] < cutoff:
                self.start = (self.start + 1) % CAPACITY
                self.count -= 1
                self.retired_time += 1
            if self.count == CAPACITY:
                self.start = (self.start + 1) % CAPACITY
                self.count -= 1
                self.dropped_capacity += 1
            slot = (self.start + self.count) % CAPACITY
            numbers = [max(-1, min(int(v), 2**63-1)) if v is not None else -1 for v in values[:6]]
            numbers += [-1] * (6-len(numbers))
            RECORD.pack_into(self.buffer, slot*RECORD.size, now, KINDS[kind], connection, *numbers)
            self.count += 1
        finally:
            self.lock.release()

    @optional
    def phase(self, name):
        if self.finished:
            return
        now = self.clock()
        previous, started = self.current
        self.current = (PHASES[name], now)
        if not self.lock.acquire(False):
            self.dropped_busy += 1
            return
        try:
            if self.finished:return
            count, total, maximum = self.bins[previous]
            elapsed = max(0, now-started)
            self.bins[previous] = [count+1, total+elapsed, max(maximum, elapsed)]
        finally:
            self.lock.release()

    @optional
    def frame(self, size):
        count, total, maximum = self.main_counts
        self.main_counts = (count+1, total+size, max(maximum, size))

    @optional
    def persist(self, store, close):
        """One owner, one exclusive file. No DB mutation or evidence overwrite."""
        if self.finished:
            return self.result
        with self.lock:
            self.finished = True
        self.result = {'status': 'NOT_SAVED', 'reason': 'DIAGNOSTIC_FAILED'}
        root = store.root
        # Atomic publication briefly has two names; both fit the unchanged cap.
        if (len(list(root.iterdir())) > 30
                or store.usage()+2*MAX_FILE >= store.cfg.storage_bytes-store.reserve
                or shutil.disk_usage(root).free < MIN_FREE+MAX_FILE):
            self.result = {'status': 'NOT_SAVED', 'reason': 'DIAGNOSTIC_STORAGE_HEADROOM'}
            return self.result
        from .recovery import code_identity
        metadata = dict(format='HW_WS_DIAGNOSTIC_V1', pid=os.getpid(), code=code_identity(),
            wall_anchor_ms=self.wall_anchor_ms, mono_anchor_ns=self.mono_anchor_ns,
            saved_wall_ms=time.time_ns()//1_000_000, saved_mono_ns=self.clock(),
            window_ns=WINDOW_NS, buffer_bytes=len(self.buffer), max_file_bytes=MAX_FILE,
            record_order='newest_first', unavailable=-1, columns=['mono_ns','kind','connection','a','b','c','d','e','f'],
            kinds=KINDS, phases=PHASES, sessions=self.sessions, stacks=list(self.stack_samples),
            dropped_busy=self.dropped_busy, dropped_capacity=self.dropped_capacity, faults=self.faults,
            max_reconnects=store.cfg.max_reconnects,
            retired_older_than_window=self.retired_time, sent_close=getattr(getattr(close,'sent',None),'code',None),
            received_close=getattr(getattr(close,'rcvd',None),'code',None))
        temporary = root/self.pending
        target = root/self.output
        fd = None
        try:
            fd = os.open(temporary, os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, 0o600)
            digest = hashlib.sha256()
            size = written = 0
            def write(value, reserve=512):
                nonlocal size
                line = json.dumps(value, separators=(',', ':'), ensure_ascii=True).encode()+b'\n'
                if size+len(line) > MAX_FILE-reserve:
                    return False
                view = memoryview(line)
                while view:
                    sent = os.write(fd, view)
                    if sent <= 0:
                        raise OSError('short diagnostic write')
                    view = view[sent:]
                digest.update(line)
                size += len(line)
                return True
            if not write(metadata):
                raise ValueError('DIAGNOSTIC_HEADER_BOUND')
            for n in range(self.count-1, -1, -1):
                slot = (self.start+n) % CAPACITY
                record = RECORD.unpack_from(self.buffer, slot*RECORD.size)
                if record[0] < metadata['saved_mono_ns']-WINDOW_NS:
                    continue
                if not write(record):
                    break
                written += 1
            if not write(dict(end=True, records_written=written, retained_records=self.count,
                              omitted_for_file_bound_or_age=self.count-written), reserve=0):
                raise ValueError('DIAGNOSTIC_FOOTER_BOUND')
            os.fsync(fd)
            os.close(fd);fd = None
            os.link(temporary, target, follow_symlinks=False)  # Atomic no-replace publication.
            os.unlink(temporary)  # Only our newly created, verified temporary name.
            directory = os.open(root, os.O_RDONLY|os.O_DIRECTORY)
            try:os.fsync(directory)
            finally:os.close(directory)
            self.result = dict(status='SAVED', file=self.output, bytes=size, sha256=digest.hexdigest(), records=written)
        except Exception:
            # Preserve a partial file rather than silently retrying/overwriting.
            self.result = dict(status='NOT_SAVED', reason='DIAGNOSTIC_WRITE_OR_PUBLICATION_FAILED')
        finally:
            if fd is not None:
                os.close(fd)
        return self.result

    def factory(self, base):
        owner = self
        class DiagnosticConnection(base):
            def __init__(self, sock, protocol, **kwargs):
                self.diagnostic = Session(owner, self, protocol)
                self.diagnostic.hook_protocol(protocol)
                super().__init__(sock, protocol, **kwargs)
                self.diagnostic.hook_flow()

            def __enter__(self):
                value = super().__enter__()
                self.diagnostic.start()
                return value

            def process_event(self, event):
                super().process_event(event)
                if isinstance(event, Frame) and event.opcode is PONG:
                    self.diagnostic.pong_processed(bytes(event.data))

            def send_data(self):
                super().send_data()
                self.diagnostic.flushed()

            def __exit__(self, *args):
                owner.phase('CLOSE')
                try:return super().__exit__(*args)
                finally:self.diagnostic.stop()

        def create(*args, **kwargs):
            return base(*args, **kwargs) if owner.finished else DiagnosticConnection(*args, **kwargs)
        return create


class Session:
    def __init__(self, owner, ws, protocol):
        self.owner, self.ws = owner, ws
        owner.sequence += 1
        self.number = owner.sequence
        self.pending = OrderedDict()
        self.ping_sequence = 0
        self.pause_since = None
        self.rx_counts = (0, 0, 0)
        self.tls = (-1, 0)
        self.next_tls = 0
        self.stopped = threading.Event()
        self.thread = None
        self.captured = set()
        if len(owner.sessions) < 16:
            self.metadata = dict(number=self.number, observer_attempt=owner.attempt, websocket=str(protocol.id))
            owner.sessions.append(self.metadata)
        else:self.metadata = {}
        owner.add('CONNECTION', self.number)

    @optional
    def ping(self, data):
        if not self.owner.lock.acquire(False):
            self.owner.dropped_busy += 1;return
        try:
            if len(self.pending) >= 8:
                self.owner.dropped_busy += 1;return
            self.ping_sequence += 1
            sequence = self.ping_sequence
            self.pending[bytes(data)] = [sequence, self.owner.clock(), 0, 0]
        finally:self.owner.lock.release()
        self.owner.add('PING_QUEUED', self.number, sequence)

    @optional
    def flushed(self):
        if not self.pending:return
        if not self.owner.lock.acquire(False):
            self.owner.dropped_busy += 1;return
        try:
            now = self.owner.clock()
            sequences = []
            for value in self.pending.values():
                if not value[2]:value[2] = now;sequences.append(value[0])
        finally:self.owner.lock.release()
        for sequence in sequences:self.owner.add('PING_FLUSHED', self.number, sequence)

    @optional
    def parsed(self, frame):
        now = self.owner.clock()
        if frame.opcode in DATA_OPCODES:
            count, total, maximum = self.rx_counts
            size = len(frame.data)
            self.rx_counts = (count+1, total+size, max(maximum, size))
        elif frame.opcode is PONG:
            if self.owner.lock.acquire(False):
                try:
                    value = self.pending.get(bytes(frame.data))
                    if value:value[3] = now
                    sequence = value[0] if value else -1
                finally:self.owner.lock.release()
                self.owner.add('PONG_PARSED', self.number, sequence)
            else:self.owner.dropped_busy += 1
        elif frame.opcode is CLOSE:
            self.owner.add('REMOTE_CLOSE', self.number,
                           int.from_bytes(frame.data[:2], 'big') if len(frame.data)>=2 else -1)
        # Only the receiver calls SSL.pending(), never a concurrent socket read.
        if now >= self.next_tls:
            method = getattr(getattr(self.ws, 'socket', None), 'pending', None)
            self.tls = (method() if method else -1, now)
            self.next_tls = now+1_000_000_000

    @optional
    def pong_processed(self, data):
        if not self.owner.lock.acquire(False):
            self.owner.dropped_busy += 1;return
        try:
            value = self.pending.get(data)
            if value:
                covered = 0
                while self.pending:
                    key, _ = self.pending.popitem(last=False)
                    covered += 1
                    if key == data:break
            else:covered = 0
        finally:self.owner.lock.release()
        self.owner.add('PONG_PROCESSED', self.number, value[0] if value else -1, covered)

    @optional
    def hook_protocol(self, protocol):
        send, receive, fail = protocol.send_ping, protocol.recv_frame, protocol.fail
        def ping(data):
            self.ping(data)
            return send(data)
        def frame(value):
            self.parsed(value)
            return receive(value)
        def failure(code, reason=''):
            self.owner.add('CLIENT_FAIL', self.number, code, int(reason=='keepalive ping timeout'))
            return fail(code, reason)
        protocol.send_ping, protocol.recv_frame, protocol.fail = ping, frame, failure

    @optional
    def hook_flow(self):
        assembler = self.ws.recv_messages
        pause, resume = assembler.pause, assembler.resume
        def paused():
            pause()
            self.paused(assembler)
        def resumed():
            resume()
            self.resumed(assembler)
        assembler.pause, assembler.resume = paused, resumed

    @optional
    def paused(self, assembler):
        self.pause_since = self.owner.clock()
        self.owner.add('READ_PAUSED', self.number, assembler.frames.qsize())

    @optional
    def resumed(self, assembler):
        now = self.owner.clock()
        since, self.pause_since = self.pause_since, None
        self.owner.add('READ_RESUMED', self.number, assembler.frames.qsize(), now-since if since else -1)

    @optional
    def start(self):
        self.metadata.update(native_ping_interval=self.ws.ping_interval,native_ping_timeout=self.ws.ping_timeout,
                             queue_high=self.ws.recv_messages.high,queue_low=self.ws.recv_messages.low)
        self.interval_started = self.owner.clock()
        self.main_start = self.owner.main_counts
        with self.owner.lock:
            self.owner.bins = [[0,0,0] for _ in self.owner.bins]
            self.owner.current = (PHASES['CONNECT'],self.interval_started)
        self.thread = threading.Thread(target=self.watch, name='hw-ws-diagnostic', daemon=True)
        self.thread.start()

    @optional
    def watch(self):
        previous_rx, previous_main = (0,0,0), self.main_start
        next_stats = 0
        while not self.stopped.wait(.1) and not self.owner.finished:
            owner = self.owner
            now = owner.clock()
            tls, sampled = self.tls
            owner.add('SAMPLE', self.number, self.ws.recv_messages.frames.qsize(),
                int(self.ws.recv_messages.paused), readable(self.ws.socket), tls,
                now-sampled if sampled else -1, now-self.pause_since if self.pause_since else -1)
            phase, entered = owner.current
            owner.add('PHASE_CURRENT', self.number, phase, entered, now-entered)
            if owner.lock.acquire(False):
                try:
                    pending = [tuple(v) for v in self.pending.values()]
                    bins = owner.bins if now>=next_stats else None
                    if bins is not None:owner.bins = [[0,0,0] for _ in owner.bins]
                finally:owner.lock.release()
            else:
                owner.dropped_busy += 1;continue
            self.captured.intersection_update(v[0] for v in pending)
            for sequence, queued, sent, parsed in pending:
                owner.add('PENDING_PING', self.number, sequence, queued, sent or -1, parsed or -1, now-queued)
                if now-queued>=15_000_000_000 and sequence not in self.captured:
                    self.captured.add(sequence)
                    roles = dict(main=owner.main_ident, receiver=self.ws.recv_events_thread.ident,
                                 keepalive=self.ws.keepalive_thread.ident if self.ws.keepalive_thread else None)
                    owner.stack_samples.append(dict(mono_ns=now,connection=self.number,ping=sequence,
                                                    threads=stacks(roles)))
            if bins is not None:
                for phase, (count, total, maximum) in enumerate(bins):
                    if count:owner.add('PHASE_INTERVAL', self.number, phase, count, total, maximum,
                                       self.interval_started,now)
                for name, values, previous in [('RECEIVER_INTERVAL',self.rx_counts,previous_rx),
                                                ('MAIN_INTERVAL',owner.main_counts,previous_main)]:
                    owner.add(name,self.number,values[0]-previous[0],values[1]-previous[1],values[2],
                              self.interval_started,now)
                previous_rx, previous_main = self.rx_counts, owner.main_counts
                self.interval_started = now
                next_stats = now+1_000_000_000

    @optional
    def stop(self):
        self.stopped.set()
        if self.thread:self.thread.join(.25)
        self.metadata['sampler_stopped'] = self.thread is None or not self.thread.is_alive()


def armed(store, *, receive_repair=False):
    """No config/schema migration or inherited credential/environment channel."""
    if store.cfg.region != 'europe':
        return None
    try:
        repaired = False
        if receive_repair:
            if any((store.root/name).exists() for name in (PENDING, REPAIR_PENDING, REPAIR_OUTPUT)):
                return None
            repaired = (store.root/OUTPUT).exists()
        elif (store.root/OUTPUT).exists() or (store.root/PENDING).exists():
            return None
        fd = os.open(store.root/ARM, os.O_RDONLY|os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid()
                    or info.st_mode&0o077 or info.st_size!=len(ARM_CONTENT)):
                return None
            if os.read(fd, 64)!=ARM_CONTENT:return None
        finally:os.close(fd)
        return Capture(receive_repair=repaired)
    except Exception:
        return None
