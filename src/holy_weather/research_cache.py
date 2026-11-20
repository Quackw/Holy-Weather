"""Bounded regional research acceleration; SQLite text remains the source of truth."""
from collections import OrderedDict
import json
from .core import canonical,cached_price

_LIMIT=16*1024*1024
_IMMUTABLE=(str,int,float,bool,type(None))


def _encode(state, previous):
    """Byte-identical canonical JSON, reusing only verified flat point values."""
    values=state.get('points',[]);old=previous or {};encoded={};parts=[]
    for point in values:
        item=old.get(id(point))
        if item is not None and item[0]==point:
            raw=item[1]
        else:
            raw=canonical(point)
            item=(dict(point),raw)if all(type(v)in _IMMUTABLE for v in point.values())else None
        parts.append(raw)
        if item is not None:encoded[id(point)]=item
    points='['+','.join(parts)+']'
    raw='{'+','.join(canonical(k)+':'+(points if k=='points'else canonical(state[k]))for k in sorted(state))+'}'
    return raw,encoded


def _bounds(points):
    if not points:return None,None
    numbers=[cached_price(p['ask'])for p in points]
    return min(numbers),max(numbers)


class ResearchCache:
    """Private SQL-keyed objects. Store returns detached events, never these objects."""
    def __init__(self):
        self.items=OrderedDict();self.bytes=0

    def take(self,token,raw):
        item=self.items.pop(token,None)
        if item is not None:
            self.bytes-=item['charge']
            if item['raw']==raw:
                return item['state'],dict(points=item['state'].get('points',[]),low=item['low'],high=item['high'],encoded=item['encoded'],retained=False,hit=True,ordered=item['ordered'])
        state=json.loads(raw)if raw is not None else None
        points=state.get('points',[])if state else[]
        return state,dict(points=points,low=None,high=None,encoded={},retained=False,hit=False,ordered=False)

    def encode(self,state,hint):
        if hint['hit']:
            raw,encoded=_encode(state,hint['encoded'])
        else:raw,encoded=canonical(state),{}
        hint['next_encoded']=encoded
        return raw

    def keep(self,token,raw,state,hint):
        points=state.get('points',[]);before=hint['points']
        if before and hint['low'] is not None and (points is before or hint['retained']and len(points)in(len(before),len(before)+1)):
            number=cached_price(points[-1]['ask']);low=min(hint['low'],number);high=max(hint['high'],number)
        else:low,high=_bounds(points)
        ordered=bool(hint['retained']and hint['ordered']) or all(
            a['received_ms']<=b['received_ms']and a['source_ms']<=b['source_ms']
            for a,b in zip(points,points[1:]))
        # Includes conservative allowance for decoded objects and point snapshots.
        charge=len(raw.encode())*12+len(points)*256+1024
        if charge>_LIMIT:return
        previous=self.items.pop(token,None)
        if previous:self.bytes-=previous['charge']
        while self.items and(self.bytes+charge>_LIMIT or len(self.items)>=64):
            _,old=self.items.popitem(last=False);self.bytes-=old['charge']
        self.items[token]=dict(raw=raw,state=state,encoded=hint['next_encoded'],low=low,high=high,charge=charge,ordered=ordered)
        self.bytes+=charge
