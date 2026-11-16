"""Subprocess restart coverage; historical source trees can be supplied offline."""
import os,sys,subprocess,json
from pathlib import Path
from dataclasses import replace
import pytest
import websockets
from holy_weather.core import Config
from holy_weather.discovery import select
from conftest import NOW
from test_europe import european

@pytest.mark.parametrize('region',['europe','us'])
def test_regional_rollback_probe(gamma,tmp_path,region):
    city='london' if region=='europe' else 'new-york'
    doc=european(gamma,city) if region=='europe' else gamma
    market=select(doc,Config(city=city,bucket='13°C' if region=='europe' else '46-47°F'),now=NOW)
    (tmp_path/'market.json').write_text(json.dumps(market))
    current=str(Path(__file__).resolve().parents[1]/'src')
    old=os.environ.get('HOLY_WEATHER_ROLLBACK_'+region.upper()+'_SOURCE',current)
    for phase,source in [('old',old),('new',current),('rollback',old)]:
        env=dict(os.environ,PYTHONPATH=source+os.pathsep+str(Path(websockets.__file__).resolve().parents[1]),PYTHONDONTWRITEBYTECODE='1')
        result=subprocess.run([sys.executable,'-B',str(Path(__file__).with_name('regional_rollback_probe.py')),region,phase,str(tmp_path)],env=env,capture_output=True,text=True,timeout=30)
        assert result.returncode==0,result.stdout+result.stderr
        data=json.loads(result.stdout);assert data['price_events']=={'old':1,'new':2,'rollback':3}[phase]
