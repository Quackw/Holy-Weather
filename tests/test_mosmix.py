import io,zipfile
import pytest
from holy_weather.mosmix import decode

AT=1794744000000 # 2026-11-15T12:00Z

def body(point='P0478',issue='2026-11-15T09:00:00Z',values='283.15 284.15',times=('2026-11-15T13:00:00Z','2026-11-15T14:00:00Z')):
    xml='<kml><IssueTime>'+issue+'</IssueTime>'+''.join('<TimeStep>'+t+'</TimeStep>' for t in times)+'<Placemark><name>'+point+'</name><Forecast elementName="TTT"><value>'+values+'</value></Forecast></Placemark></kml>'
    b=io.BytesIO()
    with zipfile.ZipFile(b,'w',zipfile.ZIP_DEFLATED) as z:z.writestr('forecast.kml',xml)
    return b.getvalue()

def run(b,**kw):return decode(b,station='EGLC',requested_ms=AT-1000,received_ms=AT,at_ms=AT,**kw)

def test_native_kelvin_and_original_issue():
    f=run(body());assert f['future_peak_c']=='11.00'
    assert f['issue_ms']<f['received_ms'] and f['freshness']=='NOT_QUALIFIED' and not f['alert_enabled']
    assert run(body())==f

@pytest.mark.parametrize('kwargs',[
    {'point':'03772'}, {'issue':'2026-11-15T15:00:00Z'}, {'values':'283.15'},
    {'times':('2026-11-15T14:00:00Z','2026-11-15T13:00:00Z')}, {'values':'9999 284.15'},
])
def test_rejects_invalid_source(kwargs):
    with pytest.raises(ValueError):run(body(**kwargs))

def test_missing_temperature_not_peak():assert run(body(values='- 284.15'))['future_peak_c'] is None

def test_no_future_receipt():
    with pytest.raises(ValueError,match='CLOCK'):decode(body(),station='EGLC',requested_ms=AT,received_ms=AT+1,at_ms=AT)
