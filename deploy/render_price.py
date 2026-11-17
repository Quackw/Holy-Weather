"""Render reviewable templates locally. Never install files or read a secret."""
import argparse,json,os
from pathlib import Path
from holy_weather.catalogue import CatalogueConfig
from holy_weather.recovery import Plan


def render(release,output):
    release=Path(release)
    if not release.is_absolute() or any(c.isspace() for c in str(release)) or '@' in str(release):raise ValueError('ABSOLUTE_RELEASE_REQUIRED')
    out=Path(output);out.mkdir(mode=0o700,parents=True,exist_ok=False)
    template=Path(__file__).with_name('holy-weather-price@.service.in').read_text()
    (out/'holy-weather-price@.service').write_text(template.replace('@RELEASE@',str(release)))
    for region,city in (('europe','london'),('us','new-york')):
        cfg=CatalogueConfig(region=region,city=city,data_dir=f'/var/lib/holy-weather/{region}/data',
                            telegram_enabled=True,price_notification_filter=True,max_reconnects=3)
        plan=Plan(cfg,None,None,archive_dir=f'/var/lib/holy-weather/{region}/archive',continuous=True)
        (out/(region+'.json')).write_text(json.dumps(plan.value(),sort_keys=True,indent=2)+'\n')
    for file in out.iterdir():file.chmod(0o600)
    return out


if __name__=='__main__':
    os.umask(0o077);p=argparse.ArgumentParser();p.add_argument('--release',required=True);p.add_argument('--output',required=True)
    a=p.parse_args();render(a.release,a.output)
