import requests
import maya
from datetime import datetime, timedelta
import json


payload = {
  "size": 1000,

  "query": {
    "filtered": {
      "query": {
        "match_all": {}
      }
    }
  },
  "_source": [
    "applianceId",
    "@timestamp",
    "type",
    "online"
  ],
}

days = []
for i in range(1, 8):
    d = datetime.now() - timedelta(days=i)
    days.append(d.strftime('%Y.%m.%d'))

for d in days:
    data = json.dumps(payload)
    path = 'http://127.0.0.1:9333/logstash-proserver-{0}/online/_search'.format(d)
    qs = 'search_type=scan&scroll=1m'
    url = '{0}?{1}'.format(path, qs)
    r = requests.post(url=url, data=data)
    hits = json.loads(r.text)['hits']
    print hits['total']

    while True:
        url = "{0}?scroll=1m&scroll_id={1}".format(path, json.loads(r.text)['_scroll_id'])
        r = requests.post(url=url, data=data)
        print r.text

        hits = json.loads(r.text)['hits']
        if len(hits['hits']) == 0:
            print 'complete...'
            break
        for h in hits['hits']:
            _s = h['_source']
            t = _s['@timestamp']
            _s['epoch'] = maya.parse(t).epoch
            print _s

            #dump2mysql

