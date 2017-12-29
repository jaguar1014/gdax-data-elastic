from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
import json

es = Elasticsearch()
ic = IndicesClient(es)


prop_body = json.loads('{"properties":{"time":{"type":"date","format":"epoch_millis"}}}')

ic.put_mapping(doc_type='ltc-data',body=json.dumps(prop_body))
