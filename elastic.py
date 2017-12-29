from datetime import datetime
from elasticsearch import Elasticsearch
import json
import gdax
from elasticsearch.client import IndicesClient


es = Elasticsearch()
ic = IndicesClient(es)

#add data type mapping
ltc_prop_body = json.loads('{"mappings":{"ltc-data":{"properties":{"time":{"type":"date","format":"epoch_second"}}}}}')
btc_prop_body = json.loads('{"mappings":{"btc-data":{"properties":{"time":{"type":"date","format":"epoch_second"}}}}}')
eth_prop_body = json.loads('{"mappings":{"eth-data":{"properties":{"time":{"type":"date","format":"epoch_second"}}}}}')
# Create new indices if not created
ic.create(index='gdax-ltc', body=json.dumps(ltc_prop_body))
ic.create(index='gdax-btc', body=json.dumps(btc_prop_body))
ic.create(index='gdax-eth', body=json.dumps(eth_prop_body))
public_client = gdax.PublicClient()

# Get product historic rates for Ether, BTC, and LTC
ethData = public_client.get_product_historic_rates('ETH-USD')
ltcData = public_client.get_product_historic_rates('LTC-USD')
btcData = public_client.get_product_historic_rates('BTC-USD')
es = Elasticsearch()

#Create mapping for es data and store in ES for each coin
for data in ltcData:
    dataDict = {
        'time' : data[0],
        'low' : data[1],
        'high' : data[2],
        'open' : data[3],
        'close' : data[4],
        'volume' : data[5]
    }
    res = es.index(index="gdax-ltc", doc_type='ltc-data', body=json.dumps(dataDict))

for data in ethData:
    dataDict = {
        'time' : data[0],
        'low' : data[1],
        'high' : data[2],
        'open' : data[3],
        'close' : data[4],
        'volume' : data[5]
    }
    res = es.index(index="gdax-eth", doc_type='eth-data', body=json.dumps(dataDict))

for data in btcData:
    dataDict = {
        'time' : data[0],
        'low' : data[1],
        'high' : data[2],
        'open' : data[3],
        'close' : data[4],
        'volume' : data[5]
    }
    res = es.index(index="gdax-btc", doc_type='btc-data', body=json.dumps(dataDict))
