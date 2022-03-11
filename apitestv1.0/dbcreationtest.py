
from os import fspath
import sqlite3
import datetime
import re
import requests
import json
import hmac
import hashlib
from urllib.parse import urlencode

url = 'https://api.binance.com'

cnx = sqlite3.connect('tstdb.sqlite')
cur = cnx.cursor()

reg_quotes = ['BNB','BTC','ETH','TRX','XRP','DOGE']
stables = ['USDT', 'BUSD']
union = reg_quotes+stables

#tblname=input('Input your table name:')

'''f = open('tst.txt', 'w+')

for i in range(10):
    f.write('This is the line number %d' % (i+1))

f.close()

fh = open('tst.txt', 'r')
fr = fh.read()'''

def msdate(self):
    import datetime
    self = str(datetime.datetime.fromtimestamp(self/1000.0)).split()[0]
    return self

api = 'okj0Glx82UUKYDrwGxgBkzlXjrwT0dRB3I0dHD9d6ThQiDCqFPqM2lWZsa1bvXDb'
secret = 'bmLi9tFQNDiemQw39FVlNiVKadMTDqOCQGQBHwmfkz8PB0eGCBgAG1l5nljesI96'

servertime = requests.get("https://api.binance.com/api/v1/time")
servertime = json.loads(servertime.text)
srvrtime = servertime['serverTime']

params = urlencode({'timestamp':srvrtime,'symbol':'BTCUSDT'})

hashdsig = hmac.new(secret.encode('utf-8'),params.encode('utf-8'),hashlib.sha256).hexdigest()

'''acc = requests.get('https://api.binance.com/api/v3/account',
    params ={
        'timestamp': srvrtime,
        #'symbol': 'BTCBUSD',
        'signature': hashdsig
    },
    headers={'X-MBX-APIKEY': api})


data = json.loads(acc.text)

print(data['balances'][0])

porto = []'''

trades = requests.get(
    url+'/api/v3/myTrades',
    params = {
        'timestamp': srvrtime,
        'symbol': 'BTCUSDT',
        'signature': hashdsig
    },
    headers={
        'X-MBX-APIKEY':api
    }
)

print(trades.url)
trades = json.loads(trades.text)
print(trades)

for i in trades:
        print(i['price'], i['qty'],msdate(int(i['time'])))