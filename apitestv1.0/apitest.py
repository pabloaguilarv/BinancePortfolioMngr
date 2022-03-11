# Python code to connect to Binance API.
#
# cd Binance/apitestv0.1
#
# Endpoints
# https://api.binance.com
# https://api1.binance.com
# https://api2.binance.com
# https://api3.binance.com
# 
# Get table names from DB: cursor.execute("SELECT name FROM sqlite_master WHERE type='table')
# 
#

import os
from binance import Client as cx
from binance.exceptions import BinanceAPIException as e
import urllib.request, urllib.error, urllib.parse
import sqlite3
import re

assets = []
stables = ['USDT', 'BUSD']
sides = ['BUY', 'SELL']

# ms to date conversion.
def msdate(self):
    import datetime
    self = str(datetime.datetime.fromtimestamp(self/1000.0)).split()[0]
    return self

# Ignore sell trades that were already computed and stored.
# Compute the new ones and store them.
def sellcheck(bs,qs,orderid):
    c_one = 'SELECT Tradeid FROM '+bs+qs+'_SELL'
    for id in cur.execute(c_one):
        if orderid == id[0]:
            continue
        cur.execute('SELECT PNL_total FROM '+bs+'_BUY')



# Get all buy orders for the pairs that were traded
# and update them in the database.
# If s = 0, BUY orders will be fetched, if s = 1, SELL orders will be fetched.
def getorders(altl,s):
    for bs in assets:
        for qs in altl:
            try:
                orders = cx.get_all_orders(symbol=bs+qs)
            except:
                continue

            for ord in orders:
                if ord['side'] == sides[s] and ord['status']=='FILLED':
                    print(ord['side'])
                    orderid = ord['orderId']
                    date = ord['time']
                    amount = ord['origQty']
                    tradeprice = ord['price']
                    quote = ord['cummulativeQuoteQty']

                    cur.execute('SELECT id FROM Pairs WHERE name=?',(bs+qs,))
                    pair_id = cur.fetchone()[0]

                    if s == 1:
                        sellcheck()
                    else:
                        cur.execute('INSERT OR IGNORE INTO '+bs+'_'+sides[s]+'''(TradeId,Date,Amount,Price,Quote,Pair_id)
                        VALUES (?,?,?,?,?,?)''',(orderid,date,amount,tradeprice,quote,pair_id)
                        )
    cnx.commit()

# API and secret for test.
bnb_api = '30UOZLdvJh1CHcchC2hEEvIRJq6s8i7fbUB8NDVQVpdCbeLgE1GKylBUbx0juiiN'
bnb_secret = 'yuIv8v8gUSL1YBXleh7wisRueOFNLGZnobaRAHNQs7B7aPAnkPkjlDXUYELW2bKw'

# API connection
print('Connecting to Binance.')
cx = cx(bnb_api, bnb_secret)
cx.API_URL = 'https://testnet.binance.vision/api'

# Get portfolio
print('Retrieving Portfolio.')
porto=cx.get_account()['balances']

# Connect to database.
print('Connecting to Database.')
cnx = sqlite3.connect('portfoliodb.sqlite')
cur = cnx.cursor()

# Create Portfolio and Pairs tables.
cur.executescript('''CREATE TABLE IF NOT EXISTS Portfolio (
	Asset TEXT NOT NULL UNIQUE,
	Balance	REAL,
	usd_Value REAL,
	DCA	REAL,
	DCA_Perf REAL);

    CREATE TABLE IF NOT EXISTS Pairs (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        name TEXT UNIQUE
    );
''')

# Create BUY and SELL tables for assets that are in the portfolio. 
# If it is already created, will be ignored.
print('Creating tables for new assets.')
for side in sides:
    for fs in porto:
        cur.execute('CREATE TABLE IF NOT EXISTS '+fs['asset']+'_'+side+'''(
            TradeId INTEGER UNIQUE PRIMARY KEY NOT NULL,
            Date INTEGER,
            Amount REAL,
            Price REAL,
            Quote REAL,
            PNL_perc REAL,
            PNL_fiat REAL,
            PNL_total Real,
            Pair_id INTEGER
        );''')


# Update Portfolio table with new assets and build assets list.
print('Updating Portfolio.')
for updt in porto:
    asset=updt['asset']
    assets.append(asset)
    balance=float(updt['free'])
    if updt['asset'] in stables: value = 1
    else:
        prc = float(cx.get_avg_price(symbol=updt['asset']+'BUSD')['price'])
        value=balance*prc
    cur.execute('''INSERT OR REPLACE INTO Portfolio (Asset,Balance,usd_Value) VALUES (?,?,?)''', (asset,balance,value))
cnx.commit()

# Create table with valid pairs.
for i in assets:
    for j in assets:
        try:
            tst = cx.get_all_orders(symbol=i+j)
        except:
            continue
        cur.execute('INSERT OR IGNORE INTO Pairs (name) VALUES (?)', (i+j,))

assets.remove('BUSD')
assets.remove('USDT')

print('Retrieving orders:')
getorders(stables)
getorders(assets)

'''
# Get table names in the DB. Gives an object to loop through.
tblnames = cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
# Loop to go through the result of execute.
for i in tblnames:
    if 'BUY' in i[0]: # i is a tuple.
        symbol=re.findall('([A-Z]+)_', i[0]) # Regexp takes the letters before the _
        # Because Portfolio is a table as well, Regexp does not match and gives null.
        if len(symbol)<1: continue
        print(symbol[0])
'''

'''th = []

tst = cx.get_account()
'''

'''
for i in porto:
    for j in porto:
        try:
            trades = cx.get_historical_trades(symbol=i['asset']+j['asset'])
            if (i['asset']+j['asset']) not in th:
                th.append(i['asset']+j['asset'])
        except:
            continue
'''

#tst = cx.get_historical_trades(symbol='LTCBTC')
#print(tst)

# print('Valid pairs:', th)


