# Python code to connect to Binance API.
#
# cd Binance/apitestv0.1
#
# Endpoints
# https://testnet.binance.vision/api
url = 'https://api.binance.com'
# https://api1.binance.com
# https://api2.binance.com
# https://api3.binance.com
# 
# Get table names from DB: cursor.execute("SELECT name FROM sqlite_master WHERE type='table')
# 
#

from binance import Client as cx
from binance.exceptions import BinanceAPIException
from binance.enums import *
import requests
import sqlite3
import re
from urllib.parse import urlencode
import json
import hmac
import hashlib
import time

assets = []
validpairs = []
reg_quotes = ['BNB','BTC','ETH','TRX','XRP','DOGE']
stables = ['USDT', 'BUSD']
union = reg_quotes+stables
sides = ['BUY', 'SELL']

# ms to date conversion.
def msdate(self):
    import datetime
    self = str(datetime.datetime.fromtimestamp(self/1000.0)).split()[0]
    return self

# Extract symbols from the pair
def separate(pair):
    for i in union:
        if i not in pair:
            continue
        bs = pair.removesuffix(i)
        qs = i
        if i not in bs:
            break
    return bs, qs

# The idea here is that when continuing with the code
# one line will be getorders(0) and the next one will
# be getorders(1) to repeat the script but use the sellcheck
# instead of the regular loop.

# Get all buy orders for the pairs that were traded
# and update them in the database.
# If s = 0, BUY orders will be fetched, if s = 1, SELL orders will be fetched.
def getorders(s):
    for pair in validpairs:

        symbols = list(separate(pair))
        try:
            orders = cx.get_all_orders(symbol=pair)
        except:
            continue

        for ord in orders:
            if ord['side'] == sides[s] and ord['status']=='FILLED':
                orderid = ord['orderId']
                date = ord['time']
                amount = ord['origQty']
                tradeprice = ord['price']
                quote = ord['cummulativeQuoteQty']

                cur.execute('SELECT id FROM Pairs WHERE name=?',(pair,))
                pair_id = cur.fetchone()[0]

                cur.execute('SELECT TradeId FROM Buycheck WHERE TradeId=?',(orderid,))
                data = cur.fetchone()

                if s == 1:
                    sellcheck(symbols,orderid,pair,date,amount,quote,tradeprice,pair_id,data,s)

                elif s == 0 and symbols[1] not in stables: # if the order is a buy between cryptos#
                    symbols[0],symbols[1]=symbols[1],symbols[0] # invert the list to treat it as a sell for the former quote#
                    amount,quote=quote,amount # Invert the amount and quotes as well#
                    sellcheck(symbols,orderid,pair,date,amount,quote,tradeprice,pair_id,data,s)

                elif data is None:
                    cur.execute('INSERT INTO '+symbols[0]+'''_BUY (TradeId,Date,Amount,Price,USD_value,Pair_id,Issell)
                    VALUES (?,?,?,?,?,?,?)''',(orderid,date,amount,tradeprice,quote,pair_id,0)
                    )
                    cur.execute('INSERT INTO Buycheck (TradeId) VALUES (?)',(orderid,))
    cnx.commit()

# Encrypt signature
def encrypt():
    servertime = requests.get(url+'/api/v1/time')
    srvrtime = json.loads(servertime.text)['serverTime']

    params = urlencode({'timestamp':srvrtime,})

    hashdsig = hmac.new(secret.encode('utf-8'),params.encode('utf-8'),hashlib.sha256).hexdigest()

    return srvrtime, hashdsig

# APPARENTLY I WILL HAVE TO CREATE ALL THE ENDPOINTS BECAUSE THE FUCKING CONNECTOR DOES NOT WORK
# PROPERLY WITH THE ONLINE API...#

# Get account information#
def getaccount():
    porto = []
    
    signature = encrypt()

    acc = requests.get(url+'/api/v3/account',
        params ={
            'timestamp': signature[0],
            'signature': signature[1]
            },
        headers={'X-MBX-APIKEY': api})

    data = json.loads(acc.text)['balances']

    for i in data:
        if float(i['free'])==0:
            continue
        else:
            porto.append(i)

    return porto

# Get asset price#
def getprice(asset):

    symbol = asset+'BUSD'

    tickprice = requests.get(
        url+'/api/v3/avgPrice',
        params = {
            'symbol': symbol,
        },
    )

    tickprice = json.loads(tickprice.text)['price']

    return tickprice

# Compute the new orders and ignore old ones.
def sellcheck(symbols,orderid,pair,date,amount,quote,tradeprice,pair_id,data,s):
    cur.execute('SELECT TradeId FROM '+symbols[0]+'_SELL WHERE TradeId=?',(orderid,)) #Search for the id in DB.
    id = cur.fetchone()

    if id is None:
        cur.execute('INSERT INTO '+symbols[0]+'''_SELL (TradeId,Date,Amount,USD_value,Price,Pair_id)
        Values (?,?,?,?,?,?)''', (orderid,date,amount,quote,tradeprice,pair_id))
        
        # If the quote symbol is an asset, insert it in the corresponding table.
        if symbols[1] not in stables and data is None:
            if s==0:
                cur.execute('INSERT INTO Buycheck (TradeId,Isbuy) VALUES (?,?)',(orderid,1))
            else:
                cur.execute('INSERT INTO Buycheck (TradeId,Issell) VALUES (?,?)',(orderid,1))
            
            #Get amount sold in dollars#
            newamount = amount * cx.get_avg_price(symbol=symbols[0]+'BUSD')

            cur.execute('INSERT OR IGNORE INTO '+symbols[1]+'''_BUY (TradeId,Date,Amount,Price,USD_value,Pair_id,Issell) 
            VALUES (?,?,?,?,?,?,?)''',(orderid,date,quote,tradeprice,newamount,pair_id,1))

        sellid = cur.execute('SELECT id FROM Pairs WHERE name=?',(pair,))
        sellid = cur.fetchone() # Get pair id to compare with buys
        group = cur.execute('SELECT TradeId,Amount,USD_value,PNL_perc,PNL_total FROM '+symbols[0]+'_BUY WHERE Pair_id=?',(sellid[0],))
        is_group_empty = cur.fetchone()
        # Get buy orders that have the same pair_id#

        if is_group_empty is None:
            group = cur.execute('SELECT TradeId,Amount,USD_value,PNL_perc,PNL_total FROM '+symbols[0]+'_BUY')

        if s == 0: 
            # If list is inverted, get the value in dollars of the amount purchased in base currency
            # amount bought for the base is the quote sold for the quote currency.# 
            quote = quote*cx.get_avg_price(symbol=symbols[1]+'BUSD')

        # Get trades in buy table that have the same pair id as the sell.
        for row in group:
            if quote > row[4]:
                quote = quote-row[4]
                cur.execute('INSERT OR IGNORE INTO Control (Buyid,Sellid,Pair_id) VALUES (?,?,?)', (row[0],orderid,sellid[0]))
                cur.execute('DELETE FROM '+symbols[0]+'_BUY WHERE TradeId=?',(row[0],))

            else:
                row[4]=(row[4]-quote)/(1+row[3]) #New quote
                row[1]=row[1]-amount #New amount
                cur.execute('UPDATE'+symbols[0]+'_BUY SET USD_value=?,Amount=? WHERE TradeId=?',(row[4],row[1],row[0]))
                break

# Get table names in the DB. Gives an object to loop through.
def gettblnames():
    names =[]
    tblnames = cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    for i in tblnames:
        if 'BUY' in i[0]: # i is a tuple.
            symbol=re.findall('([A-Z]+)_', i[0]) # Regexp takes the letters before the _
            names.append(symbol[0])
            # If there is no _, var is empty.
            if len(symbol)<1: continue
    return names

def calcs(numer,denom,i,avgprice,r): #Had to take it out because assets below $1 fuck it all up
    dca = round(numer/denom,r) # Gives average price of buys
    dca_perf = str(round(((avgprice-dca)/dca)*100,2))
    cur.execute('SELECT Balance FROM Portfolio WHERE Asset=?',(i,))
    balance = cur.fetchone()[0]
    dca_fiat = round(balance * dca * (1+float(dca_perf)/100),2)
    cur.execute('UPDATE Portfolio SET DCA=?,DCA_Perf=?,DCA_Fiat=? WHERE Asset=?',('$'+str(dca),dca_perf+'%','$'+str(dca_fiat),i))

# Calculate DCA,DCA_perf and the other performance indicators#
def compute():
    invest,pnl = 0,0

    for i in assets:
        numer,denom=0,0
        data = cur.execute('SELECT TradeId,Price,USD_value FROM '+i+'_BUY')
        data = cur.fetchall()

        avgprice = float(getprice(i))

        if len(data)<1:
            continue

        for row in data:
            pnl_perc = str(round(((avgprice - row[1])/row[1]) * 100,2))
            pnl_fiat = round(float(pnl_perc)/100 * row[2],2)
            pnl_total = round(row[2]*(1+float(pnl_perc)/100),2)

            numer += row[1]*row[2]
            denom += row[2]

            cur.execute('UPDATE '+i+'_BUY SET PNL_perc=?, PNL_fiat=?, PNL_total=? WHERE TradeId=?',(pnl_perc+'%','$'+str(pnl_fiat),'$'+str(pnl_total),row[0]))
            invest += row[2]
            pnl += pnl_fiat

        if numer<1:
            calcs(numer,denom,i,avgprice,10)
        else:
            calcs(numer,denom,i,avgprice,2)

    cur.execute('UPDATE Summary SET Invested=?,Profit=? WHERE id=1',('$'+str(round(invest,2)),'$'+str(round(pnl,2))))

    cnx.commit()


#HERE GOES THE API SECRET AND KEY

# API connection
''''print('Connecting to Binance.')
cx = cx(api, secret)
cx.API_URL = 'https://api.binance.com'''

# Get portfolio
print('Retrieving Portfolio.')
porto = getaccount()

# Connect to database.
print('Connecting to Database.')
# portfoliodb.sqlite
cnx = sqlite3.connect('realpdb.sqlite')
cur = cnx.cursor()

# Create Portfolio and Pairs tables.
# Portfolio contains all active assets, performance, etc.

# Pairs is to not repeat text in buy/sell tables.

# Buycheck contains list of saved buy orders so if a sell
# erases one from a buy table, it will not be loaded again.

# Control has a record of buy oders that have been erased
# along with corresponding sell order.

# YON is to know whether a buy order is a sell from another
# pair.#
cur.executescript('''CREATE TABLE IF NOT EXISTS Portfolio (
	Asset TEXT NOT NULL UNIQUE,
	Balance	REAL,
	usd_Value REAL,
	DCA	REAL,
	DCA_Perf REAL,
    DCA_Fiat REAL
    );

    CREATE TABLE IF NOT EXISTS Pairs (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        name TEXT UNIQUE
    );

    CREATE TABLE IF NOT EXISTS Buycheck (
        TradeId INTEGER NOT NULL PRIMARY KEY UNIQUE,
        Issell INTEGER,
        Isbuy INTEGER
    );

    CREATE TABLE IF NOT EXISTS Control (
        Buyid INTEGER NOT NULL UNIQUE,
        Sellid INTEGER NOT NULL UNIQUE,
        Pair_id INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS YoN (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        name TEXT UNIQUE
    );

    INSERT OR IGNORE INTO YON (name) VALUES ('Yes') ; 
    INSERT OR IGNORE INTO YON (name) VALUES ('No') ;

    CREATE TABLE IF NOT EXISTS Summary (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        Invested REAL,
        Profit REAL
    );

    INSERT OR IGNORE INTO Summary (id) VALUES (1);

''')

# Create BUY and SELL tables for assets that are in the portfolio. 
# If it is already created, will be ignored.
print('Creating tables for new assets.')
for side in sides:
    for tick in porto:
        cur.execute('CREATE TABLE IF NOT EXISTS '+tick['asset']+'_'+side+'''(
            TradeId INTEGER UNIQUE PRIMARY KEY NOT NULL,
            Date INTEGER,
            Amount REAL,
            Price REAL,
            USD_value REAL,
            PNL_perc REAL,
            PNL_fiat REAL,
            PNL_total REAL,
            Pair_id INTEGER,
            Issell INTEGER
        );''')

# Update Portfolio table with new assets and build assets list.
print('Updating Portfolio.')
ans = input('Want to fully update Portfolio?')
for ele in porto:
    value = 0
    asset=ele['asset']
    assets.append(asset)
    balance=float(ele['free'])

    cur.execute('SELECT USD_value FROM '+asset+'_BUY')
    data = cur.fetchall()

    if len(data) < 1:
        continue

    for row in data:
        value += row[0]
    
    if ans in ['y', 'Y']:
        cur.execute('''INSERT OR REPLACE INTO Portfolio (Asset,Balance,usd_Value) VALUES (?,?,?)''', (asset,balance,'$'+str(round(value,2))))
cnx.commit()

# Create table with valid pairs.
for i in assets:
    for j in assets:
        if i==j or i in stables or j not in union:
            continue
        validpairs.append(i+j)
        # tst = cx.get_all_orders(symbol=i+j)
        cur.execute('INSERT OR IGNORE INTO Pairs (name) VALUES (?)', (i+j,))

assets.remove('BUSD')
assets.remove('USDT')
#assets.extend(['ADA','SHIB','VET'])

if input('Want to update Database?') in ['y', 'Y']:
    print('Retrieving orders:')
    print('Fetching Buy orders:')
    getorders(0)
    print('Fetching Sell orders:')
    getorders(1)

if input('Place order?') in ['y', 'Y']:
    order = cx.create_order(
        symbol = 'ETHBTC',
        side = SIDE_SELL,
        type = ORDER_TYPE_MARKET,
        quantity = 0.5
    )

if input('Want to compute?') in ['y', 'Y']:
    compute()


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


