import pandas as pd
import requests
import os
import psycopg2
import json
import datetime
from pytz import timezone

markets = requests.get('https://ftx.com/api/markets').json()
df_markets = pd.DataFrame(markets['result'])
df_markets.set_index('name', inplace = True)
df_markets['symbol'] = df_markets.index

db_columns = ['last','bid','ask','price','change1h','change24h','changeBod','volumeUsd24h']
df_market_db = pd.DataFrame(df_markets.loc['BTC/USD',db_columns]).T

host="rdsnske.c7sczatjnphy.ap-northeast-1.rds.amazonaws.com"
dbname="bot"
user="rdsuser"
password="rds%5678"
insert_base = "insert into ftx_market_btc values('{basetime}','{symbol}','{last}','{bid}','{ask}','{price}','{change1h}','{change24h}','{changeBod}','{volumeUsd24h}');"

# データをDBに登録
with psycopg2.connect(host=host, dbname=dbname, user=user, password=password) as conn:
    with conn.cursor() as cur:
        basetime = datetime.datetime.now()
        basetime = basetime.astimezone(timezone('Asia/Tokyo'))
        basetime = datetime.datetime.strftime(basetime, '%Y-%m-%dT%H:%M:00')
        insert_sql = insert_base.format(basetime=basetime
                                        ,symbol='BTC/USD'
                                        ,last=df_market_db["last"].values[0]
                                        ,bid=df_market_db["bid"].values[0]
                                        ,ask=df_market_db["ask"].values[0]
                                        ,price=df_market_db["price"].values[0]
                                        ,change1h=df_market_db["change1h"].values[0]
                                        ,change24h=df_market_db["change24h"].values[0]
                                        ,changeBod=df_market_db["changeBod"].values[0]
                                        ,volumeUsd24h=df_market_db["volumeUsd24h"].values[0])
        cur.execute(insert_sql)
    conn.commit()