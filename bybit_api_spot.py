import os
import psycopg2
import requests
import json
import datetime
from pytz import timezone
import pandas as pd
import requests

spot = requests.get('https://api.bybit.com/spot/quote/v1/ticker/24hr').json()
df_spot = pd.DataFrame(spot['result'])
df_spot['timestamp'] = pd.to_datetime(df_spot['time'], unit='ms')

host="rdsnske.c7sczatjnphy.ap-northeast-1.rds.amazonaws.com"
dbname="bot"
user="rdsuser"
password="rds%5678"
insert_base = "insert into bybit_api_spot values('{basetime}','{symbol}','{best_bid_price}','{best_ask_price}','{volume}','{volume_quote}','{last}','{high}','{low}','{open}');"

# データをDBに登録
with psycopg2.connect(host=host, dbname=dbname, user=user, password=password) as conn:
    with conn.cursor() as cur:
        basetime = datetime.datetime.now()
        basetime = basetime.astimezone(timezone('UTC'))
        basetime = datetime.datetime.strftime(basetime, '%Y-%m-%dT%H:%M:00')
        for d in df_spot.itertuples():
            insert_sql = insert_base.format(basetime=basetime
                                            ,symbol=d.symbol
                                            ,best_bid_price=d.bestBidPrice
                                            ,best_ask_price=d.bestAskPrice
                                            ,volume=d.volume
                                            ,volume_quote=d.quoteVolume
                                            ,last=d.lastPrice
                                            ,high=d.highPrice
                                            ,low=d.lowPrice
                                            ,open=d.openPrice)
            cur.execute(insert_sql)
    conn.commit()
