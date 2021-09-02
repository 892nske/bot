import os
import psycopg2
import requests
import json
import datetime
from pytz import timezone
import pandas as pd

futures = requests.get('https://ftx.com/api/futures').json()
df_futures = pd.DataFrame(futures['result'])
df_futures.set_index('name', inplace = True)
df_futures['symbol'] = df_futures.index
df_futures_db = df_futures.query('type != "move" & underlyingDescription == "Bitcoin"')
db_columns = ['symbol','expiry','last','bid','ask','index','mark','lowerBound','upperBound','marginPrice','change1h','change24h','changeBod','volumeUsd24h','volume','openInterest','openInterestUsd']
df_futures_db = df_futures_db[db_columns]

host="rdsnske.c7sczatjnphy.ap-northeast-1.rds.amazonaws.com"
dbname="bot"
user="rdsuser"
password="rds%5678"
insert_base = "insert into ftx_futures_btc values('{basetime}','{symbol}','{expiry}','{last}','{bid}','{ask}','{index}','{mark}','{lowerBound}','{upperBound}','{marginPrice}','{change1h}','{change24h}','{changeBod}','{volumeUsd24h}','{volume}','{openInterest}','{openInterestUsd}');"

# データをDBに登録
with psycopg2.connect(host=host, dbname=dbname, user=user, password=password) as conn:
    with conn.cursor() as cur:
        basetime = datetime.datetime.now()
        basetime = basetime.astimezone(timezone('Asia/Tokyo'))
        basetime = datetime.datetime.strftime(basetime, '%Y-%m-%dT%H:%M:00')
        for d in df_futures_db.itertuples():
            insert_sql = insert_base.format(basetime=basetime
                                            ,symbol=d.symbol
                                            ,expiry=d.expiry
                                            ,last=d.last
                                            ,bid=d.bid
                                            ,ask=d.ask
                                            ,index=d.index
                                            ,mark=d.mark
                                            ,lowerBound=d.lowerBound
                                            ,upperBound=d.upperBound
                                            ,marginPrice=d.marginPrice
                                            ,change1h=d.change1h
                                            ,change24h=d.change24h
                                            ,changeBod=d.changeBod
                                            ,volumeUsd24h=d.volumeUsd24h
                                            ,volume=d.volume
                                            ,openInterest=d.openInterest
                                            ,openInterestUsd=d.openInterestUsd)
            insert_sql = insert_sql.replace('None','2099-12-31T00:00:00')
            cur.execute(insert_sql)
    conn.commit()
