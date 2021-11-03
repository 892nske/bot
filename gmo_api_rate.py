import os
import psycopg2
import requests
import json
import datetime
from pytz import timezone
import pandas as pd
import requests

# apiから情報を取得
rate = requests.get('https://api.coin.z.com/public/v1/ticker?symbol=').json()
df_rate = pd.DataFrame(rate['data'])

# DB設定
host="rdsnske.c7sczatjnphy.ap-northeast-1.rds.amazonaws.com"
dbname="bot"
user="rdsuser"
password="rds%5678"
insert_base = "insert into gmo_rate values('{basetime}','{symbol}','{ask}','{bid}','{high}','{last}','{low}','{volume}');"

# データをDBに登録
with psycopg2.connect(host=host, dbname=dbname, user=user, password=password) as conn:
    with conn.cursor() as cur:
        basetime = datetime.datetime.now()
        basetime = basetime.astimezone(timezone('UTC'))
        basetime = datetime.datetime.strftime(basetime, '%Y-%m-%dT%H:%M:00')
        for d in df_rate.itertuples():
            insert_sql = insert_base.format(basetime=basetime
                                            ,symbol=d.symbol
                                            ,ask=d.ask
                                            ,bid=d.bid
                                            ,high=d.high
                                            ,last=d.last
                                            ,low=d.low
                                            ,volume=d.volume)
            cur.execute(insert_sql)
    conn.commit()