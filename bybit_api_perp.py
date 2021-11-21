import os
import psycopg2
import requests
import json
import datetime
from pytz import timezone
import pandas as pd
import requests

perp = requests.get('https://api-testnet.bybit.com/v2/public/tickers').json()
df_perp = pd.DataFrame(perp['result'])

df_perp.loc[df_perp['delivery_fee_rate'].str.len() == 0, 'delivery_fee_rate'] = 0
df_perp.loc[df_perp['predicted_delivery_price'].str.len() == 0, 'predicted_delivery_price'] = 0
df_perp.loc[df_perp['delivery_time'].str.len() == 0, 'delivery_time'] = '1900-01-01T00:00:00Z'
df_perp.loc[df_perp['next_funding_time'].str.len() == 0, 'next_funding_time'] = '1900-01-01T00:00:00Z'

host="rdsnske.c7sczatjnphy.ap-northeast-1.rds.amazonaws.com"
dbname="bot"
user="rdsuser"
password="rds%5678"
insert_base = "insert into bybit_api_futures values('{basetime}','{symbol}','{bid_price}','{ask_price}','{last_price}','{last_tick_direction}','{prev_price_24h}','{price_24h_pcnt}','{high_price_24h}','{low_price_24h}','{prev_price_1h}','{price_1h_pcnt}','{mark_price}','{index_price}','{open_interest}','{open_value}','{total_turnover}','{turnover_24h}','{total_volume}','{volume_24h}','{funding_rate}','{predicted_funding_rate}','{next_funding_time}','{countdown_hour}','{delivery_fee_rate}','{predicted_delivery_price}','{delivery_time}');"

# データをDBに登録
with psycopg2.connect(host=host, dbname=dbname, user=user, password=password) as conn:
    with conn.cursor() as cur:
        basetime = datetime.datetime.now()
        basetime = basetime.astimezone(timezone('UTC'))
        basetime = datetime.datetime.strftime(basetime, '%Y-%m-%dT%H:%M:00')
        for d in df_perp.itertuples():
            insert_sql = insert_base.format(basetime=basetime
                                            ,symbol=d.symbol
                                            ,bid_price=d.bid_price
                                            ,ask_price=d.ask_price
                                            ,last_price=d.last_price
                                            ,last_tick_direction=d.last_tick_direction
                                            ,prev_price_24h=d.prev_price_24h
                                            ,price_24h_pcnt=d.price_24h_pcnt
                                            ,high_price_24h=d.high_price_24h
                                            ,low_price_24h=d.low_price_24h
                                            ,prev_price_1h=d.prev_price_1h
                                            ,price_1h_pcnt=d.price_1h_pcnt
                                            ,mark_price=d.mark_price
                                            ,index_price=d.index_price
                                            ,open_interest=d.open_interest
                                            ,open_value=d.open_value
                                            ,total_turnover=d.total_turnover
                                            ,turnover_24h=d.turnover_24h
                                            ,total_volume=d.total_volume
                                            ,volume_24h=d.volume_24h
                                            ,funding_rate=d.funding_rate
                                            ,predicted_funding_rate=d.predicted_funding_rate
                                            ,next_funding_time=d.next_funding_time
                                            ,countdown_hour=d.countdown_hour
                                            ,delivery_fee_rate=d.delivery_fee_rate
                                            ,predicted_delivery_price=d.predicted_delivery_price
                                            ,delivery_time=d.delivery_time)
            cur.execute(insert_sql)
    conn.commit()
