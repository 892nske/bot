import traceback
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import chromedriver_binary
import time
import datetime
from pytz import timezone
import pandas as pd

url = 'https://www.bybt.com/'

# 取得結果
columns = ['Rank','OI','Site','Pair','Price','Long_24h_v','Long_24h_r','Short_24h_r','Short_24h_v','Volume_24h','Funding','Open_Interest']
df = pd.DataFrame(columns=columns)

options = Options()
# ヘッドレスモードで実行する場合
options.add_argument("--headless")
driver = webdriver.Chrome(options=options)


try:
    # 取得先URLにアクセス
    driver.get(url)
    
    # コンテンツが描画されるまで待機
    time.sleep(10)

    # 対象を抽出
    values = driver.find_element_by_class_name("ivu-layout-content")
    table = values.find_elements_by_tag_name("table")
    trs = table[1].find_elements_by_tag_name("tr")
    tds = trs[0].find_elements_by_tag_name("td")
    for tr in trs:
        tdl = []
        tds = tr.find_elements_by_tag_name("td")
        for td in tds:
            tdl.append(str(td.text))
        df = df.append(pd.Series(tdl, index=columns), ignore_index= True)
        
            
finally:
    # プラウザを閉じる
    driver.quit()



df['OI'] = df['OI'].str.replace('\%', '')
df['Funding'] = df['Funding'].str.replace('\%', '')

df2 = df.astype({'Rank': 'int8'
                 ,'OI': 'float'
                 ,'Site': 'str'
                 ,'Pair': 'str'
                 ,'Price':'float'
                 ,'Long_24h_v': 'str'
                 ,'Long_24h_r': 'str'
                 ,'Short_24h_r': 'str'
                 ,'Short_24h_v': 'str'
                 ,'Volume_24h': 'str'
                 ,'Funding': 'float'
                 ,'Open_Interest': 'str'
                })

df2=df2.round({'OI': 2})

df2['Long_24h_v']=df2['Long_24h_v'].str.replace('M', '0000')
df2['Long_24h_v']=df2['Long_24h_v'].str.replace('B', '0000000')
df2['Long_24h_v']=df2['Long_24h_v'].str.replace('\.', '')

df2['Short_24h_v']=df2['Short_24h_v'].str.replace('M', '0000')
df2['Short_24h_v']=df2['Short_24h_v'].str.replace('B', '0000000')
df2['Short_24h_v']=df2['Short_24h_v'].str.replace('\.', '')

df2['Volume_24h']=df2['Volume_24h'].str.replace('M', '0000')
df2['Volume_24h']=df2['Volume_24h'].str.replace('B', '0000000')
df2['Volume_24h']=df2['Volume_24h'].str.replace('\.', '')

df2['Open_Interest']=df2['Open_Interest'].str.replace('M', '0000')
df2['Open_Interest']=df2['Open_Interest'].str.replace('B', '0000000')
df2['Open_Interest']=df2['Open_Interest'].str.replace('\.', '')

basetime = datetime.datetime.now()
basetime = basetime.astimezone(timezone('Asia/Tokyo'))
basetime = datetime.datetime.strftime(basetime, '%Y-%m-%dT%H:%M:00')
df2.insert(0, 'basetime', basetime)

df2.to_csv('bybt_perp.csv', index=False)