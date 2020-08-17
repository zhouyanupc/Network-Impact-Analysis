# 使用request + BeautifulSoup提取汽车信息
import requests
from bs4 import BeautifulSoup
import pandas as pd

# 根据request_url得到soup
def get_page_content(request_url):
    # 得到页面的内容
    headers={'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36'}
    html=requests.get(request_url,headers=headers,timeout=10)
    content = html.text
    #print(content)

    # 通过content创建BeautifulSoup对象
    soup = BeautifulSoup(content, 'html.parser', from_encoding='utf-8')
    return soup

# 分析当前页面的投诉
def analysis(soup):
    # 创建DataFrame
    df = pd.DataFrame(columns = ['name', 'price_lower', 'price_higher', 'pic_url'])

    item_list = soup.find_all('div', class_ = 'search-result-list-item')
    for item in item_list:
        # ToDo：提取汽车信息
        name = item.find('p',class_='cx-name').text
        price = item.find('p', class_='cx-price').text
        if price == '暂无':
            price_lower, price_higher = -1, -1
        else:
            price_lower, price_higher = price.split('-')
            price_higher = price_higher.replace('万', '')
        pic_url = item.find('img', class_='img')['src']
        pic_url = pic_url.replace('//', '')
        #print('name {} price_lower {} price_higher {} pic_url {}'.format(name, price_lower, price_higher, pic_url))
        # 将数据添加到dataframe中
        temp = {}
        temp['name'], temp['price_lower'], temp['price_higher'], temp['pic_url'] = name, price_lower, price_higher, pic_url
        df = df.append(temp,ignore_index=True)
    return df

# 从指定request_url获取汽车数据
request_url = 'http://car.bitauto.com/xuanchegongju/?l=8&mid=8'
soup = get_page_content(request_url)
print(request_url)
df = analysis(soup)
# 将数据保存到csv
#df.to_csv('car_data.csv', index=False)

from utils import data_processing
# 将数据保存到mysql
# 获取session
engine, session = data_processing.get_db_session()
# event完成，更新player_event表
def insert_car_basic(session, name, price_lower, price_higher, pic_url):
    insert_stmt = 'insert ignore into car_basic (name, price_lower, price_higher, pic_url) VALUES \
                    (:name, :price_lower, :price_higher, :pic_url)'
    session.execute(insert_stmt, {'name': name, 'price_lower': price_lower, 'price_higher': price_higher, 'pic_url': pic_url})

for index, item in df.iterrows():
    #print(item.name, item.price_lower, item.price_higher, item.pic_url)
    insert_car_basic(session, item['name'], item['price_lower'], item['price_higher'], item['pic_url'])
    
# 提交到数据库
session.commit()
session.close()

