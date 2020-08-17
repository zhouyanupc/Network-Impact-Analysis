"""
  下载汽车抖音内容
"""
import json
import time
import datetime
import sys
import pandas as pd
from utils import data_processing

# 得到idataapi的API key
api_key = data_processing.get_api_key()

def work(car_id, key):
	page_url = 'http://api01.idataapi.cn:8000/video/douyin?kw=' + key + '&apikey=' + api_key
	print(page_url)
	#获取txt
	text = data_processing.get_html_text(page_url)
	# 将json格式转换为字典
	text = json.loads(text)

	# 没有数据返回空
	if 'data' not in text.keys():
		return

	douyins = text['data']
	print(douyins)
	for douyin in douyins:
		temp = {}
		url = douyin['url']
		cover_url = douyin["coverUrl"]
		event_title = douyin["eventTitle"]
		description = douyin["description"]

		like_count = douyin["likeCount"]
		share_count = douyin["shareCount"]
		comment_count = douyin["commentCount"]

		publish_date = douyin['publishDateStr'].replace('T', ' ')
		poster_screen_name = douyin["posterScreenName"]

		temp = {'url': url, 'cover_url': cover_url, 'event_title': event_title, 'publish_date': publish_date, 'like_count': like_count, 'shareCount': share_count, 'comment_count': comment_count, 'posterScreenName': poster_screen_name}
		# 插入player_weibo表
		insert_stmt = 'insert ignore into car_douyin (car_id, url, cover_url, event_title, description, publish_date, like_count, share_count, comment_count, poster_screen_name) VALUES \
						(:car_id, :url, :cover_url, :event_title, :description, :publish_date, :like_count, :share_count, :comment_count, :poster_screen_name)'
		session.execute(insert_stmt, {'car_id': car_id, 'url': url, 'cover_url': cover_url, 'event_title': event_title, 'description': description, 'publish_date': publish_date, 'like_count': like_count, 'share_count': share_count, 'comment_count': comment_count, 'poster_screen_name': poster_screen_name})
		print(temp)
	session.commit()

if __name__ == '__main__':
	# 初始化数据库连接:
	engine, session = data_processing.get_db_session()

	# 得到汽车list
	car_list = data_processing.get_car_list(session)
	# 对每个车型下载热门抖音
	for car in car_list:
		print(car['id'], car['name'])
		# 得到该车型的热门抖音
		temp = work(car['id'], car['name'])

	# 提交到数据库
	session.commit()
	session.close()

