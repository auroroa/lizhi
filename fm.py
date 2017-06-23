# -*- coding:utf-8 -*-

from urllib import request
from urllib.parse import quote
from bs4 import BeautifulSoup
import re, time, random, pymysql, datetime
import http.cookiejar
import logging,random,time

logging.basicConfig(
	level = logging.INFO,
	format = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
	datefmt='%d %b %Y %H:%M:%S'
	)

TIMEOUT = 30
USER_AGENTS = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5 AppleWebKit 537.36 (KHTML, like Gecko) Chrome)"

def sleeping(max_seconds):
	sleep_time = random.randint(2,max_seconds)
	logging.info('...sleep...' + str(sleep_time) + ' seconds')
	time.sleep(sleep_time)

#存储一级类别-播客类型
def store_level_1(cur, data_lists):
	for data_list in data_lists:
		sql = "insert into fm_lizhi_list(list_level_1,list_level_2,url,type,dt) values(%s,%s,%s,%s,%s)"
		logging.info(sql)
		cur.execute(sql,(data_list[0],data_list[1],data_list[2],data_list[3],data_list[4],data_listdatetime.datetime.now()))
	cur.connection.commit()

#爬取一级类别-播客类型，返回列表
def get_level_1(url=None):
	url = 'http://www.lizhi.fm/'
	headers = { 'User-Agent' : USER_AGENTS }  
	req = request.Request(url, headers=headers)
	html = request.urlopen(req)
	soup = BeautifulSoup(html, 'html.parser')
	titles = soup.find_all('div', {'class':'left tagName fontYaHei'})
	data_lists = []
	for title in titles:
		levlel_1 = title.get_text()
		details = list(map(lambda a: [a.get_text(),a.attrs['href'].replace('//','')], title.parent.find_all('a')))
		data_list = []
		for detail in details:
			data_one = []
			data_one.append(levlel_1)
			data_one.extend(detail)
			data_list.append(data_one)
		data_lists.extend(data_list)
	return data_lists

#爬取一级类别-播客类型，返回二维列表
def fetch_level_1(cur, start=0, end=1):
	cur.execute("select * from fm_lizhi_list where status = '0' limit " + str(start) + ',' + str(end))
	return cur.fetchall()

#更新一级类别-播客类型的爬取状态（'0':未爬取，'1':已爬取，'2'：爬取中）
def change_level_1_status(cur, status, id):
	cur.execute("update fm_lizhi_list set status = %s where id = %s", (status, id))
	cur.connection.commit()

#爬取二级类别-播客基本信息，返回是否有下一页、播客基本信息
def get_level_2(cur, url, page_num):
	url = 'http://' + url + str(page_num) + '.html'
	headers = {'User-Agent' : USER_AGENTS }
	req = request.Request(url, headers=headers)
	html = request.urlopen(req).read().decode('utf-8')
	soup = BeautifulSoup(html, 'html.parser')
	bool_next_page = bool(soup.find('a', {'class':'next'}))
	radio_lists = soup.find_all('li', {'class':'radio_list'})
	radios = list(map(lambda radio:radio.find_all('a'), radio_lists))
	radio_detail = []
	for radio in radios:
		radio_detail.append(list(map(lambda x:x.strip(), [radio[0].get_text(),radio[0].attrs['href'].replace('/',''),radio[1].get_text(),radio[1].attrs['href'].replace('//',''),radio[2].get_text(),radio[2].attrs['href'].replace('//','')])))
	return bool_next_page,radio_detail

def check_level_2(cur, data_lists):
	pass

#存储二级类别-播客基本信息
def store_level_2(cur, level_id, page_num, data_lists):
	logging.info('level_id: ' + str(level_id) + ', page_num: ' + str(page_num))
	for data_list in data_lists:
		#val = "','".join(data_list)
		sql = "insert into fm_lizhi_radios(level_id,radioCover,radioCover_url,radioName,radioName_url,radioAuthor,radioAuthor_url,page_num,dt) values('" + str(level_id) + "',%s,%s,%s,%s,%s,%s,%s,%s)"
		cur.execute(sql,(data_list[0],data_list[1],data_list[2],data_list[3],data_list[4],data_list[5],page_num,datetime.datetime.now()))
	cur.connection.commit()


#Action：获取并存储一级类别-播客类型
def run_get_lvl_1():
	data_lists = get_level_1()
	store_level_1(cur, data_lists)


#Action：获取并存储二级类别-播客基本信息
def run_get_lvl_2():
	level_1_lines = fetch_level_1(cur)
	while level_1_lines:
		for level_1_line in level_1_lines:
			change_level_1_status(cur, '2', level_1_line[0])
			page_num = 1
			while True:
				bool_next_page, data_lists = get_level_2(cur, level_1_line[4], page_num)
				store_level_2(cur, level_1_line[0], page_num, data_lists)
				sleeping(10)
				page_num += 1
				if not bool_next_page:
					break
			change_level_1_status(cur, '1', level_1_line[0])
		level_1_lines = fetch_level_1(cur)

if __name__ == '__main__':
	try:
		conn = pymysql.connect(host='localhost', unix_socket='/tmp/mysql.sock', user='spider', passwd='spider', db='spider', charset='utf8')
		cur = conn.cursor()
		cur.execute("use spider")
		cur.execute("set names 'utf8mb4'")
		#run_get_lvl_1()
		run_get_lvl_2()
	except Exception as e:
		print(str(e))
	finally:
		cur.close()
		conn.close()


'''
create table spider.fm_lizhi_list
(
	`id` int(11) NOT NULL AUTO_INCREMENT,
  `list_level_1` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  `list_level_2` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  `name` varchar(256) COLLATE utf8_unicode_ci DEFAULT NULL,
  `type` char(1) COLLATE utf8_unicode_ci DEFAULT NULL,
  `url` varchar(128) COLLATE utf8_unicode_ci DEFAULT NULL,
  `dt` datetime DEFAULT NULL,
  primary key(id)
)


create table spider.fm_lizhi_radios
(
	`id` int(11) NOT NULL AUTO_INCREMENT,
  `level_id` int(11) COLLATE utf8_unicode_ci DEFAULT NULL,
  `radioCover` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `radioCover_url` varchar(128) COLLATE utf8_unicode_ci DEFAULT NULL,
  `radioName` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `radioName_url` char(128) COLLATE utf8_unicode_ci DEFAULT NULL,
  `radioAuthor` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `radioAuthor_url` varchar(128) COLLATE utf8_unicode_ci DEFAULT NULL,
  `page_num` int(6) COLLATE utf8_unicode_ci DEFAULT NULL,
  `dt` datetime DEFAULT NULL,
  primary key(id)
)

'''
