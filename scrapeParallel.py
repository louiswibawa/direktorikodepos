import requests
import urllib.request
import time
from bs4 import BeautifulSoup
import dbConfig
import multiprocessing as mp
import csv

def get_wilayahs():
	url = 'https://direktorikodepos.org/'
	response = requests.get(url)
	soup = BeautifulSoup(response.text, 'html.parser')
	wilayahsUrl = []
	for td in soup.findAll('td'):
		wilayahsUrl.append(td.find('a')['href'])
	print("There are {} wilayah in Indonesia".format(len(wilayahsUrl)))
	return wilayahsUrl


def get_info(db, page):
	titleSkipCount = 3
	rows = page.findAll('tr')
	province = page.find_all('td')[1].text
	with open('area_master_data.csv', mode='a') as master_data:
		writer = csv.writer(master_data, delimiter=',')
		for row in rows:
			if titleSkipCount > 0:
				titleSkipCount -= 1
				continue
			cols = row.find_all('td')
			cols = [ele.text.strip() for ele in cols]
			cols.append(province)
			writer.writerow(cols)

def page_generator(db, url):
	pageCount = 1
	while True:
		if pageCount == 1:
			response = requests.get(url)
			print("url: ", url)
		else:
			pageUrl = url + 'page/' + str(pageCount) + '/'
			response = requests.get(pageUrl, allow_redirects=False)
			print("url:", pageUrl)
			if response.status_code != 200:
				print("[X] RESPONSE CODE IS ", response.status_code, "FOR", pageUrl)
				break
		page = BeautifulSoup(response.text, 'html.parser')
		pageCount += 1
		get_info(db, page)


def main():
	db = None
	pool = mp.Pool(mp.cpu_count())
	wilayahsUrl = get_wilayahs()
	result_objects = [pool.apply_async(page_generator, args=(db, url)) for i, url in enumerate(wilayahsUrl)]
	pool.close()
	pool.join()

if __name__ == '__main__':
    main()