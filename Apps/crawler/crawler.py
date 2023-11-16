import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time



visited_urls = set()
sitemap_urls = ['https://www.liputan6.com/cek-fakta/sitemap.xml', 'https://www.liputan6.com/cek-fakta/sitemap_web.xml']  # Replace with your sitemap URLs
counter = 0
current_date = datetime.now().date() # is this 

def parse_sitemaps():
    global visited_urls, counter, current_date
    # Reset counter if the date has changed
    if datetime.now().date() != current_date:
        counter = 0
        current_date = datetime.now().date()
    for sitemap_url in sitemap_urls:
        response = requests.get(sitemap_url)
        soup = BeautifulSoup(response.content, 'xml')
        urls = [element.text for element in soup.find_all('loc')]
        for url in urls:
            if url not in visited_urls:
                scrape_webpage(url)
                visited_urls.add(url)
                counter += 1


def scrape_webpage(url):
    global counter
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    content = soup.find(class_='article-content-body')
    if content:        
        with open(f'crawler/article_{current_date}_{counter}.txt', 'w') as f:
            # write url first


            f.write(url + "\n" + content.text)

parse_sitemaps()
       