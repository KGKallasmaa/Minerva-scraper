from bs4 import BeautifulSoup
import re
import scraper.language as language
import requests
import os
from multiprocessing import Process


# MAIN FUNCTIONALITY
def scrape(url):
    try:
        # 1. Make a request
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        # 2. Get title
        title = soup.title.string
        print(title)
        # 3. Get meta
        meta = [meta['content'] for meta in soup.findAll(attrs={"name": re.compile(r"description", re.I)})]
        # 4. Get all divs
        list_of_divs = soup.findAll('div')
        # 5. Get all headings
        list_of_headings = [headlines.text.strip() for headlines in soup.find_all(re.compile('^h[1-6]$'))]

        # 6. Get all urls
        urls = [link.get('href') for link in soup.find_all('a')]
        urls = list(filter(None, urls))
        add_prefit = lambda x: x if (("http://" in x) or ("https://" in x)) else url + x
        urls = set(map(add_prefit, urls))

        # 7. Get all word counts
        word_count = language.word_count(soup.get_text())

        # 8. Crawl new urls

        with open("./toScrape/alreadyCrawledUrls.txt", "a") as f:
            f.write(url + "\n")

        urls_to_crawl = []
        with open("./toScrape/alreadyCrawledUrls.txt") as f:
            already_crawled = f.readlines()
            urls_to_crawl = urls_to_crawl + [x for x in urls if x not in already_crawled]

        crawl(urls_to_crawl)

        return urls

    except:
        return []


def crawl(urls_to_scrape):
    processes = []
    # Creating jobs
    for url in urls_to_scrape:
        process = Process(target=scrape, args=(url,))
        processes.append(processes)
        # Start a new spider
        process.start()


if __name__ == '__main__':
    # Initial array of URLs to scrape
    urls_to_scrape = [line.rstrip('\n') for line in open('./toScrape/webpages.txt')]

    # Destroy previously crawled sites
    if os.path.exists("./toScrape/alreadyCrawledUrls.txt"):
        os.remove("./toScrape/alreadyCrawledUrls.txt")

    crawl(urls_to_scrape)

"""
#TODO:
1. Crawl sitemaps
2. Crawl robots.txt
3. Send data to db
"""
