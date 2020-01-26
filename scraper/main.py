import time

from bs4 import BeautifulSoup
import requests

from tornado import concurrent

from scraper.database.database import add_page, add_to_reverse_index, delete_duplicate_keywords_from_db
from scraper.toScrape.scraper import extract_content
import numpy as np


# TODO: implement functionality to crawl the urls that were discovered


# TODO: problem scraping pages that don't do server side rendering

# MAIN FUNCTIONALITY
from scraper.toScrape.urls_from_sitemaps import get_sitemaps_from_url


def scrape(url):
    try:
        headers = {
            'User-Agent': '*',
            'From': 'karl.gustav1789@gmail.com'
        }

        response = requests.get(url, headers=headers)
        if 200 <= response.status_code < 300:
            soup = BeautifulSoup(response.text, "html.parser")
            # Extract the info
            content = extract_content(url, soup)

            # 8. Save results to DB
            if content["title"] and content["word_count_dict"]:
                page_id = add_page(content["title"], url, content["meta"], content["favicon"], content["urls"])
                # 9.Update the reverse index
                discovered_keywords = [*content["word_count_dict"].keys()]
                add_to_reverse_index(discovered_keywords, page_id)
                print("completed scraping for", url)
            else:
                print("Problem scraping ", url + ".")
            # craw_new_urls(urls)
           # return content["urls"]




    except Exception as e:
        # print("catching errors")
        print("[" + url + "]", e)
      #  return []
    finally:
        return None


def crawl(urls_to_scrape):
    if urls_to_scrape.shape[0] > 0:
        print("Starting to crawl", len(urls_to_scrape), "urls")

        with concurrent.futures.ThreadPoolExecutor(max_workers=None) as x:
            x.map(scrape, urls_to_scrape)


def start_scraper():
    # Initial array of URLs to scrape
    #  urls_to_scrape = [line.rstrip('\n') for line in open('./scraper/toScrape/data/webpages.txt')]
    urls_to_scrape = [line.rstrip('\n') for line in open('./scraper/toScrape/data/test.txt')]
    # urls_to_scrape = [line.rstrip('\n') for line in open('./scraper/toScrape/data/one_million.txt')]

    # Extract .robots.txt and sitemap.xml info
    print("Starting to scrape", len(urls_to_scrape), "domains")
    start_time = time.process_time()

    # Crawl each url individually
    for url in urls_to_scrape:
        discovered_urls = np.array(get_sitemaps_from_url(url))
        crawl(discovered_urls)
        print("Completed crawling", len(discovered_urls), "pages for", url)
        delete_duplicate_keywords_from_db() #TODO: it should be done in a seperate thread. we can start crawling new pages

    end_time = time.process_time()
    print("Scraping done in", (end_time - start_time) / 60, "minutes")


"""
#TODO:
2. What page info do we want to save
3. Crawl pages reqursivly
"""
