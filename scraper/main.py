import time

from bs4 import BeautifulSoup
import requests
import os

from tornado import concurrent

from scraper.database.database import add_page, add_to_reverse_index
from scraper.toScrape.scraper import extract_content, get_urls_from_sitemap, url_is_valid


# TODO: implement functionality to grawl the urls that were discovered

# MAIN FUNCTIONALITY
def scrape(url):
    try:
        # print(url)
        # Make a request
        headers = {
            'User-Agent': '*',
            'From': 'karl.gustav1789@gmail.com'
        }

        response = requests.get(url, headers=headers)
        if 200 <= response.status_code < 300:
            soup = BeautifulSoup(response.text, "html.parser")
            # Exctract the info
            content = extract_content(url, soup)

            # 8. Save results to DB
            if content["title"] and content["word_count_dict"]:
                page_id = add_page(content["title"], url, content["meta"], content["urls"])
                # 9.Update the reverse index
                add_to_reverse_index(content["word_count_dict"].keys(), page_id)
                print("completed scraping for", url)
            else:
                print("Problem scraping ", url + ".")

            # craw_new_urls(urls)
            return content["urls"]


    except Exception as e:
        # print("catching errors")
        print("[" + url + "]", e)
        return []

def crawl(urls_to_scrape):
    if len(urls_to_scrape) > 0:
        print("Starting to scrape", len(urls_to_scrape), "urls")

        with concurrent.futures.ThreadPoolExecutor(max_workers=None) as x:
            results = x.map(scrape, urls_to_scrape)


def start_scraper():
    # Initial array of URLs to scrape
    #  urls_to_scrape = [line.rstrip('\n') for line in open('./scraper/toScrape/data/webpages.txt')]
    urls_to_scrape = [line.rstrip('\n') for line in open('./scraper/toScrape/data/test.txt')]
    # urls_to_scrape = [line.rstrip('\n') for line in open('./scraper/toScrape/data/one_million.txt')]

    # Extract .robots.txt and sitemap.xml info

    print("Starting to extract urls from sitemaps")
    start_time = time.process_time()
    discovered_urls_from_sitemaps = list(map(lambda url: get_urls_from_sitemap(url), urls_to_scrape)) + urls_to_scrape
    discovered_urls_from_sitemaps = [item for sublist in discovered_urls_from_sitemaps for item in sublist]
    # print(discovered_urls_from_sitemaps)
    discovered_urls_from_sitemaps = set(discovered_urls_from_sitemaps)
    end_time = time.process_time()

    # print(discovered_urls_from_sitemaps)

    # Destroy previously crawled sites
    if os.path.exists("./toScrape/alreadyCrawledUrls.txt"):
        os.remove("./toScrape/alreadyCrawledUrls.txt")

    discovered_urls_from_sitemaps = list(filter(lambda url: url_is_valid(url), discovered_urls_from_sitemaps))

    print("Extraction done in", (end_time - start_time), "seconds")

    start_time = time.process_time()
    # crawl(urls_to_scrape)
    crawl(discovered_urls_from_sitemaps[0:500])

    end_time = time.process_time()
    print("Scraping done in", (end_time - start_time), "seconds")


"""
#TODO:
1. Send data to db
2. What page info do we want to save
3. Crawl pages reqursivly
"""
