import time

from bs4 import BeautifulSoup
import requests
import os
import multiprocessing as mp
import threading
import math


from scraper.database.database import add_page, add_to_reverse_index
from scraper.toScrape.scraper import extract_content, get_urls_from_sitemap, url_is_valid


def craw_new_urls(urls):
    with open("./toScrape/alreadyCrawledUrls.txt") as f:
        already_crawled = f.readlines()
        urls_to_crawl = [x for x in urls if x not in already_crawled]
    crawl(urls_to_crawl)


# MAIN FUNCTIONALITY
def scrape(url):
    try:
        # print("tupsik")
        # Make a request
        response = requests.get(url)
        if 200 <= response.status_code < 300:
            soup = BeautifulSoup(response.text, "html.parser")
            # Exctract the info
            content = extract_content(url, soup)

            # 8. Save results to DB
            if content["title"] and content["word_count_dict"]:
                page_id = add_page(content["title"], url,content["meta"])
                # 9.Update the reverse index
                add_to_reverse_index(content["word_count_dict"].keys(), page_id)
                print("completed scraping for", url)

            # 10. Update already crawled urls
            with open("./toScrape/alreadyCrawledUrls.txt", "a") as f:
                f.write(url + "\n")

            # craw_new_urls(urls)
            return content["urls"]

    except Exception as e:
        # print("catching errors")
        print(e)
        return []
    finally:
        return []


def scrape_all(urls_to_scrape):
    for url in urls_to_scrape:
        scrape(url)


def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


def crawl(urls_to_scrape):
    # TODO: implement multithreading
    size = math.ceil(len(urls_to_scrape) / (max(mp.cpu_count() - 1, 1)))
    # print(size)
    chunks = list(divide_chunks(urls_to_scrape, size))
    threads = []
    for x in chunks:
        thread = threading.Thread(target=scrape_all, args=(x,))
        threads.append(thread)
    for thread in threads:
        thread.start()



# TODO: implement functionality to grawl the urls that were discovered


if __name__ == '__main__':
    # Initial array of URLs to scrape
    urls_to_scrape = [line.rstrip('\n') for line in open('./toScrape/data/webpages.txt')]
    # urls_to_scrape = [line.rstrip('\n') for line in open('./toScrape/data/test.txt')]
    # urls_to_scrape = [line.rstrip('\n') for line in open('./toScrape/data/one_million.txt')]

    # Extract .robots.txt and sitemap.xml info

    print("Starting to extract urls from sitemaps")
    start_time = time.process_time()
    discovered_urls_from_sitemaps = list(map(lambda url: get_urls_from_sitemap(url), urls_to_scrape)) + urls_to_scrape
    discovered_urls_from_sitemaps = [item for sublist in discovered_urls_from_sitemaps for item in sublist]
    end_time = time.process_time()
    print("Extraction done in", (end_time - start_time), "seconds")

    # Destroy previously crawled sites
    if os.path.exists("./toScrape/alreadyCrawledUrls.txt"):
        os.remove("./toScrape/alreadyCrawledUrls.txt")

    discovered_urls_from_sitemaps = set(discovered_urls_from_sitemaps)
    discovered_urls_from_sitemaps = list(filter(lambda url: url_is_valid(url), discovered_urls_from_sitemaps))

    print("Starting to scrape", len(discovered_urls_from_sitemaps), "urls")
    start_time = time.process_time()
    result = crawl(urls_to_scrape)

    end_time = time.process_time()
    print("Scraping done in", (end_time - start_time), "seconds")

"""
#TODO:
1. Send data to db
2. What page info do we want to save
3. Crawl pages reqursivly
"""
