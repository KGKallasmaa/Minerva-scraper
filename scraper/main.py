import time

from bs4 import BeautifulSoup
import requests
import os
import multiprocessing

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
        # Make a request
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        # Exctract the info
        content = extract_content(url, soup)

        # 8. Save results to DB
        page_id = add_page(content["title"], url)

        # 9.Update the reverse index
        add_to_reverse_index(content["word_count_dict"].keys(), page_id)

        # 8. Crawl new urls
        with open("./toScrape/alreadyCrawledUrls.txt", "a") as f:
            f.write(url + "\n")

        time.sleep(10)
        # craw_new_urls(urls)
        return content["urls"]

    except Exception as e:
        print(e)
        return []


def crawl(urls_to_scrape):
    pool = multiprocessing.Pool(4)
    for url in urls_to_scrape:
        pool.apply_async(scrape, args=(url,))
    pool.close()
    pool.join()


if __name__ == '__main__':
    # Initial array of URLs to scrape
    # urls_to_scrape = [line.rstrip('\n') for line in open('./toScrape/data/webpages.txt')]
    urls_to_scrape = [line.rstrip('\n') for line in open('./toScrape/data/test.txt')]
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

    discovered_urls_from_sitemaps = list(set(discovered_urls_from_sitemaps))
    discovered_urls_from_sitemaps = list(filter(lambda url: url_is_valid(url), discovered_urls_from_sitemaps))

    print("Starting to scrape", len(discovered_urls_from_sitemaps), "urls")
    start_time = time.process_time()
    crawl(discovered_urls_from_sitemaps)

    end_time = time.process_time()
    print("Scraping done in", (end_time - start_time), "seconds")

"""
#TODO:
1. Send data to db
2. What page info do we want to save
3. Crawl pages reqursivly
"""
