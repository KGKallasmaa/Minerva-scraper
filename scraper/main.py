import datetime
import time

from bs4 import BeautifulSoup
import requests

from tornado import concurrent

from scraper.database.database import add_page, add_to_reverse_index, delete_duplicate_keywords_from_db, \
    add_page_statistics
from scraper.entity.page_statisitcs import PageStatistics
from scraper.language.language import nr_of_words_in_url
from scraper.scraping.scraper import extract_content

# TODO: implement functionality to crawl the urls that were discovered


# TODO: problem scraping pages that don't do server side rendering

from scraper.scraping.urls_from_sitemaps import get_sitemaps_from_url
from bs4 import BeautifulSoup

# MAIN FUNCTIONALITY
i = 0
headers = {
    'User-Agent': '*',
    'From': 'karl.gustav1789@gmail.com'
}


def scrape(url):
    global i
    global headers

    current_milli_time = lambda: int(round(time.time() * 1000))

    current_time = datetime.datetime.utcnow()

    response_time_ms = current_milli_time()
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    response_time_ms = current_milli_time() - response_time_ms

    # Extract the page info
    word_count, page = extract_content(url, soup, current_time)

    # Save results to DB
    if page.title and word_count is not None:
        page_id = add_page(page=page,
                           current_time=current_time)

        page_statistics = PageStatistics(page_id=page_id,
                                          current_time=current_time,
                                          page=page,
                                          speed=response_time_ms)

        add_page_statistics(page_statistics, current_time)

        # Update the reverse index
        discovered_keywords = [*word_count.keys()]
        add_to_reverse_index(discovered_keywords, page_id)
        print("completed scraping for", url)
        i += 1
    else:
        print("Problem scraping ", url + ".")


def crawl(urls_to_scrape):
    if urls_to_scrape.shape[0] > 0:
        print("Starting to crawl", len(urls_to_scrape), "urls")
        with concurrent.futures.ThreadPoolExecutor(max_workers=None) as x:
            x.map(scrape, urls_to_scrape)


def start_scraper():
    global i
    # Initial array of URLs to scrape
    urls_to_scrape = [line.rstrip('\n') for line in open('./scraper/scraping/test_domains.txt')]

    print("Starting to scrape", len(urls_to_scrape), "domains")
    start_time = time.process_time()

    # Crawl each url individually
    for url in urls_to_scrape:
        discovered_urls = get_sitemaps_from_url(url)
        if discovered_urls is not None:
            crawl(discovered_urls)
            print("Completed crawling", i, "pages for", url)
        else:
            print("No urls found for", url)
        i = 0
        delete_duplicate_keywords_from_db()

    end_time = time.process_time()
    print("Scraping done in", (end_time - start_time) / 60, "minutes")
