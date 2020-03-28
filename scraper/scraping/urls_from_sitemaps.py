from functools import reduce
import requests
from bs4 import BeautifulSoup
import urllib.robotparser as urobot
import ssl
import numpy as np
from tornado import concurrent
import asyncio
from tornado.platform.asyncio import AnyThreadEventLoopPolicy

from scraper.database.database import pages_we_will_not_crawl, get_client
from scraper.scraping.scraper import get_domain
from scraper.utils.utils import compress_urls, de_compress

asyncio.set_event_loop_policy(AnyThreadEventLoopPolicy())

rp = urobot.RobotFileParser()
ssl._create_default_https_context = ssl._create_unverified_context


def get_urls_from_xml(url, is_testing=False):
    if url is None:
        return None

    response = requests.get(url)
    if response.status_code != 200:
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    urls = np.array([link.get_text() for link in soup.find_all('loc')])

    extracted_urls = list(filter(lambda url: ".xml" not in url, urls))
    to_be_crawled_urls = np.array(list(filter(lambda url: ".xml" in url, urls)))

    # Remove urls we should not crawl, because they haven't been changed since last crawling time
    url_lastmod = {}

    for el in soup.find_all("url"):
        last_mod = [link.get_text() for link in el.find_all('lastmod')]
        last_mod = last_mod[0] if len(last_mod) > 0 else None

        new_url = [link.get_text() for link in el.find_all('loc')]
        new_url = new_url[0] if len(new_url) > 0 else None

        if new_url:
            url_lastmod[new_url] = last_mod

    client = get_client()
    pages_not_to_crawl = []
    if not is_testing:
        pages_not_to_crawl = pages_we_will_not_crawl(url_lastmod, client)
    extracted_urls = list(set(extracted_urls) - set(pages_not_to_crawl))
    all_urls = [compress_urls(np.array(extracted_urls))]

    with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
        for result in executor.map(get_urls_from_xml, to_be_crawled_urls):
            if result is not None:
                all_urls.append(result)

    client.close()
    return all_urls


def get_urls_from_domain(url):
    domain = get_domain(url)

    response = requests.get(domain + "/robots.txt")

    if response.status_code != 200:
        return np.array([])

    soup = BeautifulSoup(response.text, "html.parser")
    lines = soup.prettify().split('\n')

    sitemaps = list(filter(lambda line: "Sitemap:" in line, lines))
    sitemaps = list(map(lambda line: line.replace('Sitemap: ', ''), sitemaps))

    # Trying a common setup
    if len(sitemaps) == 0:
        sitemaps.append(domain + "/sitemap.xml")

    # Fetch all URLS
    extracted_compressed_urls = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
        for result in executor.map(get_urls_from_xml, sitemaps):
            if result is not None:
                extracted_compressed_urls.extend(result)

    # Decompress all
    urls_two_d_array = [None] * len(extracted_compressed_urls)
    for i in range(len(extracted_compressed_urls)):
        array = extracted_compressed_urls[i]
        if type(extracted_compressed_urls[i]) is list:
            array = extracted_compressed_urls[i][0]

        urls_two_d_array[i] = de_compress(array)

    # Remove None values
    urls_two_d_array = list(filter(None, urls_two_d_array))

    # Flatten the array

    # TODO: implement.We should only crawl pages that we are allowed to
    # todo: we should respect robots.txt. Twitter robot s.txt has very good documentation. study it
    if len(urls_two_d_array) > 0:
        return np.array(reduce(lambda z, y: z + y, urls_two_d_array))

    return np.array(urls_two_d_array)


def robot_url_fetching_check(domain, urls):
    """
    TODO: add aditional support
    1. How many requests reuests can be made per seond?
    CHECK for resourses: https://docs.python.org/3/library/urllib.robotparser.html
    """

    """"
    
   
    
    
    forbidden_domains = ["http://analytics.google.com"]
    if domain in forbidden_domains:
        return []
    global rp
    rp.set_url(domain + "/robots.txt")
    rp.read()
    print("hi")
    print(rp)
    res = list(filter(lambda url: rp.can_fetch("*", url), urls))
    print(res)
    return  urls
    
    """
    robots_url = domain + "/robots.txt"
    #   response = requests.get(robots_url, headers=headers)

    #    rp = urllib.robotparser.RobotFileParser()
    #    rp.set_url(robots_url)
    #   rp.read()
    #   rrate = rp.request_rate("*")
    #  if rrate is not None:
    #     print("Number of request can be crawled in ", rrate.seconds, " seconds is", rrate.requests)
    # print("Crawl delay", rp.crawl_delay("*"))

    # res = list(filter(lambda url: rp.can_fetch("*", url), urls))
    # print("pages we can crawl")
    # print(res)

    return None


"""
TODO: implement
https://julien.danjou.info/python-and-fast-http-clients/
https://github.com/tornadoweb/tornado/issues/2531
"""
