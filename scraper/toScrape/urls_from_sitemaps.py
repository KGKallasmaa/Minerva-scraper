import requests

from bs4 import BeautifulSoup
from urllib.parse import urlparse
import urllib.robotparser as urobot
import ssl
import numpy as np
from tornado import concurrent

import asyncio
from tornado.platform.asyncio import AnyThreadEventLoopPolicy

asyncio.set_event_loop_policy(AnyThreadEventLoopPolicy())

rp = urobot.RobotFileParser()
ssl._create_default_https_context = ssl._create_unverified_context


def get_urls_from_xml(url):
    # TODO: implement numpy array
    if url is None:
        return None

    response = requests.get(url)

    if response.status_code >= 300:
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    raw_urls = np.array([link.get_text() for link in soup.find_all('loc')])

    extracted_urls = np.array(list(filter(lambda url: ".xml" not in url, raw_urls)))

    to_be_crawled_urls = np.array(list(filter(lambda url: ".xml" in url, raw_urls)))

    if len(to_be_crawled_urls) > 0:
        with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
            for result in executor.map(get_urls_from_xml, to_be_crawled_urls):
                if result is not None and len(result) > 0:
                    extracted_urls = np.concatenate((extracted_urls, result), axis=None)

    if len(extracted_urls) < 1:
        return None
    return extracted_urls


def get_sitemaps_from_url(url):
    domain = urlparse(url).netloc
    domain = "http://" + domain if "http://" in url else "https://" + domain

    response = requests.get(domain + "/robots.txt")

    soup = BeautifulSoup(response.text, "html.parser")
    lines = soup.prettify().split('\n')

    sitemaps = list(filter(lambda line: "Sitemap:" in line, lines))
    sitemaps = list(map(lambda line: line.replace('Sitemap: ', ''), sitemaps))

    # Trying a common setup
    if len(sitemaps) == 0:
        sitemaps.append(domain + "/sitemap.xml")

    extracted_urls = None

    for sitemap in sitemaps:
        result = get_urls_from_xml(sitemap)
        if result is not None:
            if extracted_urls is None:
                extracted_urls = result
            else:
                extracted_urls = np.concatenate((extracted_urls, result), axis=None)

    # TODO: implement.We should only crawl pages that we are allowed to
    # todo: we should respect robots.txt. Twitter robots.txt has very good documentation. study it

    return extracted_urls


def robot_url_fetching_check(domain, urls):
    """
    TODO: add aditional support
    1. How many requests reuests can be made per seond?
    CHECK for resourses: https://docs.python.org/3/library/urllib.robotparser.html
    """
    forbidden_domains = ["http://analytics.google.com"]
    if domain in forbidden_domains:
        return []
    global rp
    rp.set_url(domain + "/robots.txt")
    rp.read()
    return list(filter(lambda url: rp.can_fetch("*", url), urls))


"""
TODO: implement
https://julien.danjou.info/python-and-fast-http-clients/
https://github.com/tornadoweb/tornado/issues/2531
"""
