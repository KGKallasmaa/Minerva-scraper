import unittest

from scraper.scraping.urls_from_sitemaps import get_urls_from_xml
from scraper.utils.utils import compress_urls


class UrlsFromSitemapsTesting(unittest.TestCase):

    def get_urls_from_xml(self):
        sitemap = "https://gowiz.eu/sitemap.xml"

        discovered_urls_compressed = get_urls_from_xml(sitemap, isTesting=False)[0]

        expected_urls = ['https://gowiz.eu', 'https://gowiz.eu/about', 'https://gowiz.eu/privacy',
                         'https://gowiz.eu/tos']
        expected_urls_compressed = compress_urls(expected_urls)

        self.assertEqual(expected_urls_compressed, discovered_urls_compressed)


