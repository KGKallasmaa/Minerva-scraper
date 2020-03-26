import unittest

from scraper.utils.utils import compress_urls, de_compress, get_domain


class UtilsTest(unittest.TestCase):

    def test_compression_and_decompression(self):
        url = "https:///example.com"
        input_urls = [url for i in range(1000)]

        compressed_urls = compress_urls(input_urls)

        de_compressed_urls = de_compress(compressed_urls)

        self.assertEqual(de_compressed_urls, input_urls)

    def test_get_domain(self):
        url1 = "https://example.com/example/example"
        url2 = "http://example.com/example/example"
        url3 = "https://www.example.com/example/example"
        url4 = "https://cdn.example.com/example/example"
        url5 = "https://www.cdn.example.com/example/example"
        domain1 = get_domain(url1)
        domain2 = get_domain(url2)
        domain3 = get_domain(url3)
        domain4 = get_domain(url4)
        domain5 = get_domain(url5)

        self.assertEqual("https://example.com", domain1)
        self.assertEqual("http://example.com", domain2)
        self.assertEqual("https://example.com", domain3)
        self.assertEqual("https://example.com", domain4)
        self.assertEqual("https://example.com", domain5)
