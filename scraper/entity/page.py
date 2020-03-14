import zlib

import pyhash

fp = pyhash.farm_fingerprint_64()


class Page:
    def __init__(self, url, title, meta, divs, headings, domain_id, current_time, urls):
        self.url = url
        self.title = title
        self.meta = meta
        self.domain_id = domain_id
        self.divs = divs
        self.headings = headings
        self.first_crawl_UTC = current_time
        self.last_crawl_UTC = current_time
        self.urls = self.compress_urls(urls)

        self.heading1 = []
        self.heading2 = []
        self.heading3 = []

        self.url_is_canonical = True

    def get_values_for_db(self):
        return {
            "url": self.url,
            "title": self.title,
            "domain_id": self.domain_id,
            "meta": self.meta,
            "urls": self.urls,
            "first_crawl_UTC": self.first_crawl_UTC,
            "last_crawl_UTC": self.last_crawl_UTC,
            "url_is_canonical": self.url_is_canonical
        }

    def compress_urls(self, urls):
        if urls is None or len(urls) == 0:
            return None
        urls.sort()
        my_string = ','.join(map(str, urls)).encode()
        return zlib.compress(my_string, 2)

    def de_compress(self, string):
        if string is None:
            return []
        decoded = zlib.decompress(string).decode("utf-8")
        return decoded.split(",")

    def add_urls(self, urls):
        current_urls = self.de_compress(self.urls)
        current_urls.extend(urls)
        self.urls = self.compress_urls(list(set(current_urls)))

    def get_fingerprint(self):
        raw_data = [self.url, self.title, self.meta, self.urls, self.headings]
        return self.get_fingerprint_from_raw_data(raw_data)

    def get_fingerprint_from_raw_data(self, raw_data):
        string = ''.join(map(str, raw_data))
        return fp(string)
