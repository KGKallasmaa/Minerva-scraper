import tldextract


class Domain:
    def __init__(self, domain, favicon, current_time):
        self.domain = domain
        self.favicon = favicon
        self.first_crawl_UTC = current_time
        self.last_crawl_UTC = current_time
        self.sitemap_is_present = True
        self.ssl_is_present = "https://" in domain
        self.domain_type = self.get_domain_type()

    def get_values_for_db(self):
        return {
            "domain": self.domain,
            "favicon": self.favicon,
            "first_crawl_UTC": self.first_crawl_UTC,
            "last_crawl_UTC": self.last_crawl_UTC,
            "sitemap_is_present": self.sitemap_is_present,
            "ssl_is_present": self.ssl_is_present,
            "domain_type": self.domain_type
        }

    def get_domain_type(self):
        ext = tldextract.extract(self.domain)
        return ext.suffix
