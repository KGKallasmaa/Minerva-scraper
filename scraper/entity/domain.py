class Domain:
    def __init__(self, domain, favicon, current_time):
        self.domain = domain
        self.favicon = favicon
        self.first_crawl_UTC = current_time
        self.last_crawl_UTC = current_time