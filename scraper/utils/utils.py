from urllib.parse import urlparse
import zlib

def get_domain(url):
    domain = urlparse(url).netloc
    return "http://" + domain if "http://" in url else "https://" + domain


def compress_urls(urls):
    if urls is None or len(urls) == 0:
        return None
    urls.sort()
    my_string = ','.join(map(str, urls)).encode()
    return zlib.compress(my_string, 2)


def de_compress(string):
    if string is None:
        return []
    decoded = zlib.decompress(string).decode("utf-8")
    return decoded.split(",")