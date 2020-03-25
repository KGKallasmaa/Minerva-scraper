import zlib
import tldextract

def get_domain(url):
    ext = tldextract.extract(url)
    if "https://" in url:
        return  "https://"+ext.registered_domain
    return "http://" + ext.registered_domain



def compress_urls(urls):
    if urls is None or len(urls) == 0:
        return None
    urls.sort()
    my_string = ','.join(map(str, urls)).encode()
    return zlib.compress(my_string, 2)


def de_compress(string):
    if string is None:
        return []

    return zlib.decompress(string).decode("utf-8").split(",")