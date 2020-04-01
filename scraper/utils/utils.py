import zlib

import pyhash
import tldextract
from tornado import concurrent

fp = pyhash.farm_fingerprint_64()


def get_domain(url):
    ext = tldextract.extract(url)
    if "https://" in url:
        return "https://" + ext.registered_domain
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


def get_fingerprint_from_raw_data(raw_data):
    string = ''.join(map(str, raw_data))
    return fp(string)


def execute_tasks(tasks):
    if len(tasks) > 0:
        update = lambda task: task.execute()

        with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
            results = {executor.submit(update, task): task for task in tasks}
