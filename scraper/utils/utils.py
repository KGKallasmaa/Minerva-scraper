import zlib
from functools import reduce

from tornado import concurrent
import pyhash
import numpy as np
import tldextract

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

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(update, tasks)


def unite_mixed_lengh_2d_array(arrays):
    # Decompress all
    urls_two_d_array = [None] * len(arrays)
    for i in range(len(arrays)):
        array = arrays[i]
        if type(arrays[i]) is list:
            array = arrays[i][0]

        urls_two_d_array[i] = de_compress(array)

    # Remove None values
    urls_two_d_array = list(filter(None, urls_two_d_array))

    # Flatten the array

    if len(urls_two_d_array) > 0:
        return np.array(reduce(lambda z, y: z + y, urls_two_d_array))

    return np.array(urls_two_d_array)
