import zlib
from functools import reduce

import tldextract
import numpy as np


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


def merge_dictionaries(dictionaries):
    merged_results = {}

    # Results as 2d array
    for dic in dictionaries:
        for key, value in dic.items():
            if key in merged_results:
                current_value = merged_results.get(key)
                current_value.append(value)
                merged_results[key] = current_value
            else:
                merged_results[key] = [value]

    for key, value in merged_results.items():
        current_value = merged_results.get(key)
        current_value = np.array(reduce(lambda z, y: z + y, current_value))
        current_value = np.unique(current_value)
        merged_results[key] = current_value


    return merged_results
