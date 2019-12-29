import numpy as np
from scipy import sparse
from fast_pagerank import pagerank_power

from scraper.database.database import update_pagrank, get_pages


def make_array(pages):
    nr_page = {}
    url_page_nr = {}
    i = 0
    for page in pages:
        nr_page[i] = page
        url_page_nr[page['page_url']] = i
        i += 1

    array = []
    for key, value in url_page_nr.items():
        urls = nr_page[value]['urls']  # the urls that the orignal page links to
        c = [url for url in urls if
             url in url_page_nr.keys()]  # the urls that are linked to that are also present in our db.
        if len(c) > 0:
            page_nrs = [page_nr for page_nr, page in nr_page.items() if page['page_url'] in c]
            array.append([[value, page_nr] for page_nr in page_nrs][0])

    np_array = np.array(array)
    return nr_page, np_array


##### Connecting to the main methods
def rank():
    # TODO. get data from db
    pages = get_pages()
    nr_page, A = make_array(pages)


    number_of_pages = len(pages)

    weights = np.ones(len(A))
    G = sparse.csr_matrix((weights, (A[:, 0], A[:, 1])), shape=(number_of_pages, number_of_pages))

    pr = pagerank_power(G, p=0.85, max_iter=5)  # pr[0] is the page rank of page nr_page[0]
    update_pagrank(nr_page, pr)
