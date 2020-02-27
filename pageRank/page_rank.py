import datetime

import numpy as np
from scipy import sparse
from fast_pagerank import pagerank_power

from scraper.database.database import bulk_update_pagerank, get_pages
from scraper.scraping.scraper import de_compress


def make_array(pages):
    nr_page = {}
    url_page_nr = {}
    i = 0
    for page in pages:
        nr_page[i] = page
        url_page_nr[page['url']] = i
        i += 1
    j = 1
    array = []
    for key, value in url_page_nr.items():
        try:
            urls = de_compress(nr_page[value]['urls'])  # the urls that the original page links to
            c = [url for url in urls if
                 url in url_page_nr.keys()]  # the urls that are linked to that are also present in our db.
            if len(c) > 0:
                j += 1
                page_nrs = [page_nr for page_nr, page in nr_page.items() if page['url'] in c]
                array.append([[value, page_nr] for page_nr in page_nrs][0])
        except:
            pass

    return nr_page, np.array(array)


##### Connecting to the main methods
def rank():
    print("Starting to fetch pages from the db")
    pages = get_pages()
    nr_page, A = make_array(pages)
    number_of_pages = len(nr_page.keys())
    weights = np.ones(len(A))
    print("Fetched", len(pages), "pages and", len(A), "urls from the db")

    print("Initialising pagerank graph")
    G = sparse.csr_matrix((weights, (A[:, 0], A[:, 1])), shape=(number_of_pages, number_of_pages))
    print("Finished creating the pageRank graph")
    print("Starting to calculate new pageRanks")
    pr = np.array(
        pagerank_power(G, p=0.85, max_iter=100)) * number_of_pages  # pr[0] is the page rank of page nr_page[0]
    print("Finished calculating new pageRanks")
    print("Starting to update pageRanks in the db")
    current_time = datetime.datetime.utcnow()
    bulk_update_pagerank(current_pages=nr_page,
                         pageranks=pr,
                         current_time=current_time)
    print("Finished updating pageRanks in the db")
