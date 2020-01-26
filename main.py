import time
import threading

from scraper.main import start_scraper


def scraper():
    start_scraper()
    pageRank()


from pageRank.page_rank import rank


def pageRank():
    start = time.process_time()
    print("Starting to calculate pageRank")
    rank()
    end = time.process_time()
    print("Pagerank calculation completed in", (end - start)/60, "minutes.")


if __name__ == '__main__':


    threads = []

    ## Starting the scraper

    thread1 = threading.Thread(target=scraper)
  #  threads.append(thread1)

    ## Starting the pageranker

    thread2 = threading.Thread(target=pageRank)
  #  threads.append(thread2)

    for t in threads:
        t.start()