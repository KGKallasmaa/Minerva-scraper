import itertools

from scraper.database.database import get_page_rank_by_page_id
import readability

from scraper.language.language import pre_process_with_spacy
from scraper.scraping.scraper import get_domain


class PageStatistics:
    def __init__(self, page_id, current_time, page, speed):
        self.page_id = page_id
        self.update_date_UTC = current_time
        self.language = self.calculate_language_statistics(page)
        self.page_rank = get_page_rank_by_page_id(self.page_id)
        self.page_load_speed = speed
        self.url_length = len(page.url)

        self.words_in_h1 = self.get_words_in_headings(page.heading1)
        self.words_in_h2 = self.get_words_in_headings(page.heading2)
        self.words_in_h3 = self.get_words_in_headings(page.heading3)

        #TODO: nr of links from gov,edu and org
        self.nr_of_links_from_gov = 0
        self.nr_of_links_from_edu = 0
        self.nr_of_links_from_org = 0

        self.nr_of_links_to_gov = 0
        self.nr_of_links_to_edu = 0
        self.nr_of_links_to_org = 0

        self.get_nr_of_links_to_gov_org_edu(page)

    def get_values_for_db(self, current_time):
        return {
            "page_id": self.page_id,
            "update_date_UTC": current_time,
            "language": self.language,
            "pageRank": self.page_rank,
            "page_load_speed": self.page_load_speed,
            "url_length": self.url_length,
            "words_in_heading1": self.words_in_h1,
            "words_in_heading2": self.words_in_h2,
            "words_in_heading3": self.words_in_h3,
            "nr_of_links_from_gov": self.nr_of_links_from_gov,
            "nr_of_links_from_edu": self.nr_of_links_from_edu,
            "nr_of_links_from_org": self.nr_of_links_from_org,
            "nr_of_links_to_gov": self.nr_of_links_to_gov,
            "nr_of_links_to_edu": self.nr_of_links_to_edu,
            "nr_of_links_to_org": self.nr_of_links_to_org,

        }

    def calculate_language_statistics(self, page):
        if page is None:
            return None
        """
        Page is the variable that contains the data we just scraped.

        Statistics will be calculated in 4 categories:

        1. Overall language statics = Measures the language metrics of the webpage, were results are counted on the div data. Header, meta and title is excluded
        2. Title statics = Measures the language metrics of the webpages title
        3. Meta statics = Measures the language metrics of the webpages meta title
        4. Header statistics =Measures the language metrics of the webpages headers, were results are counted per header basis and then the average result is provided
        """

        # Overall statistics
        results_overall = self.get_language_statistics(page.divs)

        # Title statistics
        results_title = self.get_language_statistics(page.title)

        # Meta statistics
        results_meta = self.get_language_statistics(page.meta)

        # Header statistics
        results_header = self.get_language_statistics(page.headings)

        results = {
            "overall": results_overall,
            "title": results_title,
            "meta": results_meta,
            "header": results_header
        }

        return results

    def get_language_statistics(self, data):
        return readability.getmeasures(data, lang='en')

    def get_words_in_headings(self, headings):
        res = [pre_process_with_spacy(heading) for heading in headings]
        res = list(set(list(itertools.chain.from_iterable(res))))
        res.sort()
        return res

    def get_nr_of_links_to_gov_org_edu(self, page):
        links_to_gov = 0
        links_to_edu = 0
        links_to_org = 0

        page_domain = get_domain(page.url)

        for link in page.de_compress(page.urls):
            if page_domain not in link:
                if ".edu" in link:
                    links_to_edu += 1
                elif ".gov" in link:
                    links_to_gov += 1
                elif ".org" in link:
                    links_to_org += 1

        self.nr_of_links_to_gov = links_to_gov
        self.nr_of_links_to_edu = links_to_edu
        self.nr_of_links_to_org = links_to_org
