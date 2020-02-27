from scraper.database.database import get_page_rank_by_page_id
import readability


class PageStatistics:
    def __init__(self, page_id, current_time, page, speed):
        self.page_id = page_id
        self.update_date_UTC = current_time
        self.language = self.calculate_language_statistics(page)
        self.page_rank = get_page_rank_by_page_id(self.page_id)
        self.page_load_speed = speed
        self.url_length = len(page.url)

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
