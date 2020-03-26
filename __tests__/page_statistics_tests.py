import unittest


from scraper.entity.page_statisitcs import PageStatistics


class PageStatisticsTest(unittest.TestCase):

    def test_readability(self):
        dummy_page = PageStatistics(None, None,None, None)


        sample_text = ('This is an example sentence .\n'
        'Note that tokens are separated by spaces and sentences by newlines .\n')
        results = dummy_page.get_language_statistics(sample_text)

        print(results)
        readability_grades = results['readability grades']
        sentence_info = results['sentence info']
        word_usage = results['word usage']
        sentence_beginnings = results['sentence beginnings']



        #Measuring readability grades
        self.assertEqual(readability_grades['Kincaid'], 7.442500000000003)
        self.assertEqual(readability_grades['ARI'],5.825624999999999)
        self.assertEqual(readability_grades['Coleman-Liau'], 9.532550312500003)
        self.assertEqual(readability_grades['FleschReadingEase'], 55.95250000000002)
        self.assertEqual(readability_grades['GunningFogIndex'],10.700000000000001)
        self.assertEqual(readability_grades['LIX'], 39.25)
        self.assertEqual(readability_grades['SMOGIndex'], 9.70820393249937)
        self.assertEqual(readability_grades['RIX'], 2.5)
        self.assertEqual(readability_grades['DaleChallIndex'], 9.954550000000001)

        #Measure sentece info
        self.assertEqual(sentence_info['characters_per_word'], 4.9375)
        self.assertEqual(sentence_info['syll_per_word'], 1.6875)
        self.assertEqual(sentence_info['words_per_sentence'], 8.0)
        self.assertEqual(sentence_info['sentences_per_paragraph'], 2.0)
        self.assertEqual(sentence_info['type_token_ratio'], 0.9375)
        self.assertEqual(sentence_info['characters'], 79)
        self.assertEqual(sentence_info['syllables'], 27)
        self.assertEqual(sentence_info['words'], 16)
        self.assertEqual(sentence_info['wordtypes'], 15)

        self.assertEqual(sentence_info['sentences'], 2)
        self.assertEqual(sentence_info['paragraphs'], 1)
        self.assertEqual(sentence_info['long_words'], 5)
        self.assertEqual(sentence_info['complex_words'], 3)
        self.assertEqual(sentence_info['complex_words_dc'], 6)

        #Measure word usage


        #Measure sentece beginnings