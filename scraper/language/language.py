# TODO: support other languages
# TODO: implement lemma support



import numpy as np
import string


def get_suitable_words():
    from nltk.corpus import stopwords, words
    stop_words = stopwords.words('english')
    words.ensure_loaded()
    a = np.array(words.words())
    b = np.array(stop_words)
    return a[~np.isin(a, b)]




def manage_real_words(words):
    all_words = np.array(words)
    suitable_values = get_suitable_words()
    suitable_words_filter = np.in1d(words, suitable_values)
    return all_words[suitable_words_filter]



def pre_process(array):
    # remove punctuation
    table = str.maketrans('', '', string.punctuation)
    stripped = [w.translate(table) for w in array]
    # remove single charactes
    no_single_char = list(filter(lambda word: len(word) > 1, stripped))
    # TODO: implement lemmatization

    return no_single_char


def word_count(string):
    my_string = pre_process(np.array(string.lower().split()))
    results = manage_real_words(my_string)
    my_dict = {}

    for i in results:
        my_dict[i] = my_dict.get(i, 0) + 1

    return my_dict
