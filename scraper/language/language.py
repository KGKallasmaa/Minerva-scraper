# TODO: support other languages
# TODO: implement lemma support

import numpy as np
import string





def manage_real_words(words):
    #Remove stopwords
    from nltk.corpus import stopwords
    stop_words = np.array(stopwords.words('english'))
    return np.setdiff1d(words,stop_words)





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
