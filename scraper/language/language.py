# TODO: support other languages

from nltk.corpus import stopwords, words
from nltk.stem import WordNetLemmatizer
import math
import multiprocessing as mp
from multiprocessing.dummy import Pool as ThreadPool
import numpy as np
from textblob import TextBlob
import string

stop_words = stopwords.words('english')
words.ensure_loaded()

a = np.array(words.words())
b = np.array(stop_words)
suitable_values = a[~np.isin(a, b)]
lemmatiser = WordNetLemmatizer()


def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


def make_singles(words):
    words_as_str = ' '.join([str(elem) for elem in words])
    blob = TextBlob(words_as_str)

    return list(map(lambda x: x.singularize(), blob.words))


def sub_manage_real_words(all_words):
    global suitable_values
    words = np.array(make_singles(all_words))
    suitable_words_filter = np.in1d(words, suitable_values)
    exclude_single_letters_logic = np.vectorize(lambda word: len(word) > 1)

    only_suitable_words = words[suitable_words_filter]
    exclude_single_letters_filter = exclude_single_letters_logic(only_suitable_words)

    return only_suitable_words[exclude_single_letters_filter]


def manage_real_words(all_words):
    size = math.ceil(len(all_words) / (max(mp.cpu_count() - 1, 1)))
    # print(size)
    chunks = list(divide_chunks(all_words, size))

    pool = ThreadPool()
    results = list(pool.imap(sub_manage_real_words, chunks))
    pool.close()
    pool.join()
    results = [item for sublist in results for item in sublist]

    return results


def pre_process(array):
    # remove punctuation
    table = str.maketrans('', '', string.punctuation)
    stripped = [w.translate(table) for w in array]
    partial_results = list(filter(lambda word: len(word) > 1, stripped))
    return [lemmatiser.lemmatize(res) for res in partial_results]


def word_count(string):
    # print(len(words.words()))
    my_string = pre_process(np.array(string.lower().split()))
    # print(my_string)
    # print("recived ", len(my_string), "words")
    # only_real_words = remove_fake_words(my_string)
    # print("only  ", len(only_real_words), "are real words")
    # stopword_free_string = remove_stop_words(only_real_words)
    results = manage_real_words(my_string)

    # print("only  ", len(results), "are real words that are not stopwords")

    my_dict = {}

    for i in results:
        my_dict[i] = my_dict.get(i, 0) + 1

    return my_dict
