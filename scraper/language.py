

# TODO: support other languages
stop_words = [line.rstrip('\n') for line in open('./language/english_stopwords.txt')]


def remove_stop_words(words):
    return [word for word in words if word not in stop_words]


def word_count(string):
    my_string = string.lower().split()
    stopword_free_string = remove_stop_words(my_string)

    my_dict = {}
    for item in stopword_free_string:
        if item in my_dict:
            my_dict[item] += 1
        else:
            my_dict[item] = 1

    return my_dict
