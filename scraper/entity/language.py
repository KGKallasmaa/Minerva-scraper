# TODO: support other languages

import spacy
from collections import Counter

nlp = spacy.load("en_core_web_sm")


# How to download it:
# pip3 install https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-2.2.0/en_core_web_sm-2.2.0.tar.gz


class Language:
    def __init__(self):
        self.am_ready = True  # dummy variable

    def pre_process_with_spacy(self, string):
        doc = nlp(string)

        tokens = [token.lemma_ for token in doc if
                  (token.is_stop is False and token.is_punct is False and token.is_space is False)]
        # Remove single characters.
        tokens = [token for token in tokens if len(token) > 1]
        # To lower case
        tokens = [x.lower() for x in tokens]

        return tokens

    def word_count(self, string):
        pre_prossessed_array = self.pre_process_with_spacy(string)
        return Counter(pre_prossessed_array)

    def nr_of_words_in_url(self, dict):
        return sum(dict.values())

    def find_unique_named_entities(self, string):
        if string is None:
            return {}
        doc = nlp(string)

        type_entities = {}  # entity_type(e.g. Person) and entities (e.g John, Mary, James)

        suitable_entity_labels = ["GPE", "PERSON", "ORG"]  # we only want some types

        for ent in doc.ents:
            if ent.label_ in suitable_entity_labels:
                if ent.label_ in type_entities:
                    current_values = type_entities[ent.label_]
                    current_values.append(ent.text)
                    type_entities[ent.label_] = current_values
                else:
                    type_entities[ent.label_] = [ent.text]

        for key, value in type_entities.items():
            type_entities[key] = list(set(value))

        return type_entities
