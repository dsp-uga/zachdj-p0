from pyspark import SparkContext, SparkConf
import numpy as np
import json
import os
import re
import config


APP_NAME = config.get("APP_NAME", "zachdj-p0-sp1")
CLUSTER = config.get("CLUSTER_URI", "local")
DATA_LOCATION = config.get("DATA_LOCATION", "testdata")

NUM_WORDS = 5   # controls how many of top words will be kept for *each* document

whitespace_regex = re.compile('\s+')

with open('stopwords.txt', 'r') as stopwords_file:
    stopwords_list = stopwords_file.readlines()

stopwords_list = list(map(lambda word: word.strip().lower(), stopwords_list))

conf = SparkConf().setAppName(APP_NAME).setMaster(CLUSTER)
sc = SparkContext(conf=conf)

stopwords = sc.broadcast(stopwords_list)
punctuation = sc.broadcast('.,:;\'`!?')

text_files = sc.wholeTextFiles(DATA_LOCATION)  # reads text files and their contents into an RDD
document_count = text_files.count()
document_count = sc.broadcast(document_count)


def doc_to_words(tuple):
    doc_index = tuple[0]
    count_vector = np.zeros(document_count.value)
    count_vector[doc_index] = 1

    words = re.split(r'\s+', tuple[1])
    return map(lambda word: (word.strip(), count_vector), words)


def tfidf(tuple):
    word = tuple[0]
    counts = tuple[1]
    N = document_count.value
    n_t = np.count_nonzero(counts)

    idf = np.log(N / n_t)
    scores = idf * counts  # tf-idf scores

    return (word, scores)


# The files are currently identified by their path names;  we would prefer numeric identifiers
text_files_numeric = text_files.zipWithIndex().map(lambda x: (x[1], x[0])) \
                        .map(lambda element: (element[0], element[1][1]))

words = text_files_numeric.flatMap(doc_to_words)
words = words.map(lambda tuple: (tuple[0].lower(), tuple[1]))
words = words.map(lambda tuple: (tuple[0].strip(punctuation.value), tuple[1]))
words = words.filter(lambda tuple: len(tuple[0]) > 1)
words = words.filter(lambda tuple: tuple[0] not in stopwords.value)
counts = words.reduceByKey(lambda vec1, vec2: vec1 + vec2)

scored_words = counts.map(tfidf)

scored_words.cache()

top_words = []  # top words across all documents will be unioned into this RDD

for doc_index in np.arange(0, document_count.value):
    doc_scored_words = scored_words.map(lambda x: (x[0], x[1][doc_index]))
    top_n = doc_scored_words.takeOrdered(NUM_WORDS, lambda x: -x[1])
    top_words.extend(top_n)

top_words = dict(top_words)

# ensure output directory exists
if not os.path.exists("output"):
    os.makedirs("output")

# serialize output as JSON dictionary
json.dump(top_words, open('output/sp4.json', 'w'))
