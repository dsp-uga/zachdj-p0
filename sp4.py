from pyspark import SparkContext, SparkConf
import numpy as np
import json
import os
import re
import config

"""  This script performs word counting for Subproject D

    This script finds the document specific TF-IDF score for each word and outputs the top 5 words from each document
    The algorithm for computing TF-IDF scores is summarized:
        - read in N text files, indexed by location
        - give each text file a numeric index in the range [0, N-1]
        - split the contents of each file F_i into word-vector pairs (word, vec), 
            where vec is a length-N vector with a one in the ith position and zeros in all other positions
            (e.g. if the word "aardvark" occurs in document five, then the pair (aardvark, [0, 0, 0, 0, 1, 0, 0, ..., 0]
             would be created)
        - sum up the count vector for each word ( resulting in an array of term frequencies for each document)
        - compute the TF-IDF score for each word in each document using the count vector (see the tfidf function below)
"""

APP_NAME = config.get("APP_NAME", "zachdj-p0-sp4")
CLUSTER = config.get("CLUSTER_URI", "local")
DATA_LOCATION = config.get("DATA_LOCATION", "testdata")

NUM_WORDS = 5   # controls how many of top words will be kept for *each* document

conf = SparkConf().setAppName(APP_NAME).setMaster(CLUSTER)
sc = SparkContext(conf=conf)

# broadcast list of punctuation symbols to strip to all nodes
punctuation = sc.broadcast('.,:;\'`!?')

# reads the text files into (location, text-content) pairs
text_files = sc.wholeTextFiles(DATA_LOCATION)

# find total number of documents N
document_count = text_files.count()
document_count = sc.broadcast(document_count)


def doc_to_words(tuple):
    """
    This function takes an (index, content) tuple and returns a list of (word, count_unit_vector) pairs for each word in
    the content.
    """
    doc_index = tuple[0]
    count_vector = np.zeros(document_count.value)
    count_vector[doc_index] = 1  # e.g. if this is document 2, the count vector will be [0, 0, 1, 0, 0, ... 0]

    words = re.split(r'\s+', tuple[1])  # splits the content of the file into whitespace-delimited words
    return map(lambda word: (word.strip(), count_vector), words)


def tfidf(tuple):
    """
    This function computes the per-document TF-IDF scores for the provided (word, count_vector) pair
    tuple[0] is the word
    tuple[1] is the vector of counts
    """
    word = tuple[0]
    counts = tuple[1]
    N = document_count.value  # total number of documents
    n_t = np.count_nonzero(counts)  # number of documents in which this word appears

    idf = np.log(N / n_t)
    scores = idf * counts  # multiplies the idf by the term frequency for each document
    return (word, scores)


# zipWithIndex gives each element an integer index, then the map makes the index the first element
# this leaves (index, (location, file_content)) tuples in the RDD
text_files_numeric = text_files.zipWithIndex().map(lambda x: (x[1], x[0]))

# now we need to strip out the location and just have (index, file_content) pairs
# x[0] is the numeric index and x[1][1] is the file content string
text_files_numeric_idx = text_files_numeric.map(lambda x: (x[0], x[1][1]))

# extract words with unit count vectors
words = text_files_numeric_idx.flatMap(doc_to_words)

# ensure case insensitivity
words = words.map(lambda tuple: (tuple[0].lower(), tuple[1]))

# strip leading/trailing punctuation and ensure that the word is longer than 1 letter (leftover from subproject C)
words = words.map(lambda tuple: (tuple[0].strip(punctuation.value), tuple[1]))
words = words.filter(lambda tuple: len(tuple[0]) > 1)

# sum up count vectors
counts = words.reduceByKey(lambda vec1, vec2: vec1 + vec2)

# convert the count vectors to tfidf scores for each document
scored_words = counts.map(tfidf)

scored_words.cache()

# TODO: there's probably a more Spark-ish way to do the following
top_words = []  # top words across all documents will be unioned into this RDD

# extract the top 5 words from each document
for doc_index in np.arange(0, document_count.value):
    doc_scored_words = scored_words.map(lambda x: (x[0], x[1][doc_index]))
    top_n = doc_scored_words.takeOrdered(NUM_WORDS, lambda x: -x[1])
    top_words.extend(top_n)

# write the top words to output/sp4.json
if not os.path.exists("output"):
    os.makedirs("output")

top_words = dict(top_words)
json.dump(top_words, open('output/sp4.json', 'w'))
