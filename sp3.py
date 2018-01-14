from pyspark import SparkContext, SparkConf
import json
import os
import config

"""  This script performs word counting for Subproject C
    
    This performs a case-insensitive word count across all documents, excluding stopwords, and stripping
    leading/trailing punctuation from words before counting.
"""

APP_NAME = config.get("APP_NAME", "zachdj-p0-sp3")
CLUSTER = config.get("CLUSTER_URI", "local")
DATA_LOCATION = config.get("DATA_LOCATION", "testdata")

NUM_WORDS = 40   # controls how many of the most frequent words are kept and output to the json file

conf = SparkConf().setAppName(APP_NAME).setMaster(CLUSTER)
sc = SparkContext(conf=conf)

# read the list of stopwords and broadcast it to all nodes
with open('stopwords.txt', 'r') as stopwords_file:
    stopwords_list = stopwords_file.readlines()

# ensure the stop words contain no whitespace or capital letters
stopwords_list = list(map(lambda word: word.strip().lower(), stopwords_list))
stopwords = sc.broadcast(stopwords_list)

# also broadcast a string containing the punctuation characters to strip
# note that ’ is the right single quotation mark (U+2019) and ` is the left single quotation mark
punctuation = sc.broadcast('.,:;\'`’!?')


# reads the lines from all text files in DATA_LOCATION into an RDD
text_file = sc.textFile(DATA_LOCATION)

# split each line into white-space delimited words
words = text_file.flatMap(lambda line: line.split())

# strip leading and trailing punctuation marks
words = words.map(lambda word: word.strip(punctuation.value))

# remove any words with a single letter
words = words.filter(lambda word: len(word) > 1)

# remove stopwords
words = words.filter(lambda word: word.lower() not in stopwords.value)

# convert to lowercase and set each individual wordcount to 1
words = words.map(lambda word: (word.lower(), 1))

# sum up the words
counts = words.reduceByKey(lambda a, b: a + b)
filtered = counts.filter(lambda x: x[1] > 2)  # leftover from subproject A: remove any words that occur < twice

# write top 40 words to output/sp3.json
top_n = filtered.takeOrdered(NUM_WORDS, lambda x: -x[1])
if not os.path.exists("output"):
    os.makedirs("output")

top_n = dict(top_n)
json.dump(top_n, open('output/sp3.json', 'w'))
