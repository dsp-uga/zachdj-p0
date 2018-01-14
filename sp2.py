from pyspark import SparkContext, SparkConf
import json
import os
import config

"""  This script performs word counting for Subproject B
    
    This script operates in identical fashion to sp1.py, counting the frequency of words across all documents.
    However, common stopwords (from stopwords.txt) are filtered out and not included in the results.
"""

APP_NAME = config.get("APP_NAME", "zachdj-p0-sp2")
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

# reads the lines from all text files in DATA_LOCATION into an RDD
text_file = sc.textFile(DATA_LOCATION)

# splits the lines into individual words
words = text_file.flatMap(lambda line: line.split())

# remove words that are in the list of stopwords
words = words.filter(lambda word: word.lower() not in stopwords.value)

# count remaining words
words = words.map(lambda word: (word.lower(), 1))
counts = words.reduceByKey(lambda a, b: a + b)
filtered = counts.filter(lambda x: x[1] > 2)   # leftover from subproject A

# write top 40 words to output/sp2.json
top_n = filtered.takeOrdered(NUM_WORDS, lambda x: -x[1])
if not os.path.exists("output"):
    os.makedirs("output")

top_n = dict(top_n)
json.dump(top_n, open('output/sp2.json', 'w'))
