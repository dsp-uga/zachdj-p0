from pyspark import SparkContext, SparkConf
import json
import os
import config

"""  This script performs word counting for Subproject A
    
    This performs a naive word count from all text files from the DATA_LOCATION configuration variable.
    The word count is case-insensitive, and words occurring fewer than twice are dropped.
    The top 40 most frequent words are output to the file ouput/sp1.json as a dictionary of (word, count) pairs
"""

# get configuration variables from the config.json file
APP_NAME = config.get("APP_NAME", "zachdj-p0-sp1")
CLUSTER = config.get("CLUSTER_URI", "local")
DATA_LOCATION = config.get("DATA_LOCATION", "testdata")

NUM_WORDS = 40   # controls how many of the most frequent words are kept and output to the json file

conf = SparkConf().setAppName(APP_NAME).setMaster(CLUSTER)
sc = SparkContext(conf=conf)

# reads the lines from all text files in DATA_LOCATION into an RDD
text_file = sc.textFile(DATA_LOCATION)

# splits the lines into individual lowercase words, each with count 1
words = text_file.flatMap(lambda line: line.split()) \
             .map(lambda word: (word.lower(), 1))

# sum up all the entries with the same key (this will find the count for each word)
counts = words.reduceByKey(lambda a, b: a + b)

# remove any words occurring less than twice
filtered = counts.filter(lambda x: x[1] > 2)

# write the top 40 words to ouput/sp1.json
top_n = filtered.takeOrdered(NUM_WORDS, lambda x: -x[1])
if not os.path.exists("output"):
    os.makedirs("output")

top_n = dict(top_n)
json.dump(top_n, open('output/sp1.json', 'w'))
