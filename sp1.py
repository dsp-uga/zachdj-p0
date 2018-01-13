from pyspark import SparkContext, SparkConf
import json
import os
import config


APP_NAME = config.get("APP_NAME", "zachdj-p0-sp1")
CLUSTER = config.get("CLUSTER_URI", "local")
DATA_LOCATION = config.get("DATA_LOCATION", "testdata")

NUM_WORDS = 40   # controls how many of the most frequent words are kept and output to the json file

conf = SparkConf().setAppName(APP_NAME).setMaster(CLUSTER)
sc = SparkContext(conf=conf)

text_file = sc.textFile(DATA_LOCATION)  # reads the lines from all text files into an RDD
words = text_file.flatMap(lambda line: line.split()) \
             .map(lambda word: (word.lower(), 1))

counts = words.reduceByKey(lambda a, b: a + b)
filtered = counts.filter(lambda x: x[1] > 2)

top_n = filtered.takeOrdered(NUM_WORDS, lambda x: -x[1])

# ensure output directory exists
if not os.path.exists("output"):
    os.makedirs("output")

# serialize output as JSON dictionary
top_n = dict(top_n)
json.dump(top_n, open('output/sp1.json', 'w'))
