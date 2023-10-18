# importing Libraries
import re
from pyspark import SparkConf, SparkContext


def normalize_words(line):
    return re.compile(r'\W+',re.UNICODE).split(line.lower())


path = "/home/haseebwaheedkhan/Downloads/GitHaseeb/ApacheSpark/Exercise-3"

conf = SparkConf().setMaster("local").setAppName("Words-Count")
sc = SparkContext(conf=conf)

input = sc.textFile(f"{path}/Book")
words = input.flatMap(normalize_words)

word_count = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
word_count_sorted = word_count.map(lambda x: (x[1],x[0])).sortByKey()

results = word_count_sorted.collect()

for word in results:
    print(word[1], word[0])
