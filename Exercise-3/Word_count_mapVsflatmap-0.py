from pyspark import SparkConf, SparkContext

path = "/home/haseebwaheedkhan/Downloads/GitHaseeb/ApacheSpark/Exercise-3"
 
    


conf = SparkConf().setMaster("local").setAppName("CountWord")
sc = SparkContext(conf=conf)

lines = sc.textFile(f"{path}/Book")
words = lines.flatMap(lambda x: x.split())

words_count = words.countByValue()

for word, count in words_count.items():

    print(word, count)