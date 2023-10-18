from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkWordCount").getOrCreate()

path = "/home/haseebwaheedkhan/Downloads/GitHaseeb/ApacheSpark/Exercise-6/book.txt"

text = spark.read.text(path)
text.show()

# func.split will act as a flatmap function in rdd so here we are 
# splitting the sentences into words in indivisual row
words = text.select(func.explode(func.split(text.value, "\\W+")).alias("word"))

# To avoid duplication we need to converts words to lower case
lowercase_words = words.select(func.lower(words.word).alias("word"))

# discarding empty rows
lowercase_words = lowercase_words.filter(lowercase_words.word != "")

word_count = lowercase_words.groupBy("word").count().sort("count")

word_count.show(word_count.count(s))

spark.stop()