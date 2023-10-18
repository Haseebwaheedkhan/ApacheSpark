from pyspark.sql import SparkSession
from pyspark.sql import Row

# Creating a Session
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

file_path = "/home/haseebwaheedkhan/Downloads/GitHaseeb/ApacheSpark/Exercise-5/fakefriends.csv"

# # Discarding reading data using this way because there is no header available to need 
# # to create a row and pass it to the dataframe
# # Reading the csv file
# people = spark.read.option("header","true").option("inferred","true").csv(file_path)

# people.printSchema()


def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1]), age=int(fields[2]), friendscount=int(fields[3]))


input = spark.sparkContext.textFile(file_path)
people = input.map(mapper) # converting a text file to typed dataframe

schema = spark.createDataFrame(people).cache()
schema.createOrReplaceTempView("people")

schema.printSchema()

# people.groupBy("age").agg("friendscount":"avg").show()
result = schema.groupBy("age").avg("friendscount").orderBy("age")
result.show()

spark.stop()


