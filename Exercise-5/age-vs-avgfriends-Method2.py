from pyspark.sql import SparkSession

path = "/home/haseebwaheedkhan/Downloads/GitHaseeb/ApacheSpark/Exercise-5/fakefriends-header.csv"

# Creating SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

df = spark.read.option("header","true").option("inferschema","true").csv(path)

# printing the schema 
df.printSchema()


df.groupBy("age").avg("friends").orderBy("age").show()
spark.stop