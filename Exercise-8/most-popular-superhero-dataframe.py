from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

graph_path = "/home/haseebwaheedkhan/Downloads/GitHaseeb/ApacheSpark/Exercise-8/Marvel+Graph"
name_path = "/home/haseebwaheedkhan/Downloads/GitHaseeb/ApacheSpark/Exercise-8/Marvel+Names"

schema_header = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

spark = SparkSession.builder.appName("MostPopularHero").getOrCreate()

names = spark.read.schema(schema_header).option("sep"," ").csv(name_path)
lines = spark.read.text(graph_path)

# connections = lines.withColumn("ID", lines.split(",")[0]).withColumn("popularity_count", func.size(lines.split(","))-1)

connections = lines.withColumn("ID", func.split(func.col("value"), " ")[0])\
    .withColumn("popularity_count", func.size(func.split(func.col("value"), " ")) -1)\
    .groupBy("ID").agg(func.sum("popularity_count").alias("connections")) 
connections.show()


most_popular = connections.sort(func.col("connections").desc()).first()
# most_popular.show()
top_hero = names.filter(func.col("id") == most_popular[0])
top_hero.show()

spark.stop


 