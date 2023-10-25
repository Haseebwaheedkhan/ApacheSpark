from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

graph_path = "/home/haseebwaheedkhan/Downloads/GitHaseeb/ApacheSpark/Exercise-8/Marvel+Graph"
names =  "/home/haseebwaheedkhan/Downloads/GitHaseeb/ApacheSpark/Exercise-8/Marvel+Names"

spark = SparkSession.builder.appName("LeastHero").getOrCreate()

schema = StructType([\
    StructField("id" , IntegerType(), True),\
    StructField("names" , StringType(), True)\
])

df_names = spark.read.option("sep", " ").schema(schema).csv(names)
df_names.show()

df_graph = spark.read.text(graph_path)
df_names_relation_count = df_graph.withColumn("id", func.split(func.col("value")," ")[0])\
    .withColumn("friends-count", func.size(func.split(func.col("value")," "))-1)\
    .groupBy("id").agg(func.sum("friends-count").alias("connections"))
    
df_names_relation_count.show()

min_connections = df_names_relation_count.agg(func.min(func.col("connections"))).first()[0]


least_popular_heros = df_names_relation_count.filter(func.col("connections") == min_connections)
least_popular_heros.show()

least_pop_heros_with_names = least_popular_heros.join(df_names, "id")
least_pop_heros_with_names.select("names").show()


spark.stop()