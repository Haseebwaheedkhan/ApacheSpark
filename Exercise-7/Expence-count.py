from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql import Row


path = "/home/haseebwaheedkhan/Downloads/customer-orders.csv"

# Creating session
spark = SparkSession.builder.appName("SparkUserExpenseCount").getOrCreate()

# people = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

# # Exploring Data
# people.printSchema()
# people.show()

def mapper(line):
    fields = line.split(',')    
    return Row( cust_id=int(fields[0]), item_id=int(fields[1]), expense=float(fields[2]))


people = spark.sparkContext.textFile(path)
people = people.map(mapper) 

schema = spark.createDataFrame(people).cache()
schema.createOrReplaceTempView("people")

person_expense = schema.select("cust_id","expense")
person_expense.show()

exp_count = person_expense.groupby("cust_id").agg(func.round(func.sum("expense"),2).alias("total_purchase"))
# exp_count = person_expense.groupby("cust_id").sum("expense")
exp_count.sort("total_purchase").show()
exp_count.sort("cust_id").show()

spark.stop()

