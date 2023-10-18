# Importing Libraries
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Customer-Expence-Count")
sc = SparkContext(conf=conf)


print("*********************** Scrip Execution Start ***********************")

lines = sc.textFile("/home/haseebwaheedkhan/Downloads/GitHaseeb/ApacheSpark/Exercise-4/customer-orders.csv")

print("Taking out First three lines from csv file to visualize the data\n")
first_3_lines  = lines.take(3)
for line in first_3_lines:
    print(f"Line : ", line)

fields = lines.map(lambda x : x.split(","))
id_expense = fields.map(lambda x : (int(x[0]),float(x[2])))

# agg_expense = id_expense.reduceByKey(lambda x,y: x+y)
# result  = agg_expense.collect()

# # for x in agg_expense.items():
# #     print(x)

# Sum the expenses for each customer ID
agg_expense = id_expense.reduceByKey(lambda x, y: x + y).sortByKey()
result = agg_expense.collect()

#Print the customer id and their expense in sorted order
for customer_id, total_expense in result:
    print(f"Customer ID: {customer_id}, Total Expense: {total_expense}")


# Finding the Customer with least expense
min_exp = agg_expense.reduceByKey(lambda x, y: min(x,y))

print("Min expense : ", min_exp)