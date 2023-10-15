from pyspark import SparkConf, SparkContext

# Path Definition
path = "/home/haseebwaheedkhan/Downloads/GitHaseeb/ApacheSpark/Exercise-1"


# Configuring Spark Context
conf = SparkConf().setMaster("local").setAppName("FriendsbyAges")
sc = SparkContext(conf=conf)

def parseline(line):
    '''
    Take line of each csv file and return a key value pair i.e dictionry of (age, number of friends) 

    output 
    Dictionary (age, number of friends)
    '''
    
    fields = line.split(",")
    age = int(fields[2])
    num_friends = int(fields[3])
    
    return (age, num_friends)

lines  = sc.textFile(f"{path}/fakefriends.csv")
rdd = lines.map(parseline)

totalByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y : (x[0]+y[0], x[1]+y[1]))

averageByAge = totalByAge.mapValues(lambda x: x[0] / x[1])

results = averageByAge.collect()

for result in results:
    print(result)