from pyspark import SparkConf, SparkContext


def parse_line(line):
    field=line.split(",")
    station_id = field[0]
    entry_type = field[2]
    temperature = float(field[3]) * 0.1 * (9.0/5.0) +32
    return (station_id, entry_type, temperature)


path = "/home/haseebwaheedkhan/Downloads/GitHaseeb/ApacheSpark/Exercise-2"
conf = SparkConf().setMaster("local").setAppName("MinTemperature")
sc = SparkContext(conf=conf)

lines = sc.textFile(f"{path}/1800.csv")
parsed_lines = lines.map(parse_line)
min_temp = parsed_lines.filter(lambda x: "TMIN" in x[1])
station_temps = min_temp.map(lambda x: (x[0],x[2])) 
minTemps = station_temps.reduceByKey(lambda x, y: min(x, y))
results = minTemps.collect()
 

for result in results:
    print(f"Minimum Temperature : {result}")

# # Finding the maximum temperation

max_temp = parsed_lines.filter(lambda x: "TMAX" in x[1]) 
max_station_temps = max_temp.map(lambda x: (x[0],x[2])) 
max_Temps = max_station_temps.reduceByKey(lambda x, y: max(x, y))
max_results = max_Temps.collect()
 

for result in max_results:
    print(f"Max Temperature : {result}")
