from pyspark.sql import SparkSession
from io import StringIO
import csv

def split_complex(x):
    res = list(csv.reader(StringIO(x), delimiter=','))[0]
    return (res[0], tuple(res[1:]))

spark = SparkSession.builder.appName("Q1RDD").getOrCreate()
sc = spark.sparkContext

# filter(date>2000, cost!=0, earnings!=0)
# map(year, (id, name, earnings percentage))
# reduce max(earnings percentage)
# sort by key
rdd = sc.textFile("hdfs://master:9000/movies/movies.csv") \
    .map(lambda x: split_complex(x)) \
    .filter(lambda x: True if(x[1][2] and x[1][2].split("-")[0] >= "2000" and float(x[1][4])!=0 and float(x[1][5])!=0) else False) \
    .map(lambda x: (x[1][2].split("-")[0], (x[0], x[1][0], (float(x[1][5])-float(x[1][4]))*100/float(x[1][4])))) \
    .reduceByKey(lambda x, y: (x if (x[0] < y[0]) else y) if (x[2] == y[2]) else (x if (x[2] > y[2]) else y)) \
    .sortByKey() \
    .collect()

print("-------------------------------------------------------------")
print("Query 1 - RDD API Output")
print("(Year, (Movie_id, Title, Profit))")
for i in rdd:
    print(i)
print("-------------------------------------------------------------")
