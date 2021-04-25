from pyspark.sql import SparkSession
from pyspark import RDD

spark = SparkSession.builder.appName("RepartitionJoin").getOrCreate()
sc = spark.sparkContext

def map_genres(x):
    x = x.split(",")
    return (x[0], x[1])

def map_ratings(x):
    x = x.split(",")
    return (x[1], (x[0], x[2], x[3]))

def red(x):
    l_buf, r_buf = [], []
    for (tag, value) in x[1]:
        if(tag == 'Left'):
            l_buf.append(value)
        elif(tag == 'Right'):
            r_buf.append(value)

    return ((x[0], (left, right)) for left in l_buf for right in r_buf)

def repartition_join(self, right):
    right = right.map(lambda x: (x[0], ('Right', x[1])))
    
    rdd = self.map(lambda x: (x[0], ('Left', x[1]))) \
        .union(right) \
        .groupByKey() \
        .flatMap(lambda x: red(x))

    return rdd

rdd1 = sc.textFile("hdfs://master:9000/movies/reduced_genres.csv") \
    .map(lambda x: map_genres(x))

rdd2 = sc.textFile("hdfs://master:9000/movies/ratings.csv") \
    .map(lambda x: map_ratings(x))

# Join with join column = key of rdd
RDD.repartition_join = repartition_join
rdd = rdd1.repartition_join(rdd2).collect()

print("-------------------------------------------------------------")
print("Repartition Join Output")
for i in rdd:
    print(i)
print("-------------------------------------------------------------")
