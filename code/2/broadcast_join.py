from pyspark.sql import SparkSession
from pyspark import RDD

spark = SparkSession.builder.appName("BroadcastJoin").getOrCreate()
sc = spark.sparkContext

def map_genres(x):
    x = x.split(",")
    return (x[0], x[1])

def map_ratings(x):
    x = x.split(",")
    return (x[1], (x[0], x[2], x[3]))

def broadcast_join(self, right):
    b_var = sc.broadcast( \
        right.map(lambda x: (x[0], x[1])) \
        .groupByKey() \
        .collectAsMap() \
    )
    
    def br_map(x):
        return ((x[0],(record,x[1])) for record in b_var.value.get(x[0], []))]
    rdd = self.flatMap(lambda x: br_map(x))

    return rdd

rdd1 = sc.textFile("hdfs://master:9000/movies/ratings.csv") \
    .map(lambda x: map_ratings(x))

rdd2 = sc.textFile("hdfs://master:9000/movies/reduced_genres.csv") \
    .map(lambda x: map_genres(x))


# Join with join column = key of rdd
RDD.broadcast_join = broadcast_join
rdd = rdd1.broadcast_join(rdd2).collect()

print("-------------------------------------------------------------")
print("Repartition Join Output")
for i in rdd:
    print(i)
print("-------------------------------------------------------------")
