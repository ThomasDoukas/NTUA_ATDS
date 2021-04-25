from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("OriginalJoin").getOrCreate()
sc = spark.sparkContext

def map_genres(x):
    x = x.split(",")
    return (x[0], x[1])

def map_ratings(x):
    x = x.split(",")
    return (x[1], (x[0], x[2], x[3]))

rdd1 = sc.textFile("hdfs://master:9000/movies/reduced_genres.csv") \
    .map(lambda x: map_genres(x))

rdd2 = sc.textFile("hdfs://master:9000/movies/ratings.csv") \
    .map(lambda x: map_ratings(x))

# Join with join column = key of rdd
rdd = rdd1.join(rdd2).collect()

print("-------------------------------------------------------------")
print("Original RDD Api Join Output")
for i in rdd:
    print(i)
print("-------------------------------------------------------------")
