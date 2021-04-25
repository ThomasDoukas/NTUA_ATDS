from pyspark.sql import SparkSession

def map_ratings(x):
    x = x.split(',')
    return (x[0], (float(x[2]), 1))    

spark = SparkSession.builder.appName("Q2RDD").getOrCreate()
sc = spark.sparkContext

# map(user_id, (rating, counter_for_all_ratings_of_a_user=1))
# reduceByKey (user_id, (sum_of_ratings, counter_of_ratings))
# map(user_id, (avg_rating_in_all_movies))
rdd = sc.textFile("hdfs://master:9000/movies/ratings.csv") \
    .map(lambda x: map_ratings(x)) \
    .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) \
    .map(lambda x: (x[0], x[1][0]/x[1][1]))

total_users = rdd.count()
users = rdd.filter(lambda x: True if(x[1] > 3.0) else False).count()
per = users*100/total_users

print("-------------------------------------------------------------")
print("Query 2 - RDD API Output")
print("Percentage = ", per, "%")
print("-------------------------------------------------------------")
