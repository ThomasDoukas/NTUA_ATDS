from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Q3RDD").getOrCreate()
sc = spark.sparkContext

def map_genres(x):
    x = x.split(",")
    return (x[0], x[1])

def map_ratings(x):
    x = x.split(",")
    return (x[1], (float(x[2]), 1)) 

# map(movie_id, (rating, rating_counter=1))
# reduceByKey(movie_id, (rating_sum, total_rates))
# map(movie_id, avg_rating)
ratings = sc.textFile("hdfs://master:9000/movies/ratings.csv") \
    .map(lambda x: map_ratings(x)) \
    .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) \
    .map(lambda x: (x[0], (x[1][0]/x[1][1])))

# map (movie_id, genre)
# join (movie_id, (genre, avg_rating)
# map (genre, (avg_rating, movies_in_genre_count=1))
# reduceByKey (genre, (ratings_sum, total_movies_in_genre))
# map (genre, (genre_rating, total_movies_in_genre))
rdd = sc.textFile("hdfs://master:9000/movies/movie_genres.csv") \
    .map(lambda x: map_genres(x)) \
    .join(ratings) \
    .map(lambda x: (x[1][0], (x[1][1], 1))) \
    .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) \
    .map(lambda x: (x[0], (x[1][0]/x[1][1], x[1][1]))) \
    .sortByKey() \
    .collect()

print("-------------------------------------------------------------")
print("Query 3 - RDD API Output")
print("(Genre, (Genre rating, Movies in genre))")
for i in rdd:
    print(i)
print("-------------------------------------------------------------")
