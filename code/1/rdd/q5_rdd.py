from pyspark.sql import SparkSession
from io import StringIO
import csv

spark = SparkSession.builder.appName("Q5RDD").getOrCreate()
sc = spark.sparkContext

# Split records - Only used in movies.csv
def split_complex(x):
    ret = list(csv.reader(StringIO(x), delimiter=','))[0]
    return (ret[0], (ret[1], float(ret[7])))

def map_genres(x):
    x = x.split(",")
    return (x[0], x[1]) 

def map_ratings(x):
    x = x.split(",")
    return (x[1], (x[0], float(x[2]), 1)) 

def red(a, b):
    if(a[1] > b[1]):
        return a
    elif(a[1] < b[1]):
        return b
    elif(a[1]==b[1]):
        return(a[0] + b[0], a[1])

def flatmap(a):
    res = []
    for item in a[1][0]:
        temp = (a[0], (item, a[1][1]))
        res.append(temp)
    return res

def most_fav(x, y):
    if (x[2] > y[2]):
        return x
    elif (x[2] < y[2]):
        return y
    elif (x[2] == y[2]):
        return x if(x[3] >= y[3]) else y

def least_fav(x, y):
    if (x[2] > y[2]):
        return y
    elif (x[2] < y[2]):
        return x
    elif (x[2] == y[2]):
        return x if(x[3] >= y[3]) else y

# map_genres (movie_id, category)
genres = sc.textFile("hdfs://master:9000/movies/movie_genres.csv") \
    .map(lambda x: map_genres(x))

# map_movies (movie_id, (popularity, movie_name))
movies = sc.textFile("hdfs://master:9000/movies/movies.csv") \
    .map(lambda x: split_complex(x))

# map_ratings (movie_id, (user_id, rating, ratings_counter))
ratings = sc.textFile("hdfs://master:9000/movies/ratings.csv") \
    .map(lambda x: map_ratings(x))

ratings2 = ratings.map(lambda x: (x[1][0], (x[0], x[1][1])))

# join (movie_id, (category, (user_id, rating, ratings_per_user_counter)))
# map (category, user_id), (rating, ratings_per_user_counter))
# reduceByKey ((category, user_id), ratings_per_user_counter)
# map(category, ((user_id), ratings_per_user_counter))
# reduceByKey (category, ((tuple_of_users_with_same_ratings_in_category), ratings_per_user_counter))
# flatMap (category, (user_id, #_of_ratings_in_category)
# map (user_id, (category, #ratings))
# join (user_id, ((category, #ratings), (movie_id, rating)))
# map (movie_id, (category, user_id, rating, #ratings))
# join (movie_id, ((category, user_id, rating, #ratings), category))
# filter -where category=category
# join (movie_id, (((category, user_id, rating, #ratings), category), (movie_name, popularity)))  )
# map ((category, user_id, #ratings), (movie_id, movie_name, rating, popularity))
rdd = genres.join(ratings) \
    .map(lambda x: ((x[1][0], x[1][1][0]) , x[1][1][2])) \
    .reduceByKey(lambda x, y: x+y) \
    .map(lambda x: (x[0][0], ((x[0][1], ), x[1]))) \
    .reduceByKey(lambda x, y: red(x, y)) \
    .flatMap(lambda x: flatmap(x)) \
    .map(lambda x: (x[1][0], (x[0], x[1][1]))) \
    .join(ratings2) \
    .map(lambda x: (x[1][1][0], (x[1][0][0], x[0], x[1][1][1], x[1][0][1]))) \
    .join(genres) \
    .filter(lambda x: True if(x[1][0][0] == x[1][1]) else False) \
    .join(movies) \
    .map(lambda x: ((x[1][0][0][0], x[1][0][0][1], x[1][0][0][3]), (x[0], x[1][1][0], x[1][0][0][2], x[1][1][1])))
    
rdd2 = rdd.reduceByKey(lambda x, y: least_fav(x, y))

# rdd3 (category, user_id, $ratings, most_fav_movie_id, most_fav_movie_name, rating, least_fav_movie_id, least_fav_movie_name, rating)
rdd3 = rdd.reduceByKey(lambda x, y: most_fav(x, y)) \
    .join(rdd2) \
    .sortByKey() \
    .map(lambda x: (x[0][0], x[0][1], x[0][2], x[1][0][0], x[1][0][1], x[1][0][2], x[1][1][0], x[1][1][1], x[1][1][2])) \
    .collect()

print("-------------------------------------------------------------")
print("Query 5 - RDD API Output")
print("Category, user_id, numOfRev, best_movie, best_movie_name, best_rating, worst_movie, worst_movie_name, worst_rating")
for i in rdd3:
    print(i)
print("-------------------------------------------------------------")
