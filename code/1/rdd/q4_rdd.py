from pyspark.sql import SparkSession
from io import StringIO
import csv

spark = SparkSession.builder.appName("Q4RDD").getOrCreate()
sc = spark.sparkContext

def map_genres(x):
    x = x.split(",")
    return (x[0], x[1]) 

# Split records - Only used in movies.csv
def split_complex(x):
    res = list(csv.reader(StringIO(x), delimiter=','))[0]
    return (res[0], (res[3], res[2]))

def fiveYear(x):
    temp = ((x//5)*5)
    return (str(temp) + '-' + str(temp+4)) 

genres = sc.textFile("hdfs://master:9000/movies/movie_genres.csv") \
    .map(lambda x: map_genres(x)) \
    .filter(lambda x: True if(x[1] == "Drama") else False)

# map (movie_id, (release_date, abstract))
# filter realease_year exists and >=2000, abstract_exists 
# map_movies (movie_id, (release_year, abstract_length, movie_counter=1))
# join (movie_id, ((release_year, abstract_length, movie_counter=1), Drama)) -> Only keep drama
# map (five_year_period, (abstract_length, movie_counter=1))
# reduceByKey (five_year_period, (total_abstract_length, movies_count))
# map (five_year_period, avg_abstract_length)
rdd = sc.textFile("hdfs://master:9000/movies/movies.csv") \
    .map(lambda x: split_complex(x)) \
    .filter(lambda x: True if (x[1][0] and x[1][1] and x[1][0].split("-")[0] >= '2000') else False) \
    .map(lambda x: (x[0], (int(x[1][0].split("-")[0]), len(x[1][1].split()), 1))) \
    .join(genres) \
    .map(lambda x: (fiveYear(x[1][0][0]) , (x[1][0][1], x[1][0][2]))) \
    .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) \
    .map(lambda x: (x[0], x[1][0]/x[1][1])) \
    .sortByKey() \
    .collect()

print("-------------------------------------------------------------")
print("Query 4 - RDD API Output")
print("(Five Years, Average words in Abstract)")
for i in rdd:
    print(i)
print("-------------------------------------------------------------")
