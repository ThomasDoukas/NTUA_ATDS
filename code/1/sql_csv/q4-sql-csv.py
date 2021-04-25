from pyspark.sql import SparkSession

def fiveyear(string):
    year = (int(string)//5)*5
    return str(year)+"-"+str(year+4)

spark = SparkSession.builder.appName("q4-sql-csv").getOrCreate()
 
movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movies/movies.csv")
movie_genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movies/movie_genres.csv")

movies.registerTempTable("movies")
movie_genres.registerTempTable("movie_genres")

spark.udf.register("wordcount", lambda x: len(x.split()), "int")
spark.udf.register("fiveyear", fiveyear, "string")

sqlString = ("SELECT d.year_span,AVG(d.wcount) as average_words\n"
            "FROM (SELECT m._c0 as id,fiveyear(YEAR(m._c3)) as year_span,wordcount(m._c2) as wcount\n"
            "FROM movies m\n"
            "JOIN movie_genres mg\n"
            "ON m._c0 = mg._c0\n"
            "WHERE YEAR(m._c3)>=2000 AND m._c2 IS NOT NULL AND mg._c1 = 'Drama') as d\n"
            "GROUP BY d.year_span\n"
            "ORDER BY d.year_span")


res = spark.sql(sqlString)
res.show(40, False)

