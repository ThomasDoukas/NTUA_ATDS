from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("q3-sql-parquet").getOrCreate()
 
movie_genres = spark.read.parquet("hdfs://master:9000/movies/movie_genres.parquet")
ratings = spark.read.parquet("hdfs://master:9000/movies/ratings.parquet")

movie_genres.registerTempTable("movie_genres")
ratings.registerTempTable("ratings")

sqlString = ("SELECT m._c1 as genre,COUNT(*) as movie_count , AVG(r1.av_movie) as average_rating\n" 
            "FROM movie_genres m\n"
            "JOIN (SELECT r._c1 as movie,AVG(r._c2) as av_movie FROM ratings r GROUP BY r._c1) as r1\n"
            "ON r1.movie = m._c0\n"
            "GROUP BY m._c1\n"
            "ORDER BY genre")

res = spark.sql(sqlString)

res.show(40, False)

