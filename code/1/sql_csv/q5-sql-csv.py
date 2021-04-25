from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("q5-sql-csv").getOrCreate()
 
movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movies/movies.csv")
movie_genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movies/movie_genres.csv")
ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movies/ratings.csv")

movies.registerTempTable("movies")
movie_genres.registerTempTable("movie_genres")
ratings.registerTempTable("ratings")

sqlString1 = ("SELECT s1.genre,s1.userid,s1.numOfRev\n"
            "FROM (SELECT r._c0 as userid,COUNT(*) as numOfRev, mg._c1 as genre FROM ratings r JOIN movie_genres mg ON r._c1 = mg._c0 GROUP BY mg._c1,r._c0) as s1\n"
            "JOIN (SELECT genre,MAX(numOfRev) as maxrev FROM (SELECT r._c0 as userid,COUNT(*) as numOfRev, mg._c1 as genre FROM ratings r JOIN movie_genres mg ON r._c1 = mg._c0 GROUP BY mg._c1,r._c0)\n"
            "GROUP BY genre) as s2\n"
            "ON s1.genre = s2.genre AND s1.numOfRev = s2.maxrev\n"
            "ORDER BY s1.genre")
res1 = spark.sql(sqlString1)
res1.registerTempTable("maxpergenre")

sqlString4 = ("SELECT m.genre,m.userid,m.numOfRev,res3.movie,res3.moviename,res3.rating,res3.popularity\n"
            "FROM maxpergenre m\n"
            "JOIN (SELECT m2.userid,m2.movie,m._c1 as moviename,m2.rating,m2.genre,m2.popularity\n"
                    "FROM (SELECT m1.userid,m1.rating,m1.movie,res2.genre,res2.popularity\n" 
                        "FROM (SELECT m.userid,r._c1 as movie,r._c2 as rating\n" 
                            "FROM (SELECT DISTINCT userid FROM maxpergenre) as m\n"
                            "JOIN ratings r\n" 
                            "ON r._c0 = m.userid) as m1\n" 
                        "JOIN (SELECT m1._c0 as movie,m1._c1 as genre,m2._c7 as popularity\n" 
                                "FROM movie_genres m1 JOIN movies m2 ON m1._c0 = m2._c0) as res2\n" 
                        "ON res2.movie = m1.movie) as m2\n"
                    "JOIN movies m\n"
                    "ON m._c0 = m2.movie) as res3\n"
            "ON m.genre = res3.genre AND m.userid = res3.userid")
res4 = spark.sql(sqlString4)
res4.registerTempTable("res4")

sqlString7 = ("SELECT res5.genre,res5.userid,res5.numOfRev,res5.best_movie,res5.best_moviename,res5.best_rating,res6.worst_movie,res6.worst_moviename,res6.worst_rating\n"
            "FROM (SELECT s1.genre,s1.userid,s1.numOfRev,s1.movie as best_movie,s1.moviename as best_moviename,s1.rating as best_rating\n"
                "FROM res4 s1\n"
                "LEFT JOIN res4 s2\n"
                "ON s1.genre = s2.genre AND s1.userid = s2.userid\n"
                "AND (s1.rating < s2.rating OR (s1.rating = s2.rating AND s1.popularity < s2.popularity ))\n"
                "WHERE s2.genre is null AND s2.userid is null ) as res5\n"
            "JOIN (SELECT s1.genre,s1.userid,s1.numOfRev,s1.movie as worst_movie,s1.moviename as worst_moviename,s1.rating as worst_rating\n"
                "FROM res4 s1\n"
                "LEFT JOIN res4 s2\n"
                "ON s1.genre = s2.genre AND s1.userid = s2.userid\n"
                "AND (s1.rating > s2.rating OR (s1.rating = s2.rating AND s1.popularity < s2.popularity ))\n"
                "WHERE s2.genre is null AND s2.userid is null ) as res6\n" 
            "ON res5.genre =res6.genre AND res5.userid = res6.userid\n"
            "ORDER BY genre")
res7 = spark.sql(sqlString7)
res7.show(50, False)

