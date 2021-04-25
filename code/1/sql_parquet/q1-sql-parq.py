from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("q1-sql-parquet").getOrCreate()

movies = spark.read.parquet("hdfs://master:9000/movies/movies.parquet")
movies.registerTempTable("movies")

sqlString = ("SELECT m1._c0 as movieid,m1._c1 as movietitle,YEAR(m1._c3) as year,((m1._c6-m1._c5)/m1._c5)*100 as profit \n"
            "FROM movies as m1 \n"
            "LEFT JOIN movies as m2 \n"
            "ON year(m1._c3) = year(m2._c3) AND (((m1._c6-m1._c5)/m1._c5)*100 < ((m2._c6-m2._c5)/m2._c5)*100 \n"
            "OR  (((m1._c6-m1._c5)/m1._c5)*100 = ((m2._c6-m2._c5)/m2._c5)*100 AND m1._c0 > m2._c0))\n"
            "WHERE m2._c0 is null and m1._c5 <>0 and m1._c6 <> 0 and year(m1._c3)>=2000\n"
            "ORDER BY year(m1._c3)")
res = spark.sql(sqlString)

res.show(40, False)

