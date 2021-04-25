from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("q2-sql-parquet").getOrCreate()
 
ratings = spark.read.parquet("hdfs://master:9000/movies/ratings.parquet")
ratings.registerTempTable("ratings")

sqlString = ("SELECT (COUNT(*)*100)/(SELECT  COUNT(DISTINCT r1._c0)  FROM ratings r1) as percentage\n" 
            "FROM (SELECT AVG(r._c2) as av_rating FROM ratings r  GROUP BY r._c0) as r2\n"
            "WHERE r2.av_rating > 3.0")

res = spark.sql(sqlString)

res.show()

