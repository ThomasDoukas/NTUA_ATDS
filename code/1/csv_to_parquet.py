from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSVtoParquet").getOrCreate()

# Read CSV into DataFrame and convert to Parquet file
# movies
df = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movies/movies.csv")
df.write.parquet("hdfs://master:9000/movies/movies.parquet")

# movie_genres
df = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movies/movie_genres.csv")
df.write.parquet("hdfs://master:9000/movies/movie_genres.parquet")

# ratings
df = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movies/ratings.csv")
df.write.parquet("hdfs://master:9000/movies/ratings.parquet")
