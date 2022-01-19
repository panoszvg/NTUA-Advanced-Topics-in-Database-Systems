from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("q3-sql-csv").getOrCreate()

ratings = spark.read.format("csv").options(headers='false', inferSchema='true').load("hdfs://master:9000/user/user/files/ratings.csv")
genres = spark.read.format("csv").options(headers='false', inferSchema='true').load("hdfs://master:9000/user/user/files/movie_genres.csv")

ratings.registerTempTable("ratings")
genres.registerTempTable("genres")

res = spark.sql("SELECT g._c1 AS Genre, AVG(r.average_rating) AS Average_Rating, COUNT(*) AS No_of_Movies_in_Genre FROM genres AS g JOIN (SELECT _c1, AVG(r._c2) AS average_rating FROM ratings AS r GROUP BY r._c1) r ON g._c0 == r._c1 GROUP BY g._c1")

res.show()