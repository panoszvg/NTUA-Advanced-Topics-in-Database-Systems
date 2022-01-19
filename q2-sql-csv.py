from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("q2-sql-csv").getOrCreate()

ratings = spark.read.format("csv").options(headers='false', inferSchema='true').load("hdfs://master:9000/user/user/files/ratings.csv")

ratings.registerTempTable("ratings")

res = spark.sql("SELECT (COUNT(Rating)/(SELECT COUNT(*) FROM (SELECT _c0 FROM ratings GROUP BY ratings._c0))) AS Percentage FROM (SELECT AVG(r._c2) AS Rating FROM ratings AS r GROUP BY r._c0 HAVING AVG(r._c2) > 3)")

res.show()