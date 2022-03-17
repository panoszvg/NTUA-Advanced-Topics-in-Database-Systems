from pyspark.sql import SparkSession
from time import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--csv', action='store_true', default=True)
parser.add_argument('-p', '--parquet', action='store_true') # if this option is given, negates csv
args = parser.parse_args()
if args.parquet:
    args.csv = False
    spark = SparkSession.builder.appName("q1-sql").config("spark.sql.parquet.binaryAsString","true").getOrCreate()
    movies = spark.read.parquet("hdfs://master:9000/user/user/files/movies.parquet")
    def get_year(date):
        if date is not None and date != "":
            return int(date.split("-")[0])
        else:
            return 1999

if args.csv:
    spark = SparkSession.builder.appName("q1-sql").getOrCreate()
    movies = spark.read.format("csv").options(headers='false', inferSchema='true').load("hdfs://master:9000/user/user/files/movies.csv")

    def get_year(date):
        if date is not None and date != "":
            return int(date.year)
        else:
            return 1999

movies.registerTempTable("movies")
spark.udf.register("get_year", get_year)


timestamp_1 = time()

res = spark.sql("SELECT Year, _c0 AS Movie_Code, _c1 AS Movie, p.Profit \
				 FROM (SELECT *, ((_c6 - _c5)/_c5)*100 AS Profit FROM MOVIES) AS p \
				 JOIN (SELECT first(get_year(_c3)) AS Year, MAX(((_c6 - _c5)/_c5)*100) AS Profit \
				 	   FROM movies AS m \
					   WHERE (get_year(m._c3) >= 2000 AND m._c5 <> 0 AND m._c6 <> 0) \
				 	   GROUP BY YEAR(m._c3)) \
				 	   AS m \
				 ON (get_year(p._c3) == m.Year AND p.Profit == m.Profit) \
				 ORDER By Year ASC")

res.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').save("hdfs://master:9000/user/user/outputs/q1-sql.csv")

timestamp_2 = time()
print(timestamp_2 - timestamp_1)
