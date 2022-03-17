from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName('hdfs-parquet').getOrCreate()

# movies
schema = StructType(
	[StructField('_c0', IntegerType(), False), \
    StructField('_c1', StringType(), False), \
    StructField('_c2', StringType(), False), \
    StructField('_c3', StringType(), True), \
	StructField('_c4', DoubleType(), True), \
	StructField('_c5', IntegerType(), False), \
	StructField('_c6', IntegerType(), True), \
	StructField('_c7', DoubleType(), True)]
)
df = spark.read.format('csv').schema(schema).option('header', 'false').load('hdfs://master:9000/user/user/files/movies.csv')
df.write.format('parquet').mode('overwrite').save('hdfs://master:9000/user/user/files/movies.parquet')
df.show()

# movie_genres
schema = StructType(
	[StructField('_c0', IntegerType(), False), \
	StructField('_c1', StringType(), False)]
)
df = spark.read.format('csv').schema(schema).option('header', 'false').load('hdfs://master:9000/user/user/files/movie_genres.csv')
df.write.format('parquet').mode('overwrite').save('hdfs://master:9000/user/user/files/movie_genres.parquet')
df.show()

# ratings
schema = StructType(
	[StructField('_c0', IntegerType(), False), \
	StructField('_c1', IntegerType(), False), \
	StructField('_c2', DoubleType(), False), \
	StructField('_c3', IntegerType(), False)]
)
df = spark.read.format('csv').schema(schema).option('header', 'false').load('hdfs://master:9000/user/user/files/ratings.csv')
df.write.format('parquet').mode('overwrite').save('hdfs://master:9000/user/user/files/ratings.parquet')
df.show()