from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("q4-rdd").getOrCreate()

sc = spark.sparkContext

rdd_movies = spark.read.csv("hdfs://master:9000/user/user/files/movies.csv").rdd
rdd_genres = spark.read.csv("hdfs://master:9000/user/user/files/movie_genres.csv").rdd


rdd_movies_t = rdd_movies\
    .map(lambda row: (row[0], (row[1],row[2],row[3])))\
    .filter(lambda row: row[1][2] and row[1][1] )

def red_function(date,desc):
    if (date < 2000):
        return None
    elif (date < 2005):
        return ('2000-2004', (len(desc.split(" ")), 1))
    elif (date < 2010):
        return ('2005-2009', (len(desc.split(" ")), 1))
    elif (date < 2015):
        return ('2010-2014', (len(desc.split(" ")), 1))
    else:
        return ('2015-2019', (len(desc.split(" ")), 1))

res = rdd_genres \
    .filter( lambda row: row._c1 == 'Drama') \
    .join(rdd_movies_t)\
    .map(lambda row: red_function(int(row[1][1][2].split("-")[0]),str(row[1][1][1])))\
    .filter( lambda row: row != None )\
    .reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1] )  )\
    .map(lambda row: (row[0],row[1][0]/row[1][1]))\
    .sortBy(lambda row: row[0])

for i in res.take(100):
    print(i)