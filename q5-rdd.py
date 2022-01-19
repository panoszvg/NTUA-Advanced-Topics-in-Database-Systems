from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("q5-rdd").getOrCreate()

sc = spark.sparkContext

rdd_genres = spark.read.csv("hdfs://master:9000/user/user/files/movie_genres.csv").rdd
rdd_ratings = spark.read.csv("hdfs://master:9000/user/user/files/ratings.csv").rdd
rdd_movies = spark.read.csv("hdfs://master:9000/user/user/files/movies.csv").rdd

rdd_movies_t = rdd_movies\
    .map(lambda row: (row._c0, (row._c1,float(row._c7))))

rdd_ratings_t = rdd_ratings \
    .map(lambda row: (row._c1, (row._c0,float(row._c2))))

def find_max(x,y):
	if x[1] > y[1]:
		return (x[0],x[1],x[2],x[3])
	elif x[1] < y[1]:
		return (y[0],y[1],y[2],y[3])
	else:
		if x[3] > y[3]:
			return (x[0], x[1],x[2], x[3])
		else: 
			return (y[0], y[1],y[2],y[3]) 


def find_min(x,y):
	if x[1] < y[1]:
		return (x[0],x[1],x[2],x[3])
	elif x[1] > y[1]:
		return (y[0],y[1],y[2],y[3])
	else:
		if x[3] > y[3]:
			return (x[0], x[1],x[2], x[3])
		else: 
			return (y[0], y[1],y[2],y[3]) 


tmp_rdd = rdd_ratings_t\
    .join(rdd_movies_t)\
    .join(rdd_genres)\
    .map(lambda row: ((row[1][0][0][0],row[1][1]),(row[0],row[1][0][0][1],row[1][0][1][0],row[1][0][1][1])))


max_rdd = tmp_rdd\
    .reduceByKey(lambda x,y: find_max(x,y))


min_rdd = tmp_rdd\
    .reduceByKey(lambda x,y: find_min(x,y))


res = rdd_genres\
    .join(rdd_ratings_t)\
    .map(lambda row: ((row[1][1][0],row[1][0]),(1)))\
    .reduceByKey(lambda x,y: (x+y))\
    .map(lambda row: (row[0][1],(row[0][0],row[1])))\
    .reduceByKey(lambda x,y: (x[0],x[1]) if x[1] > y[1] else (y[0],y[1]) )\
    .map(lambda row: ((row[1][0],row[0]),(row[1][1])))\
    .join(max_rdd)\
    .join(min_rdd)\
    .map(lambda row: (row[0][1],row[0][0],row[1][0][0],row[1][0][1][2],row[1][0][1][1],row[1][1][2],row[1][1][1]))\
    .sortBy(lambda row: row[0])

## for max/min_rdd 
# after join
# (movie_id , (((user_id,rating),(title,popularity)),category))

# After map
# ( (user_id,category), (movie_id,rating,title,popularity))
# join with user with most ratings (2 joings (min_rdd and max_rdd)
# ( (user_id, category) , ((max_ratings,(movie_id,max_rating,title,popularity)),(movie_id,min_rating,title,popularity)))

for i in res.take(20):
    print(i)