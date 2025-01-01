# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, explode, split, avg, broadcast, spark_partition_id
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, FloatType

spark = SparkSession.builder \
    .appName("MovieProcessing") \
    .getOrCreate()

# Define file paths
movies_path = "/FileStore/tables/movies.dat"
ratings_path = "/FileStore/tables/ratings.dat"
users_path = "/FileStore/tables/users.dat"

# Define Partial schema to reduce the amount of data read
movies_schema = StructType([StructField("movieId", IntegerType(), True),
                            StructField("title", StringType(), True),
                            StructField("genre", StringType(), True)])             

ratings_schema = StructType([StructField("userId", IntegerType(), True),
                            StructField("movieId", IntegerType(), True),
                            StructField("rating", FloatType(), True)])           

users_schema = StructType([StructField("userId", IntegerType(), True),
                           StructField("age", IntegerType(), True)])                 

# Applying filters while reading to reduce the data been read
movies_df = spark.read.csv(movies_path, sep="::", schema=movies_schema, inferSchema=False).withColumn("Year", regexp_extract(col("Title"), r"\((\d{4})\)$", 1)).filter("Year > 1989")
ratings_df = spark.read.csv(ratings_path, sep="::", schema=ratings_schema, inferSchema=False)                            
users_df = spark.read.csv(users_path, sep="::", schema=users_schema, inferSchema=False).filter("age in (18,25,35,45)")           # Write the data in parquet at this step for faster subsequent processing(can be used in a medallion architecture)

#Exploding genre column since one movie is part of multiple genre
exploded_movies_df = movies_df.withColumn("genre",explode(split("genre","\|")))

joined_ratings_user_df = ratings_df.join(broadcast(users_df),["userId"],"inner")                                                    #Broadcasting users_df & movies_df to avoid shuffle

joined_movie_rating_user_df = joined_ratings_user_df.join(broadcast(exploded_movies_df),["movieId"],"inner")                        #Check for skewness for specific combination of genre & year and Reparttion here on genre and year if needed  

agg_result = joined_movie_rating_user_df.groupby("year","genre").agg(avg("rating").alias("Avg_Rating")).sortWithinPartitions("year","genre")    #Set shuffle partition if needed

agg_result.write.parquet("/FileStore/tables/results", mode="overwrite")
