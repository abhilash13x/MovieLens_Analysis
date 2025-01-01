# MovieLens_Analysis
Analysis of a Movie dataset using PySpark


Things to consider while scaling using this code:
  1. While Reading the the incoming users rating df use a watermark column or read using "incoming_ratings_timestamp > max(existing_timestamp) or by date partition column to avoid reprocessing the entire dataset everytime.
  2. To Check for data skewness use:

                                     print(joined_movie_rating_user_df.rdd.getNumPartitions())
                                     partition_num = ratings_df.select(spark_partition_id().alias("partid")).groupBy("partid").count().show()
     Use Repartion() or Salting if needed.
  4. Adjust "spark.sql.shuffle.partitions" to help distribute the data more evenly and leverage parallelism.
  5. Depending upon the requirement Partitioning the tables based on columns with high cardinality or frequent filter conditions and combining it with bucketing might help for eq: "join all 3 tables(ratings,users,movie) and parttion by year and genre and bucket it by age".
  6. Consider using Open table format such as Iceberg to help with the incremental update data rather than reprocessing the entire dataset everyday.
