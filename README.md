# MovieLens_Analysis
Analysis of a Movie dataset using PySpark

== Analyzed Logical Plan ==
genre: string, year: string, Avg_Rating: double
Sort [genre#1615 ASC NULLS FIRST, year#1597 ASC NULLS FIRST], false
+- Aggregate [genre#1615, year#1597], [genre#1615, year#1597, avg(rating#1605) AS Avg_Rating#1640]
   +- Project [movieId#1604, userId#1603, rating#1605, age#1610, title#1592, genre#1615, Year#1597]
      +- Join Inner, (movieId#1604 = movieId#1591)
         :- Project [userId#1603, movieId#1604, rating#1605, age#1610]
         :  +- Join Inner, (userId#1603 = userId#1609)
         :     :- Relation [userId#1603,movieId#1604,rating#1605] csv
         :     +- ResolvedHint (strategy=broadcast)
         :        +- Filter age#1610 IN (18,25,35,45)
         :           +- Relation [userId#1609,age#1610] csv
         +- ResolvedHint (strategy=broadcast)
            +- Project [movieId#1591, title#1592, genre#1615, Year#1597]
               +- Generate explode(split(genre#1593, \|, -1)), false, [genre#1615]
                  +- Filter (cast(Year#1597 as int) > 1989)
                     +- Project [movieId#1591, title#1592, genre#1593, regexp_extract(Title#1592, \((\d{4})\)$, 1) AS Year#1597]
                        +- Relation [movieId#1591,title#1592,genre#1593] csv

== Optimized Logical Plan ==
Sort [genre#1615 ASC NULLS FIRST, year#1597 ASC NULLS FIRST], false
+- Aggregate [genre#1615, year#1597], [genre#1615, year#1597, avg(rating#1605) AS Avg_Rating#1640]
   +- Project [rating#1605, genre#1615, Year#1597]
      +- Join Inner, (movieId#1604 = movieId#1591), rightHint=(strategy=broadcast)
         :- Project [movieId#1604, rating#1605]
         :  +- Join Inner, (userId#1603 = userId#1609), rightHint=(strategy=broadcast)
         :     :- Filter (isnotnull(userId#1603) AND isnotnull(movieId#1604))
         :     :  +- Relation [userId#1603,movieId#1604,rating#1605] csv
         :     +- Project [userId#1609]
         :        +- Filter (age#1610 IN (18,25,35,45) AND isnotnull(userId#1609))
         :           +- Relation [userId#1609,age#1610] csv
         +- Project [movieId#1591, genre#1615, Year#1597]
            +- Generate explode(split(genre#1593, \|, -1)), [1], false, [genre#1615]
               +- Project [movieId#1591, genre#1593, regexp_extract(Title#1592, \((\d{4})\)$, 1) AS Year#1597]
                  +- Filter ((isnotnull(Title#1592) AND (cast(regexp_extract(Title#1592, \((\d{4})\)$, 1) as int) > 1989)) AND isnotnull(movieId#1591))
                     +- Relation [movieId#1591,title#1592,genre#1593] csv

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   LocalTableScan <empty>, [genre#1615, year#1597, Avg_Rating#1640]
+- == Initial Plan ==
   Sort [genre#1615 ASC NULLS FIRST, year#1597 ASC NULLS FIRST], false, 0
   +- HashAggregate(keys=[genre#1615, year#1597], functions=[finalmerge_avg(merge sum#1649, count#1650L) AS avg(rating#1605)#1639], output=[genre#1615, year#1597, Avg_Rating#1640])
      +- Exchange hashpartitioning(genre#1615, year#1597, 200), ENSURE_REQUIREMENTS, [plan_id=4394]
         +- HashAggregate(keys=[genre#1615, year#1597], functions=[partial_avg(rating#1605) AS (sum#1649, count#1650L)], output=[genre#1615, year#1597, sum#1649, count#1650L])
            +- Project [rating#1605, genre#1615, Year#1597]
               +- BroadcastHashJoin [movieId#1604], [movieId#1591], Inner, BuildRight, false, true
                  :- Project [movieId#1604, rating#1605]
                  :  +- BroadcastHashJoin [userId#1603], [userId#1609], Inner, BuildRight, false, true
                  :     :- Filter (isnotnull(userId#1603) AND isnotnull(movieId#1604))
                  :     :  +- FileScan csv [userId#1603,movieId#1604,rating#1605] Batched: false, DataFilters: [isnotnull(userId#1603), isnotnull(movieId#1604)], Format: CSV, Location: InMemoryFileIndex(1 paths)[dbfs:/FileStore/tables/ratings.dat], PartitionFilters: [], PushedFilters: [IsNotNull(userId), IsNotNull(movieId)], ReadSchema: struct<userId:int,movieId:int,rating:float>
                  :     +- Exchange SinglePartition, EXECUTOR_BROADCAST, [plan_id=4385]
                  :        +- Project [userId#1609]
                  :           +- Filter (age#1610 IN (18,25,35,45) AND isnotnull(userId#1609))
                  :              +- FileScan csv [userId#1609,age#1610] Batched: false, DataFilters: [age#1610 IN (18,25,35,45), isnotnull(userId#1609)], Format: CSV, Location: InMemoryFileIndex(1 paths)[dbfs:/FileStore/tables/users.dat], PartitionFilters: [], PushedFilters: [In(age, [18,25,35,45]), IsNotNull(userId)], ReadSchema: struct<userId:int,age:int>
                  +- Exchange SinglePartition, EXECUTOR_BROADCAST, [plan_id=4389]
                     +- Project [movieId#1591, genre#1615, Year#1597]
                        +- Generate explode(split(genre#1593, \|, -1)), [movieId#1591, Year#1597], false, [genre#1615]
                           +- Project [movieId#1591, genre#1593, regexp_extract(Title#1592, \((\d{4})\)$, 1) AS Year#1597]
                              +- Filter ((isnotnull(Title#1592) AND (cast(regexp_extract(Title#1592, \((\d{4})\)$, 1) as int) > 1989)) AND isnotnull(movieId#1591))
                                 +- FileScan csv [movieId#1591,title#1592,genre#1593] Batched: false, DataFilters: [isnotnull(title#1592), (cast(regexp_extract(title#1592, \((\d{4})\)$, 1) as int) > 1989), isnotn..., Format: CSV, Location: InMemoryFileIndex(1 paths)[dbfs:/FileStore/tables/movies.dat], PartitionFilters: [], PushedFilters: [IsNotNull(title), IsNotNull(movieId)], ReadSchema: struct<movieId:int,title:string,genre:string>
