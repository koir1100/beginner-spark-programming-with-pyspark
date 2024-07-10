from pyspark.sql.functions import expr
import pyspark

spark = SparkSession()

# sc = SparkContext

df = spark.range(1, 1000000).toDF("id")
print(df.show(5))

"""
+---+
| id|
+---+
|  1|
|  2|
|  3|
|  4|
|  5|
+---+
only showing top 5 rows
"""

df10 = df.repartition(10)
df10_square = df10.withColumn("square", expr("id*id"))

"""
+------+-----------+
|    id|     square|
+------+-----------+
| 29077|  845471929|
| 34667| 1201800889|
|100683|10137066489|
| 31583|  997485889|
| 33736| 1138117696|
+------+-----------+
only showing top 5 rows
"""

df10_square.cache()
# DataFrame[id: bigint, square: bigint]
"""
[Row(id=29077, square=845471929), Row(id=34667, square=1201800889), Row(id=100683, square=10137066489), Row(id=31583, square=997485889), Row(id=33736, square=1138117696), Row(id=78042, square=6090553764), Row(id=6093, square=37124649), Row(id=35943, square=1291899249), Row(id=52891, square=2797457881), Row(id=32230, square=1038772900)]
"""

df10_square.count()
# 999999

df10_square.unpersist()
"""
2024-07-09 11:58:27,007 INFO rdd.MapPartitionsRDD: Removing RDD 20 from persistence list
2024-07-09 11:58:27,014 INFO storage.BlockManager: Removing RDD 20
DataFrame[id: bigint, square: bigint]
"""
# 20은 RDD id를 지칭

# ==============sparksql===============
df10_square.createOrReplaceTempView("df10_square")

spark.sql("CACHE TABLE df10_square")
# DataFrame[]
# spark.sql("CACHE LAZY TABLE df10_square")

spark.sql("UNCACHE TABLE df10_square")
"""
2024-07-09 12:03:49,928 INFO rdd.MapPartitionsRDD: Removing RDD 41 from persistence list
2024-07-09 12:03:49,930 INFO storage.BlockManager: Removing RDD 41
"""
# DataFrame[]

spark.catalog.isCached("df10_square")
# False

df10_squared_filtered = df10_square.select("id", "square").filter("id > 50000").cache()
# lazy execution

df10_squared_filtered.count()
# 949999

df10_square.filter("id > 50000").select("id", "square").count()
# 949999

df10_square.filter("id > 50000").select("id", "square").explain()
"""
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [id#0L, (id#0L * id#0L) AS square#9L]
   +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [plan_id=300]
      +- Filter (id#0L > 50000)
         +- Range (1, 1000000, step=1, splits=8)
"""

df10_squared_filtered.filter("square > 100000").count()
# 949999

df10_squared_filtered.filter("square > 100000").explain()
"""
== Physical Plan ==
*(1) Filter (square#9L > 100000)
+- InMemoryTableScan [id#2L, square#9L], [(square#9L > 100000)]
      +- InMemoryRelation [id#2L, square#9L], StorageLevel(disk, memory, deserialized, 1 replicas)
            +- *(2) Project [id#0L, (id#0L * id#0L) AS square#9L]
               +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [plan_id=179]
                  +- *(1) Filter (id#0L > 50000)
                     +- *(1) Range (1, 1000000, step=1, splits=8)
"""

df10_square.select("id", "square").filter("id > 50000").filter("id > 200000").count()
# 799999

"""
== Physical Plan ==
*(1) Filter (id#2L > 200000)
+- InMemoryTableScan [id#2L, square#9L], [(id#2L > 200000)]
      +- InMemoryRelation [id#2L, square#9L], StorageLevel(disk, memory, deserialized, 1 replicas)
            +- *(2) Project [id#0L, (id#0L * id#0L) AS square#9L]
               +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [plan_id=179]
                  +- *(1) Filter (id#0L > 50000)
                     +- *(1) Range (1, 1000000, step=1, splits=8)
"""

# caching 된 것을 재사용하고 싶은 경우, caching한 것을 dataframe으로 reference 하고,
# 그 이름을 가지고 계속 작업하는 형태가 되어야 함.
# 이는 조금만 순서가 달라지면 cache되지 않은 것으로 인지하기 때문임.

df_persisted = df10.withColumn("square", expr("id*id")).persist(
    pyspark.StorageLevel(False, True, False, True, 1))

"""
StorageLevel(useDisk: bool, useMemory: bool, useOffHeap: bool, deserialized: bool, replication: int = 1)
Flags for controlling the storage of an RDD.
Each StorageLevel records whether to use memory,
whether to drop the RDD to disk if it falls out of memory,
whether to keep the data in memory in a JAVA-specific serialized format, 
and whether to replicate the RDD partitions on multiple nodes.
Also contains static constants for some commonly used storage levels, MEMORY_ONLY.
Since the data is always serialized on the Python side,
all the constants use the serialized formats.
"""


