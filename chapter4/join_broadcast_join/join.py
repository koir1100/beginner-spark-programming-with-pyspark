from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession \
    .builder \
    .appName("Shuffle Join Demo") \
    .master("local[3]") \
    .config("spark.sql.shuffle.partitions", 3) \
    .config("spark.sql.adaptive.enabled", False) \
    .getOrCreate()

# Spark Higher Version default setting is enabled.
print(f"""Before Broadcast Threshold >>>>>>>>>>>> {spark.conf.get("spark.sql.autoBroadcastJoinThreshold")}""")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
print(f"""After Broadcast Threshold >>>>>>>>>>>> {spark.conf.get("spark.sql.autoBroadcastJoinThreshold")}""")

df_large = spark.read.json("large_data/")
df_small = spark.read.json("small_data/")

join_expr = df_large.id == df_small.id
join_df = df_large.join(df_small, join_expr, "inner")

join_df.collect()
input("Waiting ...")

spark.stop()
