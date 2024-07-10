# pyspark --num-executors 5 --driver-memory 2g --executor-memory 2g --conf spark.sql.catalogImplementation=hive

# =====Common Environment Setting=====

spark.sql("CREATE DATABASE IF NOT EXISTS aqe_demo;")
# DataFrame[]

spark.sql("DROP TABLE IF EXISTS items;")
spark.sql("DROP TABLE IF EXISTS sales;")

spark.sql("""CREATE TABLE items 
USING parquet
AS
    SELECT id,
        CAST(rand() * 1000 AS INT) AS price
    FROM
        RANGE(30000000);
""")

spark.table("items").show(5)

"""
+--------+-----+
|      id|price|
+--------+-----+
|18750000|  812|
|18750001|  588|
|18750002|  282|
|18750003|  105|
|18750004|  646|
+--------+-----+
only showing top 5 rows
"""

spark.sql("""CREATE TABLE sales
    USING parquet
    AS
        SELECT
            CASE WHEN rand() < 0.8 THEN 100 ELSE CAST(rand() * 30000000 AS INT) END AS item_id,
            CAST(rand() * 100 AS INT) AS quantity,
            DATE_ADD(current_date(), - CAST(rand() * 360 AS INT)) AS date
        FROM
            RANGE(1000000000);
""")

spark.table("sales").show(5)
"""
+--------+--------+----------+
| item_id|quantity|      date|
+--------+--------+----------+
|     100|      63|2024-05-26|
|15677280|       1|2024-01-13|
|     100|      27|2023-12-29|
|     100|      63|2023-10-07|
|     100|      44|2024-02-25|
+--------+--------+----------+
only showing top 5 rows
"""

# =====Dynamically coalescing shuffle partitions=====

spark.conf.set("spark.sql.adaptive.enabled", False) # AQE disable

df_f = spark.sql("""SELECT date, sum(quantity) AS q
    FROM sales
        GROUP BY date
        ORDER BY q;
""")

# 360

df_f.rdd.getNumPartitons()
# 200

spark.conf.set("spark.sql.adaptive.enabled", True) # AQE enable

df_t = spark.sql("""SELECT date, sum(quantity) AS q
    FROM sales
        GROUP BY date
        ORDER BY q;
""")

# 360 (결과 동일)

df_t.rdd.getNumPartitons()
# 1

# ===== Dynamically switching join (BroadcastJoin) =====

spark.conf.set("spark.sql.adaptive.enabled", False)
df_f = spark.sql("""SELECT date, SUM(quantity * price) AS total_sales
    FROM sales AS s
        INNER JOIN items AS i ON s.item_id = i.id
    WHERE
        price < 10
    GROUP BY date
    ORDER BY total_sales DESC;
""")

# 360 (13 min)

spark.conf.set("spark.sql.adaptive.enabled", True)
df_t = spark.sql("""SELECT date, SUM(quantity * price) AS total_sales
    FROM sales AS s
        INNER JOIN items AS i ON s.item_id = i.id
    WHERE
        price < 10
    GROUP BY date
    ORDER BY total_sales DESC;
""")

# 360 (3 min)

# ===== Dynamically optimizing skew joins =====
# pyspark --driver-memory 4g --executor-memory 8g --conf spark.sql.catalogImplementation=hive

spark.conf.set("spark.sql.adaptive.enabled", False)

df_f = spark.sql("""SELECT date, SUM(quantity * price) AS total_sales
    FROM sales AS s
        INNER JOIN items AS i
            ON s.item_id = i.id
        GROUP BY date
        ORDER BY total_sales DESC;
""")

df_f.count()
# 360 (15 min)

spark.conf.set("spark.sql.adaptive.enabled", True)

df_t = spark.sql("""SELECT date, SUM(quantity * price) AS total_sales
    FROM sales AS s
        INNER JOIN items AS i
            ON s.item_id = i.id
        GROUP BY date
        ORDER BY total_sales DESC;
""")

df_t.count()
# 360 (7.7 min)


