import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

print(spark.version)
## 3.3.2

schema = types.StructType(
    [
        types.StructField("dispatching_base_num", types.StringType(), True),
        types.StructField("pickup_datetime", types.TimestampType(), True),
        types.StructField("dropoff_datetime", types.TimestampType(), True),
        types.StructField("PULocationID", types.IntegerType(), True),
        types.StructField("DOLocationID", types.IntegerType(), True),
        types.StructField("SR_Flag", types.StringType(), True),
        types.StructField("Affiliated_base_number", types.StringType(), True),
    ]
)
df = (
    spark.read.option("header", "true").schema(schema).csv("fhvhv_tripdata_2021-06.csv")
)
df = df.repartition(12)
df.write.parquet("data/pq/fhvhv/2021/06/", mode="overwrite")

#### average size is 24MB

df.printSchema()

print(
    df.withColumn("pickup_datetime", F.to_date(df.pickup_datetime))
    .filter("pickup_datetime = '2021-06-15'")
    .count()
)

# 452470

##################

df.registerTempTable("fhvhv_2021_06")

spark.sql(
    """
SELECT
COUNT(1)
FROM 
fhvhv_2021_06
WHERE
to_date(pickup_datetime) = '2021-06-15';
"""
).show()
# 452470


#################
print(
    df.withColumn(
        "duration", df.dropoff_datetime.cast("long") - df.pickup_datetime.cast("long")
    )
    .withColumn("pickup_date", F.to_date(df.pickup_datetime))
    .groupBy("pickup_date")
    .max("duration")
    .orderBy("max(duration)", ascending=False)
    .limit(5)
    .show()
)

spark.sql(
    """
SELECT
to_date(pickup_datetime) AS pickup_date,
MAX((CAST(dropoff_datetime AS LONG) - CAST(pickup_datetime AS LONG)) / 60) AS duration
FROM 
fhvhv_2021_06
GROUP BY
1
ORDER BY
2 DESC
LIMIT 10;
"""
).show()
#############
zones_df = spark.read.option("header", "true").csv("taxi_zone_lookup.csv")

zones_df.write.parquet("zones", mode="overwrite")

zones_df = spark.read.parquet("zones/")
zones_df.registerTempTable("zones")
spark.sql(
    """SELECT
zones.Zone AS pickup_id,
count(1) AS pickup_counts
FROM 
fhvhv_2021_06
inner join
zones
on fhvhv_2021_06.PULocationID = zones.LocationID
GROUP BY
1
ORDER BY
2 DESC
LIMIT 10;
"""
).show()
