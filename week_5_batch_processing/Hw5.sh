export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"

export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"

export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"

####################################

wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz

gzip -d fhvhv_tripdata_2021-06.csv.gz

###############################

python code

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
df.write.parquet("data/pq/fhvhv/2021/06/")

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
## 66.8788889 Hour

+-----------+-------------+
|pickup_date|max(duration)|
+-----------+-------------+
| 2021-06-25|       240764|
| 2021-06-22|        91979|
| 2021-06-27|        71931|
| 2021-06-26|        65510|
| 2021-06-23|        59281|
+-----------+-------------+


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


+--------------------+-------------+
|           pickup_id|pickup_counts|
+--------------------+-------------+
| Crown Heights North|       231279|
|        East Village|       221244|
|         JFK Airport|       188867|
|      Bushwick South|       187929|
|       East New York|       186780|
|TriBeCa/Civic Center|       164344|
|   LaGuardia Airport|       161596|
|            Union Sq|       158937|
|        West Village|       154698|
|             Astoria|       152493|
+--------------------+-------------+