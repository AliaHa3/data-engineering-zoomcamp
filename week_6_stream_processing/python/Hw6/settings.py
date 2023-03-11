import pyspark.sql.types as T

INPUT_DATA_PATH = '../../resources/rides.csv'
GREEN_RIDES_DATA_PATH = '../../resources/green_tripdata_2019-01.csv'
FHVH_RIDES_DATA_PATH = '../../resources/fhv_tripdata_2019-01.csv'

BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_WINDOWED_VENDOR_ID_COUNT = 'vendor_counts_windowed'

PRODUCE_TOPIC_GREEN_RIDES_CSV = CONSUME_TOPIC_GREEN_RIDES_CSV = 'green_rides_csv'
PRODUCE_TOPIC_FHVH_RIDES_CSV = CONSUME_TOPIC_FHVH_RIDES_CSV = 'fhv_rides_csv'
PRODUCE_TOPIC_RIDES_CSV = CONSUME_TOPIC_RIDES_CSV = 'rides_csv'

RIDE_SCHEMA = T.StructType(
    [T.StructField("vendor_id", T.IntegerType()),
     T.StructField('tpep_pickup_datetime', T.TimestampType()),
     T.StructField('tpep_dropoff_datetime', T.TimestampType()),
     T.StructField("passenger_count", T.IntegerType()),
     T.StructField("trip_distance", T.FloatType()),
     T.StructField("payment_type", T.IntegerType()),
     T.StructField("total_amount", T.FloatType()),
     ])

GREEN_RIDES_SCHEMA = T.StructType(
    [
     T.StructField("vendor_id", T.IntegerType()),
     T.StructField('pickup_datetime', T.TimestampType()),
     T.StructField('dropoff_datetime', T.TimestampType()),
     T.StructField("store_and_fwd_flag", T.StringType()),
     T.StructField("RatecodeID", T.IntegerType()),
     T.StructField("PULocationID", T.IntegerType()),
     T.StructField("DOLocationID", T.IntegerType()),
     T.StructField("passenger_count", T.IntegerType())
     ])

FHV_RIDES_SCHEMA =T.StructType(
    [
     T.StructField("dispatching_base_num", T.StringType()),
     T.StructField('pickup_datetime', T.TimestampType()),
     T.StructField('dropoff_datetime', T.TimestampType()),
     T.StructField("PUlocationID", T.IntegerType()),
     T.StructField("DOlocationID", T.IntegerType()),
     T.StructField("SR_Flag", T.StringType()),
     T.StructField("Affiliated_base_number", T.StringType()),
     ])