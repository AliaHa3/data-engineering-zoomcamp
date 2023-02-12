##link template
## 1) download dataset files
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-02.csv.gz
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-03.csv.gz
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-04.csv.gz
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-05.csv.gz
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-06.csv.gz
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-07.csv.gz
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-08.csv.gz
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-09.csv.gz
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-11.csv.gz
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-12.csv.gz

## 2) upload to gcs

## 3) create external table an BQ table

CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp-375819.trips_data_all.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_dezoomcamp-375819/fhv/fhv_tripdata_2019-*.csv.gz']
);

CREATE OR REPLACE TABLE `dezoomcamp-375819.trips_data_all.fhv_tripdata_non_partitoned` AS
SELECT * FROM `dezoomcamp-375819.trips_data_all.fhv_tripdata`;

SELECT * FROM `dezoomcamp-375819.trips_data_all.fhv_tripdata` limit 10;


## 4) Q1 query
SELECT count(*) FROM `dezoomcamp-375819.trips_data_all.fhv_tripdata`;
# A: 43,244,696


## 5) Q2 query
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `dezoomcamp-375819.trips_data_all.fhv_tripdata`;
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `dezoomcamp-375819.trips_data_all.fhv_tripdata_non_partitoned`;
# A: 0 MB for the External Table and 317.94MB for the BQ Table

## 6) Q3 query
SELECT COUNT(*) FROM `dezoomcamp-375819.trips_data_all.fhv_tripdata`
WHERE PUlocationID is null and DOlocationID is null;
# A: 717748

## 7) Q4 query
# A: Partition by pickup_datetime Cluster on affiliated_base_number


## 8) Q5 query

CREATE OR REPLACE TABLE `dezoomcamp-375819.trips_data_all.fhv_tripdata_partitioned`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS (
  SELECT * FROM `dezoomcamp-375819.trips_data_all.fhv_tripdata`
);

SELECT count(DISTINCT(affiliated_base_number)) FROM  `dezoomcamp-375819.trips_data_all.fhv_tripdata_non_partitoned`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';


SELECT count(DISTINCT(affiliated_base_number)) FROM  `dezoomcamp-375819.trips_data_all.fhv_tripdata_partitioned`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';
# A: 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

## 9) Q6 query
# A: GCP Bucket

## 10) Q7 query
# A: False



