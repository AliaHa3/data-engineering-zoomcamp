CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp-375819.trips_data_all.green_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_dezoomcamp-375819/green/green_tripdata_2019-*.csv.gz','gs://dtc_data_lake_dezoomcamp-375819/green/green_tripdata_2020-*.csv.gz']
);


CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp-375819.trips_data_all.yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_dezoomcamp-375819/yellow/yellow_tripdata_2019-*.csv.gz','gs://dtc_data_lake_dezoomcamp-375819/yellow/yellow_tripdata_2020-*.csv.gz']
);

dbt build --var 'is_test_run: false'

##Q1
SELECT count(*) FROM `dezoomcamp-375819.Production.fact_trips`;
#A 61,604,283 ~ 61648442

SELECT count(*) FROM `dezoomcamp-375819.Production.fact_trips`
WHERE pickup_datetime BETWEEN '2019-01-01' AND '2020-12-31';

#A
report link:
https://lookerstudio.google.com/reporting/49e78717-3e23-4c34-88ce-a7ed7a59a812

##Q2
#A 89.8/10.2 ~ 89.9/10.1


##Q3
SELECT count(*) FROM `dezoomcamp-375819.Production.stg_fhv_tripdata`;
#A 43,244,696

##Q4
SELECT count(*) FROM `dezoomcamp-375819.Production.fact_fhv_trips`;
#A 22,998,722

##Q5
report link:
https://lookerstudio.google.com/reporting/3747c6a5-65de-4a5a-9b13-4836974a6e1e

#A January