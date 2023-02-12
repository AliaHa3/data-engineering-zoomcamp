# 1- istall requirements 
pip install -r requirements.txt

## prefect commands

prefect orion start
prefect agent start --work-queue "default"
prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"
prefect deployment apply etl_parent_flow-deployment.yaml


# 2- test locclly: 
# docker run postgres 
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v D:/Study/DE-ZOOMCAMP/week1/2_docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5433:5432 \
  postgres:13

# test ingest data script
python flows\01_start\ingest_data.py

# test ingest data script with flow SQLALchemy Block
python flows\01_start\ingest_data_flow.py

# test postgres locally
pgcli -h localhost -p 5433 -u root -d ny_taxi



# 3- GCP-Block
python flows\02_gcp\etl_web_to_gcs.py

# 4- GCP to GBQ


#Week 2 Homework

#Q1
python flows\02_gcp\etl_web_to_gcs.py
#A:447770

#Q2
# A: 0 5 1 * *

#Q3
prefect deployment build flows\02_gcp\etl_gcs_to_bq_new.py:etl_gcs_to_bq -n "Q3-step1"
prefect deployment apply etl_gcs_to_bq-deployment.yaml

prefect deployment build flows\03_deployments\parameterized_flow.py:etl_parent_flow -n "Q3-step2"
prefect deployment apply etl_parent_flow-deployment.yaml
# A:14,851,920

#Q4
pip install prefect-github
prefect block register -m prefect_github

python flows\02_gcp\etl_web_to_gcs_git.py

## cd in git root folder
prefect deployment build -n "Q4-gitB" -sb github/dezoomcamp-git flows/etl_web_to_gcs_git.py:etl_web_to_gcs --apply


# A:88605

#Q5

python flows\02_gcp\etl_web_to_gcs_slack.py

# A:514392

#Q6
# A:8



