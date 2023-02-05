ssh -i ~/.ssh/gcp alia@34.22.131.236


wget https://repo.anaconda.com/archive/Anaconda3-2022.10-Linux-x86_64.sh



# ###########
ssh -i ~/.ssh/gcp alia@34.22.131.236

touch config

############

ssh de-zoomcamp

#################

export GOOGLE_APPLICATION_CREDENTIALS="D:\Study\data-engineering-zoomcamp\week_1_basics_n_setup\dezoomcamp-375819-433f99121d3e.json"
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS

##################

export GOOGLE_APPLICATION_CREDENTIALS=~/.gc/dezoomcamp-375819-433f99121d3e.json
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS

############
terraform init
terraform plan
terraform apply
terraform destroy
