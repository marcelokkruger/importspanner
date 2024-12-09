# importspanner
Tool command line for imports a csv file into a cloud spanner table

# Setup

1 Clone this repository

    git clone https://github.com/hanknac/spannerimport.git

2 Set the application default login using gcloud

    gcloud auth application-default login

2 Set venv and install google-cloud-spanner

    cd importspanner
    python3 -m venv ~/py_envs
    source ~/py_envs/bin/activate
    python3 -m pip install google-cloud-spanner

# Usage

Below you can see the command line used to import data

    python3 import.py --project_id=teste-spanner-1736 --instance_id=spanner-test --database_id=test --table_id=Person --batchsize=10000 --data_file=person.csv --schema_file=person.schema

Parameters

Arg | Explain
--- | ---
project_id | Your Google Project ID
instance_id | Your Cloud Spanner instance ID
database_id | Your Cloud Spanner database ID
table_id | Your table name
batch_size | Number of rows to insert in a batch
data_file | Path of csv input data file
head_file | If the header exists in the CSV, set the option to true to disregard it
schema_file | Path of schema file describing the input data file
