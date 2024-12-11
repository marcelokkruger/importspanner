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

Arg | Explain | Details
--- | --- | ---
project_id | Your Google Project ID | 
instance_id | Your Cloud Spanner instance ID | 
database_id | Your Cloud Spanner database ID | 
table_id | Your table name | 
batch_size | Number of rows to insert in a batch | 
data_file | Path of csv input data file | 
head_file | If the header exists in the CSV, set the option to true to disregard it | 
schema_file | Path of schema file describing the input data file | 
format_date | Sets the date format for conversion | Default is: %Y-%m-%d. For more formats check https://www.w3schools.com/python/python_datetime.asp

# Example

In the example folder, there are 3 files that will help you with your first import into Spanner.

* person.ddl: This file contains the code for creating the table. This code must be run first in Spanner. The import does not create the table; it must be previously created.
* person.schema: This is the schema of the data that will be imported. The application will read this schema file, and for each column listed in it, it will convert the data read from person.csv. The schema file is always organized as follows: the first column is the name of the field in the data file, and the second column is the data type.

Schema Format

    field,type

Supported data types are:

    integer
    float
    bytes
    date
    string
    boolean

Schema Example

    document,string
    name,string
    dt_nascimento,date
    nu_idade_obito,integer
    id,integer


* person.csv: Data files are always separated by commas.

