import argparse
import csv
from decimal import Decimal
import numbers
import base64
import os
import warnings
from datetime import datetime

from google.cloud import spanner

def castFloat(value):
  try:
    dec = Decimal(value)
    return dec
  except:
    return None
    
def castInt(value):
	try:
		return int(value)
	except ValueError:
		return None
   
def castBytes(value):
	try:
		return base64.b64encode(value)
	except ValueError:
		return None

def castDate(value, format):
	try:
		return datetime.strptime(value, format).strftime("%Y-%m-%d")
	except:
		return None

def castBoolean(value):
  match value.upper():
    case "T":
      return True
    case "TRUE":
      return True
    case "1":
      return True
    case "F":
      return False
    case "FALSE":
      return False
    case "0":
      return False
    case _:
      return None
		
def batch_data(project_id, instance_id, database_id, table_id, batch_size, data_file, head_file, schema_file, format_date):

    warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")

    if project_id!="":
      os.environ["GCLOUD_PROJECT"] = project_id

    if project_id=="" and os.environ["GCLOUD_PROJECT"]=="":
        print("You need to fill the GCLOUD_PROJECT environment variable or the --project_id parameter with the name of the project in GCP.")
        exit(1)

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    
    schemafile = open(schema_file, "r")
    schemareader = csv.reader(schemafile)

    collist = []
    typelist = []
    
    for col in schemareader:
    	collist.append(col[0])
    	typelist.append(col[1])
    	
    numcols = len(collist)
    
    datafile  = open(data_file, "r")
    reader = csv.reader(datafile,delimiter=",")

    datalist = []
    numberrow = 0

    batchrows = int(batch_size)

    for row in reader:
      if head_file=="true" and numberrow==0:
        numberrow = numberrow + 1
        continue

      for columnValue in range(0,numcols):
        match typelist[columnValue]:
          case "integer":
            row[columnValue] = castInt(row[columnValue])
          case "float":
            row[columnValue] = castFloat(row[columnValue])
          case "bytes":
            row[columnValue] = castBytes(row[columnValue])
          case "date":
            row[columnValue] = castDate(row[columnValue], format_date)
          case "boolean":
            row[columnValue] = castBoolean(row[columnValue])
          case "string":
            row[columnValue] = row[columnValue]
          case _:
            print("Data type {} is not supported".format(typelist[columnValue]))
            exit(1)
      
      datalist.append(row)

      numberrow = numberrow + 1

      if numberrow % batchrows == 0:
        with database.batch() as batch:
          batch.insert(
            table=table_id,
            columns=collist,
            values=datalist
          )

          datalist.clear() 

          print ("Inserted {0} rows".format(numberrow))

    
    print ("Finish! Inserted {0} rows".format(numberrow))         
  		    		
    datafile.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--project_id", help="Your Google Project ID")
    parser.add_argument(
        "--instance_id", help="Your Cloud Spanner instance ID")
    parser.add_argument(
        "--database_id", help="Your Cloud Spanner database ID")
    parser.add_argument(
    	  "--table_id", help="Your table name")
    parser.add_argument(
		    "--batch_size", help="Number of rows to insert in a batch")
    parser.add_argument(
		    "--data_file", help="Path of csv input data file")
    parser.add_argument(
		    "--head_file", help="If the header exists in the CSV, set the option to true to disregard it", default="true")
    parser.add_argument(
        "--schema_file", help="Path of schema file describing the input data file")
    parser.add_argument(
		    "--format_date", help="Sets the date format for conversion. Default is: %Y-%m-%d", default="%Y-%m-%d")
		
    args = parser.parse_args()

    batch_data(args.project_id, args.instance_id, args.database_id, args.table_id, args.batch_size, args.data_file, args.head_file, args.schema_file, args.format_date)
