import argparse
import csv
import numbers
import base64
import os
import warnings

from google.cloud import spanner

def castFloat(value):
  try:
    return float(value)
  except ValueError:
    return 0.0
    
def castInt(value):
	try:
		return int(value)
	except ValueError:
		return 0
		
def batch_data(project_id, instance_id, database_id, table_id, batch_size, data_file, head_file, schema_file):

    warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")

    if project_id!="":
      os.environ["GCLOUD_PROJECT"] = project_id

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    
    schemafile = open(schema_file, 'r')
    schemareader = csv.reader(schemafile)

    collist = []
    typelist = []
    
    for col in schemareader:
    	collist.append(col[0])
    	typelist.append(col[1])
    	
    numcols = len(collist)
    
    datafile  = open(data_file, "r")
    reader = csv.reader(datafile,delimiter=',')

    datalist = []
    numberrow = 0

    batchrows = int(batch_size)

    for row in reader:
      if head_file=='true' and numberrow==0:
        numberrow = numberrow + 1
        continue

      for columnValue in range(0,numcols):
        if typelist[columnValue] == 'integer':
          row[columnValue] = castInt(row[columnValue])
        if typelist[columnValue] == 'float':
          row[columnValue] = castFloat(row[columnValue])
        if typelist[columnValue] == 'bytes':
          row[columnValue] = base64.b64encode(row[columnValue])
      
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

          print ('Inserted {0} rows'.format(numberrow))

    
    print ('Finish! Inserted {0} rows'.format(numberrow))         
  		    		
    datafile.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--project_id', help='Your Google Project ID')
    parser.add_argument(
        '--instance_id', help='Your Cloud Spanner instance ID')
    parser.add_argument(
        '--database_id', help='Your Cloud Spanner database ID')
    parser.add_argument(
    	  '--table_id', help='Your table name')
    parser.add_argument(
		    '--batch_size', help='Number of rows to insert in a batch')
    parser.add_argument(
		    '--data_file', help='Path of csv input data file')
    parser.add_argument(
		    '--head_file', help='If the header exists in the CSV, set the option to true to disregard it', default='true')
    parser.add_argument(
        '--schema_file', help='Path of schema file describing the input data file')
		
    args = parser.parse_args()

    batch_data(args.project_id, args.instance_id, args.database_id, args.table_id, args.batch_size, args.data_file, args.head_file, args.schema_file)
