# --------------------------------------------------------------------------------------------------------------------------------
# | Developer: Aarif Munwar Jahan                                                                                                |
# | Date: 10/29/21                                                                                                               |
# | Commit Version: 0.0.1                                                                                                        |
# | Code Function: Access dataset from nyc open data using Socrata API, then ingest data into AWS Elastic Search using Bulk API. |
# | For CIS 9760 - Big Data Technologies Class Project 1 with Professor Mottaqui Karim - Fall 2021                               |
# --------------------------------------------------------------------------------------------------------------------------------


# Import all necessary packages 
import sys
import os
import json
import time

import argparse
import requests
from requests.auth import HTTPBasicAuth
from sodapy import Socrata
from datetime import datetime


# Use argparse to add page size and num pages arguments
parser = argparse.ArgumentParser(description='Process data from project')
parser.add_argument('--page_size', type=int, help = 'how many rows to fetch per page', required = True)
parser.add_argument('--num_pages', type=int, help = 'how many pages to fetch in total')
args = parser.parse_args(sys.argv[1:])


# Environment variables to be passed from the docker run command
DATASET_ID = os.environ["DATASET_ID"]
APP_TOKEN = os.environ["APP_TOKEN"]
ES_HOST = os.environ["ES_HOST"]
INDEX_NAME = os.environ["INDEX_NAME"]
ES_USERNAME = os.environ["ES_USERNAME"]
ES_PASSWORD = os.environ["ES_PASSWORD"]

# Main loop
if __name__ == '__main__':
    
    try:
        
        # Define requests row insert parameters for the given index
        resp = requests.put(
            f"{ES_HOST}"/f"{INDEX_NAME}",
            auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            
            # Define data settings and mapping
            json = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                
                "mappings": {
                    "properties": {
                        "plate": {"type": "keyword"},
                        "state": {"type": "keyword"},
                        "license_type": {"type": "keyword"},
                        "summons_number": {"type": "keyword"},
                        "issue_date": {"type": "date"},         # Important to have as date variable for Kibana index
                        "violation_time": {"type": "keyword"},
                        "violation": {"type": "keyword"},
                        "fine_amount": {"type": "float"},
                        "penalty_amount": {"type": "float"},
                        "interest_amount": {"type": "float"},
                        "reduction_amount": {"type": "float"},
                        "payment_amount": {"type": "float"},
                        "amount_due": {"type": "float"},
                        "precinct": {"type": "keyword"},
                        "county": {"type": "keyword"},
                        "issuing_agency": {"type": "keyword"},
                    }
                },
            }
        )
        # Get response object for debugging
        resp.raise_for_status()
        
    # Handle error if index already exists and continue
    except Exception:
        print("\nIndex already exists!\n")
    
    # Define client as the nyc open data using the Socrata API with the passed App Token
    client = Socrata(
        "data.cityofnewyork.us",
        APP_TOKEN,
    )
    
    # Handle case of optional num pages argument not being populated
    if args.num_pages is not None:              # if declared, use num_pages to multiply the number of get call
        page_number = args.num_pages
    elif args.num_pages is None:                # if not declared, no multiplication - just get number declared in page sizes argument
        page_number = 1
    
    # Timestamp to measure run time start
    start = time.time()
    
    # Call the function based on the number of pages given
    for i in range(page_number):
        
        # Get rows from the passed Dataset depending on the page size and num pages
        rows = client.get(DATASET_ID, limit=args.page_size, offset=i*(args.page_size))
        
        # Keep count of number of rows for error handling
        count = 0
        
        # Declare empty list to capture multiple rows for bulk processing
        es_rows = []
        
        # Loop over each row in the dataset 
        for row in rows:
            
            # Define numeric and string attributes in the data
            numeric_att = ["fine_amount","penalty_amount", "interest_amount", "reduction_amount", "amount_due"]
            string_att = ["county","precinct", "violation", "state", "plate"]
            
            # Increase row count for error handling and performance monitoring
            count += 1
        
            try:
                
                # Define empty dictionary to load individual row data with key
                es_row = {}
                
                # Add issue date - apply appropriate datetime conversion to meet elastic search datetime format - this is important since issue date is our Kibana index field
                # Reference - https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
                es_row['issue_date']=datetime.strptime(row['issue_date'],'%m/%d/%Y')
                es_row['issue_date']=datetime.strftime(es_row['issue_date'],'%Y-%m-%d')
                
                # Loop over each numeric attributes, copy data only if attribute exists for row, assign default value of 0 if missing
                for attribute in numeric_att:
                    if attribute in row:
                        es_row[attribute] = float(row[attribute])
                    else:
                        es_row[attribute] = 0
                    
                # Loop over each string attributes, copy data only if attribute exists for row, assign default value of "missing" if missing
                for attribute in string_att:
                    if attribute in row:
                        es_row[attribute] = row[attribute]
                    else:
                        es_row[attribute] = "Missing"
                
            # Handle error with data collection - missing attributes, empty attributes etc., Skip rows if issue and continue with next 
            except Exception as e:
                print(f"Data Collection Error!: {e}, Row Number: {count}, Skipping Row: {row}")
                continue
            
            # Collect all rows in a big list in preparation for uploading data in bulk
            es_rows.append(es_row)
        
        
        # Define bulk data string
        bulk_data = ""
        
        # Loop through each row in collective rows list, 
        for i, row in enumerate(es_rows):
            
            # Define header for each bulk_data element
            header = '{"index": {"_index": "'+INDEX_NAME+'", "_type" : "_doc"}}'  
           
            # Define data for the header in json format
            data = json.dumps(row)                                                  
            
            # Combine data with header and data to create a bulk data row
            bulk_data += f"{header}\n"
            bulk_data += f"{data}\n"
    

        try:
            # Insert row data on Elastic Search with proper document hyperlink and correct authentication
            
            # Timestamp to get posting time
            start_post = time.time()
            
            resp = requests.post(
                
                # Use bulk api method
                f"{ES_HOST}/_bulk",
                
                # Provide authetnication for ES access
                auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
                
                # Define data and headers
                data=bulk_data,
                headers={
                    "Content-Type": "application/x-ndjson"
                }
            )
            # Get response object for debugging
            resp.raise_for_status()
            
            # Timestamp to get posting time
            end_post = time.time()
            
            # Handle error with row upload on Elastic Search, Skip rows if issue and continue with next
        except Exception as e:
            print(f"Failed to insert row in Elastic Search: {e}, Row Number: {count}, Skipping Row: {row}")
            continue
    
    # Calculate posting time    
    post_time = round(end_post-start_post, 1)
    
    # Timestamp to measure run time start
    end = time.time()
    
    # Calculate total run time
    total_time = round(end-start,1)
    
    # Print timing calculations on screen
    print("Bulk Row Upload Time:", post_time, "seconds\n")
    print("Total Runtime:" , total_time, "seconds\n")    
        

