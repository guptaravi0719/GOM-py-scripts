
import urllib
import csv
import json
import pandas as pd
import sys
import getopt
import pprint
from pymongo import MongoClient
# CSV to JSON Conversion

MAL_MONGO_DB_USER = "ps"
MAL_MONGO_DB_PASSWORD = "M@r!b0r0"
MAL_MONGO_DB_NAME = 'prodMalaysia'\

MAL_MONGO_DB_URI = f"mongodb+srv://{MAL_MONGO_DB_USER}:{urllib.parse.quote(MAL_MONGO_DB_PASSWORD)}@staging.q2lp8.gcp.mongodb.net/" \
    f"{MAL_MONGO_DB_NAME}?retryWrites=true&w=majority"
csvfile = open(
    r'C:\Users\gupta\Pictures\Screenshots\deal_car_price_mapping.csv', 'r')
reader = csv.DictReader(csvfile)
DB = MongoClient(MAL_MONGO_DB_URI)[MAL_MONGO_DB_NAME]

db = DB
collection = DB['deal_car_price_mapping']
header = ['service_id', 'mapping_id']

data = {}
for each in reader:
    row = {}
    for field in header:
        # data[field]=each[field]
        # data[field]=each[field]
        row[field] = int(each[field])
    print(row)

    service_id = row['service_id']
    mapping_id = row['mapping_id']

    filter_query = {'service_id': service_id}
    new_value = {"$set": {'mapping_id': mapping_id}}

    collection.update_many(filter_query, new_value, upsert=False)
    print("Done inserting !")
