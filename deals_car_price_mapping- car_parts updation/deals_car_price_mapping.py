
import urllib
import csv
import json
import pandas as pd
import sys
import getopt
import pprint
from pymongo import MongoClient
from datetime import datetime

MAL_MONGO_DB_USER = "ps"
MAL_MONGO_DB_PASSWORD = "M@r!b0r0"
MAL_MONGO_DB_NAME = 'prod'

MAL_MONGO_DB_URI = f"mongodb+srv://{MAL_MONGO_DB_USER}:{urllib.parse.quote(MAL_MONGO_DB_PASSWORD)}@staging.q2lp8.gcp.mongodb.net/" \
    f"{MAL_MONGO_DB_NAME}?retryWrites=true&w=majority"
csvfile = open(
    r'/home/ravi/Documents/service_tbl.csv', 'r')
reader = csv.DictReader(csvfile)
DB = MongoClient(MAL_MONGO_DB_URI)[MAL_MONGO_DB_NAME]

db = DB
collection1 = DB['deal_car_price_mapping']

collection2= DB['car_tbl']

car_ids_list = []   # list of all car_ids

car_ids_json = collection2.find({},{ 'id':1,'_id':0})      #response from db


for x in car_ids_json:
    if int(x.get('id')) not in car_ids_list:
            car_ids_list.append(int(x.get('id')))   #appending ids of car in a list

print(car_ids_list)
print(len(car_ids_list))

header = ['service_id', 'mapping_id','price', 'deal_id']

data = []    #array in which all dict will be appended for insert_many in deal_car_price_mapping collection

id_count= 90403543957  # first id to be inserted in deal_car_price_mapping is id_count+1 


csvfile = open(
    r'/home/ravi/Documents/service_tbl.csv', 'r')
reader = csv.DictReader(csvfile)
def update_global_dict_array(car_id):     #updates data array with new dicts

    
    for each in reader:
        row = {}
        for field in header:
        # data[field]=each[field]
        # data[field]=each[field]
            row[field] = int(each[field])
        # print(row)

        
        global id_count
        # filter_kwargs = {'id': id_count }
        data_to_insert ={
                # "id":id_count,
                # "deal_id":13,
                # "work_done":4,
                "car_id":car_id,
                # 'price':row['price'],
                "service_id":row['service_id'],
                "mapping_id":row['mapping_id'],
                # "package_id":None,
                # "quantity":1,
                # "material_cost":row['price'],
                # "labour_cost":0,
                # "brand":None,
                # "work_done":None,
                # "created_at":datetime.now(),
                # "updated_at":datetime.now(),
                # "brand_1_name":"brand_1_name",
                # "brand_1_price":None,
                # "brand_2_name":"brand_2_name",
                # "brand_2_price":None,
                # "brand_3_name":None,
                # "brand_3_price":None,
                # "brand_4_name":None,
                # "brand_4_price":None,
                # "strike_through_price":None,
                # "material_cost_oes":0,
                # "inclusive_of_tax":1,
                # "recommended_brand":None
                }
        data.append(data_to_insert)        
        id_count+=1
        break
        # print(data_to_insert)
        # print("........................................................")
        # 90403546626
        

car_ids_list.sort()
for x in car_ids_list:
    update_global_dict_array(car_id=x)
     

print(data)
# collection1.insert_many(data)    

# update_many to db here , in deal_car_price_mapping
# print(data)
# collection1.update_many(
#   {'id' : {'$gte': 90403536277}},
#   { '$set': {"work_done": 4} },
#   upsert= False
# )
# print("done Insertion")



