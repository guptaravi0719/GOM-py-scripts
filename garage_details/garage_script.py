import csv



import urllib
import csv
import json
import pandas as pd
import sys
import getopt
import pprint
from pymongo import MongoClient
# CSV to JSON Conversion

# # Prod India DB
MONGO_DB_USER = 'ps'
MONGO_DB_PASSWORD = 'M@r!b0r0'
MONGO_DB_NAME = 'prod'
MONGO_DB_URI = f'mongodb+srv://{MONGO_DB_USER}:{urllib.parse.quote(MONGO_DB_PASSWORD)}@prod.q2lp8.gcp.mongodb.net/' \
               f'{MONGO_DB_NAME}?retryWrites=true&w=majority'

# csvfile = open(
#     r'C:\Users\gupta\Pictures\Screenshots\deal_car_price_mapping.csv', 'r')
# reader = csv.DictReader(csvfile)
DB = MongoClient(MONGO_DB_URI)[MONGO_DB_NAME]
cluster_details_collection = DB['cluster_details']
central_cr_cluster_mapping_collection = DB['central_cr_cluster_mapping']
users_collection = DB['users']
final_data_to_write = []
garage_id_list = []
count = 0
users_collection_data = users_collection.find({})
cluster_details_collection_data= cluster_details_collection.find({})
with open('/home/ravi/Downloads/Details.csv', 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        x = int(row[0])
        cluster_data = cluster_details_collection.find_one({'garage_id':int(row[0])},{'_id':0,'cr_id':1, 'captain_id':1,'city_head_id':1,'cluster_id':1})
        # print(cluster_data.get('cr_id'))
        # print(cluster_data)
        temp_list = []
        temp_list.append(row[0])
        temp_list.append(row[1])
        if cluster_data is not None:
            cluster_id = cluster_data.get('cluster_id')
            cr_id =  cluster_data.get('cr_id')
            captain_id = cluster_data.get('captain_id')
            city_head_id =cluster_data.get('city_head_id')
            central_cr_id_dict = central_cr_cluster_mapping_collection.find_one({'cluster_id':cluster_id},{'central_cr_id':1})
            if central_cr_id_dict is not None:
                central_cr_id =central_cr_id_dict.get('central_cr_id')
            else: 
                central_cr_id = None   
            ucdcentralcr = users_collection.find_one({'id':0},{'_id':0,'email':1})  
            ucdcr = users_collection.find_one({'id':cr_id},{'_id':0,'email':1})  
            ucd_captain=users_collection.find_one({'id':captain_id},{'_id':0,'email':1})
            ucd_cityhead = users_collection.find_one({'id':city_head_id},{'_id':0,'email':1})

            central_cr_email= ucdcentralcr.get('email') if ucdcentralcr is not None else 'N/A'
            cr_email = ucdcr.get('email') if ucdcr is not None else 'N/A'
            captain_email = ucd_captain.get('email') if ucd_captain is not None else 'N/A'
            city_head_email = ucd_cityhead.get('email') if ucd_cityhead is not None else None

            temp_list.append(cr_email)  
            temp_list.append(captain_email) 
            temp_list.append(central_cr_email) 
            temp_list.append(city_head_email) 
            

        else: 
            temp_list.append('N/A')  
            temp_list.append('N/A') 
            temp_list.append('N/A') 
            temp_list.append('N/A') 
        
        final_data_to_write.append(temp_list)
        print(row[0])
        print(count)
        count= count+1
        # if count ==20:
        #     break
            
        


# field names 
fields = ['Garage ID', 'Garage', 'CR Email', 'Captain Email','Central CR Email', 'City Head Email'] 

    
# name of csv file 
filename = "garage_crs_email.csv"
    
# writing to csv file 
with open(filename, 'w') as csvfile: 
    # creating a csv writer object 
    csvwriter = csv.writer(csvfile) 
        
    # writing the fields 
    csvwriter.writerow(fields) 
        
    # writing the data rows 
    csvwriter.writerows(final_data_to_write)