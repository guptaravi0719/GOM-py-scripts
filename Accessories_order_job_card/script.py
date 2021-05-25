import urllib
import csv
import json
import pandas as pd
import sys
import getopt
import pprint
from pymongo import MongoClient
import json
from datetime import datetime
import requests
from requests.models import CaseInsensitiveDict

MAL_MONGO_DB_USER = "ps"
MAL_MONGO_DB_PASSWORD = "M@r!b0r0"
MAL_MONGO_DB_NAME = 'prod'

MAL_MONGO_DB_URI = f"mongodb+srv://{MAL_MONGO_DB_USER}:{urllib.parse.quote(MAL_MONGO_DB_PASSWORD)}@staging.q2lp8.gcp.mongodb.net/" \
    f"{MAL_MONGO_DB_NAME}?retryWrites=true&w=majority"

DB = MongoClient(MAL_MONGO_DB_URI)[MAL_MONGO_DB_NAME]

db = DB
orders_collection = DB['orders']
deal_car_price_mapping_collection= DB['deal_car_price_mapping']
labours_work_done_mapping = DB['labours_work_done_mapping']
service_tbl_collection = DB['service_tbl']



def get_gm_num_for_mapping_id(mapping_id):
    gm_num= labours_work_done_mapping.find_one({'id':mapping_id},{'gm_num':1})
    return gm_num.get('gm_num')
data_to_push = {}
def create_job_card(order_id):

    data_for_job_card={
    'fleet_id': int(orders_collection.find_one({'order_id':order_id},{'fleet_id':1}).get('fleet_id')),
    'order_id':order_id,
    'channel': orders_collection.find_one({'order_id':order_id},{'channel':1}).get('channel')
                    }
    
    job_card_items_list =[]               
    order_data = orders_collection.find_one({'order_id':order_id},{'_id':0,'accessories_booked':1})

    if order_data.get('accessories_booked'):
        accessories_item_list = order_data.get('accessories_booked')
        for accessory_item in accessories_item_list:
            print(accessory_item.get('id'))
            mapping_id = deal_car_price_mapping_collection.find_one({'service_id':int(accessory_item.get('id'))},{'mapping_id':1}).get('mapping_id')
          
            job_card_item_dict= {
            'description': '',
            'gm_num' :get_gm_num_for_mapping_id(mapping_id=mapping_id),
            'is_labour':0,
            'package_name':  '',
             'service_name': service_tbl_collection.find_one({'id':int(accessory_item.get('id'))},{'name':1}).get('name'),
            'work_done':4,
            
            }
            
            job_card_items_list.append(job_card_item_dict)

        data_for_job_card.update({'job_card_items':job_card_items_list})
        global data_to_push
        data_to_push= data_for_job_card    
        # print(str(data_for_job_card))

       

create_job_card(order_id= "20210510993941614") 

url = 'http://127.0.0.1:8000/v1/oauth/order/create_job_card_for_acc'
myobj = data_to_push
headers = CaseInsensitiveDict()

data_json = json.dumps(data_to_push)

print(data_json)

headers["Authorization"] = "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiIzIiwibmJmIjoxNjIwOTAzMzMzLjAsImlhdCI6MTYyMDkwMzMzMy4wLCJzdWIiOiI0MzI4NTQiLCJzZXNzaW9uX2tleSI6IkZZUVZJWDZQN1ciLCJzY29wZXMiOltdLCJqdGkiOiJiNGJjM2NhZjVkMWVhOTlkYzk2YjQzM2NjYzQzMDI0ZTAyM2I0MGM2YjQ5ZjExN2JjMDk5OGY2MWU3ZDI1ZjM2MTU1YWU5ZDIxNjE2ZTc5NSIsImV4cCI6MTYyMjExMjkzMy4wfQ.FcM_4qLhA8lR5oMfzqZVyk8sFVJbuByLj4CXqCw9x56YPJ53IxTTLzSCMQ4z-mU0wIwOeVGEiyB_O5NNLiH7Uw0v9LZJxG1yhskIegKysTXNB_0RW2SCkLq4E5k8piKBYRDtIBtc8fEU-KmStoUuCsFM8_uW6D92Xq7Z3dDoGFxWXBoLhl6Z4ikO9L9C9wZ6pfU6_tnWQgA81dPuCxHKxQeYpwEueI1qOA3klqOm9VACwyr65mZ-UlM8PUb6SPH0Al0zB78etybDIeC2hNC3tUwVqLw7jpuh0Istsuiwbm1SJWHSCm0c6wB7ukhjrvMg2IEX_l7GEt3tMQCj0ygrwuw7vnn6O_5yoVInj5RANH8LVCTVZWU5IyaO1-EdAaD1o8smy9w7RhpWNI4gNtGC7V1YHIAk2EJ7nHvhhk9rG5yboNRQtAfe5XO7ZLQJRs7KVjTsQjNUTLsczBcsDdX539eCIj-ERJjbqfWU0aEHdP7pW-ICo2L1n1ilS2V2CfQsG5Y7GXP5aGC552Ovh-Q-wvhNMpojB2MgQBGbl2WoZai7-uXEOqpyj7-wHcU73OyFZFOBTY201prjLEZX3QMjBLoGLlPP6kUmkPAkKzwBatAQaBkdj5LJe18zoO-lgF8zSoAHv2v4VDxYBaQyCjPHKSaOnshDuLoovw5SKmsu9zg"
headers['Content-Type']= "application/json"
x = requests.post(url, data = data_json,headers=headers)
print(x.text)


