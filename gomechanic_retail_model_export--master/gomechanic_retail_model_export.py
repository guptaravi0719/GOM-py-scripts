import pymongo
from pymongo import MongoClient
from pymongo import collection
import urllib
import json
from bson.json_util import dumps, loads



MAL_MONGO_DB_USER = 'ps'
MAL_MONGO_DB_PASSWORD = 'M@r!b0r0'
MAL_MONGO_DB_NAME = 'prodMalaysia'
MAL_MONGO_DB_URI = f'mongodb+srv://{MAL_MONGO_DB_USER}:{urllib.parse.quote(MAL_MONGO_DB_PASSWORD)}@staging.q2lp8.gcp.mongodb.net/' \
    f'{MAL_MONGO_DB_NAME}?retryWrites=true&w=majority'

DB = MongoClient(MAL_MONGO_DB_URI)[MAL_MONGO_DB_NAME]  

car_tbl = DB['car_tbl']
custom_car_tbl = DB['custom_car_tbl']
car_specs = DB['car_specs']
car_models = DB['car_models']
target_collection = DB['temp_model_json']
ans = []   #final ans 

  
car_models_temp =  list (car_models.find({}, {'_id': 0}))   
custom_car_tbl_temp = list (custom_car_tbl.find({}, {'_id': 0})) 
car_spec_data = list(car_specs.find({},{"_id": 0, "alloy_wheels": 1,  "arm_rest": 1, "car_type_id": 1, "d_odometer": 1, "doors": 1, "dual_tone": 1,
                                                    "engine_capacity": 1, "fog_lights": 1, "id": 1, "leather_seats": 1, "power": 1, "sun_roof": 1, "torque": 1, "transmission": 1, "turning_radius": 1, "tyre_size": 1, "tyre_type": 1}))
print(car_spec_data)

for x in car_models_temp:

    temp = x.get('id')

    for y in custom_car_tbl_temp:
        
        custom = {}
        print("segment" + str(x.get('segment')))
        custom.update({"segment": x.get('segment') if x.get('segment')!= None else "" })
        
        custom.update({"id":x.get('id') if x.get('id')!= None else ""})

        custom.update({"image_path": x.get('image_path') if x.get('image_path')!= None else ""})

        print("Name : "+str(x.get("name")))
        custom.update({"name": x.get("name") if x.get('name')!= None else ""})

        model = y.get('model')
        custom.update(
            {"year_of_manufacture": model.get('year_of_manufacture')  if model.get('year_of_manufacture')!= None else ""})
        if x['id'] == y['model_id']:
            custom.update({"start_year": y.get("start_year")  if y.get('start_year')!= None else ""})
            custom.update({"end_year": y.get('end_year') if y.get('end_year')!= None else ""})

            custom.update({"brand_id": y.get('brand_id')  if y.get('brand_id')!= None else ""})

            tempfuelarray = []
            fuelObject = y.get('fuel')

            for k in fuelObject:
                car_type_id = k.get('car_type_id') if k.get('car_type_id')!= None else ""
                print("car_spec loop here")
                car_spec_data_temp =  [a_dict  for a_dict in car_spec_data if a_dict['car_type_id']==car_type_id]
                car_specs_arr = []
                for idx in car_spec_data_temp:  # getting specs using car type id in fuel
                    car_specs_arr.append(idx)

                tempfuelarray.append({"car_type_id": k.get('car_type_id') if k.get('car_type_id')!= None else "", "id": k.get(
                    'id') if k.get('id')!= None else "", 'name': k.get('name') if k.get('name')!= None else "", 'car_specs': car_specs_arr})  # fuel data
            
            custom.update({"fuel": tempfuelarray})
            print("updating")
            ans.append(custom)






f = open(r"C:\Users\gupta\Desktop\new_export.txt", "w")

f.write(str(ans))
f.close()


