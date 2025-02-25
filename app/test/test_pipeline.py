import json
from flask import Flask
from lib.utils import clean_spot_query, set_area

test_input = {
   "area":{
      "type":"area",
      "value":"koblenz"
   },
   "nodes":[
      {
         "id":0,
         "type":"nwr",
         "filters":[
            {
               "and":[
                  {
                     "or":[
                        {
                           "key":"amenity",
                           "operator":"=",
                           "value":"restaurant"
                        },
                        {
                           "key":"amenity",
                           "operator":"=",
                           "value":"food_court"
                        },
                        {
                           "key":"amenity",
                           "operator":"=",
                           "value":"fast_food"
                        }
                     ]
                  },
                  {
                     "key":"cuisine",
                     "operator":"=",
                     "value":"italian"
                  }
               ]
            }
         ],
         "name":"restaurant",
         "display_name":"restaurants"
      }
   ]
}


app = Flask(__name__)

with app.app_context():
   cleaned_spot_query = clean_spot_query(test_input)
   print(cleaned_spot_query)
   set_area(cleaned_spot_query)
   print(cleaned_spot_query)
# query = constructor.construct_query_from_graph(cleaned_spot_query)