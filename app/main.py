# 1
from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient
load_dotenv(find_dotenv())

password = os.environ.get("mongodb_pwd")
connection_string = f"mongodb+srv://mongodb:{password}@projectmongodatabase.i8h8ics.mongodb.net/?retryWrites=true&w=majority" 
client = MongoClient(connection_string)
dbs = client.list_database_names()

#print(dbs) # This checks how many DB
test_db = client.test
collections = test_db.list_collection_names()
#print(collections)
def insert_test_doc():
   collections = test_db.test
   test_document = {"name":"computer","type":"test"}
   inserted_id = collections.insert_one(test_document).inserted_id
   print(inserted_id) # This id is known as BSON object ID
#insert_test_doc()

# 2. Reading documents
# Adding new DB to insert more
production = client.production
person_collection = production.person_collection
def create_documents():
   first_names = ["computer","mouse","speaker","phone"]
   last_names = ["computer_laptop","mouse_small one","speaker_sound","phone_mobile"]
   ages = [41,66,86,20]
   docs = []
   for first_name, last_name, age in zip(first_names, last_names, ages):
      doc = {"first_name":first_name, "last_name": last_name, "ages":age }
      docs.append(doc)
   person_collection.insert_many(docs)
#create_documents()

# To print all the collections/documents in a specific format use pprint
printer = pprint.PrettyPrinter()
def find_all_people():
   people = person_collection.find()
   for person in people:
      printer.pprint(person)
#find_all_people()

# To find specific id document
def find_computer():
   computer = person_collection.find_one({"first_names":"computer"})
   printer.pprint(computer)
#find_computer()

# To count how many 
def counter_all_people():
   count = person_collection.count_documents(filter={})
   print("Number of people",count)
#counter_people_id()

# To get specifically from ID
def get_person_by_id(person_id):
   from bson import ObjectId
   _id = ObjectId(person_id)
   person = person_collection.find_one({"_id":_id})
   printer.pprint(person)
#get_person_by_id("639567bb59d7df8dd7716cc2")

#get min & max in between range
def get_age_range(min_age, max_age):
   query = {"$and":[
      {"ages":{"$gte": min_age}},
      {"ages":{"$lte": max_age}}
      ]}
   people = person_collection.find(query).sort("ages")
   for person in people:
      printer.pprint(person)
#get_age_range(86,87)

# project specific columns
def project_specific_columns():
   columns = {"_id":0, "first_names":1, "last_names":1}
   people = person_collection.find({}, columns)
   for person in people:
      printer.pprint(person)
#project_specific_columns()

#3. update doc by id
def update_person_id(person_id):
   from bson.objectid import ObjectId
   _id = ObjectId(person_id)
   all_updates = {
      "$set": {"new_field":True},
      "$inc": {"ages":1},
      "$rename": {"first_names":"first", "last_names":"last"}
   }
   person_collection.update_one({"_id": _id}, all_updates)
#update_person_id("639567bb59d7df8dd7716cc5")

# remove 
def remove_person_id(person_id):
   from bson.objectid import ObjectId
   _id = ObjectId(person_id)
   person_collection.update_one({"_id":_id}, {"$unset":{"new_field": ""}})
#remove_person_id("639567bb59d7df8dd7716cc5")

#  replace by one
def replace_one(person_id):
   from bson.objectid import ObjectId
   _id = ObjectId(person_id)
   new_doc = {
      "first_names":"new first name",
      "last_names":"new last name",
      "age" : 100
   }
   person_collection.replace_one({"_id": _id}, new_doc)
#replace_one("639567bb59d7df8dd7716cc5")

# delete 
def delete_doc_by_id(person_id):
   from bson.objectid import ObjectId
   _id = ObjectId(person_id)
   person_collection.delete_one({"_id":_id})
   # to delete all the document
   #person_collection.delete_many({})
#delete_doc_by_id("639567bb59d7df8dd7716cc5")

# 4. relationships
address = {
   "id": "639567bb59d7df8dd7716cc5",
   'street': "street",
   "number": 701,
   "city": "unknown",
   "country":"mars",
   "zip": "40321"
}
def add_address_method(person_id, address):
   from bson.objectid import ObjectId
   _id = ObjectId(person_id)
   person_collection.update_one({"_id":_id}, {"$addToSet":{'address':address}})
#add_address_method("639567bb59d7df8dd7716cc4", address)

#another method
def add_address_relationship(person_id, address):
   from bson.objectid import ObjectId
   _id = ObjectId(person_id)
   address = address.copy()
   address["owner_id"] = person_id
   address_collection = production.address
   address_collection.insert_one(address)
add_address_relationship("639567bb59d7df8dd7716cc2", address)