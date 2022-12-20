from dotenv import load_dotenv, find_dotenv
import os
import pprint 
from pymongo import MongoClient
from datetime import datetime as dt
load_dotenv(find_dotenv())

password = os.environ.get("mongodb_pwd")
connection_string = f"mongodb+srv://mongodb:{password}@projectmongodatabase.i8h8ics.mongodb.net/?retryWrites=true&w=majority&authSource=admin" 
client = MongoClient(connection_string)

dbs = client.list_database_names()
production = client.production

printer = pprint.PrettyPrinter()

#1. schema validation
def create_collection():
   book_validator = {
      "$jsonSchema": {
         "bsonType": "object",
         "required": ["title", "authors", "publish_date", "type","copies"],
         "properties": {
            "title": {
               "bsonType": "string",
               "description": "must be a string and is required"},
            "authors":{
               "bsonType": "array",
            "items": {
               "bsonType":"objectId", 
               "description":"must be an objectId and is required"}
            },
            "publish_rate":{
               "bsonType": "date",
               "description": "must be a date is required"},
            "type": {
               "enum": ["Fiction", "Non-Fiction"],
               "description": "must be a double if the field exists"},
            "copies":{
               "bsonType": "int",
               "minimum": 0, 
               "description": "must be a integer greater than 0 and is required"},}}}
   try:
      production.create_collection("book")
   except Exception as e:
      print(e)
   production.command("collMod","book", validator=book_validator)
#create_collection()

def create_author_collection():
   author_validator = {
      "$jsonSchema": {
         "bsonType": "object",
         "required": ["first_name", "last_name", "date_of_birth"],
         "properties": {
            "first_name":{
               "bsonType":"string",
               "description": "'first_name' must be a string and it is required"},
            "last_name":{
               "bsonType": "string",
               "description": "'last_name' must be a string and it is required"},
            "date_of_birth":{
               "bsonType": "date",
               "description":"must be a date and its required"},}}}

   try:
      production.create_collection("author")
   except Exception as e:
      print(e)
   production.command("collMod","author", validator = author_validator)
#create_author_collection()

# 2. Bulk insert
def create_data():
   authors = [
      {"first_name":"Tim", "last_name": "Rusica", "date_of_birth": dt(2000, 7,3,0)},
      {"first_name":"George","last_name": "orwell","date_of_birth": dt(8800, 10, 5,0)},
      {"first_name":"Herman", "last_name": "Melville", "date_of_birth": dt(1500, 3,9,0)}, 
      {"first_name":"F. scott", "last_name": "Fitzgernald", "date_of_birth": dt(1900, 10,3,0)}
      ]
   author_collection = production.author
   authors = author_collection.insert_many(authors).inserted_ids
   #author_collection.insert_many(authors)
   books = [
      {"title":"Mongodb advanced tutorial","authors": [authors[0]],"publish_date": dt.today(), "type": "Non-Fiction", "copies": 5},
      {"title":"python for dummies", "authors": [authors[0]], "publish_date": dt(2022,1,17,0), "type": "Non-Fiction", "copies": 5},
      {"title":"Nineteen Eighty_four", "authors": [authors[1]], "publish_date": dt(8786, 6,8,0), "type": "Fiction", "copies": 5},
      {"title":"The great gatsby", "authors": [authors[3]], "publish_date": dt(2014,5,23,0), "type": "Fiction", "copies": 5},
      {"title":"Moby Dick", "authors": [authors[2]], "publish_date": dt(1941,5,3,0), "type": "Fiction", "copies": 5}
      ]
   book_collection = production.book
   #books = book_collection.insert_many(books).inserted_ids
   book_collection.insert_many(books)
#create_data()

# 3. Advanced queries
books_containing_a = production.book.find({"title":{"$regex": "a{1}"}})
#printer.pprint(list(books_containing_a))

#Authors
authors_and_books = production.author.aggregate([{
   "$lookup": {
      "from": "book",
      "localField":"_id",
      "foreignField":"authors",
      "as":"books"
   }
}])
#printer.pprint(list(authors_and_books)) 

# AUthors and their books counts
authors_and_count = production.author.aggregate([{
   "$lookup": {
      "from": "book",
      "localField":"_id",
      "foreignField":"authors",
      "as":"books"
   }},
   {
      "$addFields": {
         "total_books": {"$size": "$books"}
      }},
      {
         "$project": {"first_name": 1, "last_name": 1, "total_books": 1, "_id":0},
      }])
#printer.pprint(list(authors_and_count))

# Grabbing authors books at certain age.
books_with_low_authors = production.book.aggregate([{
   "$lookup": {
      "from": "author",
      "localField":"authors",
      "foreignField":"_id",
      "as":"authors"
   }},
   {
      "$set":{
         "authors":{
            "$map":{
               "input":"$authors",
               "in": {
                  "age":{
                     "$dateDiff": {
                        "startDate": "$$this.date_of_birth",
                        "endDate": "$$NOW",
                        "unit": "year"
                     }
                  },
                  "first_name" : "$$this.first_name",
                  "last_name" : "$$this.last_name",
               }
            }
         }
      }},
      {
         "$match":{
            "$and": [
               {"authors.age":{"$gte":50}},
               {"authors.age":{"$lte": 150}},
            ]
         }
      },
      {
         "$sort":{
            "age": 1
         }
      }])
#printer.pprint(list(books_with_low_authors))

# Pymongo Arrow Demo
import pyarrow
from pymongoarrow.api import Schema
from pymongoarrow.monkey import patch_all
from bson import ObjectId
import pandas
patch_all()
author = Schema({"_id":ObjectId, "first_name": pyarrow.string(),"last_name":pyarrow.string(), "date_of_birth": dt})
df = production.author.find_pandas_all({},schema=author)
#print(df)
arrow_table = production.author.find_arrow_all({}, schema=author)
#print(arrow_table)
ndarrays = production.author.find_numpy_all({}, schema=author)
#print(ndarrays)

