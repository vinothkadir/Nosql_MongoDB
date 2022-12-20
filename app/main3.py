from dotenv import load_dotenv, find_dotenv
import os
import pprint
import json
from pprint import PrettyPrinter
from pymongo import MongoClient
load_dotenv(find_dotenv())

# 1. Connect and create DB
password = os.environ.get("mongodb_pwd")
connection_string = f"mongodb+srv://mongodb:{password}@projectmongodatabase.i8h8ics.mongodb.net/?retryWrites=true&w=majority" 
client = MongoClient(connection_string)
jeoprady_db = client.jeoprady_db
question = jeoprady_db.question
printer = PrettyPrinter()

# 2 & 3. Search results matching & Fuzzy matching
def fuzzy_matching():
   result = question.aggregate([{
      "$search":{
         "index": "language_search",
         "text": {   
            "query":"computer",
            "path":"category",
            "fuzzy":{}
      }
   }
   }])
   printer.pprint(list(result))
#fuzzy_matching()

# 4. synonyms matching
def fuzzy_synonym_matching():
   result = question.aggregate([{
      "$search":{
         "index": "language_search",
         "text": {   
            "query" : "computer",
            "path" : "category",
            "synonyms" : "mapping"
      }
   }
   }])
   printer.pprint(list(result))
#fuzzy_synonym_matching()

# 5. Auto-complete
def auto_complete():
   result = question.aggregate([{
      "$search": {
         "index": "language_search",
         "autocomplete": {
            "query":"programmer",
            "path":"question",
            "tokenOrder":"sequential",
            "fuzzy":{}
         }
      }},
      {
         "$project":{
            "_id":0,
            "question": 1
         }
      }
   ])
   printer.pprint(list(result))
#auto_complete()

#6. compound queries
def compound_queries():
   result = question.aggregate([
      {
         "$search": {
            "index":"language_search",
            "compound":{
               "must":[
                  {
                     "text":{
                        "query": ["computer","coding"], 
                        "path":"category"
                     }
                  }
               ],
               "mustNot": [{
                  "text": {
                     "query":"codes",
                     "path":"category"
                  }
               }],
               "should":[
                  {
                     "text": {
                        "query":"application",
                        "path":"answer"
                     }
                  }
               ]
            }
         }},
         {
            "$project": {
               "question": 1,
               "answer": 1,
               "category": 1,
               "score": {"$meta": "searchScore"}
            }
         }])
   printer.pprint(list(result))
#compound_queries()

# Relevancy search
def relevance():
   result = question.aggregate([
      {
         "$search": {
            "index":"language_search",
            "compound":{
               "must":[
                  {
                     "text":{
                        "query": "geography", 
                        "path":"category"
                     }
                  },
               ],
               "should": [{
                  "text": {
                     "query":"Final Jeopardy",
                     "path":"round",
                     "score": {"boost":{"value":3.0}}
                  }
               },
               {
                  "text": {
                     "query": "Double Jeopardy",
                     "path":"round",
                     "score": {"boost":{"value":2.0}}
                  }
               }],
            }
         }},
         {
            "$project": {
               "question": 1,
               "answer": 1,
               "category": 1,
               "score": {"$meta": "searchScore"}
            }
         },{
            "$limit":10
         }])
   printer.pprint(list(result))
relevance()