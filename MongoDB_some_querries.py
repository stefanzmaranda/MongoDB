import os
import pprint

from datetime import datetime as dt
from bson import ObjectId

from dotenv import find_dotenv, load_dotenv
from pymongo import MongoClient


import pyarrow
from pymongoarrow.api import Schema
from pymongoarrow.monkey import patch_all
import pymongoarrow as pma

load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")


connection_string = f"mongodb+srv://zmstef1997:{password}@pythondb.gsgql.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(connection_string)


dbs = client.list_database_names()
production = client.production  #create data base Production

def create_book_colection():
    book_validator = {
        "$jsonSchema": {
            "bsonType":"object",
            "required":["title","authors","publish_date","type","copies"],
            "properties":{
                "title":{
                    "bsonType":"string",
                    'description':"must be a string and is required"
                },
                "authors":{
                    "bsonType":"objectId",
                    "description":"must be an objectid and is required"
                },
            "publish_date":{
                "bsonType":"date",
                "description":"must be a date and is required"
            },
            "type":{
                "enum":["Fiction","Non-Fiction"],
                "description": "can only be one of the enum values and is required"
            },
            "copies":{
                "bsonType":'int',
                "minimum":0,
                'description':'Must be a integer greater than 0 and is required'
            },
        }
    }
    }

    try:
        production.create_collection("book")
    except Exception as e:
        print(e)

    production.command("collMod","book",validator = book_validator)

def create_author_collection():
    author_validator = {
        "$jsonSchema": {
            "bsonType":"object",
            "required":["first_name","last_name","date_of_birth"],
            "properties":{
                "first_name":{
                    'bsonType':'string',
                    'description':'must be a string and is required'
                },
                "last_name":{
                    'bsonType':'string',
                    'description':'must be a string and is required'
                },
                "date_of_birth":{
                    'bsonType':'date',
                    'description':'must be a date and is required'
                }
            }
        }
    }
    try:
        production.create_collection("author")
    except Exception as e:
        print(e)

    production.command("collMod","author",validator = author_validator)


def create_data():
    authors = [
        {
            'first_name':'Jhon',
            'last_name':'Stick',
            'date_of_birth': dt(1945,3,15)
        },
        {
            'first_name':'George',
            'last_name':'Orwell',
            'date_of_birth': dt(1903,6,25)
        },
        {
            'first_name':'Tim',
            'last_name':'Pumba',
            'date_of_birth': dt(1819,8,1)
        },
        {
            'first_name':'F. Scott',
            'last_name':'Fitzgerald',
            'date_of_birth': dt(1896,9,24)
        },
        {
            'first_name':'Jemam',
            'last_name':'Coupa',
            'date_of_birth': dt(1832,6,25)
        },
        {
            'first_name':'Flawn Lott',
            'last_name':'Nazgul',
            'date_of_birth': dt(1930,6,25)
        },
        
    ]

    author_collection=production.author
    authors= author_collection.insert_many(authors).inserted_ids


    books = [
        {
            "title":'Childhood memories',
            'authors':[authors[0]],
            "publish_date":dt.today(),
            "type":"Fiction",
            "copies":24124
        },
        {
            "title":'Amsterdam',
            'authors':[authors[0]],
            "publish_date":dt.today(),
            "type":"Fiction",
            "copies":1
        },
        {
            "title":'Breakfast of Champions',
            'authors':[authors[1]],
            "publish_date":dt.today(),
            "type":"Fiction",
            "copies":424
        },
        {
            "title":'Lord of the Rings',
            'authors':[authors[3]],
            "publish_date":dt.today(),
            "type":"Fiction",
            "copies":313
        },
        {
            "title":'Norwegian Wood',
            'authors':[authors[2]],
            "publish_date":dt.today(),
            "type":"Fiction",
            "copies":22
        },
        
    ]

    book_colection = production.book
    book_colection.insert_many(books)


printer = pprint.PrettyPrinter()
books_containing_a = production.book.find({"title":{"$regex":"a{1}"}}) # find all the books that contain letter a in title
# for book in books_containing_a:
#     printer.pprint(book)

authors_and_books = production.author.aggregate([{             #create a join on books and authors
    "$lookup":{
        "from":"book",
        "localField":"_id",
        "foreignField":"authors",
        "as":"books"
    }
}])

#printer.pprint(list(authors_and_books))

authors_book_count = production.author.aggregate([
    {             
    "$lookup":{
        "from":"book",
        "localField":"_id",
        "foreignField":"authors",
        "as":"books"
        }
    },
    {
        "$addFields":{                                     #adding a new field for the books.count(book)
            "total_books":{"$size":"$books"}
        }
    },
    {
        "$project":{"first_name":1,"last_name":1,"total_books":1,"_id":0},
    }
])

#printer.pprint(list(authors_book_count))

authors_old_books = production.book.aggregate([
    {
        "$lookup":{
            "from":"author",
            "localField":"authors",
            "foreignField":"_id",
            "as":"authors"
        }
    },
    {
        "$set":{
            'authors':{
                "$map":{
                    "input":"$authors",
                    "in":{
                        "age":{
                            "$dateDiff":{
                                "startDate":"$$this.date_of_birth",
                                "endDate":"$$NOW",
                                "unit":"year"
 ,                           }
                        },
                        "first_name":"$$this.first_name",
                        "last_name":"$$this.last_name",
                    }
                }
            }
        }
    },
    {
        "$match":{
            "$and":[
                {"authors.age":{"$gte":50}},
                {"authors.age":{"$lte":150}},
            ]
        }
    },
    {
        "$sort":{
            "age":1
        }
    }
])

#printer.pprint(list(authors_old_books))


import pyarrow
from pymongoarrow.api import Schema
from pymongoarrow.monkey import patch_all
import pymongoarrow as pma

patch_all()

author  = Schema({"_id":ObjectId,'first_name':pyarrow.string(),'last_name':pyarrow.string(),'date_of_birth':dt})
df = production.author.find_pandas_all({},schema=author)

print(df.head())
