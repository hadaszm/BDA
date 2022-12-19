from pymongo import MongoClient
import json
import requests

# constants
DATABASE_NAME = 'bda'
HOST = 'localhost'
PORT = 27017
USERNAME = 'mongoadmin'
PASSWORD = 'pass1234'
API_ADRESS = 'https://api.nextbike.net/maps/nextbike-live.json'


# functions

def create_mong_client():
    client = MongoClient(HOST, PORT, username= USERNAME, password=PASSWORD)
    return client


def return_collecion(client):
    # create database
    database = client[DATABASE_NAME]
    bikes = database.bikes
    return bikes

def get_API_response():
    '''Upload data from API, if status code 200 resturn its data'''
    try:
        response = requests.get(API_ADRESS)
        if response.status_code != 200:
            raise Exception(f"Response from API {response.status_code}")
        return response.json()
    except Exception as e:
        print(e)

def insert_document(documentToInsert,collection):
    '''Insert document to mongo, checks if one document was inserted'''
    try:
        number_before_insert = collection.count_documents({})
        collection.insert_one(documentToInsert)
        number_after_insert = collection.count_documents({})
        if number_after_insert - number_before_insert != 1:
            raise Exception("Exception while inserting into mongo")
    except Exception as e:
        print(e)


def get_data(event):
    '''main function'''
    client = create_mong_client()
    collection = return_collecion(client)
    response = get_API_response()
    insert_document(response,collection)
    return 'Data fetched from Nextbixe and saved.'
    

if __name__ == "__main__":
    get_data()