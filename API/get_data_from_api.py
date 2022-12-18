from pymongo import MongoClient
import json
import requests
import datetime


# constants
DATABASE_NAME = 'bda'
CONNECTION_STRING = "mongodb://34.118.42.39:27017/"
API_ADRESS = 'https://api.nextbike.net/maps/nextbike-live.json'
COLLECTION_NAME = 'bikes'

# list of need fields
country_list_names = ['name','country_name','booked_bikes','set_point_bikes','available_bikes']
cities_list_names = ['uid','name','available_bikes','num_places', 'booked_bikes','set_point_bikes']
places_list_names = ['uid','lat','lng','bikes_available_to_rent','bike_racks','free_racks','special_racks','bike_numbers','name','booked_bikes','bikes','free_special_racks']


# functions

def create_mong_client():
    client = MongoClient(CONNECTION_STRING, directConnection=True)
    return client


def return_collecion(client):
    # create database
    database = client[DATABASE_NAME]
    bikes = database[COLLECTION_NAME]
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


def get_cities_data(cities):
    cities_res = []
    for city in cities:
        city_entity = {x:city[x] for x in cities_list_names}            
        city_entity['places'] =[{x:place[x] for x in places_list_names} for place in city['places']]           
        cities_res.append(city_entity)
    return cities_res

def transform_response(response):
    final_list = []
    for country in response['countries']:
        entity = {x:country[x] for x in country_list_names}
        entity['cities'] = get_cities_data(country['cities'])
        final_list.append(entity)
    res = {'countries':final_list, 'timestamp':datetime.datetime.now()}
    return res

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


def get_data():
    '''main function'''
    client = create_mong_client()
    collection = return_collecion(client)
    response = get_API_response()
    transformed_response = transform_response(response)
    insert_document(transformed_response,collection)


if __name__ == "__main__":
    get_data()
