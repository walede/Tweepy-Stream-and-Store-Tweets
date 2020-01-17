from elasticsearch import Elasticsearch
import requests
import json

def connect_elasticsearch():
    """Connects to the elasticsearch server 
       and returns the ES object.
    """
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Connected!')
    else:
        print('Could not connect to Elasticsearch server')
    return _es

def create_index(es_object, index_name, set_params):
    """Create an index with the passed in schema.
       Returns boolean if created or not.
       ----------------------------
       arg1:ES object
       arg2: index name
       arg3: the settings for the index
    """
    created = False
    # index settings
    settings = set_params
    try:
        if not es_object.indices.exists(index_name):
            es_object.indices.create(index=index_name, body=settings)
            print(f'Created Index "{index_name}"')
            created = True
        else: print('Index already exists.')
    except Exception as ex:
        print(str(ex))
    finally:
        return created

def insert_into_index(es_object, index_name, record):
    """Inserts the record into the index with index_name.
       ----------------------------
       arg1:ES object
       arg2: index name
       arg3: the record
    """    
    try:
        outcome = es_object.index(index=index_name, body=record)
        print(f"Inserted object into Index '{index_name}'.")
        print(str(record))
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))

def search(es_object, index_name, search_query):
    '''search_object = {'query': {'match': {'calories': '102'}}}'''
    res = es_object.search(index=index_name, body=search_query)
    return res

def delete_index(es_object, index_name):
    """Deletes the index of index name.
       ----------------------------
       arg1:ES object
       arg2: index name
    """
    try:
        es_object.indices.delete(index_name)
        print(f"Deleted Index '{index_name}'.")
    except Exception as ex:
        print(f"Index '{index_name}' DNE.")
        pass
