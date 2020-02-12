import pandas as pd
from elasticsearch import Elasticsearch, helpers
import json

#location of the json file
file_loc = "..\\files\\netflix_shows.csv"

index_name = "series"
type_name = "serie"

# index data to elasticsearch with default params
def index_data(data, index_name = index_name, type_name = type_name):
    jsonArr = json.loads(data)
    for index in jsonArr:
        es.index(index=index_name, doc_type=type_name, id=index, body=jsonArr[index])
        print("Data in index ", index, " indexed!")

#get all data from elasticsearch index and type are optional
def get_data(index_name=index_name, type_name=type_name):
    es_response = helpers.scan(
        es,
        index=index_name,
        doc_type=type_name,
        query={"query": {"match_all": {}}})

    for item in es_response:
        print(json.dumps(item))

#connect to elastisearch
es = Elasticsearch(['localhost'], port=9200)

#chek connection
if not es.ping():
    raise ValueError("Cannot connect to elastic search server!")
else:

    #read the csv file, use ISO-8859-1 or latin-1 encoding and set the first rows as headers
    df = pd.read_csv(file_loc, sep=",", encoding='ISO-8859-1', header=0)

    #convert json file with index
    df_json = df.to_json(orient='index')

    index_data(df_json)

    #get_data()
