from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch, helpers
import requests
import json
from urllib.request import urlopen

es = Elasticsearch(['localhost'], port=9200)
url = "https://www.allrecipes.com/recipes/96/salad/"
index_name = "recipes"
type_name = "recipe"

#get all data from elasticsearch index and type are optional
def get_data(index_name=index_name, type_name=type_name):
    es_response = helpers.scan(
        es,
        index=index_name,
        doc_type=type_name,
        query={"query": {"match_all": {}}})

    for item in es_response:
        print(json.dumps(item))


def contains_word_title(word="Awesome"):
    res = helpers.scan(
        es,
        index=index_name,
        doc_type=type_name,
        query={"query": {"match": {"title": "*"+word+"*"}}})

    for hit in res:
        print(hit["_source"])


def start_with_character(charac="s"):
    res = helpers.scan(
        es,
        index=index_name,
        doc_type=type_name,
        query={"query": {"match_phrase_prefix": {"title": charac+"*"}}})

    for hit in res:
        print(hit["_source"])

#get calories
def get_calories_val(calorie_val = 303):
    res = helpers.scan(
        es,
        index=index_name,
        doc_type=type_name,
        query={"query": {"match": {"num_calories": calorie_val}}})

    for hit in res:
        print(hit["_source"])

#function to check connection
def check_connection_es():
    if not es.ping():
        raise ValueError("Vous n’êtes pas connecté")
    else:
        print("Vous êtes connecté!")
    return es

# index data to elasticsearch with default params
def index_data(data, index_name = index_name, type_name = type_name):
    i = 0
    isInserted = False

    for index in data:
        es.index(index=index_name, doc_type=type_name, id=i, body=index)
        print("Data in index ", i, " inserted!")
        i += 1
        isInserted = True

    if isInserted:
        print("data indexed successfully !")
    else:
        print("Error inserting data")

#function to create index and type
def create_index(index_name = index_name):
    # ignore 400 cause by IndexAlreadyExistsException when creating an index
    es.index(index=index_name, ignore=400)
    if es.indices.exists(index=index_name):
        print("index created!!")

#get the calories
def get_calories(soup_page_info):
    calories = soup_page_info.find("span", attrs={"itemprop": "calories"})
    if calories:
        calories = calories.text
        if calories == " calories;":
            calories = "0 calories;"
    else:
        cl = soup_page_info.find("div", attrs={"class": "nutrition-top"}).text.strip()

        #get the colories and remove the spaces
        cl = cl.split("Calories:", 1)[1].strip()
        calories = cl+" calories;"

    #get just the number and return val
    return int("".join(filter(str.isdigit, calories)))

def get_list_ingredients(soup):
    ingredient_list = []
    divs_ingredients = soup.find_all("div", attrs={"id": "polaris-app"})
    if divs_ingredients:
        for ingredient in divs_ingredients:
            for label in ingredient.find_all("span", attrs={"class": "recipe-ingred_txt"}):
                # appends the list of ingredients
                ingredient_list.append(label.text.strip())

        # remove last 2 ingredients "Add all ingredients to list"
        ingredient_list = ingredient_list[:len(ingredient_list) - 3]
    else:
        span_ingredients = soup.find_all("span", attrs={"class": "ingredients-item-name"})
        for span_name in span_ingredients:
            ingredient_list.append(span_name.text.strip())

    return ingredient_list


def get_receip_info(div_recepies):
    list_recepies = []

    for div in div_recepies:
        title = div.find("span", attrs={'class': 'fixed-recipe-card__title-link'}).text.strip()
        description = div.find("div", attrs={'class': 'fixed-recipe-card__description'}).text.strip()
        href = div.find("a", attrs={'class': 'fixed-recipe-card__title-link'}).get("href")
        info = div.find("ul", attrs={'class': 'cook-submitter-info'}).text.strip()
        # get just the name
        submitter_name = info[3:]

        page_info = requests.get(href).text
        soup_page_info = BeautifulSoup(page_info, 'lxml')

        # get the list of ingredients
        list_ingredients = get_list_ingredients(soup_page_info)

        # get the calories
        num_calories = get_calories(soup_page_info)

        dict_recepies = {}
        dict_recepies.update({"num_calories": num_calories})
        dict_recepies.update({"submitter_name": submitter_name})
        dict_recepies.update({"title": title})
        dict_recepies.update({"description": description})
        dict_recepies.update({"list_ingredients": list_ingredients})
        #appends dict to list
        list_recepies.append(dict_recepies)
    return list_recepies



try:
    #check if ther's internet connection
    urlopen(url)
    #print("yes connected to the internet!")

    # Make a GET request to fetch the raw HTML content
    html_content = requests.get(url).text

    # Parse the html content
    soup = BeautifulSoup(html_content, 'html.parser')

    div_recipes = soup.find_all("div", attrs={"class": "fixed-recipe-card__info"})

    list_recipes = get_receip_info(div_recipes)
    check_connection_es()
    index_data(list_recipes)

    #querying the  data

    #get_data()
    #get_calories_val()
    #contains_word_title()
    #start_with_character()

except:
    print("No internet connect, cannot get data")
