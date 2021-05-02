import pandas as pd
import numpy as np
import requests
import os
import json
import urllib
import datetime
from openpyxl import load_workbook


#twitter token 
def auth():
    token = 'TOKEN HERE'
    return token

#https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-recent
#creates a url with specific filters (in twitter API v2, not all fields are declared and must be manually added) 
def create_url(query):
    max_results = "max_results=100"
    tweet_fields = "tweet.fields=author_id,conversation_id,created_at,text,geo,entities"
    expansions = "expansions=author_id,geo.place_id"
    user_fields = "user.fields=location,username"
    place_fields = "place.fields=contained_within,country,country_code,full_name,geo,id,name,place_type"
    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}&{}&{}".format(
        query, max_results,tweet_fields, expansions, user_fields, place_fields
    )
    return url

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

#obtains the response in json format from the twitter API using the GET REST method 
def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    #print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

#creates json files using the json response (used mainly for inspection before manual cleaning further in the script)
def create_json_file(json_response, query):
    dateTimeObj = datetime.datetime.now()
    datestampStr = dateTimeObj.strftime("%d%b%Y")
    with open('json_files/Query_' + query + '_' + datestampStr +'.json', 'w') as json_file:
        json.dump(json_response, json_file, sort_keys = True, indent = 4)

#pass in a query and obtains the json response from Twitter API 
def get_tweets(query):
    bearer_token = auth()
    url = create_url(query)
    print (url)
    headers = create_headers(bearer_token)
    json_response = connect_to_endpoint(url, headers)
    create_json_file(json_response, query)
    #print (json.dumps(json_response, indent=4, sort_keys=True))
    return json_response 

#parses the json response into a dataframe, combines the data and users objects using the author's unique id
#does not flatten the json completely (the nested entities are not flattened, just stored in a column)
def json_data_parse(json_response):
    df_data = pd.io.json.json_normalize(json_response[u'data'])
    df_users = pd.io.json.json_normalize(json_response[u'includes'][u'users'])
    #in the users object, the 'author_id' is originally called 'id', renamed to avoid confusion with 'id' in the data object which represents the unique id for the tweet
    df_users = df_users.rename(columns={'id':'author_id'})
    df_joined = pd.merge(df_data, df_users, on='author_id',how='outer')
    return df_joined

#flattens the entities.annotations columns that is in json format and generates a dataframe from the flattened json 
def entity_generation(df_data):
    df_entity = pd.DataFrame(df_data, columns = ['id','entities.annotations'] )
    #drops all null values in annotations (not all tweets have annotations) to avoid issues with the json_normalize function
    df_entity = df_entity.dropna()
    #convert back into a json before generating a new dataframe wit the entities flattened 
    new_json = df_entity.to_json(orient='records')
    dictr = json.loads(new_json)
    df_entity = pd.io.json.json_normalize(dictr,'entities.annotations',['id'])
    return df_entity

def main():
    
    #LIST OF untility companies and internet companies we want to get tweets of 
    utilityCompany = ['TorontoHydro', 'HydroOne','OakvilleHydro', 'LondonHydro','hydroottawa', 'burlingtonHydro', 'GuelphHydro', 'Festival_Hydro']
    internetCompany = ['RogersHelps', 'Bell_Support','FidoSolutions','VMCcare']

    df_all_data = pd.DataFrame()
    df_all_entities = pd.DataFrame()

    #iterate through power utility list
    for company in utilityCompany:
        query = 'to:' + company + ' (power OR out OR #outage) '
        query = urllib.parse.quote_plus (query)
        json_response = get_tweets(query)
        #skip if the response has no tweets returned 
        if json_response['meta']['result_count'] != 0:
            #create the dataframe with all the columns obtained from the filtered GET response
            df_data = json_data_parse(json_response)
            #create the dataframe with only the entities annotations 
            df_entity = entity_generation(df_data)
            df_all_data = df_all_data.append(df_data)
            df_all_entities = df_all_entities.append(df_entity)


    #iterate through internet company list 
    for company in internetCompany:
        query = 'to:' + company + ' (disconnect OR wifi OR internet down OR wifi down) '
        query = urllib.parse.quote_plus (query)
        json_response = get_tweets(query)
        #skip if the response has no tweets returned 
        if json_response['meta']['result_count'] != 0:
            #create the dataframe with all the columns obtained from the filtered GET response
            df_data = json_data_parse(json_response)
            #create the dataframe with only the entities annotations 
            df_entity = entity_generation(df_data)
            df_all_data = df_all_data.append(df_data)
            df_all_entities = df_all_entities.append(df_entity)
    
    #rearrange the columns 
    df_all_data = df_all_data[['id','conversation_id','author_id', 'created_at', 'username', 'text', 'location']]
    df_all_entities = df_all_entities[['id', 'probability', 'type', 'normalized_text']]

    #clean dataframe - drop duplicates on id tweets
    df_all_data = df_all_data.drop_duplicates(subset=['id'])
    df_all_data = df_all_data.drop_duplicates(subset=['text'])

    #Converting from UTC (format of Twitter API) to EST timezone
    df_all_data['created_at'] = pd.to_datetime(df_all_data['created_at'],utc=True)
    df_all_data['created_at'] = df_all_data['created_at'].dt.tz_convert('America/New_York')
    df_all_data['created_at'] = df_all_data['created_at'].dt.strftime('%m/%d/%Y %H:%M:%S')

    # create excel sheet with current datestamp
    dateTimeObj = datetime.datetime.now()
    datestampStr = dateTimeObj.strftime("%d%b%Y")
    writer = pd.ExcelWriter('RecentSearchGrab_'+ datestampStr + '.xlsx') # pylint: disable=abstract-class-instantiated
    # write dataframes to excel sheet 
    df_all_data.to_excel(writer, 'OutageTweets', index=False)
    df_all_entities.to_excel(writer,'EntityAnnotations', index=False)
    # save the excel file
    writer.save()

if __name__ == "__main__":
    main()