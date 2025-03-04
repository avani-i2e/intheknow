#this code is for ui guys to fetch the list of all kols and detail of 49 listed kols 

#https://tczyjmj1w7.execute-api.us-east-1.amazonaws.com/Stage2/opensearch-api?get_all_kols

#https://tczyjmj1w7.execute-api.us-east-1.amazonaws.com/Stage2/opensearch-api?get_kol_details=Alan%20Paul%20Venook

import json
import boto3
import os
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
 
# AWS Credentials & OpenSearch Config
region = "us-east-1"
service = "es"
opensearch_host = os.environ['OPENSEARCH_HOST']
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
 
# OpenSearch Client
opensearch = OpenSearch(
    hosts=[{'host': opensearch_host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)
 
def lambda_handler(event, context):
    query_params = event.get("queryStringParameters", {})
 
    print("Received Query Parameters:", query_params)
 
   
    if "get_all_kols" in query_params:
        return get_all_kols()
 
   
    elif "get_kol_details" in query_params:
        kol_name = query_params.get("get_kol_details")
        if not kol_name:
            return create_response(400, {"error": "Missing kol_name parameter"})
        return get_kol_details(kol_name)
 
   
    return create_response(400, {"error": "Invalid request. Use either 'get_all_kols' or 'get_kol_details'."})
 
def get_all_kols():
    try:
        query = {
            "query": {"match_all": {}},
            "_source": ["full_name", "title", "phone", "email", "country","image_url"],  
            "size": 100
        }
        response = opensearch.search(index="kol_details", body=query)
 
        print("DEBUG: OpenSearch Response:", json.dumps(response, indent=2))  
 
        kols = [
            {
                "full_name": doc['_source'].get('full_name', "Unknown"),
                "title": doc['_source'].get('title', "Not Available"),
                "phone": doc['_source'].get('phone', "Not Available"),
                "email": doc['_source'].get('email', "Not Available"),
                "country": doc['_source'].get('country', "Not Available"),
                "image_url": doc['_source'].get('image_url', "Not Available")
            }
            for doc in response.get('hits', {}).get('hits', [])
        ]
       
       
        return create_response(200, {"kols": json.loads(json.dumps(kols, ensure_ascii=False))})
 
    except Exception as e:
        return create_response(500, {"error": str(e)})
 
 
def get_kol_details(kol_name):
    try:
        query = {
            "query": {
                "match_phrase": {  
                    "full_name": kol_name
                }
            }
        }
        response = opensearch.search(index="kol_details", body=query)
        kol_details = response.get('hits', {}).get('hits', [])
 
        if not kol_details:
            return create_response(404, {"error": "KOL not found"})
 
       
        return create_response(200, json.loads(json.dumps(kol_details[0]['_source'], ensure_ascii=False)))
 
    except Exception as e:
        return create_response(500, {"error": str(e)})
 
 
def create_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS, GET",
            "Access-Control-Allow-Headers": "Content-Type"
        },
        "body": json.dumps(body, ensure_ascii=False)  
    }
