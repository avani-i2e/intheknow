# this code fetches data from pubmed then the articles are processed in comprehend to find sentiment and entities

import json
import boto3
import os
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# AWS Configurations
region = "us-east-1"
service = "es"
opensearch_host = os.environ['OPENSEARCH_HOST']
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

s3 = boto3.client('s3')
comprehend = boto3.client('comprehend')

# OpenSearch Client
opensearch = OpenSearch(
    hosts=[{'host': opensearch_host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def extract_entities(text):
    if not text:
        return {"Entities": []}

    response = comprehend.detect_entities(Text=text, LanguageCode="en")
    entities = {"Entities": []}

    for entity in response.get("Entities", []):
        mention_sentiment = comprehend.detect_sentiment(Text=entity["Text"], LanguageCode="en")
        
        entity_obj = {
            "DescriptiveMentionIndex": [entity.get("BeginOffset", 0)],
            "Mentions": [
                {
                    "Score": entity.get("Score", 0),
                    "GroupScore": 1,  # Assuming GroupScore as 1 by default
                    "Text": entity.get("Text"),
                    "Type": entity.get("Type"),
                    "MentionSentiment": {
                        "Sentiment": mention_sentiment["Sentiment"],
                        "SentimentScore": mention_sentiment["SentimentScore"],
                    },
                    "BeginOffset": entity.get("BeginOffset", 0),
                    "EndOffset": entity.get("EndOffset", 0),
                }
            ]
        }
        entities["Entities"].append(entity_obj)

    return entities


def lambda_handler(event, context):
    bucket_name = "intheknow-25"
    folder_prefix = "pubmed_articles/"

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

    if "Contents" not in response:
        return {"message": "No files found in S3 folder."}

    opensearch_responses = []
    
    for file in response["Contents"]:
        file_key = file["Key"]
        if file_key.endswith("/"):
            continue  # Skip folders

        print(f"Processing file: {file_key}")

        # Read File
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        article_data = json.loads(obj["Body"].read().decode("utf-8"))
        article_text = article_data.get("article_text", "")
        article_summary = article_data.get("article_summary", "")

        if not article_text:
            continue  # Skip empty articles

        # Sentiment Analysis
        sentiment_response = comprehend.detect_sentiment(Text=article_text, LanguageCode="en")
        sentiment_data = {
            "sentiment": sentiment_response["Sentiment"],
            "positive_sentiment": sentiment_response["SentimentScore"]["Positive"],
            "negative_sentiment": sentiment_response["SentimentScore"]["Negative"],
            "neutral_sentiment": sentiment_response["SentimentScore"]["Neutral"],
            "mixed_sentiment": sentiment_response["SentimentScore"]["Mixed"],
        }

        # Validate time_date field
        time_date = article_data.get("time_date", "")
        if time_date in ["N/A", "", None]:
            time_date = None  # Remove invalid dates

        # Extract Entities
        article_text_entities = extract_entities(article_text)
        article_summary_entities = extract_entities(article_summary)

        # Prepare document
        doc = {
            "article_id": file_key.split("/")[-1].split(".")[0],
            "article_title": article_data.get("article_title"),
            "web_article_url": article_data.get("web_article_url"),
            "authors": article_data.get("authors"),
            "article_type": article_data.get("article_type"),
            "time_date": time_date,  # Only store valid dates
            "status": article_data.get("status"),
            "article_text": article_text,
            "article_summary": article_summary,
            "article_category": article_data.get("article_category"),
            "keywords": article_data.get("keywords"),
            **sentiment_data,
            "article_text_entities": article_text_entities,
            "article_summary_entities": article_summary_entities,
        }

        # Index document in OpenSearch
        index_name = "pubmed-articles"
        os_response = opensearch.index(index=index_name, body=doc, id=doc["article_id"])
        opensearch_responses.append(os_response)
    
    return {"message": "Processing completed successfully."}



#env variable :
# OPENSEARCH_HOST
