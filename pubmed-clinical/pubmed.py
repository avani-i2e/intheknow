#bhavya's pubmed code

# it fetches the articles and stores them in the s3 bucket


import json
import requests
import boto3
import os
import xml.etree.ElementTree as ET
from datetime import datetime
import re
 
# AWS S3 Configuration
S3_BUCKET_NAME = "intheknow-25"
s3_client = boto3.client("s3")
 
def fetch_pubmed_articles(therapeutic_area, start_date, end_date, max_studies):
    """Fetch article IDs from PubMed based on search criteria."""
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {
        "db": "pubmed",
        "term": therapeutic_area,
        "retmax": max_studies,
        "datetype": "pdat",
        "mindate": start_date,
        "maxdate": end_date,
        "retmode": "json"
    }
 
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        raise Exception(f"Error fetching data from PubMed API: {response.text}")
 
    data = response.json()
    return list(set(data.get("esearchresult", {}).get("idlist", [])))
 
def fetch_articles_metadata(article_ids):
    """Fetch metadata for multiple articles in one request."""
    if not article_ids:
        return {}
 
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    params = {
        "db": "pubmed",
        "id": ",".join(article_ids),
        "retmode": "json"
    }
 
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        return {}
 
    return response.json().get("result", {})
 
def format_date(date_str):
    """Ensure the date is in 'YYYY-MM-DD' format."""
    if not date_str or date_str == "N/A":
        return "N/A"
 
    formats = [
        "%Y-%m-%d", "%Y/%m/%d", "%Y %b %d", "%b %d, %Y",
        "%Y %B %d", "%Y-%b-%d", "%Y %b", "%Y-%m"
    ]
 
    for fmt in formats:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            continue
 
    match = re.match(r"^(\d{4}) (\w{3,})$", date_str)
    if match:
        year, month = match.groups()
        try:
            date_obj = datetime.strptime(f"{year} {month} 01", "%Y %b %d")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            return f"{year}-01-01"
 
    return "N/A"
 
def fetch_article_details(article_ids):
    """Fetch full article details from PubMed, including authors and keywords."""
    if not article_ids:
        return {}
 
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        "db": "pubmed",
        "id": ",".join(article_ids),
        "retmode": "xml"
    }
 
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        return {}
 
    details = {}
    response_content = response.content.decode("utf-8", errors="replace")
    root = ET.fromstring(response_content)
 
    for article in root.findall(".//PubmedArticle"):
        article_id = article.find(".//PMID").text if article.find(".//PMID") is not None else "N/A"
        article_text = article.find(".//AbstractText").text if article.find(".//AbstractText") is not None else "N/A"
        pub_date_element = article.find(".//PubDate")
        pub_date = " ".join(pub_date_element.itertext()).strip() if pub_date_element is not None else "N/A"
 
        authors = []
        for author in article.findall(".//Author"):
            last_name = author.find(".//LastName").text if author.find(".//LastName") is not None else ""
            fore_name = author.find(".//ForeName").text if author.find(".//ForeName") is not None else ""
            initials = author.find(".//Initials").text if author.find(".//Initials") is not None else ""
 
            full_name = f"{fore_name} {initials} {last_name}".strip()
            authors.append(full_name)
 
        keywords = []
        for keyword in article.findall(".//Keyword"):
            keyword_text = keyword.text.strip() if keyword.text else ""
            if keyword_text:
                keywords.append(keyword_text)
 
        details[article_id] = {
            "article_text": article_text,
            "pub_date": format_date(pub_date),
            "authors": authors,
            "keywords": keywords
        }
 
    return details
 
def upload_to_s3(file_name, data):
    """Uploads JSON data to S3 bucket."""
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_name,
            Body=json.dumps(data, indent=4, ensure_ascii=False),
            ContentType="application/json"
        )
        print(f" File uploaded to S3: {file_name}")
    except Exception as e:
        print(f" Error uploading {file_name} to S3: {str(e)}")
 
def lambda_handler(event, context):
    """AWS Lambda function to fetch PubMed articles and store in S3."""
    try:
        therapeutic_area = event["therapeutic_area"]
        start_date = event["start_date"]
        end_date = event["end_date"]
        max_studies = int(event["max_studies"])
 
        article_ids = fetch_pubmed_articles(therapeutic_area, start_date, end_date, max_studies)
        articles_metadata = fetch_articles_metadata(article_ids)
        detailed_info = fetch_article_details(article_ids)
 
        for article_id in article_ids:
            article_data = {
                "article_id": article_id,
                "article_title": articles_metadata.get(article_id, {}).get("title", "N/A"),
                "web_article_url": f"https://pubmed.ncbi.nlm.nih.gov/{article_id}/",
                "authors": detailed_info.get(article_id, {}).get("authors", []),
                "article_type": "Pubmed",
                "time_date": detailed_info.get(article_id, {}).get("pub_date", "N/A"),
                "status": "published",
                "article_text": detailed_info.get(article_id, {}).get("article_text", "N/A"),
                "article_summary": articles_metadata.get(article_id, {}).get("title", "N/A"),
                "article_category": articles_metadata.get(article_id, {}).get("source", "N/A"),
                "keywords": detailed_info.get(article_id, {}).get("keywords", [])
            }
 
            file_name = f"pubmed_articles/{article_id}.json"
            upload_to_s3(file_name, article_data)
 
        return {"statusCode": 200, "body": json.dumps({"message": "Articles successfully saved to S3"})}
 
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
 
