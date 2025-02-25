#what does this code do:
#OpenSearch – Fetching authors (KOLs) from stored PubMed articles.
#PubMed API – Getting KOL's affiliation, collaborators, and research data.
#OpenAI (GPT-4o) – Generating AI-enriched metadata about KOLs.
#OpenSearch – Storing the extracted and generated KOL details for future use.


import json
import requests
import time
import boto3
import os
import xml.etree.ElementTree as ET
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from openai import OpenAI
 
# AWS Credentials & OpenSearch Config
region = "us-east-1"
service = "es"
opensearch_host = os.environ['OPENSEARCH_HOST']  # Set this in Lambda Environment Variables
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
 
# OpenAI client initialization
client = OpenAI(
    base_url="https://models.inference.ai.azure.com",
    api_key=os.getenv("OPENAI_API_KEY"),
)
 
PUBMED_EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
MAX_RETRIES = 5
def fetch_with_retries(url, params, headers=None):
    """Handles 429 errors with exponential backoff."""
    for attempt in range(MAX_RETRIES):
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 429:
            wait_time = 2 ** attempt  # Exponential backoff
            time.sleep(wait_time)
        else:
            response.raise_for_status()
            return response
    return None
# Function to fetch publication details from OpenSearch
def fetch_opensearch_publication_details():
    query = {"query": {"match_all": {}}}
    response = opensearch.search(index="articles_index", body=query)
    return [{"authors": pub['_source'].get('authors', [])} for pub in response['hits']['hits']]
 
# Function to fetch KOL data from PubMed
 
def fetch_pubmed_affiliation_and_collaborators_and_research(kol_name):
    try:
        esearch_url = f"{PUBMED_EUTILS_BASE}esearch.fcgi"
        esearch_params = {
            "db": "pubmed",
            "term": f"{kol_name}[au]",
            "retmode": "json",
            "retmax": "10"  # Limit the number of articles to fetch
        }
        esearch_response = fetch_with_retries(esearch_url, esearch_params)
        if not esearch_response:
            return {"error": "PubMed request failed"}
        esearch_response.raise_for_status()
        esearch_data = esearch_response.json()
 
        article_ids = esearch_data.get("esearchresult", {}).get("idlist", [])
        if not article_ids:
            return {
                "affiliation": "Affiliation not found",
                "authors": [],
                "geographic_influence": [],
                "research": []
            }
 
        efetch_url = f"{PUBMED_EUTILS_BASE}efetch.fcgi"
        efetch_params = {
            "db": "pubmed",
            "id": ",".join(article_ids),
            "retmode": "xml",
            "rettype": "xml"
        }
        efetch_response = fetch_with_retries(efetch_url, efetch_params)
        efetch_response.raise_for_status()
 
        root = ET.fromstring(efetch_response.text)
 
        # primary affiliation
        affiliation_element = root.find(".//Affiliation")
        primary_affiliation = (
            affiliation_element.text if affiliation_element is not None else "Affiliation not found"
        )
 
        # authors and geographic influence
        authors = root.findall(".//Author")
        affiliations = root.findall(".//AffiliationInfo/Affiliation")
        authors_list = []
        geographic_influence = []
        research = []
 
        # Processing authors and their affiliations
        for author, affiliation in zip(authors, affiliations):
            last_name = author.find("LastName")
            fore_name = author.find("ForeName")
            if last_name is not None and fore_name is not None:
                full_name = f"{fore_name.text.strip()} {last_name.text.strip()}"
                authors_list.append(full_name)
                if affiliation is not None:
                    geographic_influence.append(affiliation.text.strip())
 
        #research articles (titles and publication years)
        articles = root.findall(".//PubmedArticle")
        for article in articles:
            title_element = article.find(".//ArticleTitle")
            year_element = article.find(".//PubDate/Year")
            title = title_element.text if title_element is not None else "Title not found"
            year = year_element.text if year_element is not None else "Year not found"
            research.append(f"title: {title} -- {year}")
 
        return {
            "affiliation": primary_affiliation,
            "authors": authors_list,
            "geographic_influence": geographic_influence,
            "research": research
        }
 
    except requests.RequestException as e:
        return {
            "affiliation": f"Error fetching PubMed affiliation: {str(e)}",
            "authors": [],
            "geographic_influence": [],
            "research": []
        }
# Function to fetch AI-generated metadata for KOLs
def fetch_ai_metadata(kol_name, primary_affiliation, geographic_influence, collaborators):
    prompt = f'''You are a data extraction assistant and also think as backend developer. Generate and retrieve this metadata for the Key Opinion Leader (KOL) of medical science "Dr. {kol_name}" with the following details:
    - Primary Affiliation: "{primary_affiliation}" geographic influence :"{geographic_influence}" collaborators :"{collaborators}" access information using this two information name and primary affiliation"
 
    Return a JSON object with:
    {{
        "full_name": "string (include salutations)",
        "gender": "string(predict by name)",
        "qualifications": ["array of strings"],
        "primary_affiliation": "string(remove electronic address if showing in primary affiliation otherwise fill this place with primary_affiliation)",
        "country": "string(get the country from primary_affiliation)",
        "department": "string",
        "title": "string(current title of it's position)",
        "email": "string(try to get this from primary_affiliation if available on it otherwise try another source)",
        "phone": "string(look into some institutional site for this or any medical reference site)",
        "fax": "string(look into some institutional site for this or any medical reference site)",
        "twitter": "url(url of their twitter handle if available)",
        "linkedin": "url(url of their linkdin handle if available)",
        "professional_summary": "string (300-500 words)",
        "education": ["array of strings"],
        "professional_history": ["array of strings"],
        "conferences_and_awards": ["array of strings"],
        "areas_of_interest": ["array of strings"],
        "collaborators":"[array of strings(just remove kol name from collaborators otherwise add all others from collaborators)"
        "Geographical_influence":"[array of strings(retrieve only location(like this format city,state,country)data from geographic_influence and also make sure there should be no duplicate data)]",
        "speaking_engagements":"[array of strings(participation in industry events(try this source such as clinicaltrials.gov,FirstWordPharma.com))]",
        "patient_advocacy":"[array of strings(Any involvement in patient advocacy by kol(try this source aacr.org,liverfoundation.org,novartis.com,accc-cancer.org and try some other source also for get this details))]"
    }}
    Guidelines:
    1. Use verified sources (institutional websites, PubMed, Google Scholar, ClinicalTrials.gov,wikipedia).
    2. Use "Not available" for missing fields.
    4. Follow strict JSON format.
    5. Escape special characters and social media links should be valid.
    '''
    try:
        response = client.chat.completions.create(
            messages=[{"role": "system", "content": "You are a helpful assistant generating structured JSON metadata."},
                      {"role": "user", "content": prompt}],
            model="gpt-4o",
            temperature=1,
            max_tokens=4000,
            top_p=1
        )
        content = response.choices[0].message.content
        json_str = content.replace('```json', '').replace('```', '').strip()
        return json.loads(json_str)
    except Exception as e:
        return {"error": str(e)}
 
# Function to store KOL details in OpenSearch
def store_kol_details(kol_metadata):
    try:
        opensearch.index(index="kol_details", body=kol_metadata)
        return True
    except Exception as e:
        return False
 
# Lambda handler
def lambda_handler(event, context):
    try:
        publication_details = fetch_opensearch_publication_details()
 
        kol_metadata_list = []
 
        for publication in publication_details:
            authors = publication["authors"]
            for kol_name in authors:
                pubmed_data = fetch_pubmed_affiliation_and_collaborators_and_research(kol_name)
                primary_affiliation = pubmed_data["affiliation"]
                collaborators = pubmed_data["authors"]
                geographic_influence = pubmed_data["geographic_influence"]
                research = pubmed_data["research"]
               
                if primary_affiliation == "Affiliation not found":
                    continue  # Skip if KOL data is incomplete
 
                metadata = fetch_ai_metadata(kol_name, primary_affiliation, geographic_influence, collaborators)
                metadata["research"] = research
 
                # Store the metadata into OpenSearch
                if store_kol_details(metadata):
                    kol_metadata_list.append(metadata)
 
        return {
            'statusCode': 200,
            'body': json.dumps(kol_metadata_list)
        }
 
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }
