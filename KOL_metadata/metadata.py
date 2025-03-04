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
 
# OpenAI client initialization
client = OpenAI(
    base_url="https://models.inference.ai.azure.com",
    api_key=os.getenv("OPENAI_API_KEY"),
)
 
PUBMED_EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
MAX_RETRIES = 5
 
# List of authors provided by the user (unchanged)
AUTHORS_LIST = [
    "Aaron James Scott", "Adam Joel Bass", "Afsaneh Barzi", "Al Bowen Benson III", "Alan Paul Venook",
    "Alexander Illya Spira", "Alexis Diane Leal", "Allen Lee Cohn", "Amy Mimi Lin", "Andrea Cercek-Hanjis",
    "Anthony Frank Shields", "Anthony William Tolcher", "Anuradha Krishnamurthy", "Anwaar Mohammed Saeed",
    "Aparna Raj Parikh", "Aung Naing", "Autumn Jackson McRee", "Axel M Grothey", "Bassel Fouad El-Rayes",
    "Ben C George", "Benjamin Adam Weinberg", "Benjamin R Tan", "Benny C Johnson", "Bert Howard O'Neil",
    "Blase Nicholas Polite", "Brian Hemendra Ramnaraign", "Brian Matthew Wolpin", "Bruce Joseph Giantonio",
    "Bryan Andrew Faller", "Carmen Joseph Allegra", "Cathy Eng", "Charles David Blanke", "Charles Lawrence Loprinzi",
    "Charles Stewart Fuchs", "Chloe E Atreya", "Christian Frederick Meyer", "Christina Sing-Ying Wu",
    "Christine Lisa Parseghian", "Christine Marie Veenstra", "Christopher Hanyoung Lieu",
    "Crystal Shereen Denlinger", "Daniel George Haller", "Daniel H Ahn", "Daniel Virgil Thomas Catenacci",
    "David A Drew", "David Brain Solit", "David P Ryan", "David Sanghyun Hong", "David Shiao-Wen Hsu"
]
 
def fetch_with_retries(url, params, headers=None):
    """Handles 429 errors with exponential backoff."""
    for attempt in range(MAX_RETRIES):
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 429:
            wait_time = 2 ** attempt
            time.sleep(wait_time)
        else:
            response.raise_for_status()
            return response
    return None
 
google_api_key = os.environ['GOOGLE_API']
google_cse_id = os.environ['GOOGLE_CSE']
 
def fetch_kol_image(kol_name):
    """Fetch KOL image using Google Custom Search API."""
    try:
        search_url = f"https://www.googleapis.com/customsearch/v1?q={kol_name}&cx={google_cse_id}&searchType=image&key={google_api_key}&num=1"
        response = requests.get(search_url).json()
        image_url = response.get("items", [{}])[0].get("link", None)
        return image_url if image_url else "Not Available"
    except Exception as e:
        print("Error fetching image URL:", str(e))
        return "Not Available"
 
def fetch_pubmed_affiliation_and_collaborators_and_research(kol_name):
    try:
        esearch_url = f"{PUBMED_EUTILS_BASE}esearch.fcgi"
        esearch_params = {
            "db": "pubmed",
            "term": f"{kol_name}[au]",
            "retmode": "json",
            "retmax": "10"
        }
        esearch_response = fetch_with_retries(esearch_url, esearch_params)
        if not esearch_response:
            return {"error": "PubMed request failed"}
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
 
        affiliation_element = root.find(".//Affiliation")
        primary_affiliation = (
            affiliation_element.text if affiliation_element is not None else "Affiliation not found"
        )
 
        authors = root.findall(".//Author")
        affiliations = root.findall(".//AffiliationInfo/Affiliation")
        authors_list = []
        geographic_influence = []
        research = []
 
        for author, affiliation in zip(authors, affiliations):
            last_name = author.find("LastName")
            fore_name = author.find("ForeName")
            if last_name is not None and fore_name is not None:
                full_name = f"{fore_name.text.strip()} {last_name.text.strip()}"
                authors_list.append(full_name)
                if affiliation is not None:
                    geographic_influence.append(affiliation.text.strip())
 
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
 
def fetch_ai_metadata(kol_name, primary_affiliation, geographic_influence, collaborators):
    prompt = f'''You are a data extraction assistant with backend development expertise. Your task is to generate and retrieve metadata for the Key Opinion Leader (KOL) of medical science, "Dr. {kol_name}", using the following details:
 
    Primary Affiliation: "{primary_affiliation}"
    Geographic Influence: "{geographic_influence}"
    Collaborators: "{collaborators}"
    Access information using the name and primary affiliation of the KOL use much details to get all data possibly.
 
    Return a JSON object with the following structure:
    {{
        "full_name": "string",
        "gender": "string (predict by name)",
        "qualifications": ["array of strings"],
        "primary_affiliation": "string (remove electronic address if present, otherwise use only primary affiliation if available other wise use any other source to get this)",
        "country": "string (derive from primary affiliation, or use other sources if not available)",
        "department": "string",
        "title": "string (current title or position in single word)",
        "email": "string (try to retrieve from primary affiliation if available or check verified sources sites)",
        "phone": "string (check institutional or medical reference sites)",
        "fax": "string (check institutional or medical reference sites)",
        "twitter": "url (Twitter handle if available)",
        "linkedin": "url (LinkedIn profile if available)",
        "professional_summary": "string (300-500 words)",
        "education": ["array of strings"],
        "professional_history": ["array of strings"],
        "conferences_and_awards": ["array of strings"],
        "areas_of_interest": ["array of strings"],
        "collaborators": ["array of strings (exclude KOL name, include other collaborators)]",
        "geographical_influence": ["array of strings (only locations in the format 'city, state, country', no duplicates)]",
        "speaking_engagements": ["array of strings (industry events participations, sources like clinicaltrials.gov, FirstWordPharma.com)]",
        "patient_advocacy": ["array of strings (involvement in patient advocacy, sources like aacr.org, liverfoundation.org, novartis.com, accc-cancer.org, and others)]"
    }}
    Guidelines:
    1.Use verified sources (institutional websites, PubMed, Google Scholar, ClinicalTrials.gov, Wikipedia).
    2.Insert "Not available" for missing fields.
    3.Follow strict JSON format.
    4.Escape special characters and ensure social media links and contacts are valid.
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
        content = response.choices[0].message.content.strip()
        # Extract JSON from content (assuming model might wrap it in markdown or extra text)
        start_idx = content.find('{')
        end_idx = content.rfind('}') + 1
        if start_idx == -1 or end_idx == 0:
            print(f"Invalid JSON response for {kol_name}: {content}")
            return {"error": "Invalid JSON format from AI model"}
        json_str = content[start_idx:end_idx]
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        print(f"JSON parsing error for {kol_name}: {str(e)} - Raw content: {content}")
        return {"error": f"JSON parsing failed: {str(e)}"}
    except Exception as e:
        print(f"Error in AI metadata fetch for {kol_name}: {str(e)}")
        return {"error": str(e)}
 
def store_kol_details(kol_metadata):
    """Store KOL details in OpenSearch."""
    try:
        opensearch.index(index="kol_details", body=kol_metadata)
        return True
    except Exception as e:
        print(f"Error storing KOL details: {str(e)}")
        return False
 
def process_author_batch(author_batch):
    """Process a batch of authors and return their metadata."""
    kol_metadata_list = []
    for kol_name in author_batch:
        # Step 1: Try PubMed first
        pubmed_data = fetch_pubmed_affiliation_and_collaborators_and_research(kol_name)
        primary_affiliation = pubmed_data["affiliation"]
        collaborators = pubmed_data["authors"]
        geographic_influence = pubmed_data["geographic_influence"]
        research = pubmed_data["research"]
 
        # Step 2: If PubMed affiliation is empty or not found, fall back to AI model
        if primary_affiliation in ["Affiliation not found", f"Error fetching PubMed affiliation: {str(Exception)}"]:
            print(f"PubMed affiliation not found for {kol_name}, falling back to AI model")
            ai_metadata = fetch_ai_metadata(kol_name, "Not available", geographic_influence, collaborators)
            if "error" in ai_metadata:
                print(f"Skipping {kol_name} due to AI model failure: {ai_metadata['error']}")
                continue
            primary_affiliation = ai_metadata.get("primary_affiliation", "Not available")
            if primary_affiliation == "Not available":
                print(f"Skipping {kol_name} as AI model also failed to provide affiliation")
                continue
        else:
            # If PubMed worked, use its data and fetch additional metadata
            ai_metadata = fetch_ai_metadata(kol_name, primary_affiliation, geographic_influence, collaborators)
            if "error" in ai_metadata:
                print(f"AI metadata fetch failed for {kol_name}: {ai_metadata['error']}, using PubMed data only")
                ai_metadata = {
                    "full_name": f"Dr. {kol_name}",
                    "primary_affiliation": primary_affiliation,
                    "collaborators": collaborators,
                    "geographical_influence": geographic_influence
                }
 
        # Combine metadata
        metadata = ai_metadata
        metadata["image_url"] = fetch_kol_image(f"Dr.{kol_name}")
        metadata["research"] = research
 
        if store_kol_details(metadata):
            kol_metadata_list.append(metadata)
        else:
            print(f"Failed to store metadata for {kol_name}")
   
    return kol_metadata_list
 
# Lambda handler
def lambda_handler(event, context):
    try:
        batch_size = 10
        kol_metadata_list = []
       
        for i in range(0, len(AUTHORS_LIST), batch_size):
            author_batch = AUTHORS_LIST[i:i + batch_size]
            print(f"Processing batch: {author_batch}")
            batch_metadata = process_author_batch(author_batch)
            kol_metadata_list.extend(batch_metadata)
 
        return {
            'statusCode': 200,
            'body': json.dumps(kol_metadata_list)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }
