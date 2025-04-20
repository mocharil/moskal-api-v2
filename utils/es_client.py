"""
Elasticsearch Client Utilities

This module provides functions for establishing and managing
connections to Elasticsearch.
"""

from elasticsearch import Elasticsearch
import urllib3
import warnings
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Mematikan warning urllib3
urllib3.disable_warnings()
warnings.filterwarnings("ignore")

def  get_elasticsearch_client(
        es_host, es_username, es_password,
        use_ssl,verify_certs,ca_certs
    ):
    """
    Create connection to Elasticsearch using environment variables
    
    Returns:
    --------
    Elasticsearch
        Elasticsearch client instance
    """
    # Get ES connection params from environment
    es_host = os.getenv('ES_HOST', 'localhost:9200') if not es_host else es_host
    es_username = os.getenv('ES_USERNAME') if not es_username else es_username
    es_password = os.getenv('ES_PASSWORD') if not es_password else es_password
    use_ssl = os.getenv('USE_SSL', 'false').lower() == 'true' if not use_ssl else use_ssl
    verify_certs = os.getenv('VERIFY_CERTS', 'false').lower() == 'true' if not verify_certs else verify_certs
    ca_certs = os.getenv('CA_CERTS') if not ca_certs else ca_certs
    
    # Check if URL already has protocol
    if not es_host.startswith(('http://', 'https://')):
        # Add protocol based on use_ssl
        protocol = "https" if use_ssl else "http"
        es_host = f"{protocol}://{es_host}"
    
    # Initialize Elasticsearch connection with various configurations
    es_config = {
        "hosts": [es_host],
        "verify_certs": verify_certs,
        "ssl_show_warn": False,
    }
    
    # Add authentication if needed
    if es_username and es_password:
        es_config["basic_auth"] = (es_username, es_password)
    
    # Add CA certificates if provided
    if ca_certs:
        es_config["ca_certs"] = ca_certs
    
    # Create Elasticsearch instance
    try:
        es = Elasticsearch(**es_config)
        print(f"Successfully connected to {es_host}")
        return es
    except Exception as e:
        print(f"Connection error: {e}")
        print("\nTry these solutions:")
        print("1. Make sure Elasticsearch is running and accessible")
        print("2. Check protocol (http/https) and port")
        print("3. If using HTTPS:")
        print("   - set USE_SSL=true in .env")
        print("   - If using self-signed certificate, set VERIFY_CERTS=false in .env")
        print("4. Check ES_USERNAME and ES_PASSWORD in .env")
        print("5. Try direct connection without SSL: http://localhost:9200")
        return None
        print("\nTry these solutions:")
        print("1. Make sure Elasticsearch is running and accessible")
        print("2. Check protocol (http/https) and port")
        print("3. If using HTTPS:")
        print("   - set use_ssl=True")
        print("   - If using self-signed certificate, set verify_certs=False")
        print("4. Check username and password")
        print("5. Try direct connection without SSL: http://localhost:9200")
        return None