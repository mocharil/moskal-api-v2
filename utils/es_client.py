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

def get_elasticsearch_client(
            es_host=None,
        es_username=None,
        es_password=None,
        use_ssl=None,
        verify_certs=None,
        ca_certs=None
):
    """
    Create connection to Elasticsearch using environment variables
    
    Returns:
    --------
    Elasticsearch
        Elasticsearch client instance
    """
    # Get ES connection params from environment
    es_host = os.getenv('ES_HOST', 'localhost:9200')
    es_username = os.getenv('ES_USERNAME')
    es_password = os.getenv('ES_PASSWORD')
    use_ssl = os.getenv('USE_SSL', 'false').lower() == 'true'
    verify_certs = os.getenv('VERIFY_CERTS', 'false').lower() == 'true'
    ca_certs = os.getenv('CA_CERTS')
    
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

        return None
 