"""
Elasticsearch Analytics Utilities Package

This package provides utilities for connecting to Elasticsearch,
building queries, and fetching data for analytical purposes.
"""

from utils.es_client import get_elasticsearch_client
from utils.es_query_builder import (
    build_elasticsearch_query,
    get_indices_from_channels,
    get_date_range,
    add_time_series_aggregation,
    add_wordcloud_aggregation
)
from utils.es_data_fetcher import (
    fetch_elasticsearch_data,
    process_time_series_results,
    keyword_trends,
    context_of_discussion
)
from utils.text_processor import (
    preprocess_text,
    get_stopwords,
    get_indonesian_stopwords,
    get_english_stopwords
)

from utils.list_of_mentions import get_mentions

__all__ = [
    # Client
    'get_elasticsearch_client',
    
    # Query Builder
    'build_elasticsearch_query',
    'get_indices_from_channels',
    'get_date_range',
    'add_time_series_aggregation',
    'add_wordcloud_aggregation',
    
    # Data Fetcher
    'fetch_elasticsearch_data',
    'process_time_series_results',
    'keyword_trends',
    'context_of_discussion',
    
    # Text Processor
    'preprocess_text',
    'get_stopwords',
    'get_indonesian_stopwords',
    'get_english_stopwords',


    'get_mentions'
]