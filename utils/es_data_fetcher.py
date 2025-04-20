"""
es_data_fetcher.py
Elasticsearch Data Fetcher Utilities

This module provides functions for fetching and processing data from Elasticsearch.
"""

from .es_client import get_elasticsearch_client
from .es_query_builder import (
    get_indices_from_channels,
    get_date_range,
    build_elasticsearch_query,
    add_time_series_aggregation
)
from .text_processor import preprocess_text, get_stopwords

def fetch_elasticsearch_data(
    es,
    indices,
    query,
    size=10000,
    scroll="2m"
):
    """
    Fetch data from Elasticsearch with pagination (scroll)
    
    Parameters:
    -----------
    es : Elasticsearch
        Elasticsearch client
    indices : str
        Comma-separated list of Elasticsearch indices
    query : dict
        Elasticsearch query
    size : int, optional
        Number of documents per batch
    scroll : str, optional
        Scroll time (example: "2m" = 2 minutes)
        
    Returns:
    --------
    list
        List of Elasticsearch documents
    """
    try:
        # Set size in query
        query_with_size = query.copy()
        query_with_size["size"] = size
        
        # Initialize scroll
        resp = es.search(
            index=indices,
            body=query_with_size,
            scroll=scroll
        )
        
        # Get scroll_id
        scroll_id = resp["_scroll_id"]
        hits = resp["hits"]["hits"]
        total_docs = resp["hits"]["total"]["value"]
        
        print(f"Total documents found: {total_docs}")
        
        # Get all documents
        documents = [hit["_source"] for hit in hits]
        
        # Continue scrolling
        while len(hits) > 0:
            # Get next batch
            resp = es.scroll(
                scroll_id=scroll_id,
                scroll=scroll
            )
            
            # Get hits and scroll_id
            scroll_id = resp["_scroll_id"]
            hits = resp["hits"]["hits"]
            
            # Add hits to documents
            documents.extend([hit["_source"] for hit in hits])
            
            print(f"Retrieved {len(documents)} of {total_docs} documents", end="\r")
        
        print(f"\nFinished retrieving {len(documents)} documents")
        return documents
        
    except Exception as e:
        print(f"Error fetching data from Elasticsearch: {e}")
        return []

def process_time_series_results(response):
    """
    Process time series aggregation results
    
    Parameters:
    -----------
    response : dict
        Elasticsearch response containing time_series aggregation
        
    Returns:
    --------
    list
        List of processed time series data
    """
    results = []
    
    try:
        # Process aggregation results
        for bucket in response['aggregations']['time_series']['buckets']:
            date = bucket['key_as_string']
            total_mentions = bucket['doc_count']
            total_reach = round(bucket['total_reach']['value'], 3)
            
            # Initialize sentiment counts
            total_positive = 0
            total_negative = 0
            total_neutral = 0
            
            # Count sentiments
            for sentiment in bucket['sentiment_breakdown']['buckets']:
                if sentiment['key'].lower() == 'positive':
                    total_positive = sentiment['doc_count']
                elif sentiment['key'].lower() == 'negative':
                    total_negative = sentiment['doc_count']
                elif sentiment['key'].lower() == 'neutral':
                    total_neutral = sentiment['doc_count']
            
            # Format date to match desired output
            formatted_date = f"{date} 00:00:00"
            
            # Create result object
            result = {
                'post_date': formatted_date,
                'total_mentions': total_mentions,
                'total_reach': total_reach,
                'total_positive': total_positive,
                'total_negative': total_negative,
                'total_neutral': total_neutral
            }
            
            results.append(result)
            
        return results
    except KeyError as e:
        print(f"Error processing time series results: {e}")
        print("Make sure the response contains 'time_series' aggregation")
        return []

def keyword_trends(
    es_host=None,
    es_username=None,
    es_password=None,
    use_ssl=False,
    verify_certs=False,
    ca_certs=None,
    keywords=None,
    sentiment=None,
    date_filter="last 30 days",
    custom_start_date=None,
    custom_end_date=None,
    channels=None,
    importance="all mentions",
    influence_score_min=None,
    influence_score_max=None,
    region=None,
    language=None,
    domain=None
):
    """
    Get social media metrics from Elasticsearch
    
    Parameters:
    -----------
    es_host : str
        Elasticsearch host
    es_username : str, optional
        Elasticsearch username
    es_password : str, optional
        Elasticsearch password
    use_ssl : bool, optional
        Use SSL for connection
    verify_certs : bool, optional
        Verify SSL certificates
    ca_certs : str, optional
        Path to CA certificates
    keywords : list, optional
        List of keywords to filter
    sentiment : list, optional
        List of sentiments ['positive', 'negative', 'neutral']
    date_filter : str, optional
        Date filter type
    custom_start_date : str, optional
        Custom start date in YYYY-MM-DD format
    custom_end_date : str, optional
        Custom end date in YYYY-MM-DD format
    channels : list, optional
        List of channels ['twitter', 'news', 'instagram', etc.]
    importance : str, optional
        'important mentions' or 'all mentions'
    influence_score_min : float, optional
        Minimum influence score (0-100)
    influence_score_max : float, optional
        Maximum influence score (0-100)
    region : list, optional
        List of regions
    language : list, optional
        List of languages
    domain : list, optional
        List of domains to filter
        
    Returns:
    --------
    list
        List of social media metrics by date
    """
    # Create Elasticsearch client
    es = get_elasticsearch_client(
        es_host=es_host,
        es_username=es_username,
        es_password=es_password,
        use_ssl=use_ssl,
        verify_certs=verify_certs,
        ca_certs=ca_certs
    )
    
    if not es:
        return []
    
    # Get indices from channels
    indices = get_indices_from_channels(channels)
    
    if not indices:
        print("Error: No valid indices")
        return []
    
    # Get date range
    start_date, end_date = get_date_range(
        date_filter=date_filter,
        custom_start_date=custom_start_date,
        custom_end_date=custom_end_date
    )
    
    # Build query
    query = build_elasticsearch_query(
        keywords=keywords,
        sentiment=sentiment,
        start_date=start_date,
        end_date=end_date,
        importance=importance,
        influence_score_min=influence_score_min,
        influence_score_max=influence_score_max,
        region=region,
        language=language,
        domain=domain,
        size=0
    )
    
    # Add time series aggregation
    query = add_time_series_aggregation(query)
    
    # Execute query
    try:
        response = es.search(
            index=",".join(indices),
            body=query
        )
        
        # Process results
        return process_time_series_results(response)
        
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return []

def context_of_discussion(
    es_host=None,
    es_username=None,
    es_password=None,
    use_ssl=False,
    verify_certs=False,
    ca_certs=None,
    keywords=None,
    sentiment=None,
    start_date=None,
    end_date=None,
    channels=None,
    importance="all mentions",
    influence_score_min=None,
    influence_score_max=None,
    region=None,
    language=None,
    domain=None,
    max_words=50,
    custom_stopwords=None,
    min_word_length=3,
    caption_field="post_caption",
    sentiment_field="sentiment"
):
    """
    Generate wordcloud data from Elasticsearch with dominant sentiment
    
    Parameters:
    -----------
    es_host : str
        Elasticsearch host
    es_username : str, optional
        Elasticsearch username
    es_password : str, optional
        Elasticsearch password
    use_ssl : bool, optional
        Use SSL for connection
    verify_certs : bool, optional
        Verify SSL certificates
    ca_certs : str, optional
        Path to CA certificates
    keywords : list, optional
        List of keywords to filter
    sentiment : list, optional
        List of sentiments ['positive', 'negative', 'neutral']
    start_date : str, optional
        Start date in YYYY-MM-DD format
    end_date : str, optional
        End date in YYYY-MM-DD format
    channels : list, optional
        List of channels ['twitter', 'news', 'instagram', etc.]
    importance : str, optional
        'important mentions' or 'all mentions'
    influence_score_min : float, optional
        Minimum influence score (0-100)
    influence_score_max : float, optional
        Maximum influence score (0-100)
    region : list, optional
        List of regions
    language : list, optional
        List of languages
    domain : list, optional
        List of domains to filter
    max_words : int, optional
        Maximum number of words to include
    custom_stopwords : list, optional
        List of custom stopwords
    min_word_length : int, optional
        Minimum word length
    caption_field : str, optional
        Field name for caption text
    sentiment_field : str, optional
        Field name for sentiment
        
    Returns:
    --------
    list
        List of wordcloud data with format:
        [
            {
                'word': 'word',
                'dominant_sentiment': 'positive',
                'total_data': 123.0
            },
            ...
        ]
    """
    # Load default stopwords
    default_stopwords = get_stopwords()
    
    # Add custom stopwords
    if custom_stopwords:
        stopwords_list = default_stopwords.union(set(custom_stopwords))
    else:
        stopwords_list = default_stopwords
    
    # Create Elasticsearch client
    es = get_elasticsearch_client(
        es_host=es_host,
        es_username=es_username,
        es_password=es_password,
        use_ssl=use_ssl,
        verify_certs=verify_certs,
        ca_certs=ca_certs
    )
    
    if not es:
        return []
    
    # Get indices from channels
    indices = get_indices_from_channels(channels)
    
    if not indices:
        print("Error: No valid indices")
        return []
    
    # Build query
    query = build_elasticsearch_query(
        keywords=keywords,
        sentiment=sentiment,
        start_date=start_date,
        end_date=end_date,
        importance=importance,
        influence_score_min=influence_score_min,
        influence_score_max=influence_score_max,
        region=region,
        language=language,
        domain=domain,
        caption_field=caption_field
    )
    
    # Fetch data from Elasticsearch
    documents = fetch_elasticsearch_data(
        es=es,
        indices=",".join(indices),
        query=query
    )
    
    if not documents:
        print("Error: No documents found")
        return []
    
    # Process documents and count words with sentiments
    word_sentiment_count = {}
    
    for doc in documents:
        # Skip if required fields are missing
        if caption_field not in doc or sentiment_field not in doc:
            continue
        
        # Preprocess text
        words = preprocess_text(doc[caption_field], stopwords_list, min_word_length)
        
        # Add words to word_sentiment_count
        sentiment_value = doc[sentiment_field].lower()
        
        for word in words:
            if word not in word_sentiment_count:
                word_sentiment_count[word] = {
                    'positive': 0,
                    'negative': 0,
                    'neutral': 0,
                    'total': 0
                }
            
            # Increment sentiment count
            if sentiment_value in word_sentiment_count[word]:
                word_sentiment_count[word][sentiment_value] += 1
            else:
                # Default to neutral if sentiment not recognized
                word_sentiment_count[word]['neutral'] += 1
            
            # Increment total count
            word_sentiment_count[word]['total'] += 1
    
    # Format data for wordcloud
    wordcloud_data = []
    
    for word, counts in word_sentiment_count.items():
        # Find dominant sentiment
        dominant_sentiment = max(
            ['positive', 'negative', 'neutral'],
            key=lambda s: counts[s]
        )
        
        # Add to wordcloud_data
        wordcloud_data.append({
            'word': word,
            'dominant_sentiment': dominant_sentiment,
            'total_data': float(counts['total'])
        })
    
    # Sort by total_data
    wordcloud_data.sort(key=lambda x: x['total_data'], reverse=True)
    
    # Limit number of words
    return wordcloud_data[:max_words]