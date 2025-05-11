from datetime import datetime
from typing import Dict, List, Literal, Optional, Union

# Import utilitas dari paket utils
from utils.es_client import get_elasticsearch_client
from utils.es_query_builder import (
    get_indices_from_channels,
    get_date_range,
    build_elasticsearch_query,
    add_time_series_aggregation
)
from utils.redis_client import redis_client

def get_keyword_trends(
    es_host=None,
    es_username=None,
    es_password=None,
    use_ssl=False,
    verify_certs=False,
    ca_certs=None,
    keywords=None,
    search_exact_phrases=False,
    case_sensitive=False,
    sentiment=None,
    start_date=None,
    end_date=None,
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
 
    # Generate cache key based on all parameters
    cache_key = redis_client.generate_cache_key(
        "keyword_trends",
        es_host=es_host,
        es_username=es_username,
        es_password=es_password,
        use_ssl=use_ssl,
        verify_certs=verify_certs,
        ca_certs=ca_certs,
        keywords=keywords,
        search_exact_phrases=search_exact_phrases,
        case_sensitive=case_sensitive,
        sentiment=sentiment,
        start_date=start_date,
        end_date=end_date,
        date_filter=date_filter,
        custom_start_date=custom_start_date,
        custom_end_date=custom_end_date,
        channels=channels,
        importance=importance,
        influence_score_min=influence_score_min,
        influence_score_max=influence_score_max,
        region=region,
        language=language,
        domain=domain
    )

    # Try to get from cache first
    cached_result = redis_client.get(cache_key)
    if cached_result is not None:
        logger.info('Returning cached result')
        return cached_result

    # Buat koneksi Elasticsearch
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
    
    # Definisikan semua channel yang mungkin
    default_channels = ['reddit','youtube','linkedin','twitter',
            'tiktok','instagram','facebook','news','threads']
    
    # Filter channels jika disediakan
    if channels:
        selected_channels = [ch for ch in channels if ch in default_channels]
    else:
        selected_channels = default_channels
    
    # Get indices from channels
    indices = get_indices_from_channels(selected_channels)
    
    if not indices:
        print("Error: No valid indices")
        return []
    
    # Get date range
    if not start_date or not end_date:
        start_date, end_date = get_date_range(
            date_filter=date_filter,
            custom_start_date=custom_start_date,
            custom_end_date=custom_end_date
        )
    
    # Bangun query manual daripada menggunakan build_elasticsearch_query langsung
    # ini untuk mendukung fitur search_exact_phrases dan case_sensitive
    must_conditions = [
        {
            "range": {
                "post_created_at": {
                    "gte": start_date,
                    "lte": end_date
                }
            }
        }
    ]
    
    # Tambahkan filter keywords jika ada
    if keywords:
        # Konversi keywords ke list jika belum
        keyword_list = keywords if isinstance(keywords, list) else [keywords]
        keyword_should_conditions = []
        
        # Tentukan field yang akan digunakan berdasarkan case_sensitive
        caption_field = "post_caption.keyword" if case_sensitive else "post_caption"
        issue_field = "issue.keyword" if case_sensitive else "issue"
        
        if search_exact_phrases:
            # Gunakan match_phrase untuk exact matching
            for kw in keyword_list:
                keyword_should_conditions.append({"match_phrase": {caption_field: kw}})
                keyword_should_conditions.append({"match_phrase": {issue_field: kw}})
        else:
            # Gunakan match dengan operator AND
            for kw in keyword_list:
                keyword_should_conditions.append({"match": {caption_field: {"query": kw, "operator": "AND"}}})
                keyword_should_conditions.append({"match": {issue_field: {"query": kw, "operator": "AND"}}})
        
        keyword_condition = {
            "bool": {
                "should": keyword_should_conditions,
                "minimum_should_match": 1
            }
        }
        must_conditions.append(keyword_condition)
    
    # Bangun filter untuk query
    filter_conditions = []
    
    # Filter untuk importance
    if importance == "important mentions":
        filter_conditions.append({
            "range": {
                "influence_score": {
                    "gt": 50
                }
            }
        })
        
    # Filter untuk influence score
    if influence_score_min is not None or influence_score_max is not None:
        influence_condition = {"range": {"influence_score": {}}}
        if influence_score_min is not None:
            influence_condition["range"]["influence_score"]["gte"] = influence_score_min
        if influence_score_max is not None:
            influence_condition["range"]["influence_score"]["lte"] = influence_score_max
        filter_conditions.append(influence_condition)
        
    # Filter untuk region menggunakan wildcard
    if region:
        region_conditions = []
        region_list = region if isinstance(region, list) else [region]
        
        for r in region_list:
            region_conditions.append({"wildcard": {"region": f"*{r}*"}})
        
        region_filter = {
            "bool": {
                "should": region_conditions,
                "minimum_should_match": 1
            }
        }
        filter_conditions.append(region_filter)
        
    # Filter untuk language menggunakan wildcard
    if language:
        language_conditions = []
        language_list = language if isinstance(language, list) else [language]
        
        for l in language_list:
            language_conditions.append({"wildcard": {"language": f"*{l}*"}})
        
        language_filter = {
            "bool": {
                "should": language_conditions,
                "minimum_should_match": 1
            }
        }
        filter_conditions.append(language_filter)
        
    # Filter untuk domain
    if domain:
        domain_condition = {
            "bool": {
                "should": [{"wildcard": {"link_post": f"*{d}*"}} for d in (domain if isinstance(domain, list) else [domain])],
                "minimum_should_match": 1
            }
        }
        filter_conditions.append(domain_condition)
    
    # Filter untuk sentiment
    if sentiment:
        sentiment_condition = {
            "terms": {
                "sentiment": sentiment if isinstance(sentiment, list) else [sentiment]
            }
        }
        filter_conditions.append(sentiment_condition)
    
    # Gabungkan semua kondisi ke dalam query utama
    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": must_conditions
            }
        }
    }
    
    # Tambahkan filter jika ada
    if filter_conditions:
        query["query"]["bool"]["filter"] = filter_conditions
    
    # Add time series aggregation
    query = add_time_series_aggregation(query)
    
    # Execute query
    try:
        response = es.search(
            index=",".join(indices),
            body=query
        )
        
        # Process results
        result = process_time_series_results(response)
        
        # Cache the results for 10 minutes (silently fails if Redis is down)
        redis_client.set_with_ttl(cache_key, result, ttl_seconds=600)
        return result
        
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return []

def process_time_series_results(response):

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
