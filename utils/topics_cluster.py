import json
import re
import pandas as pd
from datetime import datetime
from typing import Dict, List, Literal, Optional, Union

# Import utilitas dari paket utils
from utils.es_client import get_elasticsearch_client
from utils.es_query_builder import (
    build_elasticsearch_query,
    get_indices_from_channels,
    get_date_range
)
from utils.redis_client import redis_client
from utils.script_score import script_score

def get_topics_cluster(
    es_host=None,
    es_username=None,
    es_password=None,
    use_ssl=False,
    verify_certs=False,
    ca_certs=None,
    keywords=None,
    search_keyword=None,
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
    domain=None,
    cluster_size=100
):
    # Generate cache key based on all parameters
    cache_key = redis_client.generate_cache_key(
        "topics_cluster",
        es_host=es_host,
        es_username=es_username,
        es_password=es_password,
        use_ssl=use_ssl,
        verify_certs=verify_certs,
        ca_certs=ca_certs,
        keywords=keywords,
        search_keyword=search_keyword,
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
        domain=domain,
        cluster_size=cluster_size
    )

    # Try to get from cache first
    cached_result = redis_client.get(cache_key)
    if cached_result is not None:
        print('Returning cached result')
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
        return {'data': [], 'total_clusters': 0}
    
    # Dapatkan indeks dari channel
    indices = get_indices_from_channels(channels)
    
    if not indices:
        print("Error: Tidak ada indeks yang valid")
        return {'data': [], 'total_clusters': 0}
    
    # Dapatkan rentang tanggal jika tidak disediakan
    if not start_date or not end_date:
        start_date, end_date = get_date_range(
            date_filter=date_filter,
            custom_start_date=custom_start_date,
            custom_end_date=custom_end_date
        )
    
    # Bangun query dasar untuk filter
    base_query = build_elasticsearch_query(
        keywords=keywords,
        search_keyword=search_keyword,
        search_exact_phrases=search_exact_phrases,
        case_sensitive=case_sensitive,
        sentiment=sentiment,
        start_date=start_date,
        end_date=end_date,
        importance=importance,
        influence_score_min=influence_score_min,
        influence_score_max=influence_score_max,
        region=region,
        language=language,
        domain=domain,
        size=0  # Set size to 0 for aggregation only
    )
    
    # Tambahkan filter untuk cluster yang ada
    cluster_filter = {
        "exists": {
            "field": "cluster"
        }
    }
    base_query["query"]["bool"]["filter"].append(cluster_filter)
    
    # Bangun aggregation query
    aggregation_query = {
        "size": 0,
        "query": base_query["query"],
        "aggs": {
            "unique_clusters": {
                "terms": {
                    "field": "cluster.keyword",
                    "size": cluster_size
                },
                "aggs": {
                    "cluster_description": {
                        "top_hits": {
                            "_source": {
                                "includes": ["cluster_description"]
                            },
                            "size": 1
                        }
                    },
                    "total_mentions": {
                        "value_count": {
                            "field": "cluster.keyword"
                        }
                    },
                    "sentiment_positive": {
                        "filter": {
                            "term": { "sentiment": "positive" }
                        }
                    },
                    "sentiment_negative": {
                        "filter": {
                            "term": { "sentiment": "negative" }
                        }
                    },
                    "sentiment_neutral": {
                        "filter": {
                            "term": { "sentiment": "neutral" }
                        }
                    },
                    "total_viral_score": {
                        "sum": {
                            "script": script_score
                        }
                    },
                    "total_reach_score": {
                        "sum": {
                            "field": "reach_score"
                        }
                    },
                    "cluster_issues": {
                        "terms": {
                            "field": "cluster.keyword",
                            "size": 1000
                        }
                    }
                }
            }
        }
    }
    # Jalankan query
    try:
        response = es.search(
            index=",".join(indices),
            body=aggregation_query
        )
        
        # Process aggregation results
        clusters_data = []
        total_all_posts = 0
        
        # Get total posts for share of voice calculation
        for bucket in response["aggregations"]["unique_clusters"]["buckets"]:
            total_all_posts += bucket["total_mentions"]["value"]
        
        for bucket in response["aggregations"]["unique_clusters"]["buckets"]:
            cluster_name = bucket["key"]
            total_posts = bucket["total_mentions"]["value"]
            
            # Get cluster description
            description = ""
            if bucket["cluster_description"]["hits"]["hits"]:
                description = bucket["cluster_description"]["hits"]["hits"][0]["_source"].get("cluster_description", "")
            
            # Get sentiment counts
            positive_count = bucket["sentiment_positive"]["doc_count"]
            negative_count = bucket["sentiment_negative"]["doc_count"]
            neutral_count = bucket["sentiment_neutral"]["doc_count"]
            
            # Get scores
            viral_score = bucket["total_viral_score"]["value"] or 0
            reach_score = bucket["total_reach_score"]["value"] or 0
            
            # Calculate share of voice
            share_of_voice = (total_posts / total_all_posts)*100 if total_all_posts > 0 else 0
            
            # Create list_issue (for now, just using the cluster name)
            # You might want to modify this based on your specific requirements
            list_issue = [cluster_name]
            
            cluster_data = {
                "unified_issue": cluster_name,
                "description": description,
                "list_issue": list_issue,
                "total_posts": total_posts,
                "viral_score": viral_score,
                "reach_score": reach_score,
                "positive": positive_count,
                "negative": negative_count,
                "neutral": neutral_count,
                "share_of_voice": share_of_voice
            }
            
            clusters_data.append(cluster_data)
        
        # Sort by total_posts descending
        clusters_data.sort(key=lambda x: x["total_posts"], reverse=True)
        
        result = clusters_data
        
        # Cache the results for 10 minutes
        redis_client.set_with_ttl(cache_key, result, ttl_seconds=600)
        return result
    
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return {'data': [], 'total_clusters': 0}
