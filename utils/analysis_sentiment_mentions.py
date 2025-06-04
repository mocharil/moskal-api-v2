"""
get_category_analytics.py - Script untuk mendapatkan analisis kategori dan sentimen

Script ini mengambil data tentang mentions berdasarkan kategori dan sentiment berdasarkan
kategori dari Elasticsearch untuk visualisasi.
"""

import argparse
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Literal, Optional, Union, Tuple

# Import utilitas dari paket utils
from utils.es_client import get_elasticsearch_client
from utils.es_query_builder import get_date_range
from utils.redis_client import redis_client

def get_category_analytics(
    es_host=None,
    es_username=None,
    es_password=None,
    use_ssl=False,
    verify_certs=False,
    ca_certs=None,
    keywords=None,
    search_keyword = None,
    search_exact_phrases=False,
    case_sensitive=False,
    start_date=None,
    sentiment = None,
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
) -> Tuple[Dict, Dict, Dict]:
    # Generate cache key based on all parameters
    cache_key = redis_client.generate_cache_key(
        "category_analytics",
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
        start_date=start_date,
        sentiment=sentiment,
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
        return {}, {}, {}
    
    # Daftar semua channel yang mungkin
    default_channels = ['reddit','youtube','linkedin','twitter',
            'tiktok','instagram','facebook','news','threads']
    
    # Gunakan channels yang disediakan, jika tidak ada gunakan default
    all_channels = channels if channels else default_channels
    
    # Mapping channel ke index Elasticsearch
    channel_to_index = {
        "twitter": "twitter_data",
        "instagram": "instagram_data",
        "linkedin": "linkedin_data",
        "reddit": "reddit_data",
        "youtube": "youtube_data",
        "tiktok": "tiktok_data",
        "news": "news_data",
        "blogs": "blogs_data",
        "facebook": "facebook_data",
        "podcasts": "podcasts_data",
        "videos": "videos_data",
        "web": "web_data",
        "other_socials": "other_socials_data"
    }
    
    # Dapatkan rentang tanggal jika tidak disediakan
    if not start_date or not end_date:
        start_date, end_date = get_date_range(
            date_filter=date_filter,
            custom_start_date=custom_start_date,
            custom_end_date=custom_end_date
        )
    
    # Bangun query dasar untuk mendapatkan data kategori dan sentimen
    def build_base_query():
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
        
        # Add keyword and search_keyword filters
        if keywords or search_keyword:
            must_conditions_inner = []
            
            # Handle regular keywords
            if keywords:
                # Konversi keywords ke list jika belum
                keyword_list = keywords if isinstance(keywords, list) else [keywords]
                keyword_should_conditions = []
                
                # Tentukan field yang akan digunakan berdasarkan case_sensitive
                caption_field = "post_caption.keyword" if case_sensitive else "post_caption"
                issue_field = "cluster.keyword" if case_sensitive else "cluster"
                
                if search_exact_phrases:
                    # Gunakan match_phrase untuk exact matching
                    for kw in keyword_list:
                        keyword_should_conditions.extend([
                            {"match_phrase": {caption_field: kw}},
                            {"match_phrase": {issue_field: kw}}
                        ])
                else:
                    # Gunakan match dengan operator AND
                    for kw in keyword_list:
                        keyword_should_conditions.extend([
                            {"match": {caption_field: {"query": kw, "operator": "AND"}}},
                            {"match": {issue_field: {"query": kw, "operator": "AND"}}}
                        ])
                
                must_conditions_inner.append({
                    "bool": {
                        "should": keyword_should_conditions,
                        "minimum_should_match": 1
                    }
                })
            
            # Handle search_keyword with same logic as keywords
            if search_keyword:
                # Konversi search_keyword ke list jika belum
                search_keyword_list = search_keyword if isinstance(search_keyword, list) else [search_keyword]
                search_keyword_should_conditions = []
                
                # Tentukan field yang akan digunakan berdasarkan case_sensitive
                caption_field = "post_caption.keyword" if case_sensitive else "post_caption"
                issue_field = "cluster.keyword" if case_sensitive else "cluster"
                
                if search_exact_phrases:
                    # Gunakan match_phrase untuk exact matching
                    for sk in search_keyword_list:
                        search_keyword_should_conditions.extend([
                            {"match_phrase": {caption_field: sk}},
                            {"match_phrase": {issue_field: sk}}
                        ])
                else:
                    # Gunakan match dengan operator AND
                    for sk in search_keyword_list:
                        search_keyword_should_conditions.extend([
                            {"match": {caption_field: {"query": sk, "operator": "AND"}}},
                            {"match": {issue_field: {"query": sk, "operator": "AND"}}}
                        ])
                
                must_conditions_inner.append({
                    "bool": {
                        "should": search_keyword_should_conditions,
                        "minimum_should_match": 1
                    }
                })
            
            # Add the combined conditions to must_conditions
            must_conditions.append({
                "bool": {
                    "must": must_conditions_inner
                }
            })
        
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
        if sentiment:
            sentiment_condition = {
                "terms": {
                    "sentiment": sentiment if isinstance(sentiment, list) else [sentiment]
                }
            }
            filter_conditions.append(sentiment_condition)

        
        # Filter untuk domain
        if domain:
            domain_condition = {
                "bool": {
                    "should": [{"wildcard": {"link_post": f"*{d}*"}} for d in (domain if isinstance(domain, list) else [domain])],
                    "minimum_should_match": 1
                }
            }
            filter_conditions.append(domain_condition)
        
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
            
        return query
    
    # Query untuk mendapatkan distribusi kategori
    def build_category_query():
        query = build_base_query()
        
        # Tambahkan aggregasi untuk channel
        query["aggs"] = {
            "categories": {
                "terms": {
                    "field": "channel",
                    "size": 20
                }
            }
        }
        
        return query
    
    # Query untuk mendapatkan distribusi sentimen per kategori
    def build_sentiment_category_query():
        query = build_base_query()
        
        # Tambahkan aggregasi untuk channel dan sentimen
        query["aggs"] = {
            "categories": {
                "terms": {
                    "field": "channel",
                    "size": 20
                },
                "aggs": {
                    "sentiment": {
                        "terms": {
                            "field": "sentiment",
                            "size": 5
                        }
                    }
                }
            }
        }
        
        return query
    
    try:
        # Dapatkan indeks yang akan di-query berdasarkan channels yang disediakan
        available_indices = [f"{channel}_data" for channel in all_channels]
        

        # Query untuk kategori
        category_query = build_category_query()
        import json
        #print(json.dumps(category_query, indent=2))

        category_response = es.search(
            index=",".join(available_indices),
            body=category_query
        )
        
        # Query untuk sentimen per kategori
        sentiment_category_query = build_sentiment_category_query()
        sentiment_category_response = es.search(
            index=",".join(available_indices),
            body=sentiment_category_query
        )
        
        import json
        #print(json.dumps(category_query, indent=2))
        #print(json.dumps(sentiment_category_query, indent=2))


        # Proses data kategori
        category_buckets = category_response["aggregations"]["categories"]["buckets"]
        total_mentions = sum(bucket["doc_count"] for bucket in category_buckets)
        
        mentions_by_category = {
            "total": total_mentions,
            "categories": []
        }
        
        # Hitung persentase dan buat data untuk pie chart
        for bucket in category_buckets:
            category_name = bucket["key"]
            count = bucket["doc_count"]
            percentage = (count / total_mentions) * 100 if total_mentions > 0 else 0
            
            mentions_by_category["categories"].append({
                "name": category_name,
                "count": count,
                "percentage": round(percentage, 2)
            })
        
        # Urutkan berdasarkan persentase tertinggi
        mentions_by_category["categories"].sort(key=lambda x: x["percentage"], reverse=True)
        
        # Proses data sentimen per kategori
        sentiment_category_buckets = sentiment_category_response["aggregations"]["categories"]["buckets"]
        
        sentiment_by_category = {
            "categories": [],
            "sentiments": ["positive", "neutral", "negative"]
        }
        
        # Buat data untuk stacked bar chart
        for bucket in sentiment_category_buckets:
            category_name = bucket["key"]
            total_category_count = bucket["doc_count"]
            sentiment_buckets = bucket["sentiment"]["buckets"]
            
            # Hitung jumlah untuk masing-masing sentiment
            sentiment_counts = {
                "positive": 0,
                "neutral": 0,
                "negative": 0
            }
            
            for sentiment_bucket in sentiment_buckets:
                sentiment = sentiment_bucket["key"].lower()
                count = sentiment_bucket["doc_count"]
                if sentiment in sentiment_counts:
                    sentiment_counts[sentiment] = count
            
            # Buat entry kategori dengan counts untuk masing-masing sentiment
            sentiment_by_category["categories"].append({
                "name": category_name,
                "total": total_category_count,
                "positive": sentiment_counts["positive"],
                "neutral": sentiment_counts["neutral"],
                "negative": sentiment_counts["negative"]
            })
        
        # Urutkan kategori berdasarkan total count tertinggi
        sentiment_by_category["categories"].sort(key=lambda x: x["total"], reverse=True)
        
        # Buat ringkasan sentiment keseluruhan
        sentiment_breakdown = {
            'positive': sum([i['positive'] for i in sentiment_by_category['categories']]),
            'negative': sum([i['negative'] for i in sentiment_by_category['categories']]),
            'neutral': sum([i['neutral'] for i in sentiment_by_category['categories']])
        }
        
        result = {
            "mentions_by_category": mentions_by_category,
            'sentiment_by_category': sentiment_by_category,
            'sentiment_breakdown': sentiment_breakdown
        }
        
        # Cache the results for 10 minutes
        redis_client.set_with_ttl(cache_key, result, ttl_seconds=100)
        return result
        
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return {
    "mentions_by_category":{},
    'sentiment_by_category':{},
    'sentiment_breakdown':{}}
