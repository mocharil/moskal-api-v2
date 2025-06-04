"""
presence_score.py - Script untuk mendapatkan dan memvisualisasikan presence score

Script ini mengambil data presence score dari Elasticsearch
dan menghasilkan visualisasi presence score seiring waktu, 
serta perbandingan dengan topik lainnya.
"""

import argparse
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Literal, Optional, Union, Tuple

# Import utilitas dari paket utils
from utils.es_client import get_elasticsearch_client
from utils.es_query_builder import get_date_range
from utils.redis_client import redis_client
from utils.script_score import script_score
def get_presence_score(
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
    sentiment=None,
    language=None,
    domain=None,
    interval="week",
    compare_with_topics=True,
    num_topics_to_compare=10
) -> Dict:
     # Generate cache key based on all parameters
    cache_key = redis_client.generate_cache_key(
        "get_presence_score",
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
        interval=interval,
        compare_with_topics=compare_with_topics,
        num_topics_to_compare=num_topics_to_compare
    )

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
        return {}
    
    # Definisikan semua indeks yang tersedia
    default_channels = ['reddit','youtube','linkedin','twitter',
            'tiktok','instagram','facebook','news','threads']
    
    # Gunakan channels yang disediakan, jika tidak ada gunakan default
    all_channels = channels if channels else default_channels
  
    # Dapatkan indeks berdasarkan channels
    available_indices = [f'{ch}_data' for ch in all_channels]

    # Dapatkan rentang tanggal jika tidak disediakan
    if not start_date or not end_date:
        start_date, end_date = get_date_range(
            date_filter=date_filter,
            custom_start_date=custom_start_date,
            custom_end_date=custom_end_date
        )
    
    # Parse tanggal
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Tentukan interval kalender untuk agregasi
    if interval == "day":
        calendar_interval = "day"
        format_str = "yyyy-MM-dd"
    elif interval == "month":
        calendar_interval = "month"
        format_str = "yyyy-MM"
    else:  # default to week
        calendar_interval = "week"
        format_str = "yyyy-MM-dd"
    
    script_score_temp = script_score.copy()
    script_score_temp["source"] = script_score_temp["source"].replace("return Math.min(score, 10.0);","return Math.min(score, 10.0)*10;")

    # Bangun query untuk mendapatkan presence score dari topik utama
    def build_presence_score_query(keywords=None,search_keyword=None):
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
            
        # Filter untuk domain
        if domain:
            domain_condition = {
                "bool": {
                    "should": [{"wildcard": {"link_post": f"*{d}*"}} for d in (domain if isinstance(domain, list) else [domain])],
                    "minimum_should_match": 1
                }
            }
            filter_conditions.append(domain_condition)

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
            },
            "aggs": {
                # Agregasi untuk presence score rata-rata menggunakan script
                "average_presence": {
                    "avg": {
                        "script": script_score_temp
                    }
                },
                # Agregasi untuk presence score seiring waktu menggunakan script
                "presence_over_time": {
                    "date_histogram": {
                        "field": "post_created_at",
                        "calendar_interval": calendar_interval,
                        "format": format_str
                    },
                    "aggs": {
                        "presence_score": {
                            "avg": {
                                "script": script_score_temp
                            }
                        }
                    }
                }
            }
        }
        
        # Tambahkan filter jika ada
        if filter_conditions:
            query["query"]["bool"]["filter"] = filter_conditions
            
        return query
    
    # Query untuk mendapatkan topik populer untuk perbandingan
    def build_popular_topics_query():
        query = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "post_created_at": {
                                    "gte": start_date,
                                    "lte": end_date
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "significant_topics": {
                    "significant_terms": {
                        "field": "post_caption.keywords",
                        "size": num_topics_to_compare + 1  # +1 untuk mengantisipasi topik utama
                    }
                }
            }
        }
        
        return query
    
    try:
        # Jalankan query untuk topik utama
        main_query = build_presence_score_query(keywords,search_keyword)

        import json
        #print(json.dumps(main_query, indent=2))

        main_response = es.search(
            index=",".join(available_indices),
            body=main_query
        )
        
        # Ambil presence score rata-rata untuk topik utama
        main_average_presence = main_response["aggregations"]["average_presence"]["value"] or 0
        
        # Ambil presence score seiring waktu untuk topik utama
        presence_over_time = []
        time_buckets = main_response["aggregations"]["presence_over_time"]["buckets"]
        
        for bucket in time_buckets:
            date_str = bucket["key_as_string"]
            score = bucket["presence_score"]["value"] or 0
            
            presence_over_time.append({
                "date": date_str,
                "score": score
            })
        
        # Bandingkan dengan topik lain jika diminta
        topics_comparison = []
        percentile = None

        # Buat hasil
        result = {
            "current_presence_score": main_average_presence,
            "presence_over_time": presence_over_time,
            "percentile": percentile,
            "topics_comparison": topics_comparison,
            "interval": interval,
            "period": {
                "start_date": start_date,
                "end_date": end_date
            }
        }
                # Cache the results for 10 minutes
        redis_client.set_with_ttl(cache_key, result, ttl_seconds=100)
        return result
        
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return {}


    if not data:
        return {}
    
    # Format current presence score
    current_score = data.get("current_presence_score", 0)
    rounded_score = round(current_score)
    
    # Format percentile
    percentile = data.get("percentile")
    percentile_statement = ""
    if percentile is not None:
        better_than_percent = round(100 - percentile)
        percentile_statement = f"Your Presence Score is higher than {better_than_percent}% of other topics"
    
    # Format time series untuk chart
    time_series = []
    if "presence_over_time" in data:
        for point in data["presence_over_time"]:
            date_str = point["date"]
            score = point["score"]
            
            # Proses format tanggal berdasarkan interval
            if data.get("interval") == "month":
                # Format: yyyy-MM -> Mmm yyyy
                date_obj = datetime.strptime(date_str, "%Y-%m")
                display_date = date_obj.strftime("%b %Y")
            else:
                # Format: yyyy-MM-dd -> dd MMM
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                display_date = date_obj.strftime("%d %b")
            
            time_series.append({
                "date": date_str,
                "display_date": display_date,
                "score": score
            })
    
    # Format hasil
    formatted_data = {
        "score": {
            "value": current_score,
            "display": str(rounded_score)
        },
        "percentile_statement": percentile_statement,
        "time_series": time_series,
        "interval": data.get("interval", "week"),
        "period": data.get("period", {})
    }
    
    return formatted_data