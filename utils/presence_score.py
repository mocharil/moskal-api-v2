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

def get_presence_score(
    es_host=None,
    es_username=None,
    es_password=None,
    use_ssl=False,
    verify_certs=False,
    ca_certs=None,
    keywords=None,
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
    """
    Mendapatkan presence score dari Elasticsearch
    
    Parameters:
    -----------
    es_host : str
        Host Elasticsearch
    es_username : str, optional
        Username Elasticsearch
    es_password : str, optional
        Password Elasticsearch
    use_ssl : bool, optional
        Gunakan SSL untuk koneksi
    verify_certs : bool, optional
        Verifikasi sertifikat SSL
    ca_certs : str, optional
        Path ke sertifikat CA
    keywords : list, optional
        Daftar keyword untuk filter (topik utama)
    search_exact_phrases : bool, optional
        Jika True, gunakan match_phrase untuk pencarian keyword, jika False gunakan match AND
    case_sensitive : bool, optional
        Jika True, pencarian keyword bersifat case-sensitive, jika False tidak memperhatikan huruf besar/kecil
    start_date : str, optional
        Tanggal awal format YYYY-MM-DD
    end_date : str, optional
        Tanggal akhir format YYYY-MM-DD
    date_filter : str, optional
        Filter tanggal untuk digunakan jika start_date dan end_date tidak disediakan
    custom_start_date : str, optional
        Tanggal awal kustom jika date_filter adalah "custom"
    custom_end_date : str, optional
        Tanggal akhir kustom jika date_filter adalah "custom"
    channels : list, optional
        Daftar channel ['twitter', 'news', 'instagram', dll]
    importance : str, optional
        'important mentions' atau 'all mentions'
    influence_score_min : float, optional
        Skor pengaruh minimum (0-100)
    influence_score_max : float, optional
        Skor pengaruh maksimum (0-100)
    region : list, optional
        Daftar region
    language : list, optional
        Daftar bahasa
    domain : list, optional
        Daftar domain untuk filter
    interval : str, optional
        Interval untuk data deret waktu ('day', 'week', 'month')
    compare_with_topics : bool, optional
        Apakah membandingkan dengan topik lain
    num_topics_to_compare : int, optional
        Jumlah topik lain untuk dibandingkan
        
    Returns:
    --------
    Dict
        Dictionary berisi data presence score
    """
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
    
    # Dapatkan indeks berdasarkan channels
    available_indices = []
    for channel in all_channels:
        if channel in channel_to_index:
            available_indices.append(channel_to_index[channel])
    
    if not available_indices:
        print("Error: Tidak ada indeks yang valid")
        return {}
    
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
    
    # Bangun query untuk mendapatkan presence score dari topik utama
    def build_presence_score_query(topic_keywords=None):
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
        if topic_keywords:
            # Konversi keywords ke list jika belum
            keyword_list = topic_keywords if isinstance(topic_keywords, list) else [topic_keywords]
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
                # Agregasi untuk presence score rata-rata
                "average_presence": {
                    "avg": {
                        "field": "influence_score"
                    }
                },
                # Agregasi untuk presence score seiring waktu
                "presence_over_time": {
                    "date_histogram": {
                        "field": "post_created_at",
                        "calendar_interval": calendar_interval,
                        "format": format_str
                    },
                    "aggs": {
                        "presence_score": {
                            "avg": {
                                "field": "influence_score"
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
        main_query = build_presence_score_query(keywords)
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
        
        if compare_with_topics and keywords:
            # Dapatkan topik populer
            popular_topics_query = build_popular_topics_query()
            popular_topics_response = es.search(
                index=",".join(available_indices),
                body=popular_topics_query
            )
            
            # Ambil significant terms
            significant_buckets = popular_topics_response["aggregations"]["significant_topics"]["buckets"]
            significant_terms = [bucket["key"] for bucket in significant_buckets]
            
            # Filter terms agar tidak memasukkan keyword utama
            if isinstance(keywords, list):
                other_topics = [term for term in significant_terms if not any(kw.lower() in term.lower() for kw in keywords)]
            else:
                other_topics = [term for term in significant_terms if keywords.lower() not in term.lower()]
            
            # Batasi jumlah topik
            other_topics = other_topics[:num_topics_to_compare]
            
            # Dapatkan presence score untuk setiap topik
            for topic in other_topics:
                topic_query = build_presence_score_query([topic])
                topic_response = es.search(
                    index=",".join(available_indices),
                    body=topic_query
                )
                
                topic_average_presence = topic_response["aggregations"]["average_presence"]["value"] or 0
                
                topics_comparison.append({
                    "topic": topic,
                    "presence_score": topic_average_presence
                })
            
            # Hitung persentil
            if topics_comparison:
                all_scores = [main_average_presence] + [topic["presence_score"] for topic in topics_comparison]
                all_scores.sort()
                main_index = all_scores.index(main_average_presence)
                percentile = (main_index / len(all_scores)) * 100
        
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
        
        return result
        
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return {}

def format_presence_score_data(data):
    """
    Format presence score data untuk tampilan yang lebih baik
    
    Parameters:
    -----------
    data : dict
        Data presence score dari get_presence_score()
        
    Returns:
    --------
    dict
        Data yang sudah diformat
    """
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