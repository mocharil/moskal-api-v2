"""
trending_hashtags.py - Script untuk mendapatkan trending hashtags dari data Elasticsearch

Script ini menganalisis data dari Elasticsearch untuk menentukan hashtag
yang sedang trending, termasuk analisis sentimen dominan untuk setiap hashtag.
Mendukung pagination untuk memudahkan navigasi hasil.
"""

import json
from datetime import datetime
from typing import Dict, List, Literal, Optional, Union

# Import utilitas dari paket utils
from utils.es_client import get_elasticsearch_client
from utils.es_query_builder import get_date_range

# Define blacklisted words for filtering hashtags
BLACKLISTED_WORDS = {'fyp', 'capcut', 'viral'}

def get_trending_hashtags(
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
    domain=None,
    limit=100,       # Total hashtags to analyze (increased for better pagination)
    page=1,          # Current page number
    page_size=10,    # Number of items per page
    sort_by="mentions"  # Sort criteria: "mentions" or "sentiment_percentage"
) -> Dict:
    """
    Mendapatkan daftar trending hashtags dengan sentimen dominan, dengan dukungan pagination
    
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
        Daftar keyword untuk filter
    search_exact_phrases : bool, optional
        Jika True, gunakan match_phrase untuk pencarian keyword, jika False gunakan match AND
    case_sensitive : bool, optional
        Jika True, pencarian keyword bersifat case-sensitive, jika False tidak memperhatikan huruf besar/kecil
    sentiment : list, optional
        Daftar sentiment ['positive', 'negative', 'neutral']
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
    limit : int, optional
        Jumlah total hashtag yang akan dianalisis dari Elasticsearch
    page : int, optional
        Nomor halaman saat ini (dimulai dari 1)
    page_size : int, optional
        Jumlah item per halaman
    sort_by : str, optional
        Kriteria pengurutan: "mentions" (berdasarkan jumlah) atau "sentiment_percentage" (berdasarkan persentase sentimen dominan)
        
    Returns:
    --------
    Dict
        Dictionary berisi daftar trending hashtags dengan sentimen dominan dan informasi pagination:
        {
            "data": [
                {
                    "hashtag": "#kadin",
                    "total_mentions": 346,
                    "dominant_sentiment": "positive",
                    "dominant_sentiment_count": 266,
                    "dominant_sentiment_percentage": 76.9
                },
                ...
            ],
            "pagination": {
                "page": 1,
                "page_size": 10,
                "total_pages": 5,
                "total_items": 42
            },
            "channels": ["twitter", "instagram", ...],
            "total_unique_hashtags": 152
        }
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
        return {
            "data": [],
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_pages": 0,
                "total_items": 0
            }
        }
    
    # Definisikan semua channel yang mungkin
    default_channels = ['reddit','youtube','linkedin','twitter',
            'tiktok','instagram','facebook','news','threads']
    
    # Filter channels jika disediakan
    if channels:
        selected_channels = [ch for ch in channels if ch in default_channels]
    else:
        selected_channels = default_channels
    
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
        "web": "web_data"
    }
    
    # Dapatkan indeks yang akan di-query
    indices = [channel_to_index[ch] for ch in selected_channels if ch in channel_to_index]
    
    if not indices:
        print("Error: No valid indices")
        return {
            "data": [],
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_pages": 0,
                "total_items": 0
            }
        }
    
    # Dapatkan rentang tanggal jika tidak disediakan
    if not start_date or not end_date:
        start_date, end_date = get_date_range(
            date_filter=date_filter,
            custom_start_date=custom_start_date,
            custom_end_date=custom_end_date
        )
    
    # Bangun query untuk mendapatkan trending hashtags
    must_conditions = [
        {
            "range": {
                "post_created_at": {
                    "gte": start_date,
                    "lte": end_date
                }
            }
        },
        {
            "exists": {
                "field": "post_hashtags"
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
    
    # Pastikan limit cukup besar untuk mendapat semua data yang diperlukan
    es_limit = max(limit, page * page_size)
    
    # PERBAIKAN: Gunakan pendekatan non-nested untuk field post_hashtags
    # Karena berdasarkan mapping, post_hashtags adalah field keyword biasa, bukan nested
    alt_query = {
        "size": 0,
        "query": {
            "bool": {
                "must": must_conditions
            }
        },
        "aggs": {
            "hashtags": {
                "terms": {
                    "field": "post_hashtags",  # Tidak perlu .keyword karena field sudah bertipe keyword
                    "size": es_limit
                },
                "aggs": {
                    "sentiment_breakdown": {
                        "terms": {
                            "field": "sentiment",
                            "size": 3  # positive, negative, neutral
                        }
                    }
                }
            },
            "total_hashtags": {
                "cardinality": {
                    "field": "post_hashtags"
                }
            }
        }
    }
    
    # Add filters to query
    if filter_conditions:
        alt_query["query"]["bool"]["filter"] = filter_conditions
    
    try:
        # Execute query
        response = es.search(
            index=",".join(indices),
            body=alt_query
        )
        
        # Process results for the alt_query approach
        hashtag_buckets = response["aggregations"]["hashtags"]["buckets"]
        total_unique_hashtags = response["aggregations"]["total_hashtags"]["value"]
        
        # Kumpulkan data hashtag
        hashtag_data = []
        
        for hashtag_bucket in hashtag_buckets:
            hashtag = hashtag_bucket["key"]
            
            # Skip hashtags yang mengandung kata-kata yang di-blacklist
            hashtag_lower = hashtag.lower()
            if any(word in hashtag_lower for word in BLACKLISTED_WORDS):
                continue
                
            mentions = hashtag_bucket["doc_count"]
            
            # Analisis sentimen untuk hashtag
            sentiment_buckets = hashtag_bucket["sentiment_breakdown"]["buckets"]
            
            # Initialize sentiment counts
            sentiment_counts = {
                "positive": 0,
                "negative": 0,
                "neutral": 0
            }
            
            # Count by sentiment
            for sentiment_bucket in sentiment_buckets:
                sentiment = sentiment_bucket["key"].lower()
                count = sentiment_bucket["doc_count"]
                if sentiment in sentiment_counts:
                    sentiment_counts[sentiment] = count
            
            # Determine dominant sentiment
            dominant_sentiment = max(sentiment_counts, key=sentiment_counts.get)
            dominant_sentiment_count = sentiment_counts[dominant_sentiment]
            
            # Calculate percentage for dominant sentiment
            dominant_sentiment_percentage = (dominant_sentiment_count / mentions) * 100 if mentions > 0 else 0
            
            hashtag_data.append({
                "hashtag": '#'+hashtag.strip("# "),
                "total_mentions": mentions,
                "dominant_sentiment": dominant_sentiment,
                "dominant_sentiment_count": dominant_sentiment_count,
                "dominant_sentiment_percentage": round(dominant_sentiment_percentage, 1)
            })
            
        # Reorder based on sort_by parameter if needed
        if sort_by == "sentiment_percentage":
            hashtag_data.sort(key=lambda x: x["dominant_sentiment_percentage"], reverse=True)
        # Default is already sorted by mentions
        
        # Calculate pagination values
        total_items = len(hashtag_data)
        total_pages = (total_items + page_size - 1) // page_size  # ceiling division
        
        # Validate page number
        if page < 1:
            page = 1
        elif page > total_pages and total_pages > 0:
            page = total_pages
            
        # Apply pagination
        start_index = (page - 1) * page_size
        end_index = min(start_index + page_size, total_items)
        
        paginated_data = hashtag_data[start_index:end_index]
        
        # Buat hasil dengan informasi pagination
        result = {
            "data": paginated_data,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "total_items": total_items
            },
            "channels": selected_channels,
            "total_unique_hashtags": total_unique_hashtags,
            "period": {
                "start_date": start_date,
                "end_date": end_date
            }
        }
        
        return result
        
    except Exception as e:
        print(f"Query failed: {e}")
        return {
            "data": [],
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_pages": 0,
                "total_items": 0
            },
            "error": str(e)
        }
