import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Literal, Optional, Union

# Import utilitas dari paket utils
from utils.es_client import get_elasticsearch_client
from utils.es_query_builder import get_date_range

def get_share_of_voice(
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
    sentiment = None,
    importance="all mentions",
    influence_score_min=None,
    influence_score_max=None,
    region=None,
    language=None,
    domain=None,
    limit=10,
    page=1,
    page_size=10,
    include_total_count=True
) -> Dict:
    """
    Mendapatkan share of voice dari akun media sosial
    
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
        Jumlah total akun yang akan dianalisis (untuk keseluruhan dataset)
    page : int, optional
        Halaman yang akan diambil (untuk pagination)
    page_size : int, optional
        Jumlah item per halaman (untuk pagination)
    include_total_count : bool, optional
        Sertakan jumlah total akun di hasil
        
    Returns:
    --------
    Dict
        Dictionary berisi share of voice dengan dukungan pagination
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
    

    # Dapatkan indeks yang akan di-query
    indices = [f"{ch}_data" for ch in selected_channels]
    
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
    
    # Bangun query untuk mendapatkan jumlah mentions per username
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

    if sentiment:
        sentiment_condition = {
            "terms": {
                "sentiment": sentiment if isinstance(sentiment, list) else [sentiment]
            }
        }
        filter_conditions.append(sentiment_condition)
           
    # Gabungkan semua kondisi ke dalam query utama
    query = {
        "size": 0,  # Kita hanya perlu agregasi
        "query": {
            "bool": {
                "must": must_conditions
            }
        },
        "aggs": {
            "by_channel": {
                "terms": {
                    "field": "channel",
                    "size": len(selected_channels)
                },
                "aggs": {
                    "by_username": {
                        "terms": {
                            "field": "username",
                            "size": limit
                        },
                        "aggs": {
                            "total_reach": {
                                "sum": {
                                    "field": "reach_score"
                                }
                            },
                            "top_hits": {
                                "top_hits": {
                                    "size": 1,
                                    "_source": ["user_image_url"]
                                }
                            }
                        }
                    }
                }
            },
            "total_mentions": {
                "value_count": {
                    "field": "link_post"
                }
            }
        }
    }
    
    # Tambahkan filter jika ada
    if filter_conditions:
        query["query"]["bool"]["filter"] = filter_conditions
    
    try:
        # Jalankan query
        response = es.search(
            index=",".join(indices),
            body=query
        )
        
        # Proses hasil untuk mendapatkan share of voice
        channel_buckets = response["aggregations"]["by_channel"]["buckets"]
        total_mentions = response["aggregations"]["total_mentions"]["value"]
        
        # Kumpulkan data dari semua channel
        sov_data = []
        
        for channel_bucket in channel_buckets:
            channel = channel_bucket["key"]
            username_buckets = channel_bucket["by_username"]["buckets"]
            
            for username_bucket in username_buckets:
                username = username_bucket["key"]
                mentions = username_bucket["doc_count"]
                reach = username_bucket["total_reach"]["value"]
                
                # Ambil user_image_url dari top_hits
                user_image_url = None
                if "top_hits" in username_bucket and username_bucket["top_hits"]["hits"]["hits"]:
                    hit = username_bucket["top_hits"]["hits"]["hits"][0]
                    if "_source" in hit and "user_image_url" in hit["_source"]:
                        user_image_url = hit["_source"]["user_image_url"]
                
                sov_percentage = (mentions / total_mentions) * 100 if total_mentions > 0 else 0
                
                # Buat fallback user_image_url untuk channel news jika tidak ada
                if channel == "news" and not user_image_url:
                    domain_name = username.replace("www.", "")
                    user_image_url = f"https://logo.clearbit.com/{domain_name}"
                
                sov_data.append({
                    "channel": channel,
                    "username": username,
                    "total_mentions": mentions,
                    "total_reach": reach,
                    "percentage_share_of_voice": round(sov_percentage, 2),
                    "user_image_url": user_image_url
                })
        
        # Sortir berdasarkan total_mentions
        sov_data.sort(key=lambda x: x["total_mentions"], reverse=True)
        
        # Pagination
        total_items = len(sov_data)
        total_pages = (total_items + page_size - 1) // page_size  # ceiling division
        
        start_index = (page - 1) * page_size
        end_index = min(start_index + page_size, total_items)
        
        paginated_data = sov_data[start_index:end_index]
        
        # Format username
        for item in paginated_data:
            if item["channel"] == "twitter" and not item["username"].startswith("@"):
                item["username"] = f"@{item['username']}"
        
        # Buat hasil dengan informasi pagination
        result = {
            "data": paginated_data,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "total_items": total_items
            }
        }
        
        # Tambahkan daftar channel yang digunakan
        result["channels"] = selected_channels
        
        # Tambahkan total mentions keseluruhan jika diminta
        if include_total_count:
            result["total_mentions"] = total_mentions
        
        return result
        
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return {
            "data": [],
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_pages": 0,
                "total_items": 0
            }
        }
    """
    Mendapatkan share of voice dari akun media sosial
    
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
        Jumlah total akun yang akan dianalisis (untuk keseluruhan dataset)
    page : int, optional
        Halaman yang akan diambil (untuk pagination)
    page_size : int, optional
        Jumlah item per halaman (untuk pagination)
    include_total_count : bool, optional
        Sertakan jumlah total akun di hasil
        
    Returns:
    --------
    Dict
        Dictionary berisi share of voice dengan dukungan pagination
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
    

    # Dapatkan indeks yang akan di-query
    indices = [f"{ch}_data" for ch in selected_channels]
    
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
    
    # Bangun query untuk mendapatkan jumlah mentions per username
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

    if sentiment:
        sentiment_condition = {
            "terms": {
                "sentiment": sentiment if isinstance(sentiment, list) else [sentiment]
            }
        }
        filter_conditions.append(sentiment_condition)
           
    # Gabungkan semua kondisi ke dalam query utama
    query = {
        "size": 0,  # Kita hanya perlu agregasi
        "query": {
            "bool": {
                "must": must_conditions
            }
        },
        "aggs": {
            "by_channel": {
                "terms": {
                    "field": "channel",
                    "size": len(selected_channels)
                },
                "aggs": {
                    "by_username": {
                        "terms": {
                            "field": "username",
                            "size": limit
                        },
                        "aggs": {
                            "total_reach": {
                                "sum": {
                                    "field": "reach_score"
                                }
                            }
                        }
                    }
                }
            },
            "total_mentions": {
                "value_count": {
                    "field": "link_post"
                }
            }
        }
    }
    
    # Tambahkan filter jika ada
    if filter_conditions:
        query["query"]["bool"]["filter"] = filter_conditions
    
    try:
        # Jalankan query
        response = es.search(
            index=",".join(indices),
            body=query
        )
        
        # Proses hasil untuk mendapatkan share of voice
        channel_buckets = response["aggregations"]["by_channel"]["buckets"]
        total_mentions = response["aggregations"]["total_mentions"]["value"]
        
        # Kumpulkan data dari semua channel
        sov_data = []
        
        for channel_bucket in channel_buckets:
            channel = channel_bucket["key"]
            username_buckets = channel_bucket["by_username"]["buckets"]
            
            for username_bucket in username_buckets:
                username = username_bucket["key"]
                mentions = username_bucket["doc_count"]
                reach = username_bucket["total_reach"]["value"]
                
                sov_percentage = (mentions / total_mentions) * 100 if total_mentions > 0 else 0
                
                sov_data.append({
                    "channel": channel,
                    "username": username,
                    "total_mentions": mentions,
                    "total_reach": reach,
                    "percentage_share_of_voice": round(sov_percentage, 2)
                })
        
        # Sortir berdasarkan total_mentions
        sov_data.sort(key=lambda x: x["total_mentions"], reverse=True)
        
        # Pagination
        total_items = len(sov_data)
        total_pages = (total_items + page_size - 1) // page_size  # ceiling division
        
        start_index = (page - 1) * page_size
        end_index = min(start_index + page_size, total_items)
        
        paginated_data = sov_data[start_index:end_index]
        
        # Format username
        for item in paginated_data:
            if item["channel"] == "twitter" and not item["username"].startswith("@"):
                item["username"] = f"@{item['username']}"
        
        # Buat hasil dengan informasi pagination
        result = {
            "data": paginated_data,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "total_items": total_items
            }
        }
        
        # Tambahkan daftar channel yang digunakan
        result["channels"] = selected_channels
        
        # Tambahkan total mentions keseluruhan jika diminta
        if include_total_count:
            result["total_mentions"] = total_mentions
        
        return result
        
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return {
            "data": [],
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_pages": 0,
                "total_items": 0
            }
        }