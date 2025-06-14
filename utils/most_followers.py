import json
from datetime import datetime
from typing import Dict, List, Literal, Optional, Union

# Import utilitas dari paket utils
from utils.es_client import get_elasticsearch_client
from utils.es_query_builder import get_date_range
from utils.redis_client import redis_client

def get_most_followers(
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
    limit=10,
    page=1,
    page_size=10,
    include_total_count=True
) -> Dict:
 

    cache_key = redis_client.generate_cache_key(
        "get_most_followers",
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
        limit=limit,
        page=page,
        page_size=page_size,
        include_total_count=include_total_count
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
    
    # Bangun query untuk mendapatkan akun dengan followers terbanyak
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
    
    # Filter untuk sentiment
    if sentiment:
        sentiment_condition = {
            "terms": {
                "sentiment": sentiment if isinstance(sentiment, list) else [sentiment]
            }
        }
        filter_conditions.append(sentiment_condition)
    
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
                            "subscribers": {
                                "max": {
                                    "field": "subscriber"
                                }
                            },
                            "followers": {
                                "max": {
                                    "field": "user_followers"
                                }
                            },
                            "connections": {
                                "max": {
                                    "field": "user_connections"
                                }
                            },
                            "influence_score": {
                                "avg": {
                                    "field": "user_influence_score"
                                }
                            },
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

        import json
        #print(json.dumps(query, indent=2))
        # Jalankan query
        response = es.search(
            index=",".join(indices),
            body=query
        )
        
        # Proses hasil untuk mendapatkan akun dengan followers terbanyak
        channel_buckets = response["aggregations"]["by_channel"]["buckets"]
        total_mentions = response["aggregations"]["total_mentions"]["value"]
        
        # Kumpulkan data dari semua channel
        followers_data = []
        
        for channel_bucket in channel_buckets:
            channel = channel_bucket["key"]
            username_buckets = channel_bucket["by_username"]["buckets"]
            
            for username_bucket in username_buckets:
                username = username_bucket["key"]
                mentions = username_bucket["doc_count"]
                subscribers = username_bucket["subscribers"]["value"]
                followers = username_bucket["followers"]["value"]
                connections = username_bucket["connections"]["value"]
                influence_score = username_bucket["influence_score"]["value"] or 0
                reach = username_bucket["total_reach"]["value"]
                
                # Ambil user_image_url dari top_hits
                user_image_url = None
                if "top_hits" in username_bucket and username_bucket["top_hits"]["hits"]["hits"]:
                    hit = username_bucket["top_hits"]["hits"]["hits"][0]
                    if "_source" in hit and "user_image_url" in hit["_source"]:
                        user_image_url = hit["_source"]["user_image_url"]
                
                # Buat fallback user_image_url untuk channel news jika tidak ada
                if channel == "news" and not user_image_url:
                    domain_name = username.replace("www.", "")
                    user_image_url = f"https://logo.clearbit.com/{domain_name}"
                
                followers_data.append({
                    "channel": channel,
                    "username": username,
                    "followers": followers or connections or subscribers or 0,
                    "influence_score": influence_score,
                    "total_mentions": mentions,
                    "total_reach": reach,
                    "user_image_url": user_image_url
                })
        
        # Sortir berdasarkan jumlah followers
        followers_data.sort(key=lambda x: x["followers"], reverse=True)
        
        # Pagination
        total_items = len(followers_data)
        total_pages = (total_items + page_size - 1) // page_size  # ceiling division
        
        start_index = (page - 1) * page_size
        end_index = min(start_index + page_size, total_items)
        
        paginated_data = followers_data[start_index:end_index]
        
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
        
        redis_client.set_with_ttl(cache_key, result, ttl_seconds=100)
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