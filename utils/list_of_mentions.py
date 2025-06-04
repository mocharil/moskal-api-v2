import argparse
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

def get_mentions(
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
    sort_type="recent",  # 'popular', 'recent', atau 'relevant'
    sort_order="desc",
    page=1,
    page_size=10,
    source=None,  # Parameter baru untuk memilih field yang akan diambil
    is_print = False
):
    # Generate cache key based on all parameters
    cache_key = redis_client.generate_cache_key(
        "list_of_mentions",
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
        sort_type=sort_type,
        sort_order=sort_order,
        page=page,
        page_size=page_size,
        source=source
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
        return {'posts': [], 'pagination': {'page': page, 'page_size': page_size, 'total_pages': 0, 'total_posts': 0}}
    
    # Dapatkan indeks dari channel
    indices = get_indices_from_channels(channels)
    
    if not indices:
        print("Error: Tidak ada indeks yang valid")
        return {'posts': [], 'pagination': {'page': page, 'page_size': page_size, 'total_pages': 0, 'total_posts': 0}}
    
    # Dapatkan rentang tanggal jika tidak disediakan
    if not start_date or not end_date:
        start_date, end_date = get_date_range(
            date_filter=date_filter,
            custom_start_date=custom_start_date,
            custom_end_date=custom_end_date
        )
    
    
    # Konversi sort_type ke field sort yang sesuai
    sort_field = None
    if sort_type == "popular":
        sort_field = "viral_score"
    elif sort_type == "recent":
        sort_field = "post_created_at"
    elif sort_type == 'top_profile':
        sort_field = "followers"
    elif sort_type == "relevant":
        sort_field = '_score'

    # Bangun query biasa untuk non-relevant sort
    query = build_elasticsearch_query(
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
        size=page_size
    )
    
    
    # Tambahkan from untuk pagination
    query["from"] = (page - 1) * page_size
    
    # Tambahkan pengurutan jika bukan relevant sort
    if sort_field:
        if sort_field == "viral_score":
            query["sort"] = [
                    {
                        "_script": {
                        "type": "number",
                        "script": script_score,
                        "order": sort_order
                        }
                    }
                    ]

        elif sort_field == "followers":
            print("FOLLOWERS")
            query["sort"] = [
                {"user_followers": {"order": sort_order, "missing": "_last", "unmapped_type": "long"}},
                {"user_connections": {"order": sort_order, "missing": "_last", "unmapped_type": "long"}},
                {"subscriber": {"order": sort_order, "missing": "_last", "unmapped_type": "long"}}
            ]

        else:
            query["sort"] = [
                {sort_field: {"order": sort_order}}
            ]
    
    # Tambahkan script fields untuk menghitung influence_score
    query["script_fields"] = {
        "calculated_influence_score": {
            "script": script_score
        }
    }
    
    # Tambahkan source parameter ke query jika disediakan
    if source is not None:
        query["_source"] = source
    else:
        query["_source"] = {
                                "excludes": ["list_word","post_media_link",
                                "list_comment","post_hashtags","post_mentions",
                                "object","list_quotes"]
                            }

    #Filter hanya data yang ada viral_score dan sentiment saja yang diambil
    mention_filter = {
            "bool": {
            "must": [
                {
                "exists": {
                    "field": "viral_score"
                }
                },
                {
                "exists": {
                    "field": "sentiment"
                }
                }
            ]
        }
    }
    query["query"]["bool"]["filter"].append(mention_filter)

    # Jalankan query
    try:
        if is_print:
            import json
            print(json.dumps(query, indent=2))
        response = es.search(
            index=",".join(indices),
            body=query
        )
        
        # Dapatkan posts dan tambahkan calculated influence score
        posts = []
        for hit in response["hits"]["hits"]:
            post = hit["_source"]
            # Ambil calculated influence score dari script fields
            if "fields" in hit and "calculated_influence_score" in hit["fields"]:
                post['influence_score'] = hit["fields"]["calculated_influence_score"][0]
            else:
                # Fallback jika script field tidak ada
                post['influence_score'] = 0
            posts.append(post)

        for i in posts:
            if i['channel'] == 'news':
                if 'username' not in i:
                    username = re.findall(r'https?://(.*?)(?:/|$)',i['link_post'])[0].replace('www.','')
                    i.update({'username':username})

                if 'user_image_url' not in i:
                    i.update({"user_image_url":f"https://logo.clearbit.com/{i['username']}"})

        # Dapatkan total posts
        total_posts = response["hits"]["total"]["value"]
        
        # Hitung total halaman
        total_pages = (total_posts + page_size - 1) // page_size  # Ceiling division
        
        # Siapkan informasi pagination
        pagination = {
            'page': page,
            'page_size': page_size,
            'total_pages': total_pages,
            'total_posts': total_posts
        }
        
        result = {
            'data': posts,
            'pagination': pagination
        }
        
        # Cache the results for 10 minutes
        redis_client.set_with_ttl(cache_key, result, ttl_seconds=100)
        return result
    
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return {'data': [], 'pagination': {'page': page, 'page_size': page_size, 'total_pages': 0, 'total_posts': 0}}