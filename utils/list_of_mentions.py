"""
list_of_mentions.py - Script untuk mengambil daftar mentions dari Elasticsearch

Script ini mengambil daftar mentions/posts dari berbagai platform
media sosial dengan dukungan pagination dan berbagai opsi sorting.
"""

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

def get_mentions(
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
    sort_type="recent",  # 'popular', 'recent', atau 'relevant'
    sort_order="desc",
    page=1,
    page_size=10,
    source=None  # Parameter baru untuk memilih field yang akan diambil
):
    """
    Mengambil daftar mentions/posts dari Elasticsearch dengan pagination
    
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
    sort_type : str, optional
        Tipe pengurutan ('popular', 'recent', atau 'relevant')
    sort_order : str, optional
        Urutan pengurutan ('asc' atau 'desc')
    page : int, optional
        Nomor halaman (dimulai dari 1)
    page_size : int, optional
        Jumlah post per halaman
    source : list or str, optional
        Field yang akan diambil dari Elasticsearch, jika None akan mengambil semua field
        
    Returns:
    --------
    dict
        Dictionary berisi posts dan informasi pagination:
        {
            'posts': [...],  # Daftar post
            'pagination': {
                'page': 1,  # Halaman saat ini
                'page_size': 10,  # Jumlah post per halaman
                'total_pages': 5,  # Total halaman
                'total_posts': 45  # Total post
            }
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
    elif sort_type == "relevant":
        # Untuk relevant, akan menggunakan _score dari Elasticsearch
        pass
    
    # Bangun query
    if sort_type == "relevant" and keywords:
        # Untuk relevant sorting
        query = {
            "size": page_size,
            "from": (page - 1) * page_size,
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
                    ],
                    "should": [],
                    "minimum_should_match": 1,
                    "filter": []
                }
            }
        }
        
        # Tambahkan filter keyword berdasarkan search_exact_phrases
        if keywords:
            # Konversi keywords ke list jika belum
            keyword_list = keywords if isinstance(keywords, list) else [keywords]
            
            # Logic untuk post_caption
            if search_exact_phrases:
                # Menggunakan match_phrase dengan boost
                if case_sensitive:
                    caption_should = [
                        {"match_phrase": {"post_caption.keyword": {"query": kw, "boost": 2.0}}} 
                        for kw in keyword_list
                    ]
                    issue_should = [
                        {"match_phrase": {"issue.keyword": {"query": kw, "boost": 1.5}}} 
                        for kw in keyword_list
                    ]
                else:
                    caption_should = [
                        {"match_phrase": {"post_caption": {"query": kw, "boost": 2.0}}} 
                        for kw in keyword_list
                    ]
                    issue_should = [
                        {"match_phrase": {"issue": {"query": kw, "boost": 1.5}}} 
                        for kw in keyword_list
                    ]
                
                query["query"]["bool"]["should"].extend(caption_should)
                query["query"]["bool"]["should"].extend(issue_should)
            else:
                # Menggunakan match dengan operator AND
                if case_sensitive:
                    caption_should = [
                        {"match": {"post_caption.keyword": {"query": kw, "operator": "AND", "boost": 2.0}}} 
                        for kw in keyword_list
                    ]
                    issue_should = [
                        {"match": {"issue.keyword": {"query": kw, "operator": "AND", "boost": 1.5}}} 
                        for kw in keyword_list
                    ]
                else:
                    caption_should = [
                        {"match": {"post_caption": {"query": kw, "operator": "AND", "boost": 2.0}}} 
                        for kw in keyword_list
                    ]
                    issue_should = [
                        {"match": {"issue": {"query": kw, "operator": "AND", "boost": 1.5}}} 
                        for kw in keyword_list
                    ]
                
                query["query"]["bool"]["should"].extend(caption_should)
                query["query"]["bool"]["should"].extend(issue_should)
        
        # Tambahkan filter sentiment jika ada
        if sentiment:
            sentiment_filter = {
                "terms": {
                    "sentiment": sentiment
                }
            }
            query["query"]["bool"]["filter"].append(sentiment_filter)
        
        # Tambahkan filter importance
        if importance == "important mentions":
            query["query"]["bool"]["filter"].append({
                "range": {
                    "influence_score": {
                        "gt": 50
                    }
                }
            })
        
        # Tambahkan filter influence score jika ada
        if influence_score_min is not None or influence_score_max is not None:
            influence_range = {"range": {"influence_score": {}}}
            if influence_score_min is not None:
                influence_range["range"]["influence_score"]["gte"] = influence_score_min
            if influence_score_max is not None:
                influence_range["range"]["influence_score"]["lte"] = influence_score_max
            query["query"]["bool"]["filter"].append(influence_range)
        
        # Tambahkan filter region jika ada
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
            query["query"]["bool"]["filter"].append(region_filter)
        
        # Tambahkan filter language jika ada
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
            query["query"]["bool"]["filter"].append(language_filter)
        
        # Tambahkan filter domain jika ada
        if domain:
            domain_filter = {
                "bool": {
                    "should": [{"wildcard": {"link_post": f"*{d}*"}} for d in domain],
                    "minimum_should_match": 1
                }
            }
            query["query"]["bool"]["filter"].append(domain_filter)
    else:
        # Bangun query biasa untuk non-relevant sort
        query = build_elasticsearch_query(
            keywords=keywords,
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
                            "script": {
                                "lang": "painless",
                                "params": {
                                "whitelist": [
                                    "kompas.com", "detik.com", "cnnindonesia.com", "cnbcindonesia.com", "suara.com", "tribunnews.com",
                                    "liputan6.com", "katadata.co.id", "apnews.com", "dawn.com", "republika.co.id", "viva.co.id",
                                    "idntimes.com", "mediaindonesia.com", "okezone.com", "tvonenews.com", "jpnn.com", "antaranews.com"
                                ],
                                "max_val": 500.0
                                },
                                "source": """
                                double logNorm(def val, double max) {
                                    return Math.log(1 + val) / Math.log(1 + max);
                                }

                                String channel = doc.containsKey('channel') && !doc['channel'].empty ? doc['channel'].value : "";
                                double likes = doc.containsKey('likes') && !doc['likes'].empty ? doc['likes'].value : 0;
                                double comments = doc.containsKey('comments') && !doc['comments'].empty ? doc['comments'].value : 0;
                                double replies = doc.containsKey('replies') && !doc['replies'].empty ? doc['replies'].value : 0;
                                double retweets = doc.containsKey('retweets') && !doc['retweets'].empty ? doc['retweets'].value : 0;
                                double reposts = doc.containsKey('reposts') && !doc['reposts'].empty ? doc['reposts'].value : 0;
                                double shares = doc.containsKey('shares') && !doc['shares'].empty ? doc['shares'].value : 0;
                                double favorites = doc.containsKey('favorites') && !doc['favorites'].empty ? doc['favorites'].value : 0;
                                double votes = doc.containsKey('votes') && !doc['votes'].empty ? doc['votes'].value : 0;
                                double views = doc.containsKey('views') && !doc['views'].empty ? doc['views'].value : 0;
                                double score = 0;

                                if (channel == 'twitter') {
                                    double E = logNorm(likes, params.max_val) * 0.4 + logNorm(replies, params.max_val) * 0.3 + logNorm(retweets, params.max_val) * 0.3;
                                    double R = logNorm(views, params.max_val);
                                    score = (0.6 * E + 0.4 * R) * 10;
                                } else if (channel == 'linkedin') {
                                    double E = logNorm(likes, params.max_val) * 0.5 + logNorm(comments, params.max_val) * 0.3;
                                    double R = logNorm(reposts, params.max_val) * 0.2;
                                    score = (0.6 * E + 0.4 * R) * 10;
                                } else if (channel == 'tiktok') {
                                    double E = logNorm(likes, params.max_val) * 0.4 + logNorm(comments, params.max_val) * 0.3 + logNorm(favorites, params.max_val) * 0.1;
                                    double R = logNorm(shares, params.max_val) * 0.2;
                                    score = (0.6 * E + 0.4 * R) * 10;
                                } else if (channel == 'instagram') {
                                    if (views > 0) {
                                    double E = logNorm(likes, params.max_val) * 0.5 + logNorm(comments, params.max_val) * 0.3;
                                    double R = logNorm(views, params.max_val) * 0.2;
                                    score = (0.6 * E + 0.4 * R) * 10;
                                    } else {
                                    double E = logNorm(likes, params.max_val) * 0.6 + logNorm(comments, params.max_val) * 0.4;
                                    score = 0.6 * E * 10;
                                    }
                                } else if (channel == 'reddit') {
                                    double E = logNorm(votes, params.max_val) * 0.6;
                                    double R = logNorm(comments, params.max_val) * 0.4;
                                    score = (0.6 * E + 0.4 * R) * 10;
                                } else if (channel == 'youtube') {
                                    double E = logNorm(likes, params.max_val) * 0.4 + logNorm(comments, params.max_val) * 0.2;
                                    double R = logNorm(views, params.max_val) * 0.4;
                                    score = (0.6 * E + 0.4 * R) * 10;
                                } else if (channel == 'news') {
                                    String username = doc.containsKey('username.keyword') && !doc['username.keyword'].empty ? doc['username.keyword'].value : "";
                                    double A = params.whitelist.contains(username) ? 1.0 : 0.0;
                                    double M = (doc.containsKey('post_media_link') && !doc['post_media_link'].empty && doc['post_media_link'].value.contains("http")) ? 1.0 : 0.0;
                                    double Q = doc.containsKey('list_quotes') && !doc['list_quotes'].empty && doc['list_quotes'].value.contains("quotes") ? 1.0 : 0.0;
                                    score = (0.6 * A + 0.2 * M + 0.2 * Q) * 10;
                                }

                                return Math.min(score, 10.0);
                                """
                            },
                            "order": sort_order
                            }
                        }
                        ]


            else:
                query["sort"] = [
                    {sort_field: {"order": sort_order}}
                ]
    
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
        response = es.search(
            index=",".join(indices),
            body=query
        )
        
        # Dapatkan posts
        posts = [hit["_source"] for hit in response["hits"]["hits"]]
  
        for i in posts:
            if i['channel'] == 'news':
                if 'username' not in i:
                    username = re.findall(r'https?://(.*?)(?:/|$)',i['link_post'])[0].replace('www.','')
                    i.update({'username':username})

                if 'user_image_url' not in i:
                    i.update({"user_image_url":f"https://logo.clearbit.com/{i['username']}"})

        from utils.influence_score import get_influence_score
        for i in posts:
            i.update({'influence_score':get_influence_score(i)})

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
        
        return {
            'data': posts,
            'pagination': pagination
        }
        
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return {'posts': [], 'pagination': {'page': page, 'page_size': page_size, 'total_pages': 0, 'total_posts': 0}}
