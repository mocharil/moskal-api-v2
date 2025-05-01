"""
popular_emojis.py - Script untuk mendapatkan emoji terpopuler dari data Elasticsearch

Script ini menganalisis data dari Elasticsearch untuk mengekstrak dan menghitung
emoji yang paling sering digunakan dalam post_caption, dengan dukungan pagination.
"""

import json
import re
import unicodedata
from datetime import datetime
from typing import Dict, List, Literal, Optional, Union

# Import utilitas dari paket utils
from utils.es_client import get_elasticsearch_client
from utils.es_query_builder import get_date_range

def extract_emojis(text):
    """
    Extract emojis from text
    
    Parameters:
    -----------
    text : str
        Text to extract emojis from
        
    Returns:
    --------
    list
        List of emojis found in the text
    """
    if not text or not isinstance(text, str):
        return []
    
    # Emoji pattern - covers most common emojis
    emoji_pattern = re.compile(
        "["
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F700-\U0001F77F"  # alchemical symbols
        "\U0001F780-\U0001F7FF"  # Geometric Shapes
        "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
        "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
        "\U0001FA00-\U0001FA6F"  # Chess Symbols
        "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
        "\U00002702-\U000027B0"  # Dingbats
        "\U000024C2-\U0001F251" 
        "]+"
    )
    
    # Find all emojis
    emojis = emoji_pattern.findall(text)
    
    # Return individual emojis
    result = []
    for emoji_str in emojis:
        # Split combined emojis
        for char in emoji_str:
            if unicodedata.category(char).startswith('So') or unicodedata.category(char).startswith('Sm'):
                result.append(char)
    
    return result

def get_popular_emojis(
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
    limit=100,       # Total emojis to analyze
    page=1,          # Current page number
    page_size=10     # Number of items per page
) -> Dict:
    """
    Mendapatkan daftar emoji terpopuler dengan dukungan pagination
    
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
        Jumlah total emoji yang akan dianalisis
    page : int, optional
        Nomor halaman saat ini (dimulai dari 1)
    page_size : int, optional
        Jumlah item per halaman
        
    Returns:
    --------
    Dict
        Dictionary berisi daftar emoji terpopuler dengan informasi pagination:
        {
            "data": [
                {
                    "emoji": "😂",
                    "total_mentions": 346
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
            "period": {
                "start_date": "2025-01-01",
                "end_date": "2025-04-30"
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
    default_channels = ["twitter", "linkedin", "reddit", "youtube", "news"]
    
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
    
    # Bangun query untuk mendapatkan data post_caption yang berisi emoji
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
                "field": "post_caption"
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
    
    # Kita perlu mengambil konten post_caption untuk ekstraksi emoji
    query = {
        "query": {
            "bool": {
                "must": must_conditions
            }
        },
        "_source": ["post_caption"],
        "size": 1000  # Mengambil 1000 dokumen per scroll
    }
    
    # Tambahkan filter jika ada
    if filter_conditions:
        query["query"]["bool"]["filter"] = filter_conditions
    
    try:
        # Gunakan scan/scroll untuk mendapatkan sejumlah besar dokumen
        emoji_counts = {}
        total_processed = 0
        max_docs = 10000  # Batasi jumlah dokumen yang diproses (opsional)
        
        # Lakukan scroll pertama
        resp = es.search(
            index=",".join(indices),
            body=query,
            scroll="2m"
        )
        
        scroll_id = resp["_scroll_id"]
        scroll_size = len(resp["hits"]["hits"])
        
        # Proses batch pertama
        for hit in resp["hits"]["hits"]:
            post_caption = hit["_source"].get("post_caption", "")
            emojis = extract_emojis(post_caption)
            
            for emoji in emojis:
                if emoji in emoji_counts:
                    emoji_counts[emoji] += 1
                else:
                    emoji_counts[emoji] = 1
                    
        total_processed += scroll_size
        
        # Lakukan scroll selanjutnya
        while scroll_size > 0 and total_processed < max_docs:
            resp = es.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = resp["_scroll_id"]
            scroll_size = len(resp["hits"]["hits"])
            
            # Proses batch ini
            for hit in resp["hits"]["hits"]:
                post_caption = hit["_source"].get("post_caption", "")
                emojis = extract_emojis(post_caption)
                
                for emoji in emojis:
                    if emoji in emoji_counts:
                        emoji_counts[emoji] += 1
                    else:
                        emoji_counts[emoji] = 1
                        
            total_processed += scroll_size
            
            # Print status
            print(f"Processed {total_processed} documents, found {len(emoji_counts)} unique emojis")
            
            # Jika tidak ada lagi hasil, keluar dari loop
            if not resp["hits"]["hits"]:
                break
        
        # Konversi ke format yang diinginkan
        emoji_data = [
            {"emoji": emoji, "total_mentions": count}
            for emoji, count in emoji_counts.items()
        ]
        
        # Urutkan berdasarkan jumlah mentions
        emoji_data.sort(key=lambda x: x["total_mentions"], reverse=True)
        
        # Batasi jumlah emoji jika diperlukan
        if limit and len(emoji_data) > limit:
            emoji_data = emoji_data[:limit]
        
        # Hitung nilai pagination
        total_items = len(emoji_data)
        total_pages = (total_items + page_size - 1) // page_size  # ceiling division
        
        # Validasi nomor halaman
        if page < 1:
            page = 1
        elif page > total_pages and total_pages > 0:
            page = total_pages
            
        # Terapkan pagination
        start_index = (page - 1) * page_size
        end_index = min(start_index + page_size, total_items)
        
        paginated_data = emoji_data[start_index:end_index]
        
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
            "total_unique_emojis": len(emoji_counts),
            "period": {
                "start_date": start_date,
                "end_date": end_date
            },
            "total_documents_processed": total_processed
        }
        
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
            },
            "error": str(e)
        }