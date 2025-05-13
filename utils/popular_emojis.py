import json
import re
import unicodedata
from datetime import datetime
from typing import Dict, List, Literal, Optional, Union

# Import utilitas dari paket utils
from utils.es_client import get_elasticsearch_client
from utils.es_query_builder import get_date_range
from utils.redis_client import redis_client
from utils.list_of_mentions import get_mentions

def extract_emojis(text):

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

    cache_key = redis_client.generate_cache_key(
        "get_popular_emojis",
        es_host=es_host,
        es_username=es_username,
        es_password=es_password,
        use_ssl=use_ssl,
        verify_certs=verify_certs,
        ca_certs=ca_certs,
        keywords=keywords,
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
        page=page,          # Current page number
        page_size=page_size     # Number of items per page
    )

    # Try to get from cache first
    cached_result = redis_client.get(cache_key)
    if cached_result is not None:
        print('Returning cached result')
        return cached_result

    if not channels:
        channels =['youtube','twitter','tiktok','instagram']

    result = get_mentions(
            source= ["channel",
                    "link_post",
                    "post_created_at",
                        "post_caption"],
            page_size=10000,
            es_host=es_host,    
            es_username=es_username,
            es_password=es_password,
            use_ssl=use_ssl,
            verify_certs=verify_certs,
            ca_certs=ca_certs,
            keywords=keywords,
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
            sort_type = 'popular'
        )

    if not result['data']:
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

    try:
        emoji_counts = {}
        for i in result['data']:
            post_caption = i['post_caption']
        
            emojis = extract_emojis(post_caption)
            
            for emoji in emojis:
                if emoji in emoji_counts:
                    emoji_counts[emoji] += 1
                else:
                    emoji_counts[emoji] = 1
                    
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
        
            
        
        # Buat hasil dengan informasi pagination
        result = {
            "data": emoji_data[:20],
            "pagination": {
                "page": 1,
                "page_size": 20
            }
        }
        
        redis_client.set_with_ttl(cache_key, result, ttl_seconds=600)
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