from utils.list_of_mentions import get_mentions
from utils.gemini import call_gemini
from utils.redis_client import redis_client
import pandas as pd
import re
import json

def get_topics_sentiment_analysis(
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
    domain=None
):

    # Generate cache key based on all parameters
    cache_key = redis_client.generate_cache_key(
        "topics_sentiment_analysis",
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
        domain=domain
    )

    # Try to get from cache first
    cached_result = redis_client.get(cache_key)
    if cached_result is not None:
        print('Returning cached result')
        return cached_result

    # Mendapatkan post positif
    post_positive = get_mentions(
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
        sentiment=['positive'],
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
        sort_type="viral_score",  # Sort berdasarkan viral_score
        sort_order="desc",
    page=1,
    page_size=50
    )

    # Mendapatkan post negatif
    post_negative = get_mentions(
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
        sentiment=['negative'],
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
        sort_type="viral_score",  # Sort berdasarkan viral_score
        sort_order="desc",
        page=1,
    page_size=50
    )

    # Menyusun prompt untuk Gemini
    prompt = f"""You are a Social Media Analyst Expert. Your task is to analyze and summarize the content based on the list of social media posts provided below. The posts are divided into two categories based on sentiment:

    POSITIVE POSTS
    {post_positive['data']}

    NEGATIVE POSTS
    {post_negative['data']}

    OUTPUT (in JSON format):
    {{
      "positive_topics": "<Provide a concise summary (2–3 sentences) that captures the key topics or themes discussed in the positive posts.>",
      "negative_topics": "<Provide a concise summary (2–3 sentences) that captures the key topics or concerns raised in the negative posts.>"
    }}
    """

    # Memanggil Gemini API
    prediction = call_gemini(prompt)
    
    try:
        # Mencoba parse JSON dari respons
        json_result = re.findall(r'\{.*\}', prediction, flags=re.I|re.S)[0]
        result = json.loads(json_result)
        
        # Cache the results for 10 minutes
        redis_client.set_with_ttl(cache_key, result, ttl_seconds=100)
        return result
    except (json.JSONDecodeError, IndexError) as e:
        # Menangani error parsing
        return {
            "positive_topics": "Error analyzing positive topics.",
            "negative_topics": "Error analyzing negative topics.",
            "error": str(e)
        }
