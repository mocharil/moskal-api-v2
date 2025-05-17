import json
from datetime import datetime, timedelta
from typing import Dict

# Import utilitas dari paket utils
from utils.es_client import get_elasticsearch_client
from utils.es_query_builder import get_date_range
from utils.redis_client import redis_client

def get_stats_summary(
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
    compare_with_previous=True  # Compare with previous period
) -> Dict:

    cache_key = redis_client.generate_cache_key(
        "get_stats_summary",
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
        compare_with_previous=compare_with_previous
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
        return {}
    
    # Definisikan kategori channels
    default_non_social_channels = ["news"]
    default_social_media_channels = ["twitter", "linkedin","youtube", "reddit","tiktok", "instagram", "facebook"]
    default_video_channels = ["youtube","tiktok"]
    default_all_channels = list(set(default_non_social_channels + default_social_media_channels + default_video_channels))
    
    # Mapping channel ke index Elasticsearch
    channel_to_index = {
        "twitter": "twitter_data",
        "instagram": "instagram_data",
        "linkedin": "linkedin_data",
        "reddit": "reddit_data",
        "youtube": "youtube_data",
        "tiktok": "tiktok_data",
        "news": "news_data",
        "facebook": "facebook_data"
    }
    
    # Filter channels jika disediakan
    if channels:
        selected_channels = [ch for ch in channels if ch in default_all_channels]
        # Update kategori channels berdasarkan filter
        non_social_channels = [ch for ch in default_non_social_channels if ch in selected_channels]
        social_media_channels = [ch for ch in default_social_media_channels if ch in selected_channels]
        video_channels = [ch for ch in default_video_channels if ch in selected_channels]
    else:
        selected_channels = default_all_channels
        non_social_channels = default_non_social_channels
        social_media_channels = default_social_media_channels
        video_channels = default_video_channels
    
    # Dapatkan indeks berdasarkan channels
    all_indices = [channel_to_index[ch] for ch in selected_channels if ch in channel_to_index]
    non_social_indices = [channel_to_index[ch] for ch in non_social_channels if ch in channel_to_index]
    social_media_indices = [channel_to_index[ch] for ch in social_media_channels if ch in channel_to_index]
    video_indices = [channel_to_index[ch] for ch in video_channels if ch in channel_to_index]
    
    if not all_indices:
        print("Error: No valid indices")
        return {}
    
    # Dapatkan rentang tanggal jika tidak disediakan
    if not start_date or not end_date:
        start_date, end_date = get_date_range(
            date_filter=date_filter,
            custom_start_date=custom_start_date,
            custom_end_date=custom_end_date
        )
    
    # Parse tanggal
    current_start = datetime.strptime(start_date, "%Y-%m-%d")
    current_end = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Hitung durasi periode
    period_duration = (current_end - current_start).days + 1
    
    # Hitung tanggal untuk periode sebelumnya
    previous_end = current_start - timedelta(days=1)
    previous_start = previous_end - timedelta(days=period_duration - 1)
    
    # Format tanggal periode sebelumnya
    previous_start_str = previous_start.strftime("%Y-%m-%d")
    previous_end_str = previous_end.strftime("%Y-%m-%d")
    
    # Bangun query dasar
    def build_base_query(query_start_date, query_end_date):
        must_conditions = [
            {
                "range": {
                    "post_created_at": {
                        "gte": query_start_date,
                        "lte": query_end_date
                    }
                }
            }
        ]
        
        # Add keyword and search_keyword filters
        if keywords or search_keyword:
            must_conditions_inner = []
            
            # Handle regular keywords
            if keywords:
                keyword_list = keywords if isinstance(keywords, list) else [keywords]
                keyword_should_conditions = []
                caption_field = "post_caption.keyword" if case_sensitive else "post_caption"
                issue_field = "issue.keyword" if case_sensitive else "issue"
                if search_exact_phrases:
                    for kw in keyword_list:
                        keyword_should_conditions.extend([
                            {"match_phrase": {caption_field: kw}},
                            {"match_phrase": {issue_field: kw}}
                        ])
                else:
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
                search_keyword_list = search_keyword if isinstance(search_keyword, list) else [search_keyword]
                search_keyword_should_conditions = []
                caption_field = "post_caption.keyword" if case_sensitive else "post_caption"
                issue_field = "issue.keyword" if case_sensitive else "issue"
                if search_exact_phrases:
                    for sk in search_keyword_list:
                        search_keyword_should_conditions.extend([
                            {"match_phrase": {caption_field: sk}},
                            {"match_phrase": {issue_field: sk}}
                        ])
                else:
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
            must_conditions.append({
                "bool": {
                    "must": must_conditions_inner
                }
            })
        filter_conditions = []
        if importance == "important mentions":
            filter_conditions.append({
                "range": {
                    "influence_score": {
                        "gt": 50
                    }
                }
            })
        if influence_score_min is not None or influence_score_max is not None:
            influence_condition = {"range": {"influence_score": {}}}
            if influence_score_min is not None:
                influence_condition["range"]["influence_score"]["gte"] = influence_score_min
            if influence_score_max is not None:
                influence_condition["range"]["influence_score"]["lte"] = influence_score_max
            filter_conditions.append(influence_condition)
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
        query = {
            "size": 0,
            "query": {
                "bool": {
                    "must": must_conditions
                }
            }
        }
        if filter_conditions:
            query["query"]["bool"]["filter"] = filter_conditions
        return query

    # Build query for video content that includes "video" in the link
    def build_video_query(query):
        video_query = json.loads(json.dumps(query))
        video_condition = {
            "wildcard": {
                "link_post": "*/*"
            }
        }
        if "filter" not in video_query["query"]["bool"]:
            video_query["query"]["bool"]["filter"] = []
        video_query["query"]["bool"]["filter"].append(video_condition)
        return video_query

    # Buat query untuk mendapatkan metrik dengan sub-agg likes & shares
    def build_metrics_query(base_query):
        query = json.loads(json.dumps(base_query))
        query["aggs"] = {
            "total_mentions": {
                "value_count": {
                    "field": "link_post"
                }
            },
            "time_series": {
                "date_histogram": {
                    "field": "post_created_at",
                    "calendar_interval": "day",
                    "format": "yyyy-MM-dd"
                },
                "aggs": {
                    "sum_likes": {
                        "sum": {
                            "field": "likes"
                        }
                    },
                    "sum_shares": {
                        "sum": {
                            "script": {
                                "source": """
                                    long shares = 0;
                                    if (doc.containsKey('shares') && !doc['shares'].empty) {
                                        shares += doc['shares'].value;
                                    }
                                    if (doc.containsKey('retweets') && !doc['retweets'].empty) {
                                        shares += doc['retweets'].value;
                                    }
                                    if (doc.containsKey('reposts') && !doc['reposts'].empty) {
                                        shares += doc['reposts'].value;
                                    }
                                    return shares;
                                """
                            }
                        }
                    }
                }
            },
            "total_likes": {
                "sum": {
                    "field": "likes"
                }
            },
            "total_comments": {
                "sum": {
                    "field": "comments"
                }
            },
            "total_shares": {
                "sum": {
                    "script": {
                        "source": """
                            long shares = 0;
                            if (doc.containsKey('shares') && !doc['shares'].empty) {
                                shares += doc['shares'].value;
                            }
                            if (doc.containsKey('retweets') && !doc['retweets'].empty) {
                                shares += doc['retweets'].value;
                            }
                            if (doc.containsKey('reposts') && !doc['reposts'].empty) {
                                shares += doc['reposts'].value;
                            }
                            return shares;
                        """
                    }
                }
            }
        }
        return query

    # Fungsi untuk menghitung growth
    def calculate_growth(current_value, previous_value):
        current_value = current_value or 0  # Convert None to 0
        result = {
            "value": current_value,
            "display": format_number(current_value),
            "growth": None,
            "growth_display": "N/A",
            "growth_percentage": None,
            "growth_percentage_display": "N/A"
        }
        if previous_value is not None and previous_value != 0:
            growth_value = current_value - previous_value
            growth_percentage = (growth_value / previous_value) * 100
            result["growth"] = growth_value
            result["growth_display"] = format_growth(growth_value)
            result["growth_percentage"] = round(growth_percentage)
            result["growth_percentage_display"] = format_percentage(growth_percentage)
        return result

    # Format number to K, M format
    def format_number(value):
        if value is None:
            return "0"
        value = float(value)  # Ensure it's a float for division
        if value >= 1_000_000:
            return f"{value/1_000_000:.1f}M"
        elif value >= 1_000:
            return f"{value/1_000:.1f}K"
        else:
            return str(int(value))

    def format_growth(value):
        if value is None:
            return "N/A"
        formatted = format_number(abs(value))
        if value > 0:
            return f"+{formatted}"
        elif value < 0:
            return f"-{formatted}"
        else:
            return "0"

    def format_percentage(value):
        if value is None:
            return "N/A"
        if value > 0:
            return f"+{round(value)}%"
        elif value < 0:
            return f"{round(value)}%"
        else:
            return "0%"

    # === Bangun query untuk periode saat ini ===
    current_base_query = build_base_query(start_date, end_date)
    current_metrics_query = build_metrics_query(current_base_query)
    current_video_query = build_video_query(current_metrics_query)

    # === Bangun query untuk periode sebelumnya (jika perlu) ===
    if compare_with_previous:
        previous_base_query = build_base_query(previous_start_str, previous_end_str)
        previous_metrics_query = build_metrics_query(previous_base_query)
        previous_video_query = build_video_query(previous_metrics_query)
    
    # === QUERY UNTUK NON-SOCIAL MEDIA ===
    if non_social_indices:
        # Current period
        current_non_social_response = es.search(
            index=",".join(non_social_indices),
            body=current_metrics_query
        )
        current_non_social_mentions = current_non_social_response["aggregations"]["total_mentions"]["value"]
        current_non_social_time_series = [{
            "date": bucket["key_as_string"],
            "value": bucket["doc_count"]
        } for bucket in current_non_social_response["aggregations"]["time_series"]["buckets"]]
        # Previous period (if needed)
        previous_non_social_mentions = 0
        if compare_with_previous and non_social_indices:
            previous_non_social_response = es.search(
                index=",".join(non_social_indices),
                body=previous_metrics_query
            )
            previous_non_social_mentions = previous_non_social_response["aggregations"]["total_mentions"]["value"]
    else:
        current_non_social_mentions = 0
        current_non_social_time_series = []
        previous_non_social_mentions = 0

    # === QUERY UNTUK SOCIAL MEDIA ===
    if social_media_indices:
        # Current period
        current_social_response = es.search(
            index=",".join(social_media_indices),
            body=current_metrics_query
        )
        current_social_mentions = current_social_response["aggregations"]["total_mentions"]["value"]
        current_social_likes = current_social_response["aggregations"]["total_likes"]["value"]
        current_social_shares = current_social_response["aggregations"]["total_shares"]["value"]
        # --- PATCH: likes & shares per bucket
        current_social_time_series = [{
            "date": bucket["key_as_string"],
            "value": bucket["doc_count"],
            "likes": bucket["sum_likes"]["value"],
            "shares": bucket["sum_shares"]["value"]
        } for bucket in current_social_response["aggregations"]["time_series"]["buckets"]]
        # Previous period (if needed)
        previous_social_mentions = 0
        previous_social_likes = 0
        previous_social_shares = 0
        if compare_with_previous and social_media_indices:
            previous_social_response = es.search(
                index=",".join(social_media_indices),
                body=previous_metrics_query
            )
            previous_social_mentions = previous_social_response["aggregations"]["total_mentions"]["value"]
            previous_social_likes = previous_social_response["aggregations"]["total_likes"]["value"]
            previous_social_shares = previous_social_response["aggregations"]["total_shares"]["value"]
    else:
        current_social_mentions = 0
        current_social_likes = 0
        current_social_shares = 0
        current_social_time_series = []
        previous_social_mentions = 0
        previous_social_likes = 0
        previous_social_shares = 0

    # === QUERY UNTUK VIDEO ===
    if video_indices:
        # Current period
        current_video_response = es.search(
            index=",".join(video_indices),
            body=current_video_query
        )
        current_video_mentions = current_video_response["aggregations"]["total_mentions"]["value"]
        current_video_time_series = [{
            "date": bucket["key_as_string"],
            "value": bucket["doc_count"]
        } for bucket in current_video_response["aggregations"]["time_series"]["buckets"]]
        # Previous period (if needed)
        previous_video_mentions = 0
        if compare_with_previous and video_indices:
            previous_video_response = es.search(
                index=",".join(video_indices),
                body=previous_video_query
            )
            previous_video_mentions = previous_video_response["aggregations"]["total_mentions"]["value"]
    else:
        current_video_mentions = 0
        current_video_time_series = []
        previous_video_mentions = 0

    # === HITUNG GROWTH ===
    non_social_mentions_data = calculate_growth(current_non_social_mentions, previous_non_social_mentions)
    social_media_mentions_data = calculate_growth(current_social_mentions, previous_social_mentions)
    video_mentions_data = calculate_growth(current_video_mentions, previous_video_mentions)
    social_media_shares_data = calculate_growth(current_social_shares, previous_social_shares)
    social_media_likes_data = calculate_growth(current_social_likes, previous_social_likes)

    # === MEMBUAT HASIL ===
    result = {
        "non_social_mentions": non_social_mentions_data,
        "social_media_mentions": social_media_mentions_data,
        "video_mentions": video_mentions_data,
        "social_media_shares": social_media_shares_data,
        "social_media_likes": social_media_likes_data,
        "period": {
            "current": {
                "start_date": start_date,
                "end_date": end_date
            },
            "previous": {
                "start_date": previous_start_str,
                "end_date": previous_end_str
            } if compare_with_previous else None
        },
        "time_series": {
            "non_social_mentions": current_non_social_time_series,
            "social_media_mentions": current_social_time_series,
            "video_mentions": current_video_time_series,
            # Tambahan array terpisah per tanggal
            "social_media_shares": [
                {"date": x["date"], "value": x["shares"]}
                for x in current_social_time_series
            ],
            "social_media_likes": [
                {"date": x["date"], "value": x["likes"]}
                for x in current_social_time_series
            ]
        }
    }
    redis_client.set_with_ttl(cache_key, result, ttl_seconds=100)
    return result
