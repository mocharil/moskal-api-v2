import argparse
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Literal, Optional, Union

# Import utilitas dari paket utils
from utils.es_client import get_elasticsearch_client
from utils.es_query_builder import get_date_range

def get_social_media_matrix(
    es_host=None,
    es_username=None,
    es_password=None,
    use_ssl=False,
    verify_certs=False,
    ca_certs=None,
    sentiment=None,
    keywords=None,
    channels= None,
    search_exact_phrases=False,
    case_sensitive=False,
    start_date=None,
    end_date=None,
    date_filter="last 30 days",
    custom_start_date=None,
    custom_end_date=None,
    importance="all mentions",
    influence_score_min=None,
    influence_score_max=None,
    region=None,
    language=None,
    domain=None,
    compare_with_previous=True  # Menambahkan flag untuk membandingkan dengan periode sebelumnya
):
    """
    Mendapatkan matriks metrik sosial media dari Elasticsearch
    
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
    compare_with_previous : bool, optional
        Bandingkan dengan periode sebelumnya untuk menghitung growth
        
    Returns:
    --------
    dict
        Dictionary berisi matriks metrik sosial media dengan informasi growth
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
    
    # Definisikan social media dan non social media channels
    social_media_channels = ['reddit','youtube','linkedin','twitter',
                             'tiktok','instagram','facebook','threads']
    non_social_media_channels = ["tiktok"]
    all_channels = social_media_channels + non_social_media_channels
    


    
    # Konversi list channels ke format indeks Elasticsearch
    channel_to_index = {
        "twitter": "twitter_data",
        "instagram": "instagram_data",
        "linkedin": "linkedin_data",
        "reddit": "reddit_data",
        "youtube": "youtube_data",
        "tiktok": "tiktok_data",
        "news": "news_data",
        "facebok":"facebook_data"
    }
    
    social_media_indices = [f"{ch}_data" for ch in social_media_channels]
    non_social_media_indices = [f"{ch}_data" for ch in non_social_media_channels]
    all_indices = [f"{ch}_data" for ch in all_channels]
    
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
            
        # Filter untuk sentiment
        if sentiment:
            sentiment_condition = {
                "terms": {
                    "sentiment": sentiment if isinstance(sentiment, list) else [sentiment]
                }
            }
            filter_conditions.append(sentiment_condition)


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
            "size": 0,
            "query": {
                "bool": {
                    "must": must_conditions
                }
            }
        }
        
        # Tambahkan filter jika ada
        if filter_conditions:
            query["query"]["bool"]["filter"] = filter_conditions
            
        return query
    
    # Buat query untuk mendapatkan metrik
    def build_metrics_query(base_query):
        # Salin query dasar
        query = base_query.copy()
        
        # Tambahkan agregasi untuk metrik
        query["aggs"] = {
            # Total mentions
            "total_mentions": {
                "value_count": {
                    "field": "link_post"
                }
            },
            # Total reach
            "total_reach": {
                "sum": {
                    "field": "reach_score"
                }
            },
            # Sentiment breakdown
            "sentiment_breakdown": {
                "terms": {
                    "field": "sentiment",
                    "size": 10
                }
            },
            # Presence score
            "presence_score": {
                "avg": {
                    "field": "influence_score"
                }
            },
            # Social media metrics
            "social_media_interactions": {
                "sum": {
                    "script": {
                        "source": """
                        int interactions = 0;
                        if (doc.containsKey('likes') && !doc['likes'].empty) {
                            interactions += doc['likes'].value;
                        }
                        if (doc.containsKey('comments') && !doc['comments'].empty) {
                            interactions += doc['comments'].value;
                        }
                        if (doc.containsKey('shares') && !doc['shares'].empty) {
                            interactions += doc['shares'].value;
                        }
                        if (doc.containsKey('retweets') && !doc['retweets'].empty) {
                            interactions += doc['retweets'].value;
                        }
                        if (doc.containsKey('replies') && !doc['replies'].empty) {
                            interactions += doc['replies'].value;
                        }
                        if (doc.containsKey('favorites') && !doc['favorites'].empty) {
                            interactions += doc['favorites'].value;
                        }
                        if (doc.containsKey('votes') && !doc['votes'].empty) {
                            interactions += doc['votes'].value;
                        }
                        return interactions;
                        """
                    }
                }
            },
            # Likes
            "total_likes": {
                "sum": {
                    "field": "likes"
                }
            },
            # Comments
            "total_comments": {
                "sum": {
                    "field": "comments"
                }
            },
            # Shares
            "total_shares": {
                "sum": {
                    "field": "shares"
                }
            },
            # Retweets
            "total_retweets": {
                "sum": {
                    "field": "retweets"
                }
            },
            # Replies
            "total_replies": {
                "sum": {
                    "field": "replies"
                }
            },
            # Channel breakdown
            "channel_breakdown": {
                "terms": {
                    "field": "channel",
                    "size": 20
                },
                "aggs": {
                    "reach": {
                        "sum": {
                            "field": "reach_score"
                        }
                    }
                }
            }
        }
        
        return query
    
    # Fungsi untuk menghitung growth
    def calculate_growth(current_value, previous_value):
        if previous_value == 0 or previous_value is None:
            return {"value": current_value, "growth": None, "growth_value": None, "growth_percentage": None}
        
        growth_value = current_value - previous_value
        growth_percentage = (growth_value / previous_value) * 100
        
        return {
            "value": current_value,
            "growth": growth_value,
            "growth_value": growth_value,
            "growth_percentage": growth_percentage
        }
 
    # === PERIODE SAAT INI ===
    # Bangun dan jalankan query untuk semua channels
    current_base_query = build_base_query(start_date, end_date)
    current_metrics_query = build_metrics_query(current_base_query)
    
    current_all_response = es.search(
        index=",".join(all_indices),
        body=current_metrics_query
    )
    
    # Bangun dan jalankan query untuk social media channels
    current_social_response = es.search(
        index=",".join(social_media_indices),
        body=current_metrics_query
    )
    
    # Bangun dan jalankan query untuk non-social media channels (news)
    current_non_social_response = es.search(
        index=",".join(non_social_media_indices),
        body=current_metrics_query
    )
    
    # === PERIODE SEBELUMNYA (jika perlu) ===
    previous_all_response = None
    previous_social_response = None
    previous_non_social_response = None
    
    if compare_with_previous:
        # Bangun dan jalankan query untuk periode sebelumnya
        previous_base_query = build_base_query(previous_start_str, previous_end_str)
        previous_metrics_query = build_metrics_query(previous_base_query)
        

        previous_all_response = es.search(
            index=",".join(all_indices),
            body=previous_metrics_query
        )
        
        previous_social_response = es.search(
            index=",".join(social_media_indices),
            body=previous_metrics_query
        )
        
        previous_non_social_response = es.search(
            index=",".join(non_social_media_indices),
            body=previous_metrics_query
        )
    
    # === EKSTRAK METRIK PERIODE SAAT INI ===
    
    # 1. Total metrics
    current_total_mentions = current_all_response["aggregations"]["total_mentions"]["value"]
    current_total_reach = current_all_response["aggregations"]["total_reach"]["value"]
    
    # 2. Sentiment metrics
    current_sentiment_buckets = {bucket["key"]: bucket["doc_count"] for bucket in current_all_response["aggregations"]["sentiment_breakdown"]["buckets"]}
    current_positive_mentions = current_sentiment_buckets.get("positive", 0)
    current_negative_mentions = current_sentiment_buckets.get("negative", 0)
    current_neutral_mentions = current_sentiment_buckets.get("neutral", 0)
    
    # 3. Presence score
    current_presence_score = current_all_response["aggregations"]["presence_score"]["value"] or 0
    
    # 4. Social media metrics
    current_social_media_mentions = current_social_response["aggregations"]["total_mentions"]["value"]
    current_social_media_reach = current_social_response["aggregations"]["total_reach"]["value"]
    current_social_media_interactions = current_social_response["aggregations"]["social_media_interactions"]["value"]
    
    # Social media engagement metrics
    current_social_media_reactions = (
        current_social_response["aggregations"]["total_likes"]["value"] +
        current_social_response["aggregations"]["total_retweets"]["value"] + 
        (current_social_response["aggregations"].get("total_favorites", {"value": 0})["value"])
    )
    current_social_media_comments = current_social_response["aggregations"]["total_comments"]["value"]
    current_social_media_shares = (
        current_social_response["aggregations"]["total_shares"]["value"] +
        current_social_response["aggregations"]["total_retweets"]["value"]
    )
    
    # 5. Non-social media metrics
    current_non_social_media_mentions = current_non_social_response["aggregations"]["total_mentions"]["value"]
    current_non_social_media_reach = current_non_social_response["aggregations"]["total_reach"]["value"]
    
    # === EKSTRAK METRIK PERIODE SEBELUMNYA (jika ada) ===
    previous_total_mentions = 0
    previous_total_reach = 0
    previous_positive_mentions = 0
    previous_negative_mentions = 0
    previous_presence_score = 0
    previous_social_media_mentions = 0
    previous_social_media_reach = 0
    previous_social_media_reactions = 0
    previous_social_media_comments = 0
    previous_social_media_shares = 0
    previous_non_social_media_mentions = 0
    previous_non_social_media_reach = 0
    previous_social_media_interactions = 0
    
    if compare_with_previous and previous_all_response:
        # 1. Total metrics
        previous_total_mentions = previous_all_response["aggregations"]["total_mentions"]["value"]
        previous_total_reach = previous_all_response["aggregations"]["total_reach"]["value"]
        
        # 2. Sentiment metrics
        previous_sentiment_buckets = {bucket["key"]: bucket["doc_count"] for bucket in previous_all_response["aggregations"]["sentiment_breakdown"]["buckets"]}
        previous_positive_mentions = previous_sentiment_buckets.get("positive", 0)
        previous_negative_mentions = previous_sentiment_buckets.get("negative", 0)
        
        # 3. Presence score
        previous_presence_score = previous_all_response["aggregations"]["presence_score"]["value"] or 0
        
        # 4. Social media metrics
        previous_social_media_mentions = previous_social_response["aggregations"]["total_mentions"]["value"]
        previous_social_media_reach = previous_social_response["aggregations"]["total_reach"]["value"]
        previous_social_media_interactions = previous_social_response["aggregations"]["social_media_interactions"]["value"]
        
        # Social media engagement metrics
        previous_social_media_reactions = (
            previous_social_response["aggregations"]["total_likes"]["value"] +
            previous_social_response["aggregations"]["total_retweets"]["value"] + 
            (previous_social_response["aggregations"].get("total_favorites", {"value": 0})["value"])
        )
        previous_social_media_comments = previous_social_response["aggregations"]["total_comments"]["value"]
        previous_social_media_shares = (
            previous_social_response["aggregations"]["total_shares"]["value"] +
            previous_social_response["aggregations"]["total_retweets"]["value"]
        )
        
        # 5. Non-social media metrics
        previous_non_social_media_mentions = previous_non_social_response["aggregations"]["total_mentions"]["value"]
        previous_non_social_media_reach = previous_non_social_response["aggregations"]["total_reach"]["value"]
    
    # === HITUNG GROWTH ===
    total_mentions_data = calculate_growth(current_total_mentions, previous_total_mentions)
    total_reach_data = calculate_growth(current_total_reach, previous_total_reach)
    positive_mentions_data = calculate_growth(current_positive_mentions, previous_positive_mentions)
    negative_mentions_data = calculate_growth(current_negative_mentions, previous_negative_mentions)
    presence_score_data = calculate_growth(current_presence_score, previous_presence_score)
    social_media_reach_data = calculate_growth(current_social_media_reach, previous_social_media_reach)
    social_media_mentions_data = calculate_growth(current_social_media_mentions, previous_social_media_mentions)
    social_media_reactions_data = calculate_growth(current_social_media_reactions, previous_social_media_reactions)
    social_media_comments_data = calculate_growth(current_social_media_comments, previous_social_media_comments)
    social_media_shares_data = calculate_growth(current_social_media_shares, previous_social_media_shares)
    non_social_media_reach_data = calculate_growth(current_non_social_media_reach, previous_non_social_media_reach)
    non_social_media_mentions_data = calculate_growth(current_non_social_media_mentions, previous_non_social_media_mentions)
    social_media_interactions_data = calculate_growth(current_social_media_interactions, previous_social_media_interactions)
    
    # === MEMBUAT HASIL MATRIKS ===
    # Format nilai
    def format_number(value):
        """Format numbers with K/M suffix"""
        if value is None:
            return None
            
        if value >= 1_000_000:
            return f"{value/1_000_000:.1f}M"
        elif value >= 1_000:
            return f"{value/1_000:.1f}K"
        else:
            return str(int(value))
    
    # Membuat matriks hasil dengan format yang diinginkan
    matrix = {
        "total_mentions": {
            "value": total_mentions_data["value"],
            "display": format_number(total_mentions_data["value"]),
            "growth_value": total_mentions_data["growth_value"],
            "growth_display": f"+{format_number(total_mentions_data['growth_value'])}" if total_mentions_data["growth_value"] is not None and total_mentions_data["growth_value"] >= 0 else format_number(total_mentions_data["growth_value"]),
            "growth_percentage": total_mentions_data["growth_percentage"],
            "growth_percentage_display": f"+{total_mentions_data['growth_percentage']:.0f}%" if total_mentions_data["growth_percentage"] is not None and total_mentions_data["growth_percentage"] >= 0 else f"{total_mentions_data['growth_percentage']:.0f}%" if total_mentions_data["growth_percentage"] is not None else None
        },
        "total_reach": {
            "value": total_reach_data["value"],
            "display": format_number(total_reach_data["value"]),
            "growth_value": total_reach_data["growth_value"],
            "growth_display": f"+{format_number(total_reach_data['growth_value'])}" if total_reach_data["growth_value"] is not None and total_reach_data["growth_value"] >= 0 else format_number(total_reach_data["growth_value"]),
            "growth_percentage": total_reach_data["growth_percentage"],
            "growth_percentage_display": f"+{total_reach_data['growth_percentage']:.0f}%" if total_reach_data["growth_percentage"] is not None and total_reach_data["growth_percentage"] >= 0 else f"{total_reach_data['growth_percentage']:.0f}%" if total_reach_data["growth_percentage"] is not None else None
        },
        "positive_mentions": {
            "value": positive_mentions_data["value"],
            "display": format_number(positive_mentions_data["value"]),
            "growth_value": positive_mentions_data["growth_value"],
            "growth_display": f"+{format_number(positive_mentions_data['growth_value'])}" if positive_mentions_data["growth_value"] is not None and positive_mentions_data["growth_value"] >= 0 else format_number(positive_mentions_data["growth_value"]),
            "growth_percentage": positive_mentions_data["growth_percentage"],
            "growth_percentage_display": f"+{positive_mentions_data['growth_percentage']:.0f}%" if positive_mentions_data["growth_percentage"] is not None and positive_mentions_data["growth_percentage"] >= 0 else f"{positive_mentions_data['growth_percentage']:.0f}%" if positive_mentions_data["growth_percentage"] is not None else None
        },
        "negative_mentions": {
            "value": negative_mentions_data["value"],
            "display": format_number(negative_mentions_data["value"]),
            "growth_value": negative_mentions_data["growth_value"],
            "growth_display": f"+{format_number(negative_mentions_data['growth_value'])}" if negative_mentions_data["growth_value"] is not None and negative_mentions_data["growth_value"] >= 0 else format_number(negative_mentions_data["growth_value"]),
            "growth_percentage": negative_mentions_data["growth_percentage"],
            "growth_percentage_display": f"+{negative_mentions_data['growth_percentage']:.0f}%" if negative_mentions_data["growth_percentage"] is not None and negative_mentions_data["growth_percentage"] >= 0 else f"{negative_mentions_data['growth_percentage']:.0f}%" if negative_mentions_data["growth_percentage"] is not None else None
        },
        "neutral_mentions": {
            "value": current_neutral_mentions,
            "display": format_number(current_neutral_mentions)
        },
        "presence_score": {
            "value": presence_score_data["value"],
            "display": format_number(presence_score_data["value"]),
            "growth_value": presence_score_data["growth_value"],
            "growth_display": f"+{format_number(presence_score_data['growth_value'])}" if presence_score_data["growth_value"] is not None and presence_score_data["growth_value"] >= 0 else format_number(presence_score_data["growth_value"]),
            "growth_percentage": presence_score_data["growth_percentage"],
            "growth_percentage_display": f"+{presence_score_data['growth_percentage']:.0f}%" if presence_score_data["growth_percentage"] is not None and presence_score_data["growth_percentage"] >= 0 else f"{presence_score_data['growth_percentage']:.0f}%" if presence_score_data["growth_percentage"] is not None else None
        },
        "social_media_reach": {
            "value": social_media_reach_data["value"],
            "display": format_number(social_media_reach_data["value"]),
            "growth_value": social_media_reach_data["growth_value"],
            "growth_display": f"+{format_number(social_media_reach_data['growth_value'])}" if social_media_reach_data["growth_value"] is not None and social_media_reach_data["growth_value"] >= 0 else format_number(social_media_reach_data["growth_value"]),
            "growth_percentage": social_media_reach_data["growth_percentage"],
            "growth_percentage_display": f"+{social_media_reach_data['growth_percentage']:.0f}%" if social_media_reach_data["growth_percentage"] is not None and social_media_reach_data["growth_percentage"] >= 0 else f"{social_media_reach_data['growth_percentage']:.0f}%" if social_media_reach_data["growth_percentage"] is not None else None
        },
        "social_media_mentions": {
            "value": social_media_mentions_data["value"],
            "display": format_number(social_media_mentions_data["value"]),
            "growth_value": social_media_mentions_data["growth_value"],
            "growth_display": f"+{format_number(social_media_mentions_data['growth_value'])}" if social_media_mentions_data["growth_value"] is not None and social_media_mentions_data["growth_value"] >= 0 else format_number(social_media_mentions_data["growth_value"]),
            "growth_percentage": social_media_mentions_data["growth_percentage"],
            "growth_percentage_display": f"+{social_media_mentions_data['growth_percentage']:.0f}%" if social_media_mentions_data["growth_percentage"] is not None and social_media_mentions_data["growth_percentage"] >= 0 else f"{social_media_mentions_data['growth_percentage']:.0f}%" if social_media_mentions_data["growth_percentage"] is not None else None
        },
        "social_media_reactions": {
            "value": social_media_reactions_data["value"],
            "display": format_number(social_media_reactions_data["value"]),
            "growth_value": social_media_reactions_data["growth_value"],
            "growth_display": f"+{format_number(social_media_reactions_data['growth_value'])}" if social_media_reactions_data["growth_value"] is not None and social_media_reactions_data["growth_value"] >= 0 else format_number(social_media_reactions_data["growth_value"]),
            "growth_percentage": social_media_reactions_data["growth_percentage"],
            "growth_percentage_display": f"+{social_media_reactions_data['growth_percentage']:.0f}%" if social_media_reactions_data["growth_percentage"] is not None and social_media_reactions_data["growth_percentage"] >= 0 else f"{social_media_reactions_data['growth_percentage']:.0f}%" if social_media_reactions_data["growth_percentage"] is not None else None
        },
        "social_media_comments": {
            "value": social_media_comments_data["value"],
            "display": format_number(social_media_comments_data["value"]),
            "growth_value": social_media_comments_data["growth_value"],
            "growth_display": f"+{format_number(social_media_comments_data['growth_value'])}" if social_media_comments_data["growth_value"] is not None and social_media_comments_data["growth_value"] >= 0 else format_number(social_media_comments_data["growth_value"]),
            "growth_percentage": social_media_comments_data["growth_percentage"],
            "growth_percentage_display": f"+{social_media_comments_data['growth_percentage']:.0f}%" if social_media_comments_data["growth_percentage"] is not None and social_media_comments_data["growth_percentage"] >= 0 else f"{social_media_comments_data['growth_percentage']:.0f}%" if social_media_comments_data["growth_percentage"] is not None else None
        },
        "social_media_shares": {
            "value": social_media_shares_data["value"],
            "display": format_number(social_media_shares_data["value"]),
            "growth_value": social_media_shares_data["growth_value"],
            "growth_display": f"+{format_number(social_media_shares_data['growth_value'])}" if social_media_shares_data["growth_value"] is not None and social_media_shares_data["growth_value"] >= 0 else format_number(social_media_shares_data["growth_value"]),
            "growth_percentage": social_media_shares_data["growth_percentage"],
            "growth_percentage_display": f"+{social_media_shares_data['growth_percentage']:.0f}%" if social_media_shares_data["growth_percentage"] is not None and social_media_shares_data["growth_percentage"] >= 0 else f"{social_media_shares_data['growth_percentage']:.0f}%" if social_media_shares_data["growth_percentage"] is not None else None
        },
        "non_social_media_reach": {
            "value": non_social_media_reach_data["value"],
            "display": format_number(non_social_media_reach_data["value"]),
            "growth_value": non_social_media_reach_data["growth_value"],
            "growth_display": f"+{format_number(non_social_media_reach_data['growth_value'])}" if non_social_media_reach_data["growth_value"] is not None and non_social_media_reach_data["growth_value"] >= 0 else format_number(non_social_media_reach_data["growth_value"]),
            "growth_percentage": non_social_media_reach_data["growth_percentage"],
            "growth_percentage_display": f"+{non_social_media_reach_data['growth_percentage']:.0f}%" if non_social_media_reach_data["growth_percentage"] is not None and non_social_media_reach_data["growth_percentage"] >= 0 else f"{non_social_media_reach_data['growth_percentage']:.0f}%" if non_social_media_reach_data["growth_percentage"] is not None else None
        },
        "non_social_media_mentions": {
            "value": non_social_media_mentions_data["value"],
            "display": format_number(non_social_media_mentions_data["value"]),
            "growth_value": non_social_media_mentions_data["growth_value"],
            "growth_display": f"+{format_number(non_social_media_mentions_data['growth_value'])}" if non_social_media_mentions_data["growth_value"] is not None and non_social_media_mentions_data["growth_value"] >= 0 else format_number(non_social_media_mentions_data["growth_value"]),
            "growth_percentage": non_social_media_mentions_data["growth_percentage"],
            "growth_percentage_display": f"+{non_social_media_mentions_data['growth_percentage']:.0f}%" if non_social_media_mentions_data["growth_percentage"] is not None and non_social_media_mentions_data["growth_percentage"] >= 0 else f"{non_social_media_mentions_data['growth_percentage']:.0f}%" if non_social_media_mentions_data["growth_percentage"] is not None else None
        },
        "total_social_media_interactions": {
            "value": social_media_interactions_data["value"],
            "display": format_number(social_media_interactions_data["value"]),
            "growth_value": social_media_interactions_data["growth_value"],
            "growth_display": f"+{format_number(social_media_interactions_data['growth_value'])}" if social_media_interactions_data["growth_value"] is not None and social_media_interactions_data["growth_value"] >= 0 else format_number(social_media_interactions_data["growth_value"]),
            "growth_percentage": social_media_interactions_data["growth_percentage"],
            "growth_percentage_display": f"+{social_media_interactions_data['growth_percentage']:.0f}%" if social_media_interactions_data["growth_percentage"] is not None and social_media_interactions_data["growth_percentage"] >= 0 else f"{social_media_interactions_data['growth_percentage']:.0f}%" if social_media_interactions_data["growth_percentage"] is not None else None
        }
    }
    
    # Tambahkan detail channels
    channel_details = {}
    for bucket in current_all_response["aggregations"]["channel_breakdown"]["buckets"]:
        channel_name = bucket["key"]
        channel_mentions = bucket["doc_count"]
        channel_reach = bucket["reach"]["value"]
        
        channel_details[channel_name] = {
            "mentions": channel_mentions,
            "reach": channel_reach
        }
    
    matrix["channels"] = channel_details
    
    # Tambahkan informasi periode
    matrix["period"] = {
        "current": {
            "start_date": start_date,
            "end_date": end_date
        },
        "previous": {
            "start_date": previous_start_str,
            "end_date": previous_end_str
        } if compare_with_previous else None
    }
    
    return matrix
