from utils.es_client import get_elasticsearch_client
from utils.es_query_builder import build_elasticsearch_query, get_indices_from_channels, get_date_range
from utils.script_score import script_score
import pandas as pd
import uuid, numpy as np
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from utils.redis_client import redis_client

# Load environment variables
load_dotenv()

# Buat koneksi ke Elasticsearch
es = get_elasticsearch_client()

def create_link_user(df):
    if df['channel'] == 'twitter':
        return f"""https://x.com/{df['username'].strip('@ ')}"""
    if df['channel'] == 'instagram':
        return f"""https://www.instagram.com/{df['username'].strip('@ ')}"""
    if df['channel'] == 'tiktok':
        return f"""https://www.tiktok.com/@{df['username'].strip('@ ')}"""
    if df['channel']=='linkedin':
        return f"""https://www.linkedin.com/in/{df['username'].strip('@ ')}"""
    if df['channel']=='reddit':
        return f"""https://www.reddit.com/{df['username'].strip('@ ')}"""
    
    return df['username']
    
def add_negative_driver_flag(df):
    # Ensure all sentiment columns exist
    for sentiment in ['positive', 'negative', 'neutral']:
        col_name = f'sentiment_{sentiment}'
        if col_name not in df.columns:
            df[col_name] = 0
    
    # Create a new column that checks if negative sentiment is the highest
    df['is_negative_driver'] = False
    
    # Compare sentiment counts and set flag if negative is highest
    condition = ((df['sentiment_negative'] > df['sentiment_positive']) & 
                 (df['sentiment_negative'] > df['sentiment_neutral']))
    
    df['is_negative_driver'] = condition
    
    return df

def create_uuid(keyword):
    # Gunakan namespace standar (ada juga untuk URL, DNS, dll)
    namespace = uuid.NAMESPACE_DNS
    return uuid.uuid5(namespace, keyword)

def rule_base_user_category(string, category):
    if pd.isna(string):
        return ''
    
    if 'news' in string.lower():
        return 'News Account'

    return category
    
def search_kol(
    owner_id = None,
    project_name = None,
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
    domain=None):

    # Generate cache key based on all parameters
    cache_key = redis_client.generate_cache_key(
        "kol_overview",
        owner_id=owner_id,
        project_name=project_name,
        keywords=keywords,
        search_keyword=search_keyword,
        search_exact_phrases=search_exact_phrases,
        case_sensitive=case_sensitive,
        start_date=start_date,
        sentiment=sentiment,
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

    print('get all data mentions using aggregation')

    if not sentiment:
        sentiment = ['positive','negative','neutral']

    # Buat koneksi Elasticsearch
    es_conn = get_elasticsearch_client(
        es_host=es_host,
        es_username=es_username,
        es_password=es_password,
        use_ssl=use_ssl,
        verify_certs=verify_certs,
        ca_certs=ca_certs
    )
    
    if not es_conn:
        return []
    
    # Dapatkan indeks dari channel
    indices = get_indices_from_channels(channels)
    
    if not indices:
        print("Error: Tidak ada indeks yang valid")
        return []
    
    # Dapatkan rentang tanggal jika tidak disediakan
    if not start_date or not end_date:
        start_date, end_date = get_date_range(
            date_filter=date_filter,
            custom_start_date=custom_start_date,
            custom_end_date=custom_end_date
        )

    # Build base query menggunakan es_query_builder
    base_query = build_elasticsearch_query(
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
        size=0  # Untuk aggregation saja
    )

    # Tambahkan aggregation untuk KOL analysis
    aggregation_query = {
        "by_username_channel": {
            "terms": {
                "script": {
                    "source": """
                        String username = doc.containsKey('username') && !doc['username'].empty ? doc['username'].value : 'unknown';
                        String channel = doc.containsKey('channel') && !doc['channel'].empty ? doc['channel'].value : 'unknown';
                        return username + '|' + channel;
                    """
                },
                "size": 1000,  # Increase size to get more KOLs
                "order": [
                    {
                        "user_influence_score_avg": "desc"
                    },
                    {
                        "followers_count": "desc"
                    },
                    {
                        "total_posts": "desc"
                    }
                ]
            },
            "aggs": {
                "username": {
                    "terms": {
                        "field": "username",
                        "size": 1
                    }
                },
                "channel": {
                    "terms": {
                        "field": "channel",
                        "size": 1
                    }
                },
                "followers_count": {
                    "max": {
                        "script": {
                            "source": """
                                // Logic untuk followers dengan fallback - menggunakan max untuk mendapatkan nilai tertinggi per user
                                if (doc.containsKey('user_followers') && !doc['user_followers'].empty) {
                                    return doc['user_followers'].value;
                                } else if (doc.containsKey('user_connections') && !doc['user_connections'].empty) {
                                    return doc['user_connections'].value;
                                } else if (doc.containsKey('subscriber') && !doc['subscriber'].empty) {
                                    return doc['subscriber'].value;
                                } else {
                                    return 0;
                                }
                            """
                        }
                    }
                },
                "total_posts": {
                    "value_count": {
                        "field": "username"
                    }
                },
                "viral_score_sum": {
                    "sum": {
                        "field": "viral_score"
                    }
                },
                "reach_score_sum": {
                    "sum": {
                        "field": "reach_score"
                    }
                },
                "unique_user_image_url": {
                    "terms": {
                        "field": "user_image_url",
                        "size": 1
                    }
                },
                "engagement_rate_sum": {
                    "sum": {
                        "field": "engagement_rate"
                    }
                },
                "unique_user_category": {
                    "terms": {
                        "field": "user_category.keyword",
                        "size": 1
                    }
                },
                "user_influence_score_avg": {
                    "avg": {
                        "script": script_score
                    }
                },
                "sentiment_positive": {
                    "filter": {
                        "term": {
                            "sentiment": "positive"
                        }
                    }
                },
                "sentiment_negative": {
                    "filter": {
                        "term": {
                            "sentiment": "negative"
                        }
                    }
                },
                "sentiment_neutral": {
                    "filter": {
                        "term": {
                            "sentiment": "neutral"
                        }
                    }
                },
                "unique_issues": {
                    "terms": {
                        "field": "cluster.keyword",
                        "size": 10
                    }
                }
            }
        }
    }

    # Tambahkan aggregation ke base query
    base_query["aggs"] = aggregation_query

    try:
        import json
        # print(json.dumps(base_query, indent=2))
        
        # Execute aggregation query
        response = es_conn.search(
            index=",".join(indices),
            body=base_query
        )
        
        # Process aggregation results
        buckets = response["aggregations"]["by_username_channel"]["buckets"]
        
        if not buckets:
            return []
        
        # Convert aggregation results to DataFrame format
        kol_data = []
        for bucket in buckets:
            username_channel = bucket["key"]
            username = bucket["username"]["buckets"][0]["key"] if bucket["username"]["buckets"] else "unknown"
            channel = bucket["channel"]["buckets"][0]["key"] if bucket["channel"]["buckets"] else "unknown"
            
            # Extract issues list
            issues_list = [issue_bucket["key"] for issue_bucket in bucket["unique_issues"]["buckets"]]
            
            # Get user_category
            user_category = ""
            if bucket["unique_user_category"]["buckets"]:
                user_category = bucket["unique_user_category"]["buckets"][0]["key"]
            
            # Get user_image_url
            user_image_url = ""
            if bucket["unique_user_image_url"]["buckets"]:
                user_image_url = bucket["unique_user_image_url"]["buckets"][0]["key"]
            
            if channel == "news":
                user_image_url = f"https://logo.clearbit.com/{username}"
                


            kol_record = {
                'username': username,
                'channel': channel,
                'link_post': bucket["total_posts"]["value"],
                'viral_score': bucket["viral_score_sum"]["value"],
                'reach_score': bucket["reach_score_sum"]["value"],
                'user_image_url': user_image_url,
                'user_followers': bucket["followers_count"]["value"],
                'engagement_rate': bucket["engagement_rate_sum"]["value"],
                'issue': issues_list,
                'user_category': user_category,
                'user_influence_score': bucket["user_influence_score_avg"]["value"],
                'sentiment_positive': bucket["sentiment_positive"]["doc_count"],
                'sentiment_negative': bucket["sentiment_negative"]["doc_count"],
                'sentiment_neutral': bucket["sentiment_neutral"]["doc_count"]
            }
            kol_data.append(kol_record)
        
        # Convert to DataFrame and clean data
        final_kol = pd.DataFrame(kol_data)
        
        if final_kol.empty:
            return []

        # Clean DataFrame - replace inf/nan values
        numeric_columns = ['link_post', 'viral_score', 'reach_score', 'user_followers', 
                          'engagement_rate', 'user_influence_score', 'sentiment_positive', 
                          'sentiment_negative', 'sentiment_neutral']
        
        for col in numeric_columns:
            if col in final_kol.columns:
                # Replace inf and -inf with 0
                final_kol[col] = final_kol[col].replace([np.inf, -np.inf], 0)
                # Fill NaN with 0
                final_kol[col] = final_kol[col].fillna(0)
                # Ensure numeric type
                final_kol[col] = pd.to_numeric(final_kol[col], errors='coerce').fillna(0)

        # Apply business logic transformations
        final_kol['user_category'] = final_kol.apply(lambda s: 'News Account' if s['channel'] == 'news' else rule_base_user_category(s['username'], s['user_category']), axis=1)
        final_kol['link_user'] = final_kol.apply(lambda s: create_link_user(s), axis=1)
        
        # Apply negative driver flag
        final_kol = add_negative_driver_flag(final_kol)
        
        # Get all unique issues for topic mapping
        list_issue = [j for i in final_kol['issue'] for j in i]

        # Check issue mapping (same logic as before)
        MAP = False
        dict_issue = {}
        # Apply unified issue mapping
        final_kol['unified_issue'] = final_kol['issue'].transform(lambda s: list(set([dict_issue.get(i, i) for i in s]))[:5])
        final_kol['user_category'] = final_kol.apply(lambda s: 'News Account' if s['channel'] == 'news' else s['user_category'], axis=1)
        # Calculate comprehensive "most_viral" score
        def calculate_most_viral_score(row):
            # Safe division function
            def safe_divide(numerator, denominator, default=0.0):
                if denominator == 0 or np.isnan(denominator) or np.isinf(denominator):
                    return default
                result = numerator / denominator
                if np.isnan(result) or np.isinf(result):
                    return default
                return result
            
            # Normalize metrics to 0-1 scale for fair comparison
            max_followers = final_kol['user_followers'].max() if final_kol['user_followers'].max() > 0 else 1
            max_influence = final_kol['user_influence_score'].max() if final_kol['user_influence_score'].max() > 0 else 1
            max_posts = final_kol['link_post'].max() if final_kol['link_post'].max() > 0 else 1
            max_reach = final_kol['reach_score'].max() if final_kol['reach_score'].max() > 0 else 1
            max_engagement = final_kol['engagement_rate'].max() if final_kol['engagement_rate'].max() > 0 else 1
            
            # Normalized scores (0-1) with safe division
            followers_score = safe_divide(row['user_followers'], max_followers)
            influence_score = safe_divide(row['user_influence_score'], max_influence)
            activity_score = safe_divide(row['link_post'], max_posts)
            reach_score = safe_divide(row['reach_score'], max_reach)
            engagement_score = safe_divide(row['engagement_rate'], max_engagement)
            
            # Sentiment impact factor
            total_sentiment = row['sentiment_positive'] + row['sentiment_negative'] + row['sentiment_neutral']
            if total_sentiment > 0:
                # Negative sentiment can increase influence (controversial = viral)
                sentiment_factor = 1 + safe_divide(row['sentiment_negative'], total_sentiment) * 0.3
                # But positive sentiment is also valuable
                sentiment_factor += safe_divide(row['sentiment_positive'], total_sentiment) * 0.2
            else:
                sentiment_factor = 1
            
            # Weighted combination - you can adjust these weights based on your priorities
            weights = {
                'influence': 0.35,      # Highest weight - our calculated influence score
                'followers': 0.25,     # Follower count matters
                'activity': 0.15,      # Posting frequency
                'reach': 0.15,         # Total reach achieved
                'engagement': 0.10     # Engagement rate
            }
            
            composite_score = (
                influence_score * weights['influence'] +
                followers_score * weights['followers'] +
                activity_score * weights['activity'] +
                reach_score * weights['reach'] +
                engagement_score * weights['engagement']
            ) * sentiment_factor
            
            # Ensure result is not inf/nan
            if np.isnan(composite_score) or np.isinf(composite_score):
                return 0.0
            
            return composite_score
        
        # Apply the scoring function
        final_kol['most_viral'] = final_kol.apply(calculate_most_viral_score, axis=1)
        
        # Clean most_viral column
        final_kol['most_viral'] = final_kol['most_viral'].replace([np.inf, -np.inf], 0).fillna(0)
        
        # Scale to 0-100 for easier interpretation
        max_viral = final_kol['most_viral'].max() if final_kol['most_viral'].max() > 0 else 1
        final_kol['most_viral'] = (final_kol['most_viral'] / max_viral) * 100
        
        # Clean scaled most_viral
        final_kol['most_viral'] = final_kol['most_viral'].replace([np.inf, -np.inf], 0).fillna(0)

        # Calculate share of voice with safe division
        total_posts = final_kol["link_post"].sum()
        if total_posts > 0:
            final_kol["share_of_voice"] = (final_kol["link_post"] / total_posts) * 100
        else:
            final_kol["share_of_voice"] = 0
        
        # Clean share_of_voice
        final_kol["share_of_voice"] = final_kol["share_of_voice"].replace([np.inf, -np.inf], 0).fillna(0)
        
        # Enhanced sorting strategy for different KOL categories
        
        # 1. Most Controversial/Negative Driver KOLs (high negative sentiment but influential)
        controversial_kol = final_kol[
            (final_kol['sentiment_negative'] >= 3) &  # At least 3 negative mentions
            (final_kol['most_viral'] >= 20)  # Above average viral score
        ].sort_values(['sentiment_negative', 'most_viral'], ascending=False)[:50]
        
        # 2. Most Viral/Influential KOLs (pure influence ranking)
        most_viral_kol = final_kol.sort_values(['most_viral'], ascending=False)[:50]
        
        # 3. Rising Stars (high engagement rate relative to followers) with safe division
        final_kol['engagement_per_follower'] = np.where(
            (final_kol['user_followers'] > 0) & 
            (np.isfinite(final_kol['user_followers'])) & 
            (np.isfinite(final_kol['engagement_rate'])),
            final_kol['engagement_rate'] / final_kol['user_followers'],
            0
        )
        
        # Clean engagement_per_follower
        final_kol['engagement_per_follower'] = final_kol['engagement_per_follower'].replace([np.inf, -np.inf], 0).fillna(0)
        
        rising_stars = final_kol[
            final_kol['user_followers'] < final_kol['user_followers'].quantile(0.7)  # Not mega influencers
        ].sort_values(['engagement_per_follower', 'most_viral'], ascending=False)[:30]
        
        # 4. High-Volume Contributors (very active KOLs)
        high_volume = final_kol[
            final_kol['link_post'] >= final_kol['link_post'].quantile(0.8)
        ].sort_values(['link_post', 'most_viral'], ascending=False)[:30]
        
        # Combine all categories and remove duplicates
        combined_kol = pd.concat([
            controversial_kol,
            most_viral_kol, 
            rising_stars,
            high_volume
        ]).drop_duplicates('link_user')
        
        # Final data cleaning before JSON serialization
        def clean_for_json(df):
            """Clean DataFrame for JSON serialization"""
            for col in df.columns:
                if df[col].dtype in ['float64', 'float32']:
                    # Replace inf and -inf with None, then fillna with 0
                    df[col] = df[col].replace([np.inf, -np.inf], np.nan).fillna(0)
                elif df[col].dtype in ['int64', 'int32']:
                    # Ensure integers are clean
                    df[col] = df[col].fillna(0).astype(int)
            return df
        
        combined_kol = clean_for_json(combined_kol)
        
        # Final ranking based on comprehensive scoring
        result = combined_kol.sort_values([
            'link_post',
            'is_negative_driver',  # Controversial KOLs first (if they exist)
            'most_viral'           # Then by viral score

        ], ascending=[False,False, False])[:150].to_dict(orient='records')
  

        # Cache the results for 10 minutes
        redis_client.set_with_ttl(cache_key, result, ttl_seconds=600)
        return result
        
    except Exception as e:
        print(f"Error executing aggregation query: {e}")
        return []