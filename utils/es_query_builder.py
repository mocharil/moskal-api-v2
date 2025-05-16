"""
es_query_builder.py
Elasticsearch Query Builder Utilities

This module provides functions for building Elasticsearch queries
for various data analysis purposes.
"""

from datetime import datetime, timedelta
import json

def get_indices_from_channels(channels=None):
    """
    Get Elasticsearch indices based on channel names
    
    Parameters:
    -----------
    channels : list, optional
        List of channels ['twitter', 'news', 'instagram', etc.]
        
    Returns:
    --------
    list
        List of Elasticsearch indices
    """
    index_mapping = {
        "tiktok": "tiktok_data",
        "news": "news_data",
        "instagram": "instagram_data",
        "twitter": "twitter_data",
        "linkedin": "linkedin_data",
        "reddit": "reddit_data",
        "youtube": "youtube_data",
        "facebook": "facebook_data",
        "threads": "threads_data"
    }
    
    if channels:
        indices = [f'{ch}_data' for ch in channels]
    else:
        indices = list(index_mapping.values())
    
    return indices

def get_date_range(date_filter="last 30 days", custom_start_date=None, custom_end_date=None):
    """
    Calculate date range based on filter type
    
    Parameters:
    -----------
    date_filter : str
        Date filter type (e.g., "all time", "yesterday", "this week", etc.)
    custom_start_date : str, optional
        Custom start date in YYYY-MM-DD format (used if date_filter is "custom")
    custom_end_date : str, optional
        Custom end date in YYYY-MM-DD format (used if date_filter is "custom")
        
    Returns:
    --------
    tuple
        (start_date, end_date) in YYYY-MM-DD format
    """
    today = datetime.now().date()
    
    if date_filter == "custom" and custom_start_date and custom_end_date:
        return custom_start_date, custom_end_date
    
    if date_filter == "yesterday":
        yesterday = today - timedelta(days=1)
        return yesterday.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")
    
    elif date_filter == "this week":
        start_of_week = today - timedelta(days=today.weekday())
        return start_of_week.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
    
    elif date_filter == "last 7 days":
        return (today - timedelta(days=7)).strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
    
    elif date_filter == "last 30 days":
        return (today - timedelta(days=30)).strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
    
    elif date_filter == "last 3 months":
        return (today - timedelta(days=90)).strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
    
    elif date_filter == "this year":
        return f"{today.year}-01-01", today.strftime("%Y-%m-%d")
    
    elif date_filter == "last year":
        return f"{today.year-1}-01-01", f"{today.year-1}-12-31"
    
    else:  # "all time" or fallback
        return "2000-01-01", today.strftime("%Y-%m-%d")

def build_elasticsearch_query(
    keywords=None,
    search_keyword=None,
    search_exact_phrases=False,
    case_sensitive=False,
    sentiment=None,
    start_date=None,
    end_date=None,
    importance="all mentions",
    influence_score_min=None,
    influence_score_max=None,
    region=None,
    language=None,
    domain=None,
    size=0,
    date_field="post_created_at",
    caption_field="post_caption",
    issue_field="issue",
    aggs=None
):
    """
    Build Elasticsearch query based on filters
    
    Parameters:
    -----------
    keywords : list, optional
        List of keywords to filter (will search in both post_caption and issue fields)
    search_keyword : list, optional
        List of keywords to search as exact phrases or with AND operator based on search_exact_phrases
    search_exact_phrases : bool, optional
        If True, use match_phrase for keyword search, if False use match with AND operator
    case_sensitive : bool, optional
        If True, perform case-sensitive keyword search, if False ignore case
    sentiment : list, optional
        List of sentiments ['positive', 'negative', 'neutral']
    start_date : str, optional
        Start date in YYYY-MM-DD format
    end_date : str, optional
        End date in YYYY-MM-DD format
    importance : str, optional
        'important mentions' or 'all mentions'
    influence_score_min : float, optional
        Minimum influence score (0-100)
    influence_score_max : float, optional
        Maximum influence score (0-100)
    region : list, optional
        List of regions
    language : list, optional
        List of languages
    domain : list, optional
        List of domains to filter
    size : int, optional
        Number of documents to return (0 for aggregations only)
    date_field : str, optional
        Field name for date filtering
    caption_field : str, optional
        Field name for caption text
    issue_field : str, optional
        Field name for issue text
    aggs : dict, optional
        Custom aggregations to include in the query
        
    Returns:
    --------
    dict
        Elasticsearch query
    """
    # Default date range (last 30 days)
    today = datetime.now().date()
    if not end_date:
        end_date = today.strftime("%Y-%m-%d")
    if not start_date:
        start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    
    # Create base query
    query = {
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            date_field: {
                                "gte": start_date,
                                "lte": end_date
                            }
                        }
                    }
                ],
                "filter": []
            }
        }
    }
    
    # Add keyword and search_keyword filters
    if keywords or search_keyword:
        must_conditions = []
        
        # Handle regular keywords
        if keywords:
            if not isinstance(keywords, list):
                keywords = [keywords]
                
            caption_field_to_use = f"{caption_field}.keyword" if case_sensitive else caption_field
            issue_field_to_use = f"{issue_field}.keyword" if case_sensitive else issue_field
            
            keyword_should_conditions = []
            
            if search_exact_phrases:
                # Add match_phrase for each keyword in post_caption and issue
                for kw in keywords:
                    keyword_should_conditions.extend([
                        {"match_phrase": {caption_field_to_use: kw}},
                        {"match_phrase": {issue_field_to_use: kw}}
                    ])
            else:
                # Add match with AND operator for each keyword in post_caption and issue
                for kw in keywords:
                    keyword_should_conditions.extend([
                        {"match": {caption_field_to_use: {"query": kw, "operator": "AND"}}},
                        {"match": {issue_field_to_use: {"query": kw, "operator": "AND"}}}
                    ])
            
            must_conditions.append({
                "bool": {
                    "should": keyword_should_conditions,
                    "minimum_should_match": 1
                }
            })
        
        # Handle search_keyword with same logic as keywords
        if search_keyword:
            if not isinstance(search_keyword, list):
                search_keyword = [search_keyword]
                
            caption_field_to_use = f"{caption_field}.keyword" if case_sensitive else caption_field
            issue_field_to_use = f"{issue_field}.keyword" if case_sensitive else issue_field
            
            search_keyword_should_conditions = []
            
            if search_exact_phrases:
                # Add match_phrase for each search_keyword in post_caption and issue
                for sk in search_keyword:
                    search_keyword_should_conditions.extend([
                        {"match_phrase": {caption_field_to_use: sk}},
                        {"match_phrase": {issue_field_to_use: sk}}
                    ])
            else:
                # Add match with AND operator for each search_keyword in post_caption and issue
                for sk in search_keyword:
                    search_keyword_should_conditions.extend([
                        {"match": {caption_field_to_use: {"query": sk, "operator": "AND"}}},
                        {"match": {issue_field_to_use: {"query": sk, "operator": "AND"}}}
                    ])
            
            must_conditions.append({
                "bool": {
                    "should": search_keyword_should_conditions,
                    "minimum_should_match": 1
                }
            })
        
        # Add the combined filter to the main query
        query["query"]["bool"]["must"].append({
            "bool": {
                "must": must_conditions
            }
        })
    
    # Add sentiment filter
    if sentiment:
        sentiment_filter = {
            "terms": {
                "sentiment": sentiment
            }
        }
        query["query"]["bool"]["filter"].append(sentiment_filter)
    
    # Add importance filter
    if importance == "important mentions":
        query["query"]["bool"]["filter"].append({
            "range": {
                "influence_score": {
                    "gt": 50
                }
            }
        })
    
    # Add custom influence score range if provided
    if influence_score_min is not None or influence_score_max is not None:
        influence_range = {"range": {"influence_score": {}}}
        if influence_score_min is not None:
            influence_range["range"]["influence_score"]["gte"] = influence_score_min
        if influence_score_max is not None:
            influence_range["range"]["influence_score"]["lte"] = influence_score_max
        query["query"]["bool"]["filter"].append(influence_range)
    
    # Add region filter
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
    
    # Add language filter
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
    
    # Add domain filter
    if domain:
        domain_filter = {
            "bool": {
                "should": [{"wildcard": {"link_post": f"*{d}*"}} for d in domain],
                "minimum_should_match": 1
            }
        }
        query["query"]["bool"]["filter"].append(domain_filter)
    
    # Add aggregations if provided
    if aggs:
        query["aggs"] = aggs
    
    return query

def add_time_series_aggregation(query, field="post_created_at", interval="day", format="yyyy-MM-dd"):
    """
    Add time series aggregation to an Elasticsearch query
    
    Parameters:
    -----------
    query : dict
        Elasticsearch query
    field : str, optional
        Date field for aggregation
    interval : str, optional
        Interval for aggregation ('day', 'week', 'month', etc.)
    format : str, optional
        Date format for aggregation results
        
    Returns:
    --------
    dict
        Updated Elasticsearch query with time series aggregation
    """
    if "aggs" not in query:
        query["aggs"] = {}
    
    query["aggs"]["time_series"] = {
        "date_histogram": {
            "field": field,
            "calendar_interval": interval,
            "format": format
        },
        "aggs": {
            "total_reach": {
                "sum": {
                    "field": "reach_score"
                }
            },
            "sentiment_breakdown": {
                "terms": {
                    "field": "sentiment"
                }
            }
        }
    }
    
    return query

def add_wordcloud_aggregation(query, field="post_caption", size=50):
    """
    Add text terms aggregation for wordcloud to an Elasticsearch query
    
    Parameters:
    -----------
    query : dict
        Elasticsearch query
    field : str, optional
        Text field for aggregation
    size : int, optional
        Number of terms to return
        
    Returns:
    --------
    dict
        Updated Elasticsearch query with terms aggregation
    """
    if "aggs" not in query:
        query["aggs"] = {}
    
    query["aggs"]["terms"] = {
        "terms": {
            "field": f"{field}.keyword",
            "size": size
        }
    }
    
    return query

def get_query_string_for_devtools(indices, query):
    """
    Generate query string for Elasticsearch DevTools
    
    Parameters:
    -----------
    indices : str or list
        Elasticsearch indices
    query : dict
        Elasticsearch query
        
    Returns:
    --------
    str
        Query string for DevTools
    """
    if isinstance(indices, list):
        indices = ",".join(indices)
    
    return f"GET {indices}/_search\n{json.dumps(query, indent=2)}"
