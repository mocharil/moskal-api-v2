import json
from datetime import datetime
from typing import Dict, List, Literal, Optional, Union

# Import utilitas dari paket utils
from utils.es_client import get_elasticsearch_client
from utils.es_query_builder import get_date_range
from utils.redis_client import redis_client

def normalize_link(link, channel):
 
    if not link or not isinstance(link, str):
        return link
    
    try:
        parts = link.split('/')
        
        # Skip invalid URLs
        if len(parts) < 3 or '://' not in link:
            return link
        
        # Extract protocol (http:, https:)
        protocol = parts[0]
        
        # Extract domain
        domain = parts[2] if len(parts) > 2 else ''
        
        if channel in ('youtube', 'linkedin'):
            # For YouTube and LinkedIn, just keep domain
            return f"{protocol}//{domain}"
        elif channel == 'reddit':
            if len(parts) > 4:
                # For Reddit, include r/subreddit if available
                return f"{protocol}//{domain}/{parts[3]}/{parts[4]}"
            else:
                return f"{protocol}//{domain}"
        else:
            # For other channels, include first path segment if available
            if len(parts) > 3:
                return f"{protocol}//{domain}/{parts[3]}"
            else:
                return f"{protocol}//{domain}"
    except Exception as e:
        print(f"Error normalizing link {link}: {e}")
        return link

def get_trending_links(
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
    limit=10000,       # Total links to analyze
    page=1,          # Current page number
    page_size=10     # Number of items per page
) -> Dict:
    
    # Generate cache key based on all parameters
    cache_key = redis_client.generate_cache_key(
        "get_trending_links",
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
        limit=limit,       # Total hashtags to analyze (increased for better pagination)
        page=page,          # Current page number
        page_size=page_size,    # Number of items per page

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
    
    # Mapping channel ke index Elasticsearch
    channel_to_index = {
        "twitter": "twitter_data",
        "instagram": "instagram_data",
        "linkedin": "linkedin_data",
        "reddit": "reddit_data",
        "youtube": "youtube_data",
        "tiktok": "tiktok_data",
        "news": "news_data",
        "blogs": "blogs_data",
        "facebook": "facebook_data",
        "podcasts": "podcasts_data",
        "videos": "videos_data",
        "web": "web_data"
    }
    
    # Dapatkan indeks yang akan di-query
    indices = [channel_to_index[ch] for ch in selected_channels if ch in channel_to_index]
    
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
    
    # Bangun query untuk mendapatkan trending links
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
                "field": "link_post"
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
            issue_field = "issue.keyword" if case_sensitive else "issue"
            
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
            issue_field = "issue.keyword" if case_sensitive else "issue"
            
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
    
    # Pastikan limit cukup besar untuk mendapat semua data yang diperlukan
    es_limit = max(limit, page * page_size)
    
    # Try two approaches: aggregation or direct scan depending on index structure
    try:
        # First approach: Try using aggregation (faster but may not work with all index configurations)
        aggs_query = {
            "size": 0,
            "query": {
                "bool": {
                    "must": must_conditions
                }
            },
            "aggs": {
                "links": {
                    "terms": {
                        "field": "link_post",
                        "size": es_limit
                    },
                    "aggs": {
                        "channel": {
                            "terms": {
                                "field": "channel",
                                "size": 1
                            }
                        }
                    }
                },
                "total_unique_links": {
                    "cardinality": {
                        "field": "link_post"
                    }
                }
            }
        }
        
        # Add filters if needed
        if filter_conditions:
            aggs_query["query"]["bool"]["filter"] = filter_conditions
            
        try:

            import json
            print(json.dumps(aggs_query, indent=2))
            # Try the aggregation approach
            print("Trying aggregation approach...")
            response = es.search(
                index=",".join(indices),
                body=aggs_query
            )
            
            # Process results for aggregation approach
            link_buckets = response["aggregations"]["links"]["buckets"]
            total_unique_links = response["aggregations"]["total_unique_links"]["value"]
            
            # Process the links with normalization
            normalized_links = {}
            
            for link_bucket in link_buckets:
                link = link_bucket["key"]
                mentions = link_bucket["doc_count"]
                
                # Get the channel (should be one primary channel per link)
                channel = "other"
                if link_bucket["channel"]["buckets"]:
                    channel = link_bucket["channel"]["buckets"][0]["key"]
                
                # Normalize the link
                normalized_link = normalize_link(link, channel)
                
                # Add to normalized links count
                if normalized_link not in normalized_links:
                    normalized_links[normalized_link] = mentions
                else:
                    normalized_links[normalized_link] += mentions
            
            # Convert to list format
            link_data = [
                {"link_post": link, "total_mentions": count}
                for link, count in normalized_links.items()
            ]
            
            # Sort by mentions
            link_data.sort(key=lambda x: x["total_mentions"], reverse=True)
            
        except Exception as e1:
            print(f"Aggregation approach failed: {e1}")
            
            # Fallback to scan/scroll approach
            print("Trying scan/scroll approach...")
            scan_query = {
                "query": {
                    "bool": {
                        "must": must_conditions
                    }
                },
                "_source": ["link_post", "channel"],
                "size": 1000
            }
            
            if filter_conditions:
                scan_query["query"]["bool"]["filter"] = filter_conditions
                
            # Execute initial scroll request
            resp = es.search(
                index=",".join(indices),
                body=scan_query,
                scroll="2m"
            )
            
            # Process all scrolled batches
            scroll_id = resp["_scroll_id"]
            scroll_size = len(resp["hits"]["hits"])
            documents = []
            
            # Initial batch
            documents.extend([hit["_source"] for hit in resp["hits"]["hits"]])
            
            # Continue scrolling if there are more results
            while scroll_size > 0:
                resp = es.scroll(scroll_id=scroll_id, scroll="2m")
                scroll_id = resp["_scroll_id"]
                batch = [hit["_source"] for hit in resp["hits"]["hits"]]
                documents.extend(batch)
                scroll_size = len(batch)
            
            # Process documents manually
            normalized_links = {}
            
            for doc in documents:
                link = doc.get("link_post")
                channel = doc.get("channel", "other")
                
                if not link:
                    continue
                    
                normalized_link = normalize_link(link, channel)
                
                if normalized_link not in normalized_links:
                    normalized_links[normalized_link] = 1
                else:
                    normalized_links[normalized_link] += 1
            
            # Convert to list format
            link_data = [
                {"link_post": link, "total_mentions": count}
                for link, count in normalized_links.items()
            ]
            
            # Sort by mentions
            link_data.sort(key=lambda x: x["total_mentions"], reverse=True)
            
            # Set total_unique_links
            total_unique_links = len(normalized_links)
        
        # Limit to top entries if needed
        if limit and len(link_data) > limit:
            link_data = link_data[:limit]
        
        # Calculate pagination values
        total_items = len(link_data)
        total_pages = (total_items + page_size - 1) // page_size  # ceiling division
        
        # Validate page number
        if page < 1:
            page = 1
        elif page > total_pages and total_pages > 0:
            page = total_pages
            
        # Apply pagination
        start_index = (page - 1) * page_size
        end_index = min(start_index + page_size, total_items)
        
        paginated_data = link_data[start_index:end_index]
        
        # Create result with pagination info
        result = {
            "data": paginated_data,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "total_items": total_items
            },
            "channels": selected_channels,
            "total_unique_links": total_unique_links,
            "period": {
                "start_date": start_date,
                "end_date": end_date
            }
        }
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
            },
            "error": str(e)
        }