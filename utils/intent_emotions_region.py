import json
import re
from datetime import datetime
from typing import Dict, List, Literal, Optional, Union

# Import utilitas dari paket utils
from utils.es_client import get_elasticsearch_client
from utils.es_query_builder import get_date_range, get_indices_from_channels
from utils.redis_client import redis_client

def get_intents_emotions_region_share(
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
    limit=10  # Jumlah intent dan emotions teratas untuk ditampilkan
) -> Dict:
 
    # Generate cache key based on all parameters
    cache_key = redis_client.generate_cache_key(
        "intents_emotions_region",
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
        limit=limit
    )

    # Try to get from cache first
    cached_result = redis_client.get(cache_key)
    if cached_result is not None:
        print('Returning cached result')
        # return cached_result

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
            "intents_share": [],
            "emotions_share": [],
            "regions_share": [],
            "total_mentions": 0,
            "period": {
                "start_date": start_date,
                "end_date": end_date
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
    
    # Dapatkan indeks berdasarkan channels menggunakan fungsi dari utils
    indices = get_indices_from_channels(selected_channels)
    
    if not indices:
        print("Error: No valid indices")
        return {
            "intents_share": [],
            "emotions_share": [],
            "regions_share": [],
            "total_mentions": 0,
            "period": {
                "start_date": start_date,
                "end_date": end_date
            }
        }
    
    # Dapatkan rentang tanggal jika tidak disediakan
    if not start_date or not end_date:
        start_date, end_date = get_date_range(
            date_filter=date_filter,
            custom_start_date=custom_start_date,
            custom_end_date=custom_end_date
        )
    
    # Define aggregations for the query - Pastikan penggunaan fields sesuai mapping
    aggs = {
        # Total mentions
        "total_mentions": {
            "value_count": {
                "field": "link_post"  # Gunakan field yang selalu ada di setiap dokumen
            }
        },
        # Intent distribution - intent sebagai keyword field
        "intent_distribution": {
            "terms": {
                "field": "intent",  # Field intent adalah keyword dalam mapping
                "size": limit,
                "missing": "unknown"
            }
        },
        # Emotions distribution - emotions sebagai text dengan subfield keyword
        "emotions_distribution": {
            "terms": {
                "field": "emotions.keyword",  # Gunakan .keyword untuk emotions
                "size": limit,
                "missing": "unknown"
            }
        },
        # Region distribution - region sebagai keyword field
        "regions_distribution": {
            "terms": {
                "field": "region",  # Field region adalah keyword dalam mapping
                "size": 100,  # Get more regions to process manually
                "missing": "unknown"
            }
        }
    }
    
    # Bangun query kustom untuk mengakomodasi case_sensitive dan search_exact_phrases
    must_conditions = [
        {
            "range": {
                "post_created_at": {
                    "gte": start_date,
                    "lte": end_date
                }
            }
        }
    ]
    
    # Tambahkan filter keywords jika ada - Pastikan field yang digunakan sesuai dengan mapping
    if keywords:
        # Konversi keywords ke list jika belum
        keyword_list = keywords if isinstance(keywords, list) else [keywords]
        keyword_should_conditions = []
        
        # Tentukan field yang akan digunakan berdasarkan case_sensitive
        if case_sensitive:
            caption_field = "post_caption.keyword"  # Gunakan subfield keyword jika case sensitive
            issue_field = "cluster.keyword"  # Gunakan subfield keyword jika case sensitive
        else:
            caption_field = "post_caption"  # Gunakan subfield case_insensitive untuk non-case sensitive
            issue_field = "cluster"  # Gunakan subfield case_insensitive untuk non-case sensitive
        
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
    
    # Gabungkan semua kondisi ke dalam query utama
    query = {
        "size": 0,  
        "query": {
            "bool": {
                "must": must_conditions
            }
        },
        "aggs": aggs
    }
    
    # Tambahkan filter jika ada
    if filter_conditions:
        query["query"]["bool"]["filter"] = filter_conditions
    
    # Add filter to exclude "Not Specified" from intent, emotions, and region
    if "filter" not in query["query"]["bool"]:
        query["query"]["bool"]["filter"] = []
    
    # Add filter to exclude "Not Specified" from intent
    intent_not_specified_filter = {
        "bool": {
            "must_not": [
                {"term": {"intent": "Not Specified"}},
                {"term": {"intent": "not specified"}},
                {"term": {"intent": "unknown"}},
                {"term": {"intent": ""}}
            ]
        }
    }
    query["query"]["bool"]["filter"].append(intent_not_specified_filter)
    
    # Add filter to exclude "Not Specified" from emotions - gunakan emotions.keyword
    emotions_not_specified_filter = {
        "bool": {
            "must_not": [
                {"term": {"emotions.keyword": "Not Specified"}},
                {"term": {"emotions.keyword": "not specified"}},
                {"term": {"emotions.keyword": "unknown"}},
                {"term": {"emotions.keyword": ""}}
            ]
        }
    }
    query["query"]["bool"]["filter"].append(emotions_not_specified_filter)
    
    # Add filter to exclude "Not Specified" from region
    region_not_specified_filter = {
        "bool": {
            "must_not": [
                {"term": {"region": "Not Specified"}},
                {"term": {"region": "not specified"}},
                {"term": {"region": "unknown"}},
                {"term": {"region": ""}}
            ]
        }
    }
    query["query"]["bool"]["filter"].append(region_not_specified_filter)
    
    try:
        #print(json.dumps(query, indent=2))
        # Execute query
        response = es.search(
            index=",".join(indices),
            body=query
        )
        
        # Extract data
        total_mentions = response["aggregations"]["total_mentions"]["value"]
        
        print('----->',total_mentions)
        # Process intents data
        intent_buckets = []
        
        # Try primary intent field first
        if "intent_distribution" in response["aggregations"] and response["aggregations"]["intent_distribution"]["buckets"]:
            intent_buckets = response["aggregations"]["intent_distribution"]["buckets"]
        
        # Process emotions data
        emotions_buckets = []
        
        # Try primary emotions field first
        if "emotions_distribution" in response["aggregations"] and response["aggregations"]["emotions_distribution"]["buckets"]:
            emotions_buckets = response["aggregations"]["emotions_distribution"]["buckets"]

        # Process region data
        regions_buckets = []
        
        # Get the basic regions first
        if "regions_distribution" in response["aggregations"] and response["aggregations"]["regions_distribution"]["buckets"]:
            regions_buckets = response["aggregations"]["regions_distribution"]["buckets"]
            
        # Format intent share data
        intents_share = []
        for bucket in intent_buckets:
            if bucket["key"] not in ["Not Specified", "not specified", "unknown", ""]:
                intents_share.append({
                    "name": bucket["key"],
                    "percentage": bucket["doc_count"]
                })
        
        # Format emotions share data
        emotions_share = []
        for bucket in emotions_buckets:
            if bucket["key"] not in ["Not Specified", "not specified", "unknown", ""]:
                
                emotions_share.append({
                    "name": bucket["key"],
                    "percentage": bucket["doc_count"]
                })
        
        # Process regions manually to handle comma separation
        region_counts = {}
        
        # First process from aggregation buckets
        for bucket in regions_buckets:
            if bucket["key"] not in ["Not Specified", "not specified", "unknown", ""]:
                region_value = bucket["key"]
                
                # Check if region contains comma
                if "," in region_value:
                    # Split by comma and count each region
                    for region in region_value.split(","):
                        region = region.strip()
                        if region and region not in ["Not Specified", "not specified", "unknown"]:
                            region_counts[region] = region_counts.get(region, 0) + bucket["doc_count"]
                else:
                    region_counts[region_value] = region_counts.get(region_value, 0) + bucket["doc_count"]
        
        # Now process from individual documents to get more accurate counts
        for hit in response["hits"]["hits"]:
            if "_source" in hit and "region" in hit["_source"] and hit["_source"]["region"]:
                region_value = hit["_source"]["region"]
                
                # Skip not specified values
                if region_value in ["Not Specified", "not specified", "unknown", "","Indonesia"]:
                    continue
                    
                # Check if region contains comma
                if "," in region_value:
                    # Split by comma and count each region
                    for region in region_value.split(","):
                        region = region.strip()
                        if region and region not in ["Not Specified", "not specified", "unknown","Indonesia"]:
                            region_counts[region] = region_counts.get(region, 0) + 1
                else:
                    region_counts[region_value] = region_counts.get(region_value, 0) + 1
        
        # Convert region counts to percentage and format
        regions_share = []
        for region, count in region_counts.items():
            if region.lower() not in ["not specified", "not specified", "unknown", "unspecified","indonesia"]:
                regions_share.append({
                    "name": region,
                    "percentage": count
                })
        
        # Sort by percentage (descending)
        regions_share.sort(key=lambda x: x["percentage"], reverse=True)
        
        # Limit to top entries
        if len(regions_share) > limit:
            regions_share = regions_share[:limit]
        
        # Format result
        result = {
            "intents_share": intents_share,
            "emotions_share": emotions_share,
            "regions_share": regions_share,
            "total_mentions": total_mentions,
            "period": {
                "start_date": start_date,
                "end_date": end_date
            }
        }
        
        # Cache the results for 10 minutes
        redis_client.set_with_ttl(cache_key, result, ttl_seconds=100)
        return result
        
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        import traceback
        traceback.print_exc()  # Print full traceback for debugging
        return {
            "intents_share": [],
            "emotions_share": [],
            "regions_share": [],
            "total_mentions": 0,
            "period": {
                "start_date": start_date,
                "end_date": end_date
            },
            "error": str(e)
        }
