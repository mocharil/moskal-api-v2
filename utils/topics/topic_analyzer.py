import pandas as pd
import uuid
from typing import List, Dict, Any, Optional
import re
from utils.gemini import call_gemini
from utils.es_client import get_elasticsearch_client
from utils.es_query_builder import get_date_range
from elasticsearch import Elasticsearch

def create_uuid(keyword: str) -> str:
    """Generate UUID for a given keyword"""
    namespace = uuid.NAMESPACE_DNS
    return str(uuid.uuid5(namespace, keyword))

def get_data_topics(
    es: Optional[Elasticsearch] = None,
    keywords=None,
    search_exact_phrases=False,
    case_sensitive=False,
    start_date=None,
    sentiment=None,
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
) -> pd.DataFrame:
    """Get aggregated analytics per issue from Elasticsearch"""
    if es is None:
        es = get_elasticsearch_client()

    default_channels = ['reddit','youtube','linkedin','twitter','tiktok','instagram','facebook','news','threads']
    all_channels = channels if channels else default_channels

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
        "web": "web_data",
        "other_socials": "other_socials_data"
    }

    available_indices = [channel_to_index[ch] for ch in all_channels if ch in channel_to_index]

    if not start_date or not end_date:
        start_date, end_date = get_date_range(
            date_filter=date_filter,
            custom_start_date=custom_start_date,
            custom_end_date=custom_end_date
        )

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

    if keywords:
        keyword_should_conditions = []
        field_caption = "post_caption.keyword" if case_sensitive else "post_caption"
        field_issue = "issue.keyword" if case_sensitive else "issue"

        for kw in (keywords if isinstance(keywords, list) else [keywords]):
            if search_exact_phrases:
                keyword_should_conditions.append({"match_phrase": {field_caption: kw}})
                keyword_should_conditions.append({"match_phrase": {field_issue: kw}})
            else:
                keyword_should_conditions.append({"match": {field_caption: {"query": kw, "operator": "AND"}}})
                keyword_should_conditions.append({"match": {field_issue: {"query": kw, "operator": "AND"}}})
        
        must_conditions.append({
            "bool": {
                "should": keyword_should_conditions,
                "minimum_should_match": 1
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
        region_conditions = [{"wildcard": {"region": f"*{r}*"}} for r in (region if isinstance(region, list) else [region])]
        filter_conditions.append({
            "bool": {
                "should": region_conditions,
                "minimum_should_match": 1
            }
        })

    if language:
        language_conditions = [{"wildcard": {"language": f"*{l}*"}} for l in (language if isinstance(language, list) else [language])]
        filter_conditions.append({
            "bool": {
                "should": language_conditions,
                "minimum_should_match": 1
            }
        })

    if sentiment:
        filter_conditions.append({
            "terms": {
                "sentiment": sentiment if isinstance(sentiment, list) else [sentiment]
            }
        })

    if domain:
        domain_conditions = [{"wildcard": {"link_post": f"*{d}*"}} for d in (domain if isinstance(domain, list) else [domain])]
        filter_conditions.append({
            "bool": {
                "should": domain_conditions,
                "minimum_should_match": 1
            }
        })

    query_body = {
        "size": 0,
        "query": {
            "bool": {
                "must": must_conditions
            }
        },
        "aggs": {
            "issues": {
                "terms": {
                    "field": "issue.keyword",
                    "size": 10000
                },
                "aggs": {
                    "total_post": {"value_count": {"field": "issue.keyword"}},
                    "total_viral_score": {"sum": {"field": "viral_score"}},
                    "total_reach_score": {"sum": {"field": "reach_score"}},
                    "sentiment_positive": {"filter": {"term": {"sentiment": "positive"}}},
                    "sentiment_negative": {"filter": {"term": {"sentiment": "negative"}}},
                    "sentiment_neutral": {"filter": {"term": {"sentiment": "neutral"}}}
                }
            }
        }
    }

    if filter_conditions:
        query_body["query"]["bool"]["filter"] = filter_conditions

    try:
        response = es.search(
            index=",".join(available_indices),
            body=query_body
        )

        buckets = response['aggregations']['issues']['buckets']
        clean_data = []

        for bucket in buckets:
            clean_data.append({
                "issue": bucket['key'],
                "total_post": int(bucket['total_post']['value']),
                "total_viral_score": bucket['total_viral_score']['value'],
                "total_reach_score": bucket['total_reach_score']['value'],
                "total_positive": bucket['sentiment_positive']['doc_count'],
                "total_negative": bucket['sentiment_negative']['doc_count'],
                "total_neutral": bucket['sentiment_neutral']['doc_count']
            })

        return pd.DataFrame(clean_data)

    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return pd.DataFrame()

def get_central_issue(data_chunk: pd.DataFrame) -> pd.DataFrame:
    """Get central issues from data chunk using Gemini"""
    issue_list = []
    for idx, row in data_chunk.iterrows():
        issue_list.append({"id": row['index'], "issue": row['issue']})

    prompt = f"""
You are a Social Media Analysis Expert specializing in thematic clustering of issues.

# TASK
Analyze and group the following list of social media issues into meaningful thematic clusters.

# IMPORTANT INSTRUCTIONS
1. Each issue ID must belong to EXACTLY ONE group (mutually exclusive).
2. Avoid any overlap of issues between groups.
3. Focus on identifying the main topic or theme of each issue.
4. Create a unified_issue name that is **clear, specific, and truly represents the essence** of the grouped issues.
5. Avoid using overly generic group names like "General Issues" or "Various Topics."
6. Provide an informative and concise description for each unified_issue that accurately summarizes its main theme.

# LIST OF ISSUES
```
{issue_list}
```

# OUTPUT FORMAT
Return the grouped results strictly in the following JSON format:
```
[
  {{
    "unified_issue": "Name of Grouped Issue 1",
    "description": "Concise description summarizing the main theme of Group 1",
    "list_issue_id": [1, 5, 10, 15]
  }},
  {{
    "unified_issue": "Name of Grouped Issue 2",
    "description": "Concise description summarizing the main theme of Group 2",
    "list_issue_id": [2, 6, 11, 16]
  }},
  ...
]
```

# QUALITY PARAMETERS
- Group based on semantic similarity of topics and keywords.
- Choose a **specific** and **meaningful** name for each unified_issue that reflects the core topic clearly.

# HARD RULES:
- Return the output ONLY in pure JSON format, without any explanation or additional comments.
- The unified_issue name and description must be in English.
"""

    centrality = call_gemini(prompt)
    return pd.DataFrame(eval(re.findall(r'\[.*\]', centrality, flags=re.I|re.S)[0]))

def check_new_data(data_chunk: pd.DataFrame, unified_issue_map: List[Dict]) -> pd.DataFrame:
    """Check new data against existing unified issues"""
    issue_list = []
    for idx, row in data_chunk.iterrows():
        issue_list.append({"id": row['index'], "issue": row['issue']})

    prompt = f"""
    You are a Social Media Analysis Expert specializing in thematic clustering of social media issues.

    # TASK
    Analyze and group the following list of social media issues into meaningful thematic clusters.

    # IMPORTANT INSTRUCTIONS
    1. Each issue ID must belong to EXACTLY ONE group (mutually exclusive).
    2. Avoid any overlap of issues between different groups.
    3. Focus on identifying the main topic or theme of each issue.
    4. Try to use an existing unified_issue category if it appropriately represents the issues.
    5. If no existing unified_issue fits well, you are allowed to create a **new** unified_issue name that clearly reflects the core theme of the issues.
    6. Create a unified_issue name that is **clear, specific, and truly represents the essence** of the grouped issues.
    7. Avoid overly generic names like "General Issues", "Miscellaneous", or "Various Topics."
    8. Provide a concise and informative description for each unified_issue that summarizes its main theme.

    # EXISTING CATEGORY UNIFIED ISSUE
    {[i['unified_issue'] for i in unified_issue_map]}

    # LIST OF ISSUES TO CLUSTER
    ```
    {issue_list}
    ```

    # OUTPUT FORMAT
    Return the grouped results strictly in the following pure JSON format:
    ```
    [
      {{
        "unified_issue": "Name of Grouped Issue 1",
        "description": "Concise description summarizing the main theme of Group 1",
        "list_issue_id": [1, 5, 10, 15]
      }},
      {{
        "unified_issue": "Name of Grouped Issue 2",
        "description": "Concise description summarizing the main theme of Group 2",
        "list_issue_id": [2, 6, 11, 16]
      }},
      ...
    ]
    ```

    # QUALITY PARAMETERS
    - Group based on semantic similarity of topics and keywords.
    - Prefer using an existing unified_issue category when relevant.
    - Create a new specific and meaningful unified_issue if necessary.

    # HARD RULES
    - Only return the output in pure JSON format without any explanations, headings, or comments.
    - Use English for all unified_issue names and descriptions.
    - Ensure all issue IDs are covered exactly once.
    """
    
    new_data = call_gemini(prompt)
    return pd.DataFrame(eval(re.findall(r'\[.*\]', new_data, flags=re.I|re.S)[0]))
