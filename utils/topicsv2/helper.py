#check apakah project sudah ada?
from elasticsearch import Elasticsearch, helpers
from typing import List, Dict, Any, Optional
import os, re
import pandas as pd
from utils.gemini import call_gemini
from utils.list_of_mentions import get_mentions
from utils.topics.es_operations import upsert_documents
import uuid
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()
ES_HOST = os.getenv('ES_HOST')
ES_USERNAME = os.getenv('ES_USERNAME')
ES_PASSWORD = os.getenv('ES_PASSWORD')
# Inisialisasi client
es_client_instance = Elasticsearch(hosts=ES_HOST, http_auth = (ES_USERNAME, ES_PASSWORD))

def get_elasticsearch_client():
    # This function can be expanded later for more complex client management if needed
    # For now, it returns the globally initialized instance or creates a new one.
    # However, it's better to pass the ES client as an argument to functions that need it.
    global es_client_instance
    if es_client_instance is None:
        es_client_instance = Elasticsearch(hosts=ES_HOST, http_auth=(ES_USERNAME, ES_PASSWORD))
    return es_client_instance

def get_data_topics(project_name: str, es: Elasticsearch):
    query_body = {
      "query": {
        "bool": {
            "must":{
                "match":{
                    "project_name": project_name
                }
            }
        }
      }
    }

    data = es.search(
        index = "topics_overview", # Corrected index name
        body = query_body,
        size = 10000
    )

    topics_overview = [i['_source'] for i in data['hits']['hits']]
    return topics_overview

def create_uuid(keyword: str) -> str:
    """Generate UUID for a given keyword"""
    namespace = uuid.NAMESPACE_DNS
    return str(uuid.uuid5(namespace, keyword))

def ingest_topic(results: List[Dict[str, Any]], project_name: str, es: Elasticsearch) -> None:
    """Ingest topic results into Elasticsearch"""
    # Removed fallback to get_elasticsearch_client() as es should be passed directly.
    # if es is None:
    #     es = get_elasticsearch_client() # Ensure get_elasticsearch_client is defined or es is always passed

    df = pd.DataFrame(results)
    df['project_name'] = project_name
    df['index'] = range(df.shape[0])
    df['uuid'] = df.apply(lambda s: create_uuid(f"{s['project_name']},{s['unified_issue']}" if len(s['list_issue'])<100\
                                                else f"{s['project_name']},{s['unified_issue']},{s['index']}"), axis=1)

    data_ingest = df[['unified_issue', 'description', 'list_issue', 'uuid', 'project_name']].to_dict(orient='records')

    updated, created, errors = upsert_documents(
        es,
        data_ingest,
        "topics_overview",
        id_field="uuid",
        fields_to_update=None,
        chunk_size=1000
    )

    print(f"Results: {updated} documents updated, {created} documents created, {len(errors)} errors")
    
def split_rows_by_list_length(df, column='list_issue', max_len=100):
    """
    Membelah baris jika panjang list dalam kolom tertentu melebihi max_len.

    Args:
        df (pd.DataFrame): DataFrame dengan kolom list.
        column (str): Nama kolom yang berisi list.
        max_len (int): Jumlah maksimum elemen per baris.

    Returns:
        pd.DataFrame: DataFrame baru dengan baris terbelah.
    """
    new_rows = []

    for _, row in df.iterrows():
        items = row[column]
        # Membagi list menjadi potongan dengan panjang maksimal max_len
        chunks = [items[i:i + max_len] for i in range(0, len(items), max_len)]
        for chunk in chunks:
            new_row = row.copy()
            new_row[column] = chunk
            new_rows.append(new_row)

    return pd.DataFrame(new_rows).reset_index(drop=True)    
    
def get_date_range(date_filter: str = "last 30 days", custom_start_date: Optional[str] = None, custom_end_date: Optional[str] = None):

    today = datetime.now().date()
    
    if date_filter == "custom" and custom_start_date and custom_end_date:
        return custom_start_date, custom_end_date
    
    if date_filter == "yesterday":
        yesterday = today - timedelta(days=1)
        return yesterday.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")
    
    elif date_filter == "this week":
        start_of_week = today - timedelta(days=today.weekday())
        return start_of_week.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")

    elif date_filter == "last 14 days":
        return (today - timedelta(days=14)).strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")

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

def _call_gemini_and_parse(prompt: str) -> pd.DataFrame:
    """Calls Gemini API and parses the JSON response."""
    centrality = call_gemini(prompt)
    try:
        # Attempt to find and parse the JSON list directly
        json_match = re.search(r'\[.*\]', centrality, flags=re.DOTALL | re.IGNORECASE)
        if json_match:
            parsed_json = eval(json_match.group(0)) # Using eval, consider safer json.loads if possible
            return pd.DataFrame(parsed_json)
        else:
            # Fallback to regex parsing if direct JSON match fails (as in original code)
            # This part might need adjustment if the Gemini output format is inconsistent
            unified_issue = [i.strip('\n ,"') for i in re.findall(r'unified_issue[\"\,\s\:]+(.*?)description', centrality, flags=re.I|re.S)]
            description = [i.strip('\n ,"') for i in re.findall(r'description[\"\,\s\:]+(.*?)list_issue_id', centrality, flags=re.I|re.S)]
            list_issue_id_str = re.findall(r'list_issue_id[\"\,\s\:]+(.*?)\}', centrality, flags=re.I|re.S)
            
            list_issue_id = []
            for item_str in list_issue_id_str:
                try:
                    # Clean up the string before eval
                    cleaned_item_str = item_str.strip('\n ,"[]')
                    if cleaned_item_str: # Ensure it's not empty
                         # Add brackets if missing for eval to parse as list
                        if not cleaned_item_str.startswith('['):
                            cleaned_item_str = '[' + cleaned_item_str
                        if not cleaned_item_str.endswith(']'):
                            cleaned_item_str = cleaned_item_str + ']'
                        list_issue_id.append(eval(cleaned_item_str))
                    else:
                        list_issue_id.append([]) # Append empty list if string is empty
                except Exception as e:
                    print(f"Error parsing list_issue_id item: {item_str} with error {e}")
                    list_issue_id.append([]) # Append empty list on error

            data = []
            for u, d, l in zip(unified_issue, description, list_issue_id):
                data.append({'unified_issue': u, 'description': d, 'list_issue_id': l})
            return pd.DataFrame(data)

    except Exception as e:
        print(f"Error processing Gemini response: {e}\nResponse was:\n{centrality}")
        return pd.DataFrame(columns=['unified_issue', 'description', 'list_issue_id'])


def _process_gemini_results_to_dataframe(gemini_output_df: pd.DataFrame, df_issue: pd.DataFrame) -> pd.DataFrame:
    """Processes the parsed Gemini output and merges it with df_issue."""
    if gemini_output_df.empty or 'list_issue_id' not in gemini_output_df.columns:
        # Return an empty DataFrame with expected columns if Gemini output is not processable
        return pd.DataFrame(columns=[
            'unified_issue', 'description', 'list_issue', 
            'total_posts', 'viral_score', 'reach_score', 
            'positive', 'negative', 'neutral', 'share_of_voice'
        ])

    df_exploded = gemini_output_df.explode('list_issue_id')
    df_exploded = df_exploded.rename(columns={"list_issue_id": "index"})

    # Ensure 'index' column in df_issue is compatible for merging
    if 'index' not in df_issue.columns:
        # If df_issue doesn't have 'index', this merge will fail or produce incorrect results.
        # This assumes df_issue has an 'index' column that corresponds to 'id' in Gemini's list_issue_id.
        print("Warning: 'index' column not found in df_issue for merging with Gemini results.")
        # Create a dummy 'index' if it's missing, though this might not be correct behavior
        # df_issue['index'] = range(len(df_issue)) # Or handle as an error
        # For now, let's proceed assuming it exists or the merge will be empty.
        pass


    df_merged = pd.merge(
        df_exploded[['index', 'unified_issue', 'description']],
        df_issue,
        on='index', # This 'index' must match the 'id's provided to Gemini
        how='left'
    )
    
    if df_merged.empty:
         return pd.DataFrame(columns=[
            'unified_issue', 'description', 'list_issue', 
            'total_posts', 'viral_score', 'reach_score', 
            'positive', 'negative', 'neutral', 'share_of_voice'
        ])


    df_final = df_merged.groupby(['unified_issue', 'description']).agg(
        list_issue=('issue', lambda x: list(x.dropna().unique())), # Ensure unique issues and handle potential NaNs
        total_posts=('total_post', 'sum'),
        viral_score=('total_viral_score', 'sum'),
        reach_score=('total_reach_score', 'sum'),
        positive=('positive_posts', 'sum'),
        negative=('negative_posts', 'sum'),
        neutral=('neutral_posts', 'sum')
    ).reset_index()

    if not df_final.empty and 'total_posts' in df_final.columns and df_final['total_posts'].sum() > 0:
        df_final['share_of_voice'] = (df_final['total_posts'] / df_final['total_posts'].sum()) * 100
    else:
        df_final['share_of_voice'] = 0
        
    df_final = df_final.sort_values('share_of_voice', ascending=False)
    return df_final

def regenerate_topics(project_name: str, suggestion_unified_issue: List[Dict[str, str]], df_issue: pd.DataFrame, limit_issues: int = 100) -> pd.DataFrame:
    #call gemini untuk clustering
    issue_list = []
    for idx, row in df_issue\
                    .sort_values(['total_post','negative_posts','total_reach_score'],
                                ascending = [False,False,False])[:limit_issues].iterrows(): # Use limit_issues
        issue_list.append({"id": row['index'], "issue": row['issue']}) # 'id' should match df_issue['index']

    if not issue_list:
        return pd.DataFrame(columns=[
            'unified_issue', 'description', 'list_issue', 
            'total_posts', 'viral_score', 'reach_score', 
            'positive', 'negative', 'neutral', 'share_of_voice'
        ])

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
    6. Provide an informative and comprehensive description for each unified_issue that:
       - Accurately summarizes its main theme,
       - Highlights prevalent sentiment(s) (e.g., positive, negative, neutral),
       - Mentions any notable patterns, concerns, or emotional tones observed,
       - Includes relevant contextual details or recurring keywords if applicable.
    7. If an issue is not related to the topic of "{project_name}", group it under the name **"Other"** and provide a description that clearly reflects the non-relevance to "{project_name}."
    8. Use the following suggestion list of possible unified_issue names and descriptions as references. If any suggestion fits well for a group of issues, feel free to use that unified_issue name and description exactly as provided.
    
    ## SUGGESTED UNIFIED_ISSUES (unified_issue and description)
    ```
    {suggestion_unified_issue}
    ```

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
    "list_issue_id": [1, 5, 10, 15] # These IDs must correspond to the 'id' values in LIST OF ISSUES
    }},
    {{
    "unified_issue": "Name of Grouped Issue 2",
    "description": "Concise description summarizing the main theme of Group 2",
    "list_issue_id": [2, 6, 11, 16] # These IDs must correspond to the 'id' values in LIST OF ISSUES
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

    gemini_output_df = _call_gemini_and_parse(prompt)
    return _process_gemini_results_to_dataframe(gemini_output_df, df_issue)

def get_data_issue(owner_id: Optional[str] = "1", # Default as string if that's common
    project_name: Optional[str] = None,
    es: Elasticsearch = None, # Made non-optional as it's crucial
    keywords: Optional[List[str]] = [], # Corrected type to List[str]
    search_keyword: Optional[List[str]] = [], # Corrected type to List[str]
    search_exact_phrases: bool = False,
    case_sensitive: bool = False, # case_sensitive is not used in the query
    sentiment: Optional[List[str]] = ['positive','negative','neutral'], # Corrected type
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    date_filter: str = "last 30 days",
    custom_start_date: Optional[str] = None,
    custom_end_date: Optional[str] = None,
    channels: Optional[List[str]] = None,
    importance: str = "all mentions", # importance is not used in the query
    influence_score_min: Optional[float] = 0,
    influence_score_max: Optional[float] = 1000,
    region: Optional[List[str]] = [], # Corrected type
    language: Optional[List[str]] = [], # Corrected type
    domain: Optional[List[str]] = [], # Corrected type
    limit: int =1000
              ):
    if es is None:
        # Fallback or raise error if ES client is not provided
        # For now, let's assume it will be provided by the caller.
        # Consider raising ValueError("Elasticsearch client must be provided")
        print("Warning: Elasticsearch client not provided to get_data_issue. Using global instance.")
        es = get_elasticsearch_client()

    if not sentiment:
      sentiment = ['positive','negative','neutral']
    if not influence_score_min:
      influence_score_min = 0
    if not influence_score_max:
      influence_score_max = 1000

    # Build keywords query
    keyword_conditions = []
    if keywords: # Ensure keywords list is not empty
        keyword_conditions.append({
            "bool": {
                "should": [
                    {"match": {"post_caption": {"query": k, "operator": "AND"}}} for k in keywords
                ] + [
                    {"match": {"issue": {"query": k, "operator": "AND"}}} for k in keywords
                ],
                "minimum_should_match": 1
            }
        })

    # Build search_keyword query
    search_keyword_conditions = []
    if search_keyword: # Ensure search_keyword list is not empty
        # The original logic for search_keyword was inside a "must" along with "keywords"
        # and then another "should" for each item in search_keyword.
        # This implies that at least one of the `keywords` must match AND at least one of the `search_keyword` must match.
        # If search_exact_phrases is True, use match_phrase.
        operator_type = "match_phrase" if search_exact_phrases else "match"
        
        search_keyword_conditions.append({
            "bool": {
                "should": [
                    {operator_type: {"post_caption": {"query": sk, "operator": "AND"}}} for sk in search_keyword
                ] + [ # Assuming search_keyword also applies to 'issue' field
                    {operator_type: {"issue": {"query": sk, "operator": "AND"}}} for sk in search_keyword
                ],
                "minimum_should_match": 1
            }
        })

    if not start_date or not end_date:
        start_date, end_date = get_date_range(
            date_filter=date_filter,
            custom_start_date=custom_start_date,
            custom_end_date=custom_end_date
        )
    
    
    query = {
      "size": 0,
      "query": {
        "bool": {
          "must": [
            {
              "range": {
                "post_created_at": { # Assuming this is the correct date field
                    "gte": start_date,
                    "lte": end_date,
                    "format": "yyyy-MM-dd" # Specify date format if not standard
                }
              }
            }
          ] + keyword_conditions + search_keyword_conditions + [ # Add keyword and search_keyword conditions
            {
                "bool":{
                    "must_not":{
                        "match":{
                            "issue":"Not Specified" # Ensure this is case-sensitive if needed
                        }
                    }
                }
            }
          ],
          "filter": [
            {
              "terms": { # Using terms for multiple sentiment values
                "sentiment": sentiment if sentiment else [] # Handle empty list
              }
            },
            {
              "range": {
                "influence_score": {
                  "gte": influence_score_min,
                  "lte": influence_score_max
                }
              }
            }
            # Optional filters based on whether the lists are provided
          ] + ([
            {
              "bool": {
                "should": [{"wildcard": {"region": f"*{r}*"}} for r in region],
                "minimum_should_match": 1 if region else 0
              }
            }
          ] if region else []) + ([
            {
              "bool": {
                "should": [{"wildcard": {"language": f"*{l}*"}} for l in language],
                "minimum_should_match": 1 if language else 0
              }
            }
          ] if language else []) + ([
            {
              "bool": {
                "should": [{"wildcard": {"link_post": f"*{d}*"}} for d in domain], # Assuming link_post for domain
                "minimum_should_match": 1 if domain else 0
              }
            }
          ] if domain else []) + [
            { # Ensure viral_score and sentiment fields exist
              "bool": {
                "must": [
                  {
                    "exists": {
                      "field": "viral_score"
                    }
                  },
                  {
                    "exists": {
                      "field": "sentiment"
                    }
                  }
                ]
              }
            }
          ]
        }
      },
      "from": 0,
      "aggs": {
        "by_issue": {
          "terms": {
            "field": "issue.keyword",
            "size": limit,
            "order": {
              "_count": "desc"
            }
          },
          "aggs": {
            "total_viral_score": {
              "sum": { "field": "viral_score" }
            },
            "total_reach_score": {
              "sum": { "field": "reach_score" }
            },
            "total_post": {
              "value_count": { "field": "issue.keyword" }
            },
            "positive_posts": {
              "filter": { "term": { "sentiment": "positive" } }
            },
            "negative_posts": {
              "filter": { "term": { "sentiment": "negative" } }
            },
            "neutral_posts": {
              "filter": { "term": { "sentiment": "neutral" } }
            },
            "sample_posts": {
              "top_hits": {
                "size": 2,
                "_source": {
                  "includes": [
                    "post_caption",
                      "link_post"
                  ]
                }
              }
            }
          }
        }
      }
    }
    

    if not channels:
        channels = ['tiktok','instagram','news','reddit','facebook','twitter','linkedin','youtube']
    
    index = ','.join([i+'_data' for i in channels])

    import json
    print(json.dumps(query, indent=2))
    hasil = es.search(index = index,
             body = query)
    
    buckets = hasil['aggregations']['by_issue']['buckets']
    
    data = []
    for bucket in buckets:
        sample_posts_list = []
        for hit in bucket['sample_posts']['hits']['hits']:
            post = hit['_source']
            sample_posts_list.append({
                "post_caption": post.get("post_caption", ""),
                "link_post": post.get("link_post", "")
            })

        data.append({
            "issue": bucket['key'],
            "total_viral_score": bucket['total_viral_score']['value'],
            "total_reach_score": bucket['total_reach_score']['value'],
            "total_post": bucket['total_post']['value'],
            "positive_posts": bucket['positive_posts']['doc_count'],
            "negative_posts": bucket['negative_posts']['doc_count'],
            "neutral_posts": bucket['neutral_posts']['doc_count'],
            "sample_posts": sample_posts_list
        })

    # Buat DataFrame
    df_issue = pd.DataFrame(data).reset_index()
    return df_issue
