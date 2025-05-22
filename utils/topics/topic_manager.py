import pandas as pd
from typing import List, Dict, Any, Optional
from elasticsearch import Elasticsearch
from utils.redis_client import redis_client
from .es_operations import upsert_documents
from .topic_analyzer import (
    get_data_topics,
    get_central_issue,
    check_new_data,
    create_uuid
)
from utils.es_client import get_elasticsearch_client

def get_unified_issue(es: Elasticsearch, project_name: str) -> List[Dict]:
    """Get existing unified issues for a project"""
    query_body = {
        "_source": ["unified_issue"],
        "query": {
            "bool": {
                "must": [
                    {"match": {"project_name.keyword": project_name}}
                ]
            }
        }
    }

    response = es.search(
        index="topic_cluster",
        body=query_body,
        size=10000
    )
    return [i['_source'] for i in response['hits']['hits']]

def new_topics(
    owner_id: Optional[str] = None,
    project_name: Optional[str] = None,
    es: Optional[Elasticsearch] = None,
    keywords: Optional[str] = None,
    search_keyword: Optional[str] = None,
    search_exact_phrases: bool = False,
    case_sensitive: bool = False,
    start_date: Optional[str] = None,
    sentiment: Optional[str] = None,
    end_date: Optional[str] = None,
    date_filter: str = "last 30 days",
    custom_start_date: Optional[str] = None,
    custom_end_date: Optional[str] = None,
    channels: Optional[List[str]] = None,
    importance: str = "all mentions",
    influence_score_min: Optional[float] = None,
    influence_score_max: Optional[float] = None,
    region: Optional[str] = None,
    language: Optional[str] = None,
    domain: Optional[str] = None
) -> List[Dict]:
    """Process new topics for a project"""
    if es is None:
        es = get_elasticsearch_client()

    data_issue = get_data_topics(
        es=es,
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

    if data_issue.empty:
        return []

    data_issue = data_issue.sort_values('total_viral_score', ascending=False).reset_index()
    df_central = get_central_issue(data_issue[:100])

    df_exploded = df_central.explode('list_issue_id')
    df_exploded = df_exploded.rename(columns={"list_issue_id": "index"})

    df_merged = pd.merge(
        df_exploded[['index', 'unified_issue', 'description']],
        data_issue,
        on='index',
        how='left'
    )

    df_final = df_merged.groupby(['unified_issue', 'description']).agg(
        list_issue=('issue', list),
        total_posts=('total_post', 'sum'),
        viral_score=('total_viral_score', 'sum'),
        reach_score=('total_reach_score', 'sum'),
        positive=('total_positive', 'sum'),
        negative=('total_negative', 'sum'),
        neutral=('total_neutral', 'sum')
    ).reset_index()

    df_final['share_of_voice'] = (df_final['total_posts']/df_final['total_posts'].sum())*100
    df_final = df_final.sort_values('share_of_voice', ascending=False)
    
    return df_final.to_dict(orient='records')

def topic_existing_data(
    owner_id: Optional[str] = None,
    project_name: Optional[str] = None,
    es: Optional[Elasticsearch] = None,
    keywords: Optional[str] = None,
    search_keyword:Optional[str] = None,
    search_exact_phrases: bool = False,
    case_sensitive: bool = False,
    start_date: Optional[str] = None,
    sentiment: Optional[str] = None,
    end_date: Optional[str] = None,
    date_filter: str = "last 30 days",
    custom_start_date: Optional[str] = None,
    custom_end_date: Optional[str] = None,
    channels: Optional[List[str]] = None,
    importance: str = "all mentions",
    influence_score_min: Optional[float] = None,
    influence_score_max: Optional[float] = None,
    region: Optional[str] = None,
    language: Optional[str] = None,
    domain: Optional[str] = None,
    unified_issue_map: List[Dict] = []
) -> List[Dict]:
    """Process topics for an existing project"""
    data_post = get_data_topics(
        es=es,
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

    if data_post.empty:
        return [], False
    
    list_issue = data_post['issue'].to_list()
    print(f"Total issues: {len(list_issue)}")
    print(f"Project name: {project_name}")

    if es is None:
        es = get_elasticsearch_client()

    query_body = {
        "_source": ["unified_issue", "list_issue", 'description'],
        "query": {
            "bool": {
                "must": [
                    {"match": {"project_name.keyword": project_name}},
                    {"terms": {"list_issue.keyword": list_issue}}
                ]
            }
        }
    }

    response = es.search(
        index="topic_cluster",
        body=query_body,
        size=10000
    )
    top_map = [i['_source'] for i in response['hits']['hits']]

    done = []
    for i in list_issue:
        for j in top_map:
            if i in j['list_issue']:
                done.append({
                    'issue': i,
                    'unified_issue': j['unified_issue'],
                    'description': j['description']
                })
                break

    undone = set(list_issue) - set([i['issue'] for i in done])

    df_undone = pd.DataFrame()
    is_ingest = False
    if undone:
        print(f'Predicting {len(undone)} new issues')
        data_undone = pd.DataFrame(undone, columns=['issue']).reset_index()
        try:
            hasil = check_new_data(data_undone[:100], unified_issue_map)

            df_exploded = hasil.explode('list_issue_id')
            df_exploded = df_exploded.rename(columns={"list_issue_id": "index"})

            df_undone = pd.merge(
                df_exploded[['index', 'unified_issue', 'description']],
                data_undone,
                on='index',
                how='left'
            ).drop('index', axis=1)
            is_ingest = True
        except Exception as e:
           
            df_undone = pd.DataFrame()

    else:
        print('All issues already predicted')

    df_done = pd.DataFrame(done)
    all_predict = pd.concat([df_done, df_undone])

    all_result = all_predict.merge(data_post, on='issue').groupby(['unified_issue']).agg(
        description=('description', 'max'),
        list_issue=('issue', list),
        total_posts=('total_post', 'sum'),
        viral_score=('total_viral_score', 'sum'),
        reach_score=('total_reach_score', 'sum'),
        positive=('total_positive', 'sum'),
        negative=('total_negative', 'sum'),
        neutral=('total_neutral', 'sum')
    ).reset_index()

    all_result['share_of_voice'] = (all_result['total_posts']/all_result['total_posts'].sum())*100
    all_result = all_result.sort_values('share_of_voice', ascending=False)
    
    return all_result.to_dict(orient='records'), is_ingest

def ingest_topic(results: List[Dict], project_name: str, es: Optional[Elasticsearch] = None) -> None:
    """Ingest topic results into Elasticsearch"""
    if es is None:
        es = get_elasticsearch_client()

    df = pd.DataFrame(results)
    df['project_name'] = project_name
    df['uuid'] = df.apply(lambda s: create_uuid(f"{s['project_name']},{s['unified_issue']}"), axis=1)

    data_ingest = df[['unified_issue', 'description', 'list_issue', 'uuid', 'project_name']].to_dict(orient='records')

    updated, created, errors = upsert_documents(
        es,
        data_ingest,
        "topic_cluster",
        id_field="uuid",
        fields_to_update=None,
        chunk_size=1000
    )

    print(f"Results: {updated} documents updated, {created} documents created, {len(errors)} errors")

def topic_overviews(
    owner_id: Optional[str] = None,
    project_name: Optional[str] = None,
    es: Optional[Elasticsearch] = None,
    keywords: Optional[str] = None,
    search_keyword: Optional[str] = None,
    search_exact_phrases: bool = False,
    case_sensitive: bool = False,
    start_date: Optional[str] = None,
    sentiment: Optional[str] = None,
    end_date: Optional[str] = None,
    date_filter: str = "last 30 days",
    custom_start_date: Optional[str] = None,
    custom_end_date: Optional[str] = None,
    channels: Optional[List[str]] = None,
    importance: str = "all mentions",
    influence_score_min: Optional[float] = None,
    influence_score_max: Optional[float] = None,
    region: Optional[str] = None,
    language: Optional[str] = None,
    domain: Optional[str] = None
) -> List[Dict]:

    if not search_keyword:
        search_keyword = []
    # Generate cache key based on all parameters
    cache_key = redis_client.generate_cache_key(
        "topics_overview",
        owner_id=owner_id,
        project_name=project_name,
        keywords=keywords,
        search_keyword =search_keyword,
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

    """Main entry point for topic overview analysis"""
    if es is None:
        es = get_elasticsearch_client()

    unified_issue_map = get_unified_issue(es, project_name)
    


    if not unified_issue_map:
        print('New project detected')
        is_ingest = True
        results = new_topics(
            es=es,
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
    else:
        print('Existing project detected')
        results, is_ingest = topic_existing_data(
            owner_id=owner_id,
            project_name=project_name,
            es=es,
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
            unified_issue_map=unified_issue_map
        )

    if is_ingest:
        print('Ingesting topics...')
        ingest_topic(results, project_name, es)
    else:
        print('No need to ingest topics')

    # Cache the results for 10 minutes
    redis_client.set_with_ttl(cache_key, results, ttl_seconds=30*60)
    return results
