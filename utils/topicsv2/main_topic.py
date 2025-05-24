from .helper import get_data_topics # Relative import
from .new_topic import new_topics # Relative import
from .old_topic import old_topics # Relative import
from elasticsearch import Elasticsearch
from typing import List, Optional, Dict, Any # Added Dict, Any
from fastapi import BackgroundTasks # For FastAPI integration

# It's good practice to initialize the ES client once and pass it around,
# or have a clear way to get it. The helper.py initializes one,
# but main_topics also accepts 'es' as a parameter.
# We will prioritize the 'es' parameter if provided.
# from topicsv2.helper import get_elasticsearch_client # If a global client getter is preferred

def main_topics(
    project_name: str, # project_name should be mandatory
    es: Elasticsearch, # es client should be mandatory
    background_tasks: BackgroundTasks, # For FastAPI background tasks
    owner_id: Optional[str] = "1",
    keywords: Optional[List[str]] = [], # Type correction
    search_keyword: Optional[List[str]] = [], # Type correction
    search_exact_phrases: bool = False,
    case_sensitive: bool = False,
    sentiment: Optional[List[str]] = ['positive','negative','neutral'], # Type correction
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    date_filter: str = "last 30 days",
    custom_start_date: Optional[str] = None,
    custom_end_date: Optional[str] = None,
    channels: Optional[List[str]] = None,
    importance: str = "all mentions",
    influence_score_min: Optional[float] = 0,
    influence_score_max: Optional[float] = 1000,
    region: Optional[List[str]] = [], # Type correction
    language: Optional[List[str]] = [], # Type correction
    domain: Optional[List[str]] = [], # Type correction
    limit: int = 1000 # limit parameter was defined but not used by new_topics/old_topics directly
    ) -> List[Dict[str, Any]]: # Define return type

    if not project_name:
        raise ValueError("project_name is required.")
    if es is None: # Ensure ES client is provided
        # es = get_elasticsearch_client() # Option: get from a global helper
        raise ValueError("Elasticsearch client 'es' is required.")

    # Check if topics overview already exists for this project
    # Pass the 'es' client to get_data_topics
    topics_overview_data = get_data_topics(project_name=project_name, es=es)

    if not topics_overview_data:
        print(f"No existing topics found for project '{project_name}'. Processing as new topics.")
        return new_topics(
            owner_id=owner_id,
            project_name=project_name,
            es=es,
            background_tasks=background_tasks, # Pass background_tasks
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
            # limit is not a direct param for new_topics, it's used in its get_data_issue call
        )
    else:
        print(f"Existing topics found for project '{project_name}'. Processing as old topics.")
        return old_topics(
            topics_overview_data=topics_overview_data, # Pass fetched data
            owner_id=owner_id,
            project_name=project_name,
            es=es,
            background_tasks=background_tasks, # Pass background_tasks
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
            # limit is not a direct param for old_topics
        )
