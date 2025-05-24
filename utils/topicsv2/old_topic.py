from .helper import ( # Relative import
    get_data_issue,
    ingest_topic,
    regenerate_topics, 
    split_rows_by_list_length
)
from elasticsearch import Elasticsearch
from typing import List, Dict, Any, Optional
import pandas as pd

# Import BackgroundTasks for type hinting
from fastapi import BackgroundTasks


def _background_process_old_topics(
    df_issue_unmapped: pd.DataFrame,
    project_name: str,
    es: Elasticsearch,
    existing_topic_suggestions: List[Dict[str, str]],
    owner_id: Optional[str],
    keywords: Optional[List[str]],
    search_keyword: Optional[List[str]],
    search_exact_phrases: bool,
    case_sensitive: bool,
    sentiment: Optional[List[str]],
    start_date: Optional[str],
    end_date: Optional[str],
    date_filter: str,
    custom_start_date: Optional[str],
    custom_end_date: Optional[str],
    channels: Optional[List[str]],
    importance: str,
    influence_score_min: Optional[float] = 0,
    influence_score_max: Optional[float] = 1000,
    region: Optional[List[str]] = [], # Added default
    language: Optional[List[str]] = [], # Added default
    domain: Optional[List[str]] = [] # Added default
):
    """
    Processes unmapped issues in the background for an existing project.
    """
    print(f"Background processing started for unmapped topics in project: {project_name}")
    
    loop = 10  # Max iterations
    df_issue_current_batch = df_issue_unmapped.copy()
    current_suggestions = existing_topic_suggestions # Start with suggestions from already mapped topics

    for i in range(loop):
        if df_issue_current_batch.empty:
            print("No more unmapped issues to process in background.")
            break
        
        print(f"Background iteration {i+1}/{loop} for project {project_name}. Unmapped issues to process: {len(df_issue_current_batch)}")

        # Regenerate topics for the current batch of unmapped issues
        df_newly_mapped_topics = regenerate_topics(
            project_name=project_name, # Pass project_name
            suggestion_unified_issue=current_suggestions,
            df_issue=df_issue_current_batch,
            limit_issues=100 # Process 100 issues per background iteration
        )

        if df_newly_mapped_topics.empty:
            print(f"No new topics mapped in background iteration {i+1}.")
            # Similar to new_topics, remove the attempted issues from the current batch
            processed_indices = df_issue_current_batch.sort_values(
                ['total_post','negative_posts','total_reach_score'],
                ascending = [False,False,False]
            )[:100]['index'].tolist()
            df_issue_current_batch = df_issue_current_batch[~df_issue_current_batch['index'].isin(processed_indices)]
            continue

        df_newly_mapped_topics_split = split_rows_by_list_length(df_newly_mapped_topics.copy()) # Apply split
        
        ingest_topic(df_newly_mapped_topics_split.to_dict(orient='records'), project_name, es=es)
        
        # Update suggestions for the next iteration
        current_suggestions.extend(
            df_newly_mapped_topics_split[['unified_issue', 'description']].drop_duplicates().to_dict(orient='records')
        )
        current_suggestions = [dict(t) for t in {tuple(d.items()) for d in current_suggestions}]


        # Remove processed issues from df_issue_current_batch
        processed_issue_texts_in_batch = [
            item for sublist in df_newly_mapped_topics_split['list_issue'].dropna() for item in sublist
        ]
        df_issue_current_batch = df_issue_current_batch[
            ~df_issue_current_batch['issue'].isin(processed_issue_texts_in_batch)
        ]
        
        print(f"Background iteration {i+1} completed. {len(df_newly_mapped_topics_split)} new topic segments ingested. Remaining unmapped issues: {len(df_issue_current_batch)}")

    print(f"Background processing finished for unmapped topics in project: {project_name}")


def old_topics(
    topics_overview_data: List[Dict[str, Any]], # Data from get_data_topics
    owner_id: Optional[str] = "1",
    project_name: Optional[str] = None,
    es: Optional[Elasticsearch] = None,
    background_tasks: BackgroundTasks = None, # Added for FastAPI
    keywords: Optional[List[str]] = [],
    search_keyword: Optional[List[str]] = [],
    search_exact_phrases: bool = False,
    case_sensitive: bool = False,
    sentiment: Optional[List[str]] = ['positive','negative','neutral'],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    date_filter: str = "last 30 days",
    custom_start_date: Optional[str] = None,
    custom_end_date: Optional[str] = None,
    channels: Optional[List[str]] = None,
    importance: str = "all mentions",
    influence_score_min: Optional[float] = 0,
    influence_score_max: Optional[float] = 1000,
    region: Optional[List[str]] = [],
    language: Optional[List[str]] = [],
    domain: Optional[List[str]] = []
):
    if es is None:
        raise ValueError("Elasticsearch client (es) must be provided to old_topics.")
    if project_name is None:
        raise ValueError("project_name must be provided to old_topics.")

    # Get all issues based on current filters, similar to new_topics
    # Limit might be different or not applied if we expect topics_overview_data to guide us
    df_all_issues_from_filters = get_data_issue(
        owner_id=owner_id, # Pass owner_id
        es=es,
        project_name=project_name,
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
        limit=1000 # Fetch a reasonable number of issues to check against existing topics
    )

    if df_all_issues_from_filters.empty:
        print(f"No issues found for project {project_name} with the given filters. Returning empty list.")
        return []

    # Use topics_overview_data (from ES 'topics_overview' index)
    # This data contains 'unified_issue', 'description', 'list_issue'
    df_existing_topics = pd.DataFrame(topics_overview_data)
    
    results_for_frontend = []
    df_issue_unmapped = df_all_issues_from_filters.copy()
    existing_topic_suggestions_for_background = []

    if not df_existing_topics.empty and 'list_issue' in df_existing_topics.columns:
        # Explode existing topics to get individual issue texts
        df_existing_topics_exploded = df_existing_topics.explode('list_issue')
        df_existing_topics_exploded = df_existing_topics_exploded.rename(columns={"list_issue": "issue_text"})
        
        # Merge with current issues from filters to get stats for existing topics
        # We need to match 'issue_text' from existing topics with 'issue' from df_all_issues_from_filters
        df_merged_for_stats = pd.merge(
            df_existing_topics_exploded[['issue_text', 'unified_issue', 'description']],
            df_all_issues_from_filters, # This contains stats per issue
            left_on='issue_text',
            right_on='issue', # Assuming 'issue' in df_all_issues_from_filters is the text
            how='inner' # Only consider issues that are in both existing topics and current filter results
        )

        if not df_merged_for_stats.empty:
            df_final_mapped = df_merged_for_stats.groupby(['unified_issue', 'description']).agg(
                list_issue=('issue_text', lambda x: list(x.dropna().unique())),
                total_posts=('total_post', 'sum'),
                viral_score=('total_viral_score', 'sum'),
                reach_score=('total_reach_score', 'sum'),
                positive=('positive_posts', 'sum'),
                negative=('negative_posts', 'sum'),
                neutral=('neutral_posts', 'sum')
            ).reset_index()

            if not df_final_mapped.empty and 'total_posts' in df_final_mapped.columns and df_final_mapped['total_posts'].sum() > 0:
                df_final_mapped['share_of_voice'] = (df_final_mapped['total_posts'] / df_final_mapped['total_posts'].sum()) * 100
            else:
                df_final_mapped['share_of_voice'] = 0
            
            df_final_mapped = df_final_mapped.sort_values('share_of_voice', ascending=False)
            results_for_frontend = df_final_mapped.to_dict(orient='records')
            print(f"Returning {len(results_for_frontend)} already mapped topics for project {project_name}.")

            # Identify unmapped issues from the current filter results
            mapped_issue_texts = [
                item for sublist in df_final_mapped['list_issue'].dropna() for item in sublist
            ]
            df_issue_unmapped = df_all_issues_from_filters[
                ~df_all_issues_from_filters['issue'].isin(mapped_issue_texts)
            ]
            
            # Suggestions for background task will be from these mapped topics
            existing_topic_suggestions_for_background = df_final_mapped[
                ['unified_issue', 'description']
            ].drop_duplicates().to_dict(orient='records')
        else:
            print(f"No overlap between existing topics and currently filtered issues for project {project_name}.")
            # All issues from current filter are considered unmapped
            df_issue_unmapped = df_all_issues_from_filters.copy()
            # No suggestions from existing topics if no overlap
            existing_topic_suggestions_for_background = []


    else: # No existing topics in topics_overview_data or it's malformed
        print(f"No existing topics found in topics_overview_data for project {project_name}. All fetched issues are considered unmapped.")
        df_issue_unmapped = df_all_issues_from_filters.copy()
        existing_topic_suggestions_for_background = []


    # Schedule background processing for unmapped issues
    if not df_issue_unmapped.empty:
        print(f"Preparing {len(df_issue_unmapped)} unmapped issues for background processing for project {project_name}.")
        if background_tasks:
            background_tasks.add_task(
                _background_process_old_topics,
                df_issue_unmapped=df_issue_unmapped.copy(),
                project_name=project_name,
                es=es,
                existing_topic_suggestions=existing_topic_suggestions_for_background,
                owner_id=owner_id, keywords=keywords, search_keyword=search_keyword,
                search_exact_phrases=search_exact_phrases, case_sensitive=case_sensitive,
                sentiment=sentiment, start_date=start_date, end_date=end_date,
                date_filter=date_filter, custom_start_date=custom_start_date,
                custom_end_date=custom_end_date, channels=channels, importance=importance,
                influence_score_min=influence_score_min, influence_score_max=influence_score_max,
                region=region, language=language, domain=domain
            )
            print(f"Scheduled background task for {len(df_issue_unmapped)} unmapped issues for project {project_name}.")
        else:
            print("BackgroundTasks not available. Running unmapped issues processing synchronously for testing.")
            _background_process_old_topics(
                df_issue_unmapped=df_issue_unmapped.copy(),
                project_name=project_name,
                es=es,
                existing_topic_suggestions=existing_topic_suggestions_for_background,
                owner_id=owner_id, keywords=keywords, search_keyword=search_keyword,
                search_exact_phrases=search_exact_phrases, case_sensitive=case_sensitive,
                sentiment=sentiment, start_date=start_date, end_date=end_date,
                date_filter=date_filter, custom_start_date=custom_start_date,
                custom_end_date=custom_end_date, channels=channels, importance=importance,
                influence_score_min=influence_score_min, influence_score_max=influence_score_max,
                region=region, language=language, domain=domain
            )
    else:
        print(f"No unmapped issues to process in background for project {project_name}.")

    return results_for_frontend
