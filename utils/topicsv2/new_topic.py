from .helper import ( # Relative import
    get_data_issue,
    ingest_topic,
    regenerate_topics, 
    split_rows_by_list_length,
    _call_gemini_and_parse,
    _process_gemini_results_to_dataframe
)
from elasticsearch import Elasticsearch
from typing import List, Dict, Any, Optional
import pandas as pd
import re # re is used in the original _call_gemini_and_parse fallback

# Import BackgroundTasks for type hinting, actual use will be in main_topic.py
from fastapi import BackgroundTasks


def _background_process_new_topics(
    df_issue_remaining: pd.DataFrame,
    project_name: str,
    es: Elasticsearch,
    initial_suggestions: List[Dict[str, str]],
    owner_id: Optional[str], # Pass through necessary params for regenerate_topics if they were used by it
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
    influence_score_min: Optional[float],
    influence_score_max: Optional[float],
    region: Optional[List[str]] = [], # Added default
    language: Optional[List[str]] = [], # Added default
    domain: Optional[List[str]] = [] # Added default
):
    """
    Processes remaining issues in the background.
    """
    print(f"Background processing started for new topics in project: {project_name}")
    df_processed_background = pd.DataFrame() # To accumulate results from background processing if needed for logging
    
    # The loop from the original new_topics, adapted for background processing
    loop = 10  # Max iterations for background processing
    df_issue_current_batch = df_issue_remaining.copy()

    # Initial suggestions for the first iteration of regenerate_topics in background
    current_suggestions = initial_suggestions

    for i in range(loop):
        if df_issue_current_batch.empty:
            print("No more issues to process in background.")
            break
        
        print(f"Background iteration {i+1}/{loop} for project {project_name}. Issues to process: {len(df_issue_current_batch)}")

        # Regenerate topics for the current batch of remaining issues
        # Pass project_name to regenerate_topics
        df_final_background_batch = regenerate_topics(
            project_name=project_name,
            suggestion_unified_issue=current_suggestions, 
            df_issue=df_issue_current_batch,
            limit_issues=100 # Process 100 issues per background iteration
        )

        if df_final_background_batch.empty:
            print(f"No new topics generated in background iteration {i+1}.")
            # Potentially break if no new topics are generated, or try with a fresh set of issues if logic allows
            # For now, we continue to ensure all parts of df_issue_current_batch are attempted
            # Update df_issue_current_batch to remove issues that might have been attempted but yielded no results
            # This depends on how regenerate_topics handles issues it can't cluster.
            # Assuming regenerate_topics processes its input and returns clusters,
            # we need to remove the *input* issues to regenerate_topics from df_issue_current_batch.
            # The current regenerate_topics takes top N, so we remove those top N.
            
            # Get the 'index' of issues that were sent to regenerate_topics
            processed_indices = df_issue_current_batch.sort_values(
                ['total_post','negative_posts','total_reach_score'],
                ascending = [False,False,False]
            )[:100]['index'].tolist()
            
            df_issue_current_batch = df_issue_current_batch[~df_issue_current_batch['index'].isin(processed_indices)]
            continue


        # Ingest these new topics
        ingest_topic(df_final_background_batch.to_dict(orient='records'), project_name, es=es)
        
        # Update suggestions for the next iteration based on newly generated topics
        current_suggestions.extend(
            df_final_background_batch[['unified_issue', 'description']].drop_duplicates().to_dict(orient='records')
        )
        # Deduplicate suggestions
        current_suggestions = [dict(t) for t in {tuple(d.items()) for d in current_suggestions}]


        # Remove processed issues from df_issue_current_batch
        # df_final_background_batch contains 'list_issue', which are the original issue texts.
        # df_issue_current_batch has 'issue' column.
        processed_issue_texts_in_batch = [
            item for sublist in df_final_background_batch['list_issue'].dropna() for item in sublist
        ]
        df_issue_current_batch = df_issue_current_batch[
            ~df_issue_current_batch['issue'].isin(processed_issue_texts_in_batch)
        ]
        
        # Accumulate processed data if needed (e.g., for logging completion)
        if df_processed_background.empty:
            df_processed_background = df_final_background_batch.copy()
        else:
            df_processed_background = pd.concat([df_processed_background, df_final_background_batch], ignore_index=True)

        print(f"Background iteration {i+1} completed. {len(df_final_background_batch)} new topics ingested. Remaining issues: {len(df_issue_current_batch)}")

    print(f"Background processing finished for new topics in project: {project_name}")
    # Optionally, do something with df_processed_background, like logging summary


def new_topics(
    owner_id: Optional[str] = "1",
    project_name: Optional[str] = None,
    es: Optional[Elasticsearch] = None,
    background_tasks: BackgroundTasks = None, # Added for FastAPI integration
    keywords: Optional[List[str]] = [], # Corrected type
    search_keyword: Optional[List[str]] = [], # Corrected type
    search_exact_phrases: bool = False,
    case_sensitive: bool = False,
    sentiment: Optional[List[str]] = ['positive','negative','neutral'], # Corrected type
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    date_filter: str = "last 30 days",
    custom_start_date: Optional[str] = None,
    custom_end_date: Optional[str] = None,
    channels: Optional[List[str]] = None,
    importance: str = "all mentions",
    influence_score_min: Optional[float] = 0,
    influence_score_max: Optional[float] = 1000,
    region: Optional[List[str]] = [], # Corrected type
    language: Optional[List[str]] = [], # Corrected type
    domain: Optional[List[str]] = []
              ):
    
    if es is None:
        # This should ideally be handled by the caller in main_topic.py
        # For now, if not provided, we might not be able to proceed.
        raise ValueError("Elasticsearch client (es) must be provided to new_topics.")
    if project_name is None:
        raise ValueError("project_name must be provided to new_topics.")

    # Buat DataFrame
    df_issue_all = get_data_issue( # Renamed to df_issue_all
        owner_id=owner_id,
        es=es,
        project_name=project_name,
        keywords=keywords,
        search_keyword=search_keyword,
        search_exact_phrases=search_exact_phrases,
        case_sensitive=case_sensitive, # Not used by get_data_issue's current query
        sentiment=sentiment,
        start_date=start_date,
        end_date=end_date,
        date_filter=date_filter,
        custom_start_date=custom_start_date,
        custom_end_date=custom_end_date,
        channels=channels,
        importance=importance, # Not used by get_data_issue's current query
        influence_score_min=influence_score_min,
        influence_score_max=influence_score_max,
        region=region,
        language=language,
        domain=domain,
        limit=1000 # Default limit for initial fetch
    )
    
    if df_issue_all.empty:
        print(f"No issues found for project {project_name} with the given filters.")
        return []

    # Take top 50 for initial processing
    df_issue_initial_batch = df_issue_all.sort_values(
        ['total_post','negative_posts','total_reach_score'],
        ascending = [False,False,False]
    )[:50]

    issue_list_for_gemini = []
    for _, row in df_issue_initial_batch.iterrows():
        # 'id' here must match the 'index' column in df_issue_initial_batch for _process_gemini_results_to_dataframe
        issue_list_for_gemini.append({"id": row['index'], "issue": row['issue']})

    if not issue_list_for_gemini:
        print(f"No issues in the initial batch for project {project_name} to send to Gemini.")
        # If even the initial batch is empty, check if there are any issues at all for background
        df_issue_remaining = df_issue_all[~df_issue_all['index'].isin(df_issue_initial_batch['index'])]
        if not df_issue_remaining.empty and background_tasks:
            # Schedule background task for remaining issues if any
            background_tasks.add_task(
                _background_process_new_topics,
                df_issue_remaining=df_issue_remaining.copy(),
                project_name=project_name,
                es=es,
                initial_suggestions=[], # No initial suggestions if first batch was empty
                owner_id=owner_id, keywords=keywords, search_keyword=search_keyword,
                search_exact_phrases=search_exact_phrases, case_sensitive=case_sensitive,
                sentiment=sentiment, start_date=start_date, end_date=end_date,
                date_filter=date_filter, custom_start_date=custom_start_date,
                custom_end_date=custom_end_date, channels=channels, importance=importance,
                influence_score_min=influence_score_min, influence_score_max=influence_score_max,
                region=region, language=language, domain=domain
            )
            print("Scheduled remaining issues for background processing as initial batch was empty but data exists.")
        return []


    # Prompt for initial clustering (top 50)
    # project_name is used in the prompt in regenerate_topics, so it should be here too.
    prompt_initial = f"""
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

    # LIST OF ISSUES
    ```
    {issue_list_for_gemini}
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
    ...
    ]
    ```
    # HARD RULES:
    - Return the output ONLY in pure JSON format, without any explanation or additional comments.
    - The unified_issue name and description must be in English.
    """

    gemini_output_df_initial = _call_gemini_and_parse(prompt_initial)
    df_final_initial = _process_gemini_results_to_dataframe(gemini_output_df_initial, df_issue_initial_batch)

    results_initial = []
    if not df_final_initial.empty:
        results_initial = df_final_initial.to_dict(orient='records')
        print(f"Initial processing for project {project_name} generated {len(results_initial)} topics.")
        ingest_topic(results_initial, project_name, es=es)
    else:
        print(f"Initial processing for project {project_name} did not generate any topics.")

    # Prepare for background processing of the rest
    df_issue_remaining = df_issue_all[~df_issue_all['index'].isin(df_issue_initial_batch['index'])]

    if not df_issue_remaining.empty:
        print(f"Preparing {len(df_issue_remaining)} remaining issues for background processing for project {project_name}.")
        initial_suggestions_for_background = []
        if not df_final_initial.empty:
            initial_suggestions_for_background = df_final_initial[['unified_issue', 'description']].drop_duplicates().to_dict(orient='records')
        
        if background_tasks:
            background_tasks.add_task(
                _background_process_new_topics,
                df_issue_remaining=df_issue_remaining.copy(), # Pass a copy
                project_name=project_name,
                es=es,
                initial_suggestions=initial_suggestions_for_background,
                owner_id=owner_id, keywords=keywords, search_keyword=search_keyword,
                search_exact_phrases=search_exact_phrases, case_sensitive=case_sensitive,
                sentiment=sentiment, start_date=start_date, end_date=end_date,
                date_filter=date_filter, custom_start_date=custom_start_date,
                custom_end_date=custom_end_date, channels=channels, importance=importance,
                influence_score_min=influence_score_min, influence_score_max=influence_score_max,
                region=region, language=language, domain=domain
            )
            print(f"Scheduled background task for {len(df_issue_remaining)} issues for project {project_name}.")
        else:
            # If BackgroundTasks is not available (e.g., during direct script execution for testing)
            # Call the function directly for now.
            print("BackgroundTasks not available. Running background process synchronously for testing.")
            _background_process_new_topics(
                df_issue_remaining=df_issue_remaining.copy(),
                project_name=project_name,
                es=es,
                initial_suggestions=initial_suggestions_for_background,
                owner_id=owner_id, keywords=keywords, search_keyword=search_keyword,
                search_exact_phrases=search_exact_phrases, case_sensitive=case_sensitive,
                sentiment=sentiment, start_date=start_date, end_date=end_date,
                date_filter=date_filter, custom_start_date=custom_start_date,
                custom_end_date=custom_end_date, channels=channels, importance=importance,
                influence_score_min=influence_score_min, influence_score_max=influence_score_max,
                region=region, language=language, domain=domain
            )
    else:
        print(f"No remaining issues to process in background for project {project_name}.")
    
    return results_initial
