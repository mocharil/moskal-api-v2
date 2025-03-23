from utils.functions import About_BQ
import pandas as pd

from dotenv import load_dotenv
import os

load_dotenv()
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_CREDS_LOCATION = os.getenv("BQ_CREDS_LOCATION")

BQ = About_BQ(project_id = BQ_PROJECT_ID, credentials_loc = BQ_CREDS_LOCATION)
def get_popular_emojis(
    keyword=None,
    sentiment=None,
    date_filter="all time",
    custom_start_date=None,
    custom_end_date=None,
    channel=None,
    influence_score_min=None,
    influence_score_max=None,
    region=None,
    language=None,
    importance="all mentions",
    domain=None
):
    # Format arrays for SQL IN clause
    def format_array(arr):
        if not arr:
            return None
        if isinstance(arr, str):
            return f"('{arr}')"
        return f"""({','.join(f"'{x}'" for x in arr)})"""

    # Base query with filters
    query = f"""
    -- First extract all characters one by one
    WITH characters AS (
        SELECT 
            post_id,
            REGEXP_EXTRACT_ALL(post_caption, r'([\s\S])') AS chars
        FROM medsos.post_analysis
        WHERE 1=1
        {f"AND LOWER(post_caption) LIKE LOWER('%{keyword}%')" if keyword else ""}
        {f"AND sentiment IN {format_array(sentiment)}" if sentiment else ""}
        {f"AND channel IN {format_array(channel)}" if channel else ""}
        {f"AND region IN {format_array(region)}" if region else ""}
        {f"AND language IN {format_array(language)}" if language else ""}
        {f"AND domain IN {format_array(domain)}" if domain else ""}
        {f"AND influence_score >= {influence_score_min}" if influence_score_min is not None else ""}
        {f"AND influence_score <= {influence_score_max}" if influence_score_max is not None else ""}
        {
            f"AND DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL {date_filter})"
            if date_filter != "all time" and not custom_start_date
            else ""
        }
        {f"AND DATE(created_at) >= '{custom_start_date}'" if custom_start_date else ""}
        {f"AND DATE(created_at) <= '{custom_end_date}'" if custom_end_date else ""}
    ),

    -- Flatten to get one character per row
    flattened_chars AS (
        SELECT 
            post_id,
            char
        FROM characters,
        UNNEST(chars) AS char
    ),

    -- Only keep emoji characters (using regex pattern)
    emoji_chars AS (
        SELECT 
            char AS emoji,
            COUNT(*) AS total_mentions
        FROM flattened_chars
        WHERE REGEXP_CONTAINS(char, r'^[\p{{Emoji}}]$')
          AND char NOT IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '#', '*', '©', 'I', 'D')
        GROUP BY char
    )

    SELECT 
        emoji, 
        total_mentions
    FROM emoji_chars
    WHERE LENGTH(emoji) > 0
    ORDER BY total_mentions DESC
    LIMIT 50;
    """

    # Execute query using BigQuery
    df = BQ.to_pull_data(query)
    
    # Convert DataFrame to list of dictionaries with int conversion
    result = []
    for _, row in df.iterrows():
        result.append({
            'emoji': row['emoji'],
            'total_mentions': int(row['total_mentions'])  # Convert numpy.int64 to regular Python int
        })
    
    return result
