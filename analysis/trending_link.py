from utils.functions import About_BQ

BQ = About_BQ(project_id="inlaid-sentinel-444404-f8", credentials_loc='./utils/inlaid-sentinel-444404-f8-be06a73c1031.json')

import pandas as pd

def format_array(arr):
    if not arr:
        return None
    if isinstance(arr, str):
        return f"('{arr}')"
    return f"""({','.join(f"'{x}'" for x in arr)})"""

def get_trending_links(
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
    query = f"""
    WITH filtered_data AS (
        SELECT *
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
    
    data AS (
        SELECT 
            CASE 
                WHEN channel IN ('youtube', 'linkedin') THEN 
                    CONCAT(SPLIT(link_post, '/')[OFFSET(0)], '//', SPLIT(link_post, '/')[SAFE_OFFSET(2)])
                WHEN channel IN ('reddit') THEN 
                    CASE
                        WHEN ARRAY_LENGTH(SPLIT(link_post, '/')) > 4 THEN
                            CONCAT(SPLIT(link_post, '/')[OFFSET(0)], '//', SPLIT(link_post, '/')[OFFSET(2)], '/', SPLIT(link_post, '/')[SAFE_OFFSET(3)],'/', SPLIT(link_post, '/')[SAFE_OFFSET(4)])
                        ELSE
                            CONCAT(SPLIT(link_post, '/')[OFFSET(0)], '//', SPLIT(link_post, '/')[SAFE_OFFSET(2)])
                    END
                ELSE 
                    CASE
                        WHEN ARRAY_LENGTH(SPLIT(link_post, '/')) > 3 THEN
                            CONCAT(SPLIT(link_post, '/')[OFFSET(0)], '//', SPLIT(link_post, '/')[OFFSET(2)], '/', SPLIT(link_post, '/')[SAFE_OFFSET(3)])
                        ELSE
                            CONCAT(SPLIT(link_post, '/')[OFFSET(0)], '//', SPLIT(link_post, '/')[SAFE_OFFSET(2)])
                    END
            END AS split_link
        FROM filtered_data
        WHERE link_post IS NOT NULL
    )

    SELECT split_link, COUNT(*) as count
    FROM data
    GROUP BY 1
    ORDER BY count DESC
    LIMIT 50
    """

    # Get data from BigQuery
    df = BQ.to_pull_data(query)
    
    # Convert DataFrame to list of dictionaries with int conversion
    result = []
    for _, row in df.iterrows():
        result.append({
            'split_link': row['split_link'],
            'count': int(row['count'])  # Convert numpy.int64 to regular Python int
        })
    
    return result
