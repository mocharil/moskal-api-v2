from utils.functions import About_BQ
from dotenv import load_dotenv
import os

load_dotenv()
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_CREDS_LOCATION = os.getenv("BQ_CREDS_LOCATION")

BQ = About_BQ(project_id = BQ_PROJECT_ID, credentials_loc = BQ_CREDS_LOCATION)
def get_most_share_of_voice(
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
    domain=None,
    limit=10
):
    """
    Get most share of voice with filters
    """
    # Construct the date filter clause
    date_clause = ""
    if date_filter == "yesterday":
        date_clause = "DATE(CAST(a.post_created_at AS TIMESTAMP)) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)"
    elif date_filter == "this week":
        date_clause = "DATE(CAST(a.post_created_at AS TIMESTAMP)) BETWEEN DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) AND CURRENT_DATE()"
    elif date_filter == "last 7 days":
        date_clause = "DATE(CAST(a.post_created_at AS TIMESTAMP)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
    elif date_filter == "last 30 days":
        date_clause = "DATE(CAST(a.post_created_at AS TIMESTAMP)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
    elif date_filter == "last 3 months":
        date_clause = "DATE(CAST(a.post_created_at AS TIMESTAMP)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)"
    elif date_filter == "this year":
        date_clause = "EXTRACT(YEAR FROM CAST(a.post_created_at AS TIMESTAMP)) = EXTRACT(YEAR FROM CURRENT_DATE())"
    elif date_filter == "last year":
        date_clause = "EXTRACT(YEAR FROM CAST(a.post_created_at AS TIMESTAMP)) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1"
    elif date_filter == "custom" and custom_start_date and custom_end_date:
        date_clause = f"DATE(CAST(a.post_created_at AS TIMESTAMP)) BETWEEN '{custom_start_date}' AND '{custom_end_date}'"
    else:  # all time - no filter
        date_clause = "1=1"  # Always true condition

    # Build filter conditions
    filter_conditions = []
    
    # Date filter
    filter_conditions.append(date_clause)
    
    # Keyword filter
    if keyword:
        filter_conditions.append(f"""SEARCH(a.post_caption, "{keyword}")""")
    
    # Channel filter
    if channel:
        if isinstance(channel, list):
            channel_condition = f"""a.channel IN ({', '.join([f'"' + c + '"' for c in channel])})"""
        else:
            channel_condition = f'a.channel = "{channel}"'
        filter_conditions.append(channel_condition)
    
    # Influence score filter
    if influence_score_min is not None:
        filter_conditions.append(f"influence_score*10 >= {influence_score_min}")
    if influence_score_max is not None:
        filter_conditions.append(f"influence_score*10 <= {influence_score_max}")
    
    # Domain filter
    if domain:
        if isinstance(domain, list):
            domain_conditions = []
            for d in domain:
                domain_conditions.append(f"a.link_post LIKE '%{d}%'")
            filter_conditions.append(f"({' OR '.join(domain_conditions)})")
        else:
            filter_conditions.append(f"a.link_post LIKE '%{domain}%'")

    # Add news channel exclusion
    filter_conditions.append("a.channel not in ('news')")

    # Combine all filter conditions
    where_clause = " AND ".join(filter_conditions)

    query = f"""
    WITH filtered_posts AS (
        SELECT a.*
        FROM medsos.post_analysis a
        WHERE {where_clause}
    ),
    topic_mentions AS (
        SELECT 
            channel, 
            username, 
            COUNT(*) AS total_mentions, 
            SUM(reach_score) AS total_reach
        FROM filtered_posts
        GROUP BY channel, username
    ),
    total_mentions_all AS (
        SELECT 
            COUNT(*) AS total_mentions_all
        FROM filtered_posts
    ),
    data AS (
        SELECT 
            tm.channel, 
            tm.username, 
            tm.total_mentions, 
            tm.total_reach, 
            ROUND((tm.total_mentions * 100.0) / tma.total_mentions_all, 2) AS percentage_share_of_voice
        FROM topic_mentions tm
        CROSS JOIN total_mentions_all tma
    )
    SELECT * 
    FROM data
    ORDER BY percentage_share_of_voice DESC
    LIMIT {limit}
    """

    result = BQ.to_pull_data(query)
    return result.to_dict(orient='records')
