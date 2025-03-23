from utils.functions import About_BQ

from dotenv import load_dotenv
import os

load_dotenv()
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_CREDS_LOCATION = os.getenv("BQ_CREDS_LOCATION")

BQ = About_BQ(project_id = BQ_PROJECT_ID, credentials_loc = BQ_CREDS_LOCATION)
def get_sentiment_by_categories(
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
    """
    Get sentiment breakdown by channel categories with filters
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

    # Combine all filter conditions
    where_clause = " AND ".join(filter_conditions)

    query = f"""
    WITH filtered_posts AS (
        SELECT a.*, c.sentiment
        FROM medsos.post_analysis a
        JOIN medsos.post_category c ON a.link_post = c.link_post
        WHERE {where_clause}
        AND LOWER(c.sentiment) IN ('positive', 'negative', 'neutral')
    )
    SELECT 
        channel,
        sum(case when sentiment = 'positive' then 1 else 0 end) positive,
        sum(case when sentiment = 'negative' then 1 else 0 end) negative,
        sum(case when sentiment = 'neutral' then 1 else 0 end) neutral,
        COUNT(*) as total_mentions
    FROM filtered_posts
    GROUP BY channel
    ORDER BY total_mentions desc
    """

    result = BQ.to_pull_data(query)
    return result.to_dict(orient='records')
