from utils.functions import About_BQ

BQ = About_BQ(project_id="inlaid-sentinel-444404-f8", credentials_loc='./utils/inlaid-sentinel-444404-f8-be06a73c1031.json')

def get_presence_score(
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
    Get presence score with filters
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
        SELECT a.*
        FROM medsos.post_analysis a
        WHERE {where_clause}
    ),
    topic_data AS (
        SELECT 
            DATE(post_created_at) AS date,
            COUNT(*) AS total_mentions,
            SUM(reach_score) AS total_reach,
            SUM(COALESCE(likes, 0) + COALESCE(shares, 0) + COALESCE(comments, 0) + COALESCE(favorites, 0)
                    + COALESCE(views, 0) + COALESCE(retweets, 0) + COALESCE(replies, 0) + COALESCE(reposts, 0)) AS total_engagement
        FROM filtered_posts
        GROUP BY 1
    ),
    max_values AS (
        SELECT 
            MAX(total_mentions) AS max_mentions,
            MAX(total_reach) AS max_reach,
            MAX(total_engagement) AS max_engagement
        FROM topic_data
    ),
    presence_score_calc AS (
        SELECT 
            t.date,
            t.total_mentions,
            t.total_reach,
            t.total_engagement,
            ROUND(
                ((t.total_mentions / NULLIF(m.max_mentions, 0)) * 40) +
                ((t.total_reach / NULLIF(m.max_reach, 0)) * 40) +
                ((t.total_engagement / NULLIF(m.max_engagement, 0)) * 20), 2
            ) AS presence_score
        FROM topic_data t
        CROSS JOIN max_values m
    )
    SELECT 
        date,
        presence_score
    FROM presence_score_calc
    ORDER BY date ASC;
    """

    result = BQ.to_pull_data(query)
    return result.to_dict(orient='records')
