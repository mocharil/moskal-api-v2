from utils.functions import About_BQ

BQ = About_BQ(project_id="inlaid-sentinel-444404-f8", credentials_loc='./utils/inlaid-sentinel-444404-f8-be06a73c1031.json')

def get_stats(
    keyword=None,
    sentiment=None,
    date_filter="last 30 days",
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
    Get statistics with filters
    """
    # Construct the date filter clause for current period
    date_clause = ""
    if date_filter == "yesterday":
        date_clause = "DATE(CAST(post_created_at AS TIMESTAMP)) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)"
    elif date_filter == "this week":
        date_clause = "DATE(CAST(post_created_at AS TIMESTAMP)) BETWEEN DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) AND CURRENT_DATE()"
    elif date_filter == "last 7 days":
        date_clause = "DATE(CAST(post_created_at AS TIMESTAMP)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
    elif date_filter == "last 30 days":
        date_clause = "DATE(CAST(post_created_at AS TIMESTAMP)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
    elif date_filter == "last 3 months":
        date_clause = "DATE(CAST(post_created_at AS TIMESTAMP)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)"
    elif date_filter == "this year":
        date_clause = "EXTRACT(YEAR FROM CAST(post_created_at AS TIMESTAMP)) = EXTRACT(YEAR FROM CURRENT_DATE())"
    elif date_filter == "last year":
        date_clause = "EXTRACT(YEAR FROM CAST(post_created_at AS TIMESTAMP)) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1"
    elif date_filter == "custom" and custom_start_date and custom_end_date:
        date_clause = f"DATE(CAST(post_created_at AS TIMESTAMP)) BETWEEN '{custom_start_date}' AND '{custom_end_date}'"
    else:
        date_clause = "DATE(CAST(post_created_at AS TIMESTAMP)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"

    # Build filter conditions
    filter_conditions = []
    
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
    where_clause = " AND ".join(filter_conditions) if filter_conditions else "1=1"

    query = f"""
    WITH filtered_data AS (
        SELECT a.*
        FROM medsos.post_analysis a
        WHERE {where_clause}
    ),
    data AS (
        SELECT 
            post_created_at,
            coalesce(likes, votes, 0) AS likes, 
            coalesce(shares, reposts, retweets, 0) AS shares,
            CASE WHEN channel IN ('linkedin', 'instagram', 'tiktok', 'twitter', 'threads', 'facebook') THEN true
                ELSE false END AS is_sosmed,
            CASE WHEN channel = 'youtube' OR link_post LIKE '%/video/%' THEN true
                ELSE false END AS is_video,
            channel
        FROM filtered_data
    ),
    current_period AS (
        SELECT 
            COUNT(CASE WHEN NOT is_sosmed THEN 1 END) AS non_social_mentions,
            COUNT(CASE WHEN is_sosmed THEN 1 END) AS social_mentions,
            COUNT(CASE WHEN is_video OR channel = 'tiktok' THEN 1 END) AS video_mentions,
            SUM(CASE WHEN is_sosmed THEN shares ELSE 0 END) AS social_shares,
            SUM(CASE WHEN is_sosmed THEN likes ELSE 0 END) AS social_likes
        FROM data
        WHERE {date_clause}
    ),
    previous_period AS (
        SELECT 
            COUNT(CASE WHEN NOT is_sosmed THEN 1 END) AS non_social_mentions,
            COUNT(CASE WHEN is_sosmed THEN 1 END) AS social_mentions,
            COUNT(CASE WHEN is_video OR channel = 'tiktok' THEN 1 END) AS video_mentions,
            SUM(CASE WHEN is_sosmed THEN shares ELSE 0 END) AS social_shares,
            SUM(CASE WHEN is_sosmed THEN likes ELSE 0 END) AS social_likes
        FROM data
        WHERE DATE(CAST(post_created_at AS TIMESTAMP)) >= DATE_SUB(
                CASE 
                    WHEN {date_clause} THEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                    ELSE DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
                END, INTERVAL 30 DAY)
            AND DATE(CAST(post_created_at AS TIMESTAMP)) < 
                CASE 
                    WHEN {date_clause} THEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                    ELSE DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                END
    )
    SELECT
        -- Non-social mentions
        cp.non_social_mentions,
        cp.non_social_mentions - pp.non_social_mentions AS non_social_mentions_diff,
        CASE 
            WHEN pp.non_social_mentions = 0 THEN NULL
            ELSE ROUND((cp.non_social_mentions - pp.non_social_mentions) * 100.0 / pp.non_social_mentions)
        END AS non_social_mentions_pct,
        
        -- Social media mentions
        cp.social_mentions,
        cp.social_mentions - pp.social_mentions AS social_mentions_diff,
        CASE 
            WHEN pp.social_mentions = 0 THEN NULL
            ELSE ROUND((cp.social_mentions - pp.social_mentions) * 100.0 / pp.social_mentions)
        END AS social_mentions_pct,
        
        -- Videos count
        cp.video_mentions,
        cp.video_mentions - pp.video_mentions AS video_mentions_diff,
        CASE 
            WHEN pp.video_mentions = 0 THEN NULL
            ELSE ROUND((cp.video_mentions - pp.video_mentions) * 100.0 / pp.video_mentions)
        END AS video_mentions_pct,
        
        -- Social media shares
        cp.social_shares,
        cp.social_shares - pp.social_shares AS social_shares_diff,
        CASE 
            WHEN pp.social_shares = 0 THEN NULL
            ELSE ROUND((cp.social_shares - pp.social_shares) * 100.0 / pp.social_shares)
        END AS social_shares_pct,
        
        -- Social media likes
        cp.social_likes,
        cp.social_likes - pp.social_likes AS social_likes_diff,
        CASE 
            WHEN pp.social_likes = 0 THEN NULL
            ELSE ROUND((cp.social_likes - pp.social_likes) * 100.0 / pp.social_likes)
        END AS social_likes_pct
    FROM current_period cp, previous_period pp;
    """

    result = BQ.to_pull_data(query)
    return result.to_dict(orient='records')
