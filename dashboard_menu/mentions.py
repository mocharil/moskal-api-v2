from utils.functions import About_BQ
import os
BQ = About_BQ(project_id="inlaid-sentinel-444404-f8", credentials_loc='./utils/inlaid-sentinel-444404-f8-be06a73c1031.json')

def list_of_mentions(
    keyword=None,
    sentiment=None,  # Can be a string or list of strings
    date_filter="all time",
    custom_start_date=None,
    custom_end_date=None,
    channel=None,  # Can be a string or list of strings
    influence_score_min=None,
    influence_score_max=None,
    region=None,  # Can be a string or list of strings
    language=None,  # Can be a string or list of strings
    importance="all mentions",
    domain=None,  # Can be a string or list of strings
    sort_by='popular_first',
    limit=10,
    offset=0
   ):
    """
    Get a list of mentions with multiple filters.
    
    Parameters:
    - keyword (str): Search term for post captions
    - sentiment (str/list): Filter by sentiment ('positive', 'negative', 'neutral') or list of multiple sentiments
    - date_filter (str): Predefined date ranges ('yesterday', 'this week', 'last 7 days', etc.)
    - custom_start_date (str): Start date in YYYY-MM-DD format when date_filter is 'custom'
    - custom_end_date (str): End date in YYYY-MM-DD format when date_filter is 'custom'
    - channel (str/list): Filter by channel or list of channels
    - influence_score_min (float): Minimum influence score
    - influence_score_max (float): Maximum influence score
    - region (str/list): Filter by region or list of regions
    - language (str/list): Filter by language or list of languages
    - importance (str): Either 'all mentions' or 'important mentions' (top 10% viral scores)
    - domain (str/list): Domain name filter for link_post
    - sort_by (str): 'popular_first' or 'recent_first'
    - limit (int): Number of results to return
    - offset (int): Offset for pagination
    
    Returns:
    - List of dictionaries with mention details
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

    # Build the filter conditions
    filter_conditions = []
    
    # Keyword filter
    if keyword:
        filter_conditions.append(f"""SEARCH(a.post_caption, "{keyword}")""")
    
    # Date filter
    filter_conditions.append(date_clause)
    
    # Channel filter
    if channel:
        if isinstance(channel, list):
            channel_condition = f"""a.channel IN ({', '.join([f'"' + c + '"' for c in channel])})"""
        else:
            channel_condition = f"a.channel = \"{channel}\""
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
    
    # Construct the sentiment filter for post_category table
    category_filter_conditions = []
    
    if sentiment:
        if isinstance(sentiment, list):
            sentiment_condition = f"""c.sentiment IN ({', '.join([f'"' + s + '"' for s in sentiment])})"""
        else:
            sentiment_condition = f"""c.sentiment = '{sentiment}'"""
        category_filter_conditions.append(sentiment_condition)
    
    # Region filter with LOWER and LIKE
    if region:
        if isinstance(region, list):
            region_conditions = []
            for r in region:
                region_conditions.append(f"LOWER(c.region) LIKE '%{r.lower()}%'")
            region_condition = f"({' OR '.join(region_conditions)})"
        else:
            region_condition = f"LOWER(c.region) LIKE '%{region.lower()}%'"
        category_filter_conditions.append(region_condition)
    
    # Language filter with LOWER and LIKE
    if language:
        if isinstance(language, list):
            language_conditions = []
            for l in language:
                if l.lower() == 'id':
                    language_conditions.append("LOWER(c.language) LIKE '%indonesia%'")
                elif l.lower() == 'en':
                    language_conditions.append("LOWER(c.language) LIKE '%english%'")
                else:
                    language_conditions.append(f"LOWER(c.language) LIKE '%{l.lower()}%'")
            language_condition = f"({' OR '.join(language_conditions)})"
        else:
            if language.lower() == 'id':
                language_condition = "LOWER(c.language) LIKE '%indonesia%'"
            elif language.lower() == 'en':
                language_condition = "LOWER(c.language) LIKE '%english%'"
            else:
                language_condition = f"LOWER(c.language) LIKE '%{language.lower()}%'"
        category_filter_conditions.append(language_condition)
    
    # Important mentions filter (using viral_score)
    importance_subquery = ""
    if importance == "important mentions":
        
        filter_channel = ''
        if channel:
            filter_channel = f"""and channel IN ({', '.join([f'"' + c + '"' for c in channel])})"""
        
        
        importance_subquery = f"""
         viral_threshold AS (
            SELECT PERCENTILE_CONT(viral_score, 0.8) OVER() AS threshold
            FROM `medsos.post_analysis`
            WHERE viral_score IS NOT NULL
            {filter_channel}
            LIMIT 1
        ),
        """
        filter_conditions.append("a.viral_score >= (SELECT threshold FROM viral_threshold)")
    
    # Combine all filter conditions
    where_clause = " AND ".join(filter_conditions)
    category_where_clause = " AND ".join(category_filter_conditions) if category_filter_conditions else "1=1"
    
    # Build the final query
    query = f"""
    WITH{importance_subquery} filtered_posts AS (
      SELECT 
        a.username, a.link_post, a.post_caption, a.comments, a.likes, a.shares, 
        a.favorites, a.viral_score, a.post_created_at, a.reposts, a.views, a.retweets,
        a.replies, a.votes,
        a.user_image_url,
        
        
        channel
      FROM `medsos.post_analysis` a
      WHERE {where_clause}
    ),
    category_filtered AS (
      SELECT 
        c.link_post, c.sentiment
      FROM medsos.post_category c
      WHERE {category_where_clause}
    )
    
    SELECT 
      fp.username, 
      u.followers_last AS followers_user, 
      u.likes_last AS likes_user, 
      u.influence_score*10 AS influence_score,
      fp.link_post, 
      fp.post_caption, 
      fp.comments, 
      fp.likes, 
      fp.shares, 
      fp.favorites, 
      fp.reposts, fp.views, fp.retweets,
        fp.replies, fp.votes,
        fp.user_image_url,
      fp.viral_score, 
      fp.post_created_at, 
      cf.sentiment,
      fp.channel
    FROM filtered_posts fp
    JOIN category_filtered cf ON fp.link_post = cf.link_post
    LEFT JOIN medsos.user_analysis u ON fp.username = u.username
    ORDER BY {('fp.viral_score DESC' if sort_by == 'popular_first' else 'fp.post_created_at DESC')}
    LIMIT {limit} OFFSET {offset}
    """

    print(query)
    data = BQ.to_pull_data(query)
    
    return data.to_dict(orient='records')