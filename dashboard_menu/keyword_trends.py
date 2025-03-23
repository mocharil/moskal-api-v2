from utils.functions import About_BQ
import os
BQ = About_BQ(project_id="inlaid-sentinel-444404-f8", credentials_loc='./utils/inlaid-sentinel-444404-f8-be06a73c1031.json')

def fill_missing_dates(df, date_filter, custom_start_date=None, custom_end_date=None):
    """
    Fill in missing dates in the dataframe with zeros.
    
    Parameters:
    - df: Pandas dataframe with 'post_date' and metric columns
    - date_filter: The date filter used in the query
    - custom_start_date: Start date for custom date filter
    - custom_end_date: End date for custom date filter
    
    Returns:
    - Pandas dataframe with filled dates
    """
    import pandas as pd
    from datetime import datetime, timedelta
    
    # Convert post_date to datetime if it's not already
    df['post_date'] = pd.to_datetime(df['post_date'])
    
    # Determine start and end dates based on filter
    today = datetime.now().date()
    
    if date_filter == "yesterday":
        start_date = today - timedelta(days=1)
        end_date = today - timedelta(days=1)
    elif date_filter == "this week":
        # Calculate start of week (Monday)
        start_date = today - timedelta(days=today.weekday())
        end_date = today
    elif date_filter == "last 7 days":
        start_date = today - timedelta(days=7)
        end_date = today
    elif date_filter == "last 30 days":
        start_date = today - timedelta(days=30)
        end_date = today
    elif date_filter == "last 3 months":
        # Approximate 3 months as 90 days
        start_date = today - timedelta(days=90)
        end_date = today
    elif date_filter == "custom" and custom_start_date and custom_end_date:
        start_date = datetime.strptime(custom_start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(custom_end_date, '%Y-%m-%d').date()
    else:
        # For other filters like "all time", "this year", etc., just return original df
        return df
    
    # Create complete date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Create template dataframe with all dates
    template_df = pd.DataFrame({'post_date': date_range})
    
    # Merge with actual data
    result_df = pd.merge(template_df, df, on='post_date', how='left')
    
    # Fill NaN values with 0
    for col in ['total_mentions', 'total_reach', 'total_positive', 'total_negative', 'total_neutral']:
        if col in result_df.columns:
            result_df[col] = result_df[col].fillna(0)
    
    # Sort by date
    result_df = result_df.sort_values('post_date', ascending=False)
    
    return result_df.to_dict(orient = 'records')

def keyword_trends_query(
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
    domain=None  # Can be a string or list of strings
    ):
    """
    Generate an optimized BigQuery SQL query for social media analysis with multiple filters.
    
    Parameters:
    - keyword (str): Search term for post captions
    - sentiment (str/list): Filter by sentiment ('positive', 'negative', 'neutral') or list of multiple sentiments
    - date_filter (str): Predefined date ranges ('yesterday', 'this week', 'last 7 days', 'last 30 days', 
                          'last 3 months', 'this year', 'last year', 'all time', 'custom')
    - custom_start_date (str): Start date in YYYY-MM-DD format when date_filter is 'custom'
    - custom_end_date (str): End date in YYYY-MM-DD format when date_filter is 'custom'
    - channel (str/list): Filter by channel (e.g., 'twitter', 'facebook') or list of channels
    - influence_score_min (float): Minimum influence score
    - influence_score_max (float): Maximum influence score
    - region (str/list): Filter by region or list of regions
    - language (str/list): Filter by language or list of languages
    - importance (str): Either 'all mentions' or 'important mentions' (top 10% viral scores)
    - domain (str): Domain name filter for link_post
    
    Returns:
    - str: SQL query for BigQuery
    """
    # Construct the date filter clause
    date_clause = ""
    if date_filter == "yesterday":
        date_clause = "DATE(a.post_created_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)"
    elif date_filter == "this week":
        date_clause = "DATE(a.post_created_at) BETWEEN DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) AND CURRENT_DATE()"
    elif date_filter == "last 7 days":
        date_clause = "DATE(a.post_created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
    elif date_filter == "last 30 days":
        date_clause = "DATE(a.post_created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
    elif date_filter == "last 3 months":
        date_clause = "DATE(a.post_created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)"
    elif date_filter == "this year":
        date_clause = "EXTRACT(YEAR FROM CAST(a.post_created_at AS TIMESTAMP)) = EXTRACT(YEAR FROM CURRENT_DATE())"
    elif date_filter == "last year":
        date_clause = "EXTRACT(YEAR FROM CAST(a.post_created_at AS TIMESTAMP)) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1"
    elif date_filter == "custom" and custom_start_date and custom_end_date:
        date_clause = f"DATE(a.post_created_at) BETWEEN '{custom_start_date}' AND '{custom_end_date}'"
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
        filter_conditions.append(f"a.influence_score >= {influence_score_min}")
    if influence_score_max is not None:
        filter_conditions.append(f"a.influence_score <= {influence_score_max}")
    
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
        importance_subquery = f"""
         viral_threshold AS (
            SELECT PERCENTILE_CONT(viral_score, 0.9) OVER() AS threshold
            FROM `medsos.post_analysis`
            WHERE viral_score IS NOT NULL
            and channel IN ({', '.join([f'"' + c + '"' for c in channel])})
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
        a.link_post,
        DATE(a.post_created_at) AS post_date,
        a.reach_score
      FROM `medsos.post_analysis` a
      WHERE {where_clause}
    ),
    category_filtered AS (
      SELECT 
        c.link_post,
        c.sentiment
      FROM medsos.post_category c
      WHERE {category_where_clause}
    )
    
    SELECT 
      post_date,
      COUNT(*) AS total_mentions,
      SUM(fp.reach_score) AS total_reach,
      SUM(CASE WHEN cf.sentiment = 'positive' THEN 1 ELSE 0 END) AS total_positive,
      SUM(CASE WHEN cf.sentiment = 'negative' THEN 1 ELSE 0 END) AS total_negative,
      SUM(CASE WHEN cf.sentiment = 'neutral' THEN 1 ELSE 0 END) AS total_neutral
    FROM filtered_posts fp
    JOIN category_filtered cf
      ON fp.link_post = cf.link_post
    GROUP BY post_date
    ORDER BY post_date DESC
    """
    
    return query
