from utils.functions import About_BQ
import os
BQ = About_BQ(project_id="inlaid-sentinel-444404-f8", credentials_loc='./utils/inlaid-sentinel-444404-f8-be06a73c1031.json')

def context_of_discussion(
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
    Get word frequency analysis from post captions with multiple filters.
    
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
    - domain (str/list): Domain name filter for link_post
    
    Returns:
    - List of dictionaries with word frequencies
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
            SELECT PERCENTILE_CONT(viral_score, 0.8) OVER() AS threshold
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
        LOWER(REGEXP_REPLACE(a.post_caption, r"[^a-zA-Z\s]", "")) AS text_cleaned
      FROM `medsos.post_analysis` a
      WHERE {where_clause}
    ),
    category_filtered AS (
      SELECT 
        c.link_post
      FROM medsos.post_category c
      WHERE {category_where_clause}
    ),
    word_list AS (
        SELECT
            LOWER(word) word,
            COUNT(*) AS word_count
        FROM filtered_posts fp
        JOIN category_filtered cf ON fp.link_post = cf.link_post,
        UNNEST(SPLIT(fp.text_cleaned, ' ')) AS word
        WHERE LENGTH(word) > 2  -- Remove short words (e.g., 'di', 'ke', etc.)
        GROUP BY word
        ORDER BY word_count DESC
    )
    SELECT * FROM word_list
    LIMIT 100;
    """
    print(query)
    data = BQ.to_pull_data(query)
    
    with open('utils/stopwords.txt') as f:
        list_stopword = f.read().split()
    
    data = data[~data['word'].isin(list_stopword)]
    
    return data.to_dict(orient='records')