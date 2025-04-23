from utils.es_client import get_elasticsearch_client
from utils.list_of_mentions import get_mentions
import pandas as pd
import uuid

def create_link_user(df):
    if df['channel'] == 'twitter':
        return f"""https://x.com/{df['username'].strip('@ ')}"""
    if df['channel'] == 'instagram':
        return f"""https://www.instagram.com/{df['username'].strip('@ ')}"""
    if df['channel'] == 'tiktok':
        return f"""https://www.tiktok.com/@{df['username'].strip('@ ')}"""
    if df['channel']=='linkedin':
        return f"""https://www.linkedin.com/in/{df['username'].strip('@ ')}"""
    
    
    return df['username']
    
def add_negative_driver_flag(df):
    """
    Add is_negative_driver column that is True when sentiment_negative 
    is the dominant sentiment
    
    Args:
        df: DataFrame with sentiment_positive, sentiment_negative, and sentiment_neutral columns
        
    Returns:
        DataFrame with added is_negative_driver column
    """
    # Ensure all sentiment columns exist
    for sentiment in ['positive', 'negative', 'neutral']:
        col_name = f'sentiment_{sentiment}'
        if col_name not in df.columns:
            df[col_name] = 0
    
    # Create a new column that checks if negative sentiment is the highest
    df['is_negative_driver'] = False
    
    # Compare sentiment counts and set flag if negative is highest
    condition = ((df['sentiment_negative'] > df['sentiment_positive']) & 
                 (df['sentiment_negative'] > df['sentiment_neutral']))
    
    df['is_negative_driver'] = condition
    
    return df

def map_issues_to_unified(final_kol, final_result):
    """
    For each list of issues in final_kol, find the corresponding unified_issue from final_result
    
    Args:
        final_kol: DataFrame with 'issue' column containing lists of issues
        final_result: DataFrame with 'unified_issue' and 'list_issue' columns
    
    Returns:
        DataFrame with added 'unified_issue' column
    """
    # Create a mapping dictionary from issues to unified_issues
    issue_to_unified = {}
    
    # Iterate through final_result to build the mapping
    for _, row in final_result.iterrows():
        unified = row['unified_issue']
        issues_list = row['list_issue']
        
        # Skip if list_issue is not a list
        if not isinstance(issues_list, list):
            continue
            
        # Map each issue in the list to its unified issue
        for issue in issues_list:
            issue_to_unified[issue] = unified
    
    # Function to map a list of issues to their unified issues
    def get_unified_issues(issues_list):
        if not isinstance(issues_list, list):
            return []
        
        # Get unified issues for each issue in the list
        unified_issues = [issue_to_unified.get(issue) for issue in issues_list if issue in issue_to_unified]
        
        # Remove duplicates and None values
        unified_issues = [issue for issue in unified_issues if issue is not None]
        return list(set(unified_issues))
    
    # Add unified_issue column to final_kol
    final_kol_with_unified = final_kol.copy()
    final_kol_with_unified['unified_issue'] = final_kol['issue'].apply(get_unified_issues)
    
    return final_kol_with_unified

def create_uuid(keyword):
    # Gunakan namespace standar (ada juga untuk URL, DNS, dll)
    namespace = uuid.NAMESPACE_DNS

    return uuid.uuid5(namespace, keyword)

def search_kol(  
    owner_id = None,
    project_name = None,
    es_host=None,
    es_username=None,
    es_password=None,
    use_ssl=False,
    verify_certs=False,
    ca_certs=None,
    keywords=None,
    search_exact_phrases=False,
    case_sensitive=False,
    sentiment=None,
    start_date=None,
    end_date=None,
    date_filter="last 30 days",
    custom_start_date=None,
    custom_end_date=None,
    channels=None,
    importance="all mentions",
    influence_score_min=None,
    influence_score_max=None,
    region=None,
    language=None,
    domain=None):
    
    print('(((((((((((((((((((((( MASUK ))))))))))))))))))))))')
    es = get_elasticsearch_client(
        es_host=es_host,
        es_username=es_username,
        es_password=es_password,
        use_ssl=use_ssl,
        verify_certs=verify_certs,
        ca_certs=ca_certs
    )
    
    
    #PROCESS
    id_ = create_uuid("{}_{}".format(owner_id, project_name))

    query = {
        "source":[],
      "query": {
        "match": {
          "_id": id_
        }
      }

    }

    response = es.search(
        index='topics',
        body=query
    )

    data_project = [hit["_source"] for hit in response["hits"]["hits"]]
    if data_project:

        #project sudah tergenerate
        final_result = pd.DataFrame(data_project[0]['topics'])
        if not sentiment:
            sentiment = ['positive', 'negative', 'neutral']
        #pake filter berdasarkan input user
        result = get_mentions(
            source= ["issue","user_connections","user_followers","user_influence_score",
                     'user_image_url',"engagement_rate",
                 "influence_score","reach_score", "viral_score",
                 "sentiment", "link_post","user_category","username",'channel'],
            page_size=10000,
            es_host=es_host,    
            es_username=es_username,
            es_password=es_password,
            use_ssl=use_ssl,
            verify_certs=verify_certs,
            ca_certs=ca_certs,
            keywords=keywords,
            search_exact_phrases=search_exact_phrases,
            case_sensitive=case_sensitive,
            sentiment=sentiment,
            start_date=start_date,
            end_date=end_date,
            date_filter=date_filter,
            custom_start_date=custom_start_date,
            custom_end_date=custom_end_date,
            channels=channels,
            importance=importance,
            influence_score_min=influence_score_min,
            influence_score_max=influence_score_max,
            region=region,
            language=language,
            domain=domain,
            sort_type = 'popular'
        )

        df_data = pd.DataFrame(result['data'])
        if 'user_category' not in df_data:
            return []
        
        kol = df_data[~df_data['user_category'].isna()]
        kol['link_user'] = kol.apply(lambda s: create_link_user(s), axis=1)        
        
        for i in set(['user_connections','user_followers']) - set(kol):
            kol[i] = 0
        
        kol['user_followers'] = kol['user_connections']+kol['user_followers']
        
        # Your groupby with sentiment pivot
        agg_kol = kol.groupby(['link_user']).agg({
            'link_post': 'size',
            'viral_score': 'sum',
            'reach_score': 'sum',
            'channel': 'max',
            'username': 'max',
            'user_image_url':'max',
            'user_followers':'max',
            "engagement_rate":'sum',
            'issue': lambda s: list(set(s)),
            'user_category': 'max',
            'user_influence_score': lambda s: max(s)*100
        })

        # Get sentiment counts per link_user using crosstab
        sentiment_counts = pd.crosstab(kol['link_user'], kol['sentiment'])

        # Rename columns to add 'sentiment_' prefix
        sentiment_counts = sentiment_counts.add_prefix('sentiment_')

        # Join the sentiment counts with the main result
        final_kol = agg_kol.join(sentiment_counts)

        # If any sentiment category is missing, add it with zeros
        for sentiment in ['positive', 'negative', 'neutral']:
            col_name = f'sentiment_{sentiment}'
            if col_name not in final_kol.columns:
                final_kol[col_name] = 0        
        

        # Apply the function to final_kol
        final_kol = add_negative_driver_flag(final_kol)

        hasil = map_issues_to_unified(final_kol, final_result)     
        
        hasil = hasil.sort_values('link_post', ascending = False).reset_index()
   
        hasil['share_of_voice'] = hasil['link_post']/hasil['link_post'].sum()*100
    
        hasil.rename(columns = {'link_post':'total_mentions',
                                'user_influence_score':'influence_score'})
    
    
        
        return hasil[hasil['unified_issue'].transform(lambda s: s!=[])].to_dict(orient = 'records')
