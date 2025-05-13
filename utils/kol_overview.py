from utils.es_client import get_elasticsearch_client
from utils.list_of_mentions import get_mentions
import pandas as pd
import uuid, numpy as np
from elasticsearch import Elasticsearch
from utils.influence_score import get_influence_score
from dotenv import load_dotenv
from utils.redis_client import redis_client

# Load environment variables
load_dotenv()

# Buat koneksi ke Elasticsearch
es = get_elasticsearch_client()

def create_link_user(df):
    if df['channel'] == 'twitter':
        return f"""https://x.com/{df['username'].strip('@ ')}"""
    if df['channel'] == 'instagram':
        return f"""https://www.instagram.com/{df['username'].strip('@ ')}"""
    if df['channel'] == 'tiktok':
        return f"""https://www.tiktok.com/@{df['username'].strip('@ ')}"""
    if df['channel']=='linkedin':
        return f"""https://www.linkedin.com/in/{df['username'].strip('@ ')}"""
    if df['channel']=='reddit':
        return f"""https://www.reddit.com/{df['username'].strip('@ ')}"""
    
    
    return df['username']
    
def add_negative_driver_flag(df):

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

def create_uuid(keyword):
    # Gunakan namespace standar (ada juga untuk URL, DNS, dll)
    namespace = uuid.NAMESPACE_DNS

    return uuid.uuid5(namespace, keyword)

def rule_base_user_category(string, category):
    if pd.isna(string):
        return ''
    
    if 'news' in string.lower():
        return 'News Account'

    return category
    
def search_kol(   owner_id = None,
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

    # Generate cache key based on all parameters
    cache_key = redis_client.generate_cache_key(
        "kol_overview",
        owner_id=owner_id,
        project_name=project_name,
        keywords=keywords,
        search_exact_phrases=search_exact_phrases,
        case_sensitive=case_sensitive,
        start_date=start_date,
        sentiment=sentiment,
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
        domain=domain
    )

    # Try to get from cache first
    cached_result = redis_client.get(cache_key)
    if cached_result is not None:
        print('Returning cached result')
        return cached_result


    print('get all data mentions')

    if not sentiment:
        sentiment = ['positive','negative','neutral']

    result = get_mentions(
            source= ["issue","user_connections","user_followers","user_influence_score",
                     'user_image_url',"engagement_rate","subscriber",
                 "influence_score","reach_score", "viral_score",
                 "sentiment", "link_post","user_category","username",'channel',
                "votes","likes",'comments','shares','retweets','reports','replies',
                 'views','favorites',"post_created_at"],
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

    if not result['data']:
        return []
    else:
        kol = pd.DataFrame(result['data'])

        #-----------------------
        if 'user_category' not in kol:
            kol['user_category']=''

        kol['user_category'] = kol.apply(lambda s: 'News Account' if s['channel'] == 'news' else rule_base_user_category(s['username'], s['user_category']), axis=1)

        kol['user_influence_score'] = kol.apply(lambda s: get_influence_score(s), axis=1)

        #-----------------------            
        for c in set(['user_connections','user_followers','subscriber']) - set(kol.columns):
            kol[c] = 0                   


        kol[['user_followers',"subscriber"]] = kol[['user_followers',"subscriber"]].fillna(0)
        kol['user_category'] = kol['user_category'].fillna('')

        kol['link_user'] = kol.apply(lambda s: create_link_user(s), axis=1)        

        for i in set(['user_connections','user_followers','subscriber']) - set(kol):
            kol[i] = 0

        kol['user_followers'] = kol['user_connections'] + kol['user_followers'] + kol['subscriber']

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
            'user_influence_score': 'mean'
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
        final_kol = add_negative_driver_flag(final_kol).reset_index()

        list_issue = [j for i in final_kol['issue'] for j in i]

        print('check topic map')
        #check issue mana yang sudah termap dan mana yg belum
        query_body = {
            "_source":["unified_issue","list_issue"],
            "query":{

                "bool": {

                    "must":[

                        {        "match": {"project_name.keyword":project_name}   },
                        {        "terms": {"list_issue.keyword":list_issue}  },


                    ]

                }
            }
        }

        response = es.search(
            index="topic_cluster",
            body=query_body,
            size = 10000
        )
        top_map = [i['_source'] for i in response['hits']['hits']]
        dict_issue = {}
        if top_map:
            print('get map')
            df_map = pd.DataFrame(top_map)
            df_map = df_map.explode('list_issue').drop_duplicates('list_issue')
            
            for _,i in df_map.iterrows():
                dict_issue.update({i['list_issue']:i['unified_issue']})

        final_kol['unified_issue'] = final_kol['issue'].transform(lambda s: list(set([dict_issue.get(i,i) for i in s]))[:5])
        final_kol['user_category'] = final_kol.apply(lambda s: 'News Account' if s['channel']=='news' else s['user_category'], axis=1)
        final_kol.drop('issue', axis=1, inplace=True)
        final_kol['most_viral'] = final_kol['user_influence_score']


        final_kol["share_of_voice"] = (final_kol["link_post"]/final_kol["link_post"].sum())*100
        
        most_negative_kol = final_kol.sort_values(['is_negative_driver','sentiment_negative','most_viral'], ascending = False)[:100]
        most_viral_kol = final_kol.sort_values('most_viral', ascending = False)[:100]
        
        final_kol = pd.concat([most_negative_kol,most_viral_kol]).drop_duplicates('link_user')


        result = final_kol.sort_values(['is_negative_driver','sentiment_negative','most_viral'], ascending = False)[:150].to_dict(orient = 'records')
        
        # Cache the results for 10 minutes
        redis_client.set_with_ttl(cache_key, result, ttl_seconds=600)
        return result
