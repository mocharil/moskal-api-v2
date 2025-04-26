from utils.es_client import get_elasticsearch_client
from utils.list_of_mentions import get_mentions
import pandas as pd
import uuid

def create_uuid(keyword):
    # Gunakan namespace standar (ada juga untuk URL, DNS, dll)
    namespace = uuid.NAMESPACE_DNS

    return uuid.uuid5(namespace, keyword)

def matching_issue(final_result, df_data):
    #final_result : dataframe yg isinya group topics 
    #df_data :dataframe yang ingin di cari grup nya
     
    hasil = []
    for _,i  in final_result.iterrows():
        list_issue = i['list_issue']
        dt = df_data[df_data['issue'].isin(list_issue)]


        if dt.empty:
            continue

        row = i.to_dict().copy()

        agg_sentiment = dt.groupby('sentiment').size().to_dict()
        sentiment_categories = ['positive', 'negative', 'neutral']
        for category in sentiment_categories:
            agg_sentiment.setdefault(category, 0)

        agg_sentiment = {key: value for key, value in agg_sentiment.items() if key in sentiment_categories}


        agg_score = dt[['viral_score','reach_score']].sum().to_dict()

        row.update(agg_sentiment)
        row.update(agg_score)
        row.update({'total_posts':dt.shape[0]})
        hasil.append(row)

    df = pd.DataFrame(hasil).fillna(0)
    df['share_of_voice'] = df['total_posts']/df['total_posts'].sum()*100

    return df.to_dict(orient = 'records')

def search_topics(  
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
            source= ["issue", "reach_score", "viral_score", "sentiment", "link_post","channel"],
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
        
        
        return matching_issue(final_result, df_data)
