from fastapi import FastAPI
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel

from utils.analysis_overview import get_social_media_matrix
from utils.analysis_sentiment_mentions import get_category_analytics
from utils.context_of_disccusion import get_context_of_discussion
from utils.intent_emotions_region import get_intents_emotions_region_share
from utils.keyword_trends import get_keyword_trends
from utils.list_of_mentions import get_mentions
from utils.topics_sentiment_analysis import get_topics_sentiment_analysis
from utils.topics_overview import search_topics
from utils.kol_overview import search_kol
from utils.most_followers import get_most_followers
from utils.popular_emojis import get_popular_emojis
from utils.presence_score import get_presence_score
from utils.share_of_voice import get_share_of_voice
from utils.summary_stats import get_stats_summary
from utils.trending_hashtags import get_trending_hashtags
from utils.trending_links import get_trending_links
import sys
import traceback

try:
    # Log versioning info
    print(f"Python version: {sys.version}")
    print("Starting import process...")
except Exception as e:
    print(f"Error during startup: {e}")
    traceback.print_exc()
    sys.exit(1)
    
app = FastAPI(
    title="Social Media Analytics API",
    description="API for analyzing social media data from Elasticsearch",
    version="1.0.0"
)

# Base model for common parameters
class CommonParams(BaseModel):
    keywords: Optional[List[str]] = None
    search_exact_phrases: bool = False
    case_sensitive: bool = False
    sentiment: Optional[List[str]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    date_filter: str = "last 30 days"
    custom_start_date: Optional[str] = None
    custom_end_date: Optional[str] = None
    channels: Optional[List[str]] = None
    importance: str = "all mentions"
    influence_score_min: Optional[float] = None
    influence_score_max: Optional[float] = None
    region: Optional[List[str]] = None
    language: Optional[List[str]] = None
    domain: Optional[List[str]] = None

# Request models for specific endpoints
class MentionsRequest(CommonParams):
    sort_type: str = "recent"
    sort_order: str = "desc"
    page: int = 1
    page_size: int = 10
    source: Optional[List[str]] = None

class FollowersRequest(CommonParams):
    limit: int = 10
    page: int = 1
    page_size: int = 10
    include_total_count: bool = True

class EmojisRequest(CommonParams):
    limit: int = 100
    page: int = 1
    page_size: int = 10

class PresenceRequest(CommonParams):
    interval: str = "week"
    compare_with_topics: bool = True
    num_topics_to_compare: int = 10

class ShareOfVoiceRequest(CommonParams):
    limit: int = 10
    page: int = 1
    page_size: int = 10
    include_total_count: bool = True

class StatsRequest(CommonParams):
    compare_with_previous: bool = True

class HashtagsRequest(CommonParams):
    limit: int = 100
    page: int = 1
    page_size: int = 10
    sort_by: str = "mentions"

class LinksRequest(CommonParams):
    limit: int = 10000
    page: int = 1
    page_size: int = 10

class TopicsOverviewRequest(CommonParams):
    owner_id: str
    project_name: str

class KolOverviewRequest(CommonParams):
    owner_id: str
    project_name: str


sample_input = """
Sample Input:
Notes: - Keyword wajib ada, sisanya bisa dikosongkan
       - custom_start_date dan custom_end_date kosongkan saja jika date_filter nya bukan custom

{"keywords": [
    "prabowo","gibran"
  ],
  "search_exact_phrases": false,
  "case_sensitive": false,
  "sentiment": [
    "positive","negative","neutral"
  ],
  "date_filter": "last 30 days",
  "custom_start_date": "2025-04-01",
  "custom_end_date": "2025-04-20",
  "channels": [
    "tiktok","instagram","news","reddit","facebook","twitter","linkedin","youtube"
  ],
  "importance": "important mentions",
  "influence_score_min": 0,
  "influence_score_max": 100,
  "region": [
    "bandung","jakarta"
  ],
  "language": [
    "indonesia","english"
  ],
  "domain": [
    "kumparan.com","detik.com"
  ]"""



########### DASHBOARD MENU ##########
@app.post("/api/v2/keyword-trends", tags = ["Dashboard Menu"])
async def keyword_trends_analysis(params: CommonParams):
    f"""Digunakan di menu:
    - Dashboard
    - Topics pada bagian Occurences
    - Summary
    - Analysis
    - Comparison
    
     {sample_input} 
     }
    
    
    """
    return get_keyword_trends(**params.dict())

@app.post("/api/v2/context-of-discussion", tags = ["Dashboard Menu"])
async def context_analysis(params: CommonParams):
    f"""Digunakan di menu:
    - Dashboard
    - Topics
    - Analysis
    - Comparison

     {sample_input} 
     }
    """
    return get_context_of_discussion(**params.dict())

@app.post("/api/v2/list-of-mentions", tags = ["Dashboard Menu"])
async def get_mentions_list(params: MentionsRequest):
    f"""Digunakan di menu:
    - Dashboard
    - Topics
    - Summary
    - Analysis
    - Comparison -> Most Viral funakan sort_type = "popular"
    
     {sample_input}, 
  "sort_type": "recent", #'popular', 'recent', atau 'relevant'
  "sort_order": "desc", #desc atau asc
  "page": 1,
  "page_size": 10,
     }
    
    """
    return get_mentions(**params.dict())


########### ANALYSIS MENU ##########
@app.post("/api/v2/analysis-overview", tags = ["Analysis Menu"])
async def analysis_overview(params: CommonParams):
    f"""Digunakan di menu:
    - Analysis -> Overview
    - Summary -> Summary
    - Comparison -> Overview

    {sample_input}
    }
    """
    return get_social_media_matrix(**params.dict())

@app.post("/api/v2/mention-sentiment-breakdown", tags=["Analysis Menu"])
async def analysis_sentiment(params: CommonParams):
    f"""Digunakan di menu:
    - Analysis : 
        1. Mention by categories
        2. Sentiment by categories
        3. Sentiment breakdown
    - Topics:
        1. Channels share -> gunakan Mention by categories
        2. Overall sentiments -> gunakan Sentiment Breakdown
    - Comparison:
        1. Sentiment breakdown
        2. Channels share -> gunakan Mention by categories

    {sample_input}
    }
    """
    return get_category_analytics(**params.dict())


@app.post("/api/v2/presence-score", tags=["Analysis Menu"])
async def presence_score_analysis(params: PresenceRequest):
    f"""Digunakan pada menu:
    - Analysis : 
        Presence Score
    - Summary:
        Presence Score -> gunakan score nya saja
        
        {sample_input},
      "interval": "week", #day, week, month
      "compare_with_topics": true,
      "num_topics_to_compare": 10
        }
        
        """
    return get_presence_score(**params.dict())


@app.post("/api/v2/most-share-of-voice", tags=["Analysis Menu"])
async def share_of_voice_analysis(params: ShareOfVoiceRequest):
    f"""Digunakan pada Menu:
    - Analysis -> Most Share of Voice
    
    {sample_input},
  "limit": 10,
  "page": 1,
  "page_size": 10,
  "include_total_count": true
    }
    """
    return get_share_of_voice(**params.dict())


@app.post("/api/v2/most-followers", tags=["Analysis Menu"])
async def most_followers_analysis(params: FollowersRequest):
    f"""Digunakan pada Menu:
    - Analysis -> Most Followers

    {sample_input},
  "limit": 10,
  "page": 1,
  "page_size": 10,
  "include_total_count": true
    }
    """
    return get_most_followers(**params.dict())


@app.post("/api/v2/trending-hashtags", tags=["Analysis Menu"])
async def trending_hashtags_analysis(params: HashtagsRequest):
    f"""Digunakan pada Menu:
    - Analysis -> Trending hashtags
    
    {sample_input},
  "limit": 100,
  "page": 1,
  "page_size": 10,
  "sort_by": "mentions" 
    }    

    
    """
    return get_trending_hashtags(**params.dict())

@app.post("/api/v2/trending-links", tags = ["Analysis Menu"])
async def trending_links_analysis(params: LinksRequest):
    f"""Digunakan pada Menu:
    - Analysis -> Trending links

        {sample_input},
  "limit": 1000,
  "page": 1,
  "page_size": 10
    }    
    
    """
    return get_trending_links(**params.dict())

@app.post("/api/v2/popular-emojis", tags=["Analysis Menu"])
async def popular_emojis_analysis(params: EmojisRequest):
    f"""Digunakan pada Menu:
    - Analysis -> Popular Emojis
    
   {sample_input},
  "limit": 1000,
  "page": 1,
  "page_size": 10
    }    
    
    """
    return get_popular_emojis(**params.dict())

########### SUMMARY MENU ##########

@app.post("/api/v2/stats", tags=["Summary Menu"])
async def stats_summary_analysis(params: StatsRequest):
    f"""Digunakan pada Menu:
    - Summary -> Stats
    
    {sample_input},
    "compare_with_previous": true
    }    
    """
    return get_stats_summary(**params.dict())

########### TOPICS MENU ##########
@app.post("/api/v2/intent-emotions-region", tags=["Topics Menu"])
async def intent_emotions_analysis(params: CommonParams):
    f"""Digunakan pada Menu:
    Untuk Parameter Keyword, gunakan list issue yang didapat ketika mendapat Topics
    - Topics:
        1. Intent Shares
        2. Emotions Shares
        3. Top Regions
        
       {sample_input}
       }  
    """
    return get_intents_emotions_region_share(**params.dict())

@app.post("/api/v2/topics-sentiment", tags=["Topics Menu"])
async def topics_sentiment_analysis(params: CommonParams):
    f"""Digunakan pada Menu:
    - Topics:
        Overall Sentiment -> di description per sentiment
           
    {sample_input}
       } 
        
        """
    return get_topics_sentiment_analysis(**params.dict())

@app.post("/api/v2/topics-overview", tags=["Topics Menu"])
async def topics_overview_analysis(params: TopicsOverviewRequest):
    f"""Digunakan pada Menu:
    - Dashboard:
        Topics to Watch
    - Topics:
        Overview
    - Comparison
        Most viral topics

       {sample_input},
  "owner_id": "5",
  "project_name": "gibran raka"
       } 
    """
    return search_topics(**params.dict())

@app.post("/api/v2/kol-overview", tags=["KOL Menu"])
async def kol_overview_analysis(params: KolOverviewRequest):
    f"""Digunakan pada Menu:
    - Dashboard:
        KOL to Watch
    - Summary:
        Influencers
    - Comparison:
        Kol to Watch
    - KOL:
        Key Opinion Leaders Overview

       {sample_input},
  "owner_id": "5",
  "project_name": "gibran raka"
    """
    return search_kol(**params.dict())

