print('preparing..')
from fastapi import FastAPI, Body
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware  # Import CORS middleware
from utils.gemini import call_gemini

from utils.analysis_overview import get_social_media_matrix
from utils.analysis_sentiment_mentions import get_category_analytics
from utils.context_of_disccusion import get_context_of_discussion
from utils.intent_emotions_region import get_intents_emotions_region_share
from utils.keyword_trends import get_keyword_trends
from utils.list_of_mentions import get_mentions
from utils.topics_sentiment_analysis import get_topics_sentiment_analysis
from utils.topics_overview import topic_overviews
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


# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)


# Base model for common parameters
class CommonParams(BaseModel):
    keywords: Optional[List[str]] = Field(
        default=None, 
        example=["prabowo", "gibran"],
        description="Keywords to search for"
    )
    search_keyword: Optional[List[str]] = Field(
        default=None,
        example=["prabowo gibran"],
        description="Search for exact phrases in addition to keywords"
    )
    search_exact_phrases: bool = Field(
        default=False, 
        example=False,
        description="Whether to search for exact phrases"
    )
    case_sensitive: bool = Field(
        default=False, 
        example=False,
        description="Whether the search is case sensitive"
    )
    sentiment: Optional[List[str]] = Field(
        default=None, 
        example=["positive", "negative", "neutral"],
        description="Sentiment filters"
    )
    start_date: Optional[str] = Field(
        default=None, 
        example=None,
        description="Start date for filtering"
    )
    end_date: Optional[str] = Field(
        default=None, 
        example=None,
        description="End date for filtering"
    )
    date_filter: str = Field(
        default="last 30 days", 
        example="last 30 days",
        description="Date filter preset"
    )
    custom_start_date: Optional[str] = Field(
        default=None, 
        example="2025-04-01",
        description="Custom start date (if date_filter is 'custom')"
    )
    custom_end_date: Optional[str] = Field(
        default=None, 
        example="2025-04-20",
        description="Custom end date (if date_filter is 'custom')"
    )
    channels: Optional[List[str]] = Field(
        default=None, 
        example=["tiktok", "instagram", "news", "reddit", "facebook", "twitter", "linkedin", "youtube"],
        description="Channel filters"
    )
    importance: str = Field(
        default="all mentions", 
        example="important mentions",
        description="Importance filter"
    )
    influence_score_min: Optional[float] = Field(
        default=None, 
        example=0,
        description="Minimum influence score"
    )
    influence_score_max: Optional[float] = Field(
        default=None, 
        example=100,
        description="Maximum influence score"
    )
    region: Optional[List[str]] = Field(
        default=None, 
        example=["bandung", "jakarta"],
        description="Region filters"
    )
    language: Optional[List[str]] = Field(
        default=None, 
        example=["indonesia", "english"],
        description="Language filters"
    )
    domain: Optional[List[str]] = Field(
        default=None, 
        example=["kumparan.com", "detik.com"],
        description="Domain filters"
    )

# Request models for specific endpoints
class MentionsRequest(CommonParams):
    sort_type: str = Field(
        default="recent",
        example="recent", 
        description="Sort type: 'popular', 'recent', or 'relevant'"
    )
    sort_order: str = Field(
        default="desc",
        example="desc", 
        description="Sort order: 'desc' or 'asc'"
    )
    page: int = Field(
        default=1,
        example=1, 
        description="Page number"
    )
    page_size: int = Field(
        default=10,
        example=10, 
        description="Number of items per page"
    )
    source: Optional[List[str]] = Field(
        default=None,
        example=None, 
        description="Source filters"
    )

class FollowersRequest(CommonParams):
    limit: int = Field(
        default=10,
        example=10, 
        description="Limit of results"
    )
    page: int = Field(
        default=1,
        example=1, 
        description="Page number"
    )
    page_size: int = Field(
        default=10,
        example=10, 
        description="Number of items per page"
    )
    include_total_count: bool = Field(
        default=True,
        example=True, 
        description="Include total count in response"
    )

class EmojisRequest(CommonParams):
    limit: int = Field(
        default=100,
        example=100, 
        description="Limit of results"
    )
    page: int = Field(
        default=1,
        example=1, 
        description="Page number"
    )
    page_size: int = Field(
        default=10,
        example=10, 
        description="Number of items per page"
    )

class PresenceRequest(CommonParams):
    interval: str = Field(
        default="week",
        example="week", 
        description="Interval: 'day', 'week', or 'month'"
    )
    compare_with_topics: bool = Field(
        default=True,
        example=True, 
        description="Compare with topics"
    )
    num_topics_to_compare: int = Field(
        default=10,
        example=10, 
        description="Number of topics to compare"
    )

class ShareOfVoiceRequest(CommonParams):
    limit: int = Field(
        default=10,
        example=10, 
        description="Limit of results"
    )
    page: int = Field(
        default=1,
        example=1, 
        description="Page number"
    )
    page_size: int = Field(
        default=10,
        example=10, 
        description="Number of items per page"
    )
    include_total_count: bool = Field(
        default=True,
        example=True, 
        description="Include total count in response"
    )

class StatsRequest(CommonParams):
    compare_with_previous: bool = Field(
        default=True,
        example=True, 
        description="Compare with previous period"
    )

class HashtagsRequest(CommonParams):
    limit: int = Field(
        default=100,
        example=100, 
        description="Limit of results"
    )
    page: int = Field(
        default=1,
        example=1, 
        description="Page number"
    )
    page_size: int = Field(
        default=10,
        example=10, 
        description="Number of items per page"
    )
    sort_by: str = Field(
        default="mentions",
        example="mentions", 
        description="Sort by field"
    )

class LinksRequest(CommonParams):
    limit: int = Field(
        default=10000,
        example=1000, 
        description="Limit of results"
    )
    page: int = Field(
        default=1,
        example=1, 
        description="Page number"
    )
    page_size: int = Field(
        default=10,
        example=10, 
        description="Number of items per page"
    )

class TopicsOverviewRequest(CommonParams):
    owner_id: str = Field(
        ...,
        example="5", 
        description="Owner ID"
    )
    project_name: str = Field(
        ...,
        example="gibran raka", 
        description="Project name"
    )

class KolOverviewRequest(CommonParams):
    owner_id: str = Field(
        ...,
        example="5", 
        description="Owner ID"
    )
    project_name: str = Field(
        ...,
        example="gibran raka", 
        description="Project name"
    )

# Example object to demonstrate in docstrings
example_json = {
    "keywords": ["prabowo", "gibran"],
    "search_exact_phrases": False,
    "case_sensitive": False,
    "sentiment": ["positive", "negative", "neutral"],
    "date_filter": "last 30 days",
    "custom_start_date": "2025-04-01",
    "custom_end_date": "2025-04-20",
    "channels": ["tiktok", "instagram", "news", "reddit", "facebook", "twitter", "linkedin", "youtube"],
    "importance": "important mentions",
    "influence_score_min": 0,
    "influence_score_max": 100,
    "region": ["bandung", "jakarta"],
    "language": ["indonesia", "english"],
    "domain": ["kumparan.com", "detik.com"]
}

########### DASHBOARD MENU ##########
@app.post("/api/v2/keyword-trends", tags=["Dashboard Menu"])
def keyword_trends_analysis(
    params: CommonParams = Body(
        ...,
        examples={
            "normal": {
                "summary": "Standard example",
                "description": "A standard example with common parameters",
                "value": example_json
            }
        }
    )
):
    """
    Analisis tren kata kunci.
    
    Digunakan di menu:
    - Dashboard
    - Topics pada bagian Occurences
    - Summary
    - Analysis
    - Comparison
    """
    params_dict = params.dict()

    # Ubah isi 'channels' jika ada
    if 'channels' in params_dict and isinstance(params_dict['channels'], list):
        params_dict['channels'] = ['news' if ch == 'media' else ch for ch in params_dict['channels']]

    return get_keyword_trends(**params_dict)

@app.post("/api/v2/context-of-discussion", tags=["Dashboard Menu"])
def context_analysis(
    params: CommonParams = Body(
        ...,
        examples={
            "normal": {
                "summary": "Standard example",
                "description": "A standard example with common parameters",
                "value": example_json
            }
        }
    )
):
    """
    Analisis konteks diskusi.
    
    Digunakan di menu:
    - Dashboard
    - Topics
    - Analysis
    - Comparison
    """
    params_dict = params.dict()

    # Ubah isi 'channels' jika ada
    if 'channels' in params_dict and isinstance(params_dict['channels'], list):
        params_dict['channels'] = ['news' if ch == 'media' else ch for ch in params_dict['channels']]

    return get_context_of_discussion(**params_dict)

@app.post("/api/v2/list-of-mentions", tags=["Dashboard Menu"])
def get_mentions_list(
    params: MentionsRequest = Body(
        ...,
        examples={
            "normal": {
                "summary": "Standard example",
                "description": "A standard example with common parameters",
                "value": {
                    **example_json,
                    "sort_type": "recent", 
                    "sort_order": "desc",
                    "page": 1,
                    "page_size": 10
                }
            },
            "popular": {
                "summary": "Popular mentions example",
                "description": "Example for retrieving popular mentions",
                "value": {
                    **example_json,
                    "sort_type": "popular", 
                    "sort_order": "desc",
                    "page": 1,
                    "page_size": 10
                }
            }
        }
    )
):
    """
    Mendapatkan daftar mentions.
    
    Digunakan di menu:
    - Dashboard
    - Topics
    - Summary
    - Analysis
    - Comparison -> Most Viral gunakan sort_type = "popular"
    
    Parameter tambahan:
    - sort_type: 'popular', 'recent', atau 'relevant'
    - sort_order: desc atau asc
    - page: halaman yang ditampilkan
    - page_size: jumlah data per halaman
    """
    params_dict = params.dict()

    # Ubah isi 'channels' jika ada
    if 'channels' in params_dict and isinstance(params_dict['channels'], list):
        params_dict['channels'] = ['news' if ch == 'media' else ch for ch in params_dict['channels']]

    return get_mentions(**params_dict)


########### ANALYSIS MENU ##########
@app.post("/api/v2/analysis-overview", tags=["Analysis Menu"])
def analysis_overview(
    params: CommonParams = Body(
        ...,
        examples={
            "normal": {
                "summary": "Standard example",
                "description": "A standard example with common parameters",
                "value": example_json
            }
        }
    )
):
    """
    Gambaran umum analisis.
    
    Digunakan di menu:
    - Analysis -> Overview
    - Summary -> Summary
    - Comparison -> Overview
    """
    return get_social_media_matrix(**params.dict())

@app.post("/api/v2/mention-sentiment-breakdown", tags=["Analysis Menu"])
def analysis_sentiment(
    params: CommonParams = Body(
        ...,
        examples={
            "normal": {
                "summary": "Standard example",
                "description": "A standard example with common parameters",
                "value": example_json
            }
        }
    )
):
    """
    Analisis sentimen dan breakdown mention.
    
    Digunakan di menu:
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
    """
    return get_category_analytics(**params.dict())


@app.post("/api/v2/presence-score", tags=["Analysis Menu"])
def presence_score_analysis(
    params: PresenceRequest = Body(
        ...,
        examples={
            "normal": {
                "summary": "Standard example",
                "description": "A standard example for presence score",
                "value": {
                    **example_json,
                    "interval": "week",
                    "compare_with_topics": True,
                    "num_topics_to_compare": 10
                }
            },
            "daily": {
                "summary": "Daily interval example",
                "description": "Example with daily interval",
                "value": {
                    **example_json,
                    "interval": "day",
                    "compare_with_topics": True,
                    "num_topics_to_compare": 5
                }
            }
        }
    )
):
    """
    Analisis skor kehadiran.
    
    Digunakan pada menu:
    - Analysis : 
        Presence Score
    - Summary:
        Presence Score -> gunakan score nya saja
        
    Parameter tambahan:
    - interval: "day", "week", "month"
    - compare_with_topics: true/false
    - num_topics_to_compare: jumlah topik untuk dibandingkan
    """
    return get_presence_score(**params.dict())


@app.post("/api/v2/most-share-of-voice", tags=["Analysis Menu"])
def share_of_voice_analysis(
    params: ShareOfVoiceRequest = Body(
        ...,
        examples={
            "normal": {
                "summary": "Standard example",
                "description": "A standard example for share of voice",
                "value": {
                    **example_json,
                    "limit": 10,
                    "page": 1,
                    "page_size": 10,
                    "include_total_count": True
                }
            }
        }
    )
):
    """
    Analisis porsi suara yang paling banyak.
    
    Digunakan pada Menu:
    - Analysis -> Most Share of Voice
    
    Parameter tambahan:
    - limit: batas jumlah data
    - page: halaman yang ditampilkan
    - page_size: jumlah data per halaman
    - include_total_count: true/false untuk menampilkan total data
    """
    return get_share_of_voice(**params.dict())


@app.post("/api/v2/most-followers", tags=["Analysis Menu"])
def most_followers_analysis(
    params: FollowersRequest = Body(
        ...,
        examples={
            "normal": {
                "summary": "Standard example",
                "description": "A standard example for most followers",
                "value": {
                    **example_json,
                    "limit": 10,
                    "page": 1,
                    "page_size": 10,
                    "include_total_count": True
                }
            }
        }
    )
):
    """
    Analisis pengikut terbanyak.
    
    Digunakan pada Menu:
    - Analysis -> Most Followers

    Parameter tambahan:
    - limit: batas jumlah data
    - page: halaman yang ditampilkan
    - page_size: jumlah data per halaman
    - include_total_count: true/false untuk menampilkan total data
    """
    return get_most_followers(**params.dict())


@app.post("/api/v2/trending-hashtags", tags=["Analysis Menu"])
def trending_hashtags_analysis(
    params: HashtagsRequest = Body(
        ...,
        examples={
            "normal": {
                "summary": "Standard example",
                "description": "A standard example for trending hashtags",
                "value": {
                    **example_json,
                    "limit": 100,
                    "page": 1,
                    "page_size": 10,
                    "sort_by": "mentions"
                }
            }
        }
    )
):
    """
    Analisis tren hashtag.
    
    Digunakan pada Menu:
    - Analysis -> Trending hashtags
    
    Parameter tambahan:
    - limit: batas jumlah data
    - page: halaman yang ditampilkan
    - page_size: jumlah data per halaman
    - sort_by: cara pengurutan data
    """
    return get_trending_hashtags(**params.dict())

@app.post("/api/v2/trending-links", tags=["Analysis Menu"])
def trending_links_analysis(
    params: LinksRequest = Body(
        ...,
        examples={
            "normal": {
                "summary": "Standard example",
                "description": "A standard example for trending links",
                "value": {
                    **example_json,
                    "limit": 1000,
                    "page": 1,
                    "page_size": 10
                }
            }
        }
    )
):
    """
    Analisis tren tautan.
    
    Digunakan pada Menu:
    - Analysis -> Trending links

    Parameter tambahan:
    - limit: batas jumlah data
    - page: halaman yang ditampilkan
    - page_size: jumlah data per halaman
    """
    return get_trending_links(**params.dict())

@app.post("/api/v2/popular-emojis", tags=["Analysis Menu"])
def popular_emojis_analysis(
    params: EmojisRequest = Body(
        ...,
        examples={
            "normal": {
                "summary": "Standard example",
                "description": "A standard example for popular emojis",
                "value": {
                    **example_json,
                    "limit": 100,
                    "page": 1,
                    "page_size": 10
                }
            }
        }
    )
):
    """
    Analisis emoji populer.
    
    Digunakan pada Menu:
    - Analysis -> Popular Emojis
    
    Parameter tambahan:
    - limit: batas jumlah data
    - page: halaman yang ditampilkan
    - page_size: jumlah data per halaman
    """
    return get_popular_emojis(**params.dict())

########### SUMMARY MENU ##########

@app.post("/api/v2/stats", tags=["Summary Menu"])
def stats_summary_analysis(
    params: StatsRequest = Body(
        ...,
        examples={
            "normal": {
                "summary": "Standard example",
                "description": "A standard example for statistics summary",
                "value": {
                    **example_json,
                    "compare_with_previous": True
                }
            }
        }
    )
):
    """
    Analisis ringkasan statistik.
    
    Digunakan pada Menu:
    - Summary -> Stats
    
    Parameter tambahan:
    - compare_with_previous: true/false untuk membandingkan dengan periode sebelumnya
    """
    return get_stats_summary(**params.dict())

########### TOPICS MENU ##########
@app.post("/api/v2/intent-emotions-region", tags=["Topics Menu"])
def intent_emotions_analysis(
    params: CommonParams = Body(
        ...,
        examples={
            "normal": {
                "summary": "Standard example",
                "description": "A standard example with common parameters",
                "value": example_json
            }
        }
    )
):
    """
    Analisis niat, emosi, dan wilayah.
    
    Digunakan pada Menu:
    Untuk Parameter Keyword, gunakan list issue yang didapat ketika mendapat Topics
    - Topics:
        1. Intent Shares
        2. Emotions Shares
        3. Top Regions
    """
    return get_intents_emotions_region_share(**params.dict())

@app.post("/api/v2/topics-sentiment", tags=["Topics Menu"])
def topics_sentiment_analysis(
    params: CommonParams = Body(
        ...,
        examples={
            "normal": {
                "summary": "Standard example",
                "description": "A standard example with common parameters",
                "value": example_json
            }
        }
    )
):
    """
    Analisis sentimen topik.
    
    Digunakan pada Menu:
    - Topics:
        Overall Sentiment -> di description per sentiment
    """


    return get_topics_sentiment_analysis(**params.dict())

@app.post("/api/v2/topics-overview", tags=["Topics Menu"])
def topics_overview_analysis(
    params: TopicsOverviewRequest = Body(
        ...,
        examples={
            "normal": {
                "summary": "Standard example",
                "description": "A standard example for topics overview",
                "value": {
                    **example_json,
                    "owner_id": "5",
                    "project_name": "gibran raka"
                }
            }
        }
    )
):
    """
    Gambaran umum topik.
    
    Digunakan pada Menu:
    - Dashboard:
        Topics to Watch
    - Topics:
        Overview
    - Comparison
        Most viral topics
        
    Parameter tambahan:
    - owner_id: ID pemilik project
    - project_name: Nama project
    """

    params_dict = params.dict()

    # Ubah isi 'channels' jika ada
    if 'channels' in params_dict and isinstance(params_dict['channels'], list):
        params_dict['channels'] = ['news' if ch == 'media' else ch for ch in params_dict['channels']]

    return topic_overviews(**params_dict)

@app.post("/api/v2/kol-overview", tags=["KOL Menu"])
def kol_overview_analysis(
    params: KolOverviewRequest = Body(
        ...,
        examples={
            "normal": {
                "summary": "Standard example",
                "description": "A standard example for KOL overview",
                "value": {
                    **example_json,
                    "owner_id": "5",
                    "project_name": "gibran raka"
                }
            }
        }
    )
):
    """
    Gambaran umum Key Opinion Leaders.
    
    Digunakan pada Menu:
    - Dashboard:
        KOL to Watch
    - Summary:
        Influencers
    - Comparison:
        Kol to Watch
    - KOL:
        Key Opinion Leaders Overview
        
    Parameter tambahan:
    - owner_id: ID pemilik project
    - project_name: Nama project
    """
    params_dict = params.dict()

    # Ubah isi 'channels' jika ada
    if 'channels' in params_dict and isinstance(params_dict['channels'], list):
        params_dict['channels'] = ['news' if ch == 'media' else ch for ch in params_dict['channels']]

    return search_kol(**params_dict)
