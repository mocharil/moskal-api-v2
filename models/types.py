"""
Type definitions for Elasticsearch data models
"""

from typing import Optional, List, Literal, Dict, Any, Union
from dataclasses import dataclass
from datetime import datetime
from pydantic import BaseModel, Field

@dataclass
class SocialMediaMetrics:
    """Social media metrics time series data"""
    post_date: str
    total_mentions: int
    total_reach: float
    total_positive: int
    total_negative: int
    total_neutral: int

@dataclass
class WordCloudItem:
    """Word cloud data item"""
    word: str
    dominant_sentiment: Literal["positive", "negative", "neutral"]
    total_data: float

@dataclass
class ElasticsearchFilter:
    """Elasticsearch filter parameters"""
    keywords: Optional[List[str]] = None
    sentiment: Optional[List[Literal["positive", "negative", "neutral"]]] = None
    date_filter: Literal[
        "all time", 
        "yesterday", 
        "this week", 
        "last 7 days", 
        "last 30 days", 
        "last 3 months", 
        "this year", 
        "last year", 
        "custom"
    ] = "all time"
    custom_start_date: Optional[str] = None
    custom_end_date: Optional[str] = None
    channels: Optional[List[Literal[
        "news", 
        "instagram", 
        "twitter", 
        "linkedin", 
        "reddit", 
        "youtube"
    ]]] = None
    influence_score_min: Optional[float] = None
    influence_score_max: Optional[float] = None
    region: Optional[List[str]] = None
    language: Optional[List[str]] = None
    importance: Literal["important mentions", "all mentions"] = "all mentions"
    domain: Optional[List[str]] = None

@dataclass
class ElasticsearchConfig:
    """Elasticsearch connection configuration"""
    host: str = "localhost:9200"
    username: Optional[str] = None
    password: Optional[str] = None
    use_ssl: bool = False
    verify_certs: bool = False
    ca_certs: Optional[str] = None

class AIFeedbackData(BaseModel):
    """Data model for AI feedback"""
    query_user: str
    response_ai: Dict[str, Any]
    feedback_user: str
    user_name: Optional[str] = None
    project_name: Optional[str] = None
    additional_info: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
