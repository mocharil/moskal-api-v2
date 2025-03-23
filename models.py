from typing import Optional, Union, List, Literal
from pydantic import BaseModel, Field, validator
from datetime import datetime

class DashboardRequest(BaseModel):
    keyword: Optional[List[str]] = None
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
    channel: Optional[List[Literal[
        "tiktok", 
        "news", 
        "instagram", 
        "twitter", 
        "linkedin", 
        "reddit", 
        "youtube"
    ]]] = None
    influence_score_min: Optional[float] = Field(None, ge=0, le=100)
    influence_score_max: Optional[float] = Field(None, ge=0, le=100)
    region: Optional[List[str]] = None
    language: Optional[List[str]] = None
    importance: Literal["important mentions", "all mentions"] = "all mentions"
    domain: Optional[List[str]] = None
     # Validasi dan contoh untuk dokumentasi
    class Config:
        schema_extra = {
            "example": {
                "keyword": ["kadin", "indonesia", "ekonomi"],
                "sentiment": ["positive", "neutral"],
                "date_filter": "last 30 days",
                "custom_start_date": None,
                "custom_end_date": None,
                "channel": ["twitter", "instagram", "news"],
                "influence_score_min": 20,
                "influence_score_max": 90,
                "region": ["Jakarta", "Surabaya", "Bandung"],
                "language": ["id", "en"],
                "importance": "all mentions",
                "domain": ["news.example.com", "blog.example.com"]
            }
        }
           
    @validator('custom_start_date', 'custom_end_date')
    def validate_date_format(cls, v, values):
        if v is not None:
            try:
                datetime.strptime(v, '%Y-%m-%d')
            except ValueError:
                raise ValueError('Date must be in YYYY-MM-DD format')
        
        if values.get('date_filter') == 'custom':
            if v is None:
                raise ValueError('custom_start_date and custom_end_date are required when date_filter is "custom"')
        
        return v
    
    @validator('influence_score_max')
    def validate_influence_score_range(cls, v, values):
        min_score = values.get('influence_score_min')
        if v is not None and min_score is not None and v < min_score:
            raise ValueError('influence_score_max must be greater than or equal to influence_score_min')
        return v

class MentionsRequest(DashboardRequest):
    sort_by: str = 'popular_first'
    limit: int = 10
    offset: int = 0

class ShareOfVoiceRequest(DashboardRequest):
    limit: int = 10

    class Config:
        schema_extra = {
            "example": {
                "keyword": "example",
                "sentiment": ["positive", "negative"],
                "date_filter": "last 7 days",
                "channel": ["twitter", "facebook"],
                "importance": "all mentions"
            }
        }
