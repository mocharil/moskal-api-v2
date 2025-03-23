from fastapi import APIRouter, HTTPException
from models import DashboardRequest
from analysis.sentiment_by_categories import get_sentiment_by_categories
import math

router = APIRouter()

def sanitize_float(value):
    """Handle non-finite float values for JSON serialization"""
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
    return value

def sanitize_data(data):
    """Recursively sanitize all float values in the data structure"""
    if isinstance(data, dict):
        return {k: sanitize_data(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize_data(item) for item in data]
    elif isinstance(data, float):
        return sanitize_float(data)
    return data

@router.post("/api/sentiment-by-categories", tags=["Analysis Menu"])
async def get_sentiment_categories_analysis(request: DashboardRequest):

    return {
        "status": "success",
        "data": [
            {
            "channel": "tiktok",
            "positive": 126,
            "negative": 18,
            "neutral": 42,
            "total_mentions": 186
            },
            {
            "channel": "twitter",
            "positive": 110,
            "negative": 5,
            "neutral": 41,
            "total_mentions": 156
            },
            {
            "channel": "news",
            "positive": 59,
            "negative": 7,
            "neutral": 26,
            "total_mentions": 92
            },
            {
            "channel": "instagram",
            "positive": 44,
            "negative": 1,
            "neutral": 8,
            "total_mentions": 53
            },
            {
            "channel": "youtube",
            "positive": 8,
            "negative": 1,
            "neutral": 7,
            "total_mentions": 16
            },
            {
            "channel": "reddit",
            "positive": 3,
            "negative": 4,
            "neutral": 5,
            "total_mentions": 12
            }
        ]
        }
    try:
        result = get_sentiment_by_categories(
            keyword=request.keyword,
            sentiment=request.sentiment,
            date_filter=request.date_filter,
            custom_start_date=request.custom_start_date,
            custom_end_date=request.custom_end_date,
            channel=request.channel,
            influence_score_min=request.influence_score_min,
            influence_score_max=request.influence_score_max,
            region=request.region,
            language=request.language,
            importance=request.importance,
            domain=request.domain
        )

        # Sanitize the data to handle non-JSON-compliant float values
        sanitized_result = sanitize_data(result)

        return {
            "status": "success",
            "data": sanitized_result
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
