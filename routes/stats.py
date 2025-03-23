from fastapi import APIRouter, HTTPException
from models import DashboardRequest
from summary.stats import get_stats
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

@router.post("/api/stats", tags = ["Summary Menu"])
async def get_statistics(request: DashboardRequest):

    return {
            "status": "success",
            "data": [
                {
                "non_social_mentions": 119,
                "non_social_mentions_diff": 65,
                "non_social_mentions_pct": 120,
                "social_mentions": 393,
                "social_mentions_diff": 168,
                "social_mentions_pct": 75,
                "video_mentions": 205,
                "video_mentions_diff": 90,
                "video_mentions_pct": 78,
                "social_shares": 15797,
                "social_shares_diff": 7764,
                "social_shares_pct": 97,
                "social_likes": 323571,
                "social_likes_diff": 191403,
                "social_likes_pct": 145
                }
            ]
            }


    try:
        result = get_stats(
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
