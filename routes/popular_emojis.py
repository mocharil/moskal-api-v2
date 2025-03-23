from fastapi import APIRouter, HTTPException
from models import DashboardRequest
from analysis.popular_emojis import get_popular_emojis
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

@router.post("/api/popular-emojis", tags=["Analysis Menu"])
async def get_emoji_analysis(request: DashboardRequest):
    return [{'emoji': '✅', 'total_mentions': 100},
            {'emoji': '🇮', 'total_mentions': 73},
            {'emoji': '🇩', 'total_mentions': 73},
            {'emoji': '📍', 'total_mentions': 27},
            {'emoji': '📌', 'total_mentions': 26},
            {'emoji': '🤝', 'total_mentions': 26},
            {'emoji': '✨', 'total_mentions': 26},
            {'emoji': '🥰', 'total_mentions': 24},
            {'emoji': '🔥', 'total_mentions': 22},
            {'emoji': '🌍', 'total_mentions': 22},
            {'emoji': '👇', 'total_mentions': 17},
            {'emoji': '🤍', 'total_mentions': 16},
            {'emoji': '💡', 'total_mentions': 15},
            {'emoji': '🚀', 'total_mentions': 15},
            {'emoji': '📊', 'total_mentions': 14},
            {'emoji': '💫', 'total_mentions': 14},
            {'emoji': '💪', 'total_mentions': 13},
            {'emoji': '🤣', 'total_mentions': 11},
            {'emoji': '📡', 'total_mentions': 10},
            {'emoji': '🙏', 'total_mentions': 10}]


    try:
        result = get_popular_emojis(
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
