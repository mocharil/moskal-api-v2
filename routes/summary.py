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

@router.post("/api/summary", tags = ['Summary Menu'])
async def get_summary(request: DashboardRequest):

    return {
            "status": "success",
            "data": [{'current_mentions': 3122,
                    'previous_mentions': 630,
                    'current_total_posts': 144485,
                    'previous_total_posts': 630,
                    'current_social_reach': 12007.363724177814,
                    'previous_social_reach': 5298.906588279827,
                    'current_non_social_reach': 3286.0499999999993,
                    'previous_non_social_reach': 676.0496975323473,
                    'current_social_interactions': 1004152,
                    'previous_social_interactions': 1410071,
                    'current_non_social_interactions': 72865,
                    'previous_non_social_interactions': 12079,
                    'current_positive': 2107,
                    'previous_positive': 406,
                    'current_negative': 279,
                    'previous_negative': 55,
                    'mentions_percent_change': 395.55555555555554,
                    'total_posts_percent_change': 22834.126984126986,
                    'social_reach_percent_change': 126.60078120146179,
                    'non_social_reach_percent_change': 386.0663368380207,
                    'social_interactions_percent_change': -28.78713199548108,
                    'non_social_interactions_percent_change': 503.237022932362,
                    'positive_percent_change': 418.96551724137925,
                    'negative_percent_change': 407.27272727272725}]
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
