from fastapi import APIRouter, HTTPException
from models import ShareOfVoiceRequest
from analysis.most_share_of_voice import get_most_share_of_voice
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

@router.post("/api/share-of-voice", tags=["Analysis Menu"])
async def get_share_of_voice_analysis(request: ShareOfVoiceRequest):

    return {
        "status": "success",
        "data": [
            {
            "channel": "twitter",
            "username": "@cahayasamar",
            "total_mentions": 6,
            "total_reach": 0,
            "percentage_share_of_voice": 11.76
            },
            {
            "channel": "twitter",
            "username": "@Dibaliklayar45",
            "total_mentions": 5,
            "total_reach": 0,
            "percentage_share_of_voice": 9.8
            },
            {
            "channel": "twitter",
            "username": "@anindyabakrie",
            "total_mentions": 2,
            "total_reach": 0,
            "percentage_share_of_voice": 3.92
            },
            {
            "channel": "twitter",
            "username": "@lwr7777777",
            "total_mentions": 2,
            "total_reach": 0,
            "percentage_share_of_voice": 3.92
            },
            {
            "channel": "twitter",
            "username": "@radaraktual",
            "total_mentions": 2,
            "total_reach": 0,
            "percentage_share_of_voice": 3.92
            },
            {
            "channel": "twitter",
            "username": "@callmebyra",
            "total_mentions": 1,
            "total_reach": 0,
            "percentage_share_of_voice": 1.96
            },
            {
            "channel": "twitter",
            "username": "@furretwalkin",
            "total_mentions": 1,
            "total_reach": 4.62,
            "percentage_share_of_voice": 1.96
            },
            {
            "channel": "twitter",
            "username": "@manaf_sabil",
            "total_mentions": 1,
            "total_reach": 0,
            "percentage_share_of_voice": 1.96
            },
            {
            "channel": "twitter",
            "username": "@TerkiniForum",
            "total_mentions": 1,
            "total_reach": 0,
            "percentage_share_of_voice": 1.96
            },
            {
            "channel": "twitter",
            "username": "@Mata_Netizen62",
            "total_mentions": 1,
            "total_reach": 4.03,
            "percentage_share_of_voice": 1.96
            }
        ]
        }
    try:
        result = get_most_share_of_voice(
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
            domain=request.domain,
            limit=request.limit
        )

        # Sanitize the data to handle non-JSON-compliant float values
        sanitized_result = sanitize_data(result)

        return {
            "status": "success",
            "data": sanitized_result
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
