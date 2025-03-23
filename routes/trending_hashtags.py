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

@router.post("/api/trending_hashtags", tags=["Analysis Menu"])
async def get_hashtags(request: DashboardRequest):
    return [{'hashtag': '#kadin',
                'total_mentions': 346,
                'dominant_sentiment': 'positive',
                'dominant_sentiment_count': 266,
                'dominant_sentiment_percentage': 76.9},
                {'hashtag': '#kadinindonesia',
                'total_mentions': 331,
                'dominant_sentiment': 'positive',
                'dominant_sentiment_count': 270,
                'dominant_sentiment_percentage': 81.6},
                {'hashtag': '#anindyabakrie',
                'total_mentions': 238,
                'dominant_sentiment': 'positive',
                'dominant_sentiment_count': 181,
                'dominant_sentiment_percentage': 76.1},
                {'hashtag': '#fyp',
                'total_mentions': 117,
                'dominant_sentiment': 'negative',
                'dominant_sentiment_count': 44,
                'dominant_sentiment_percentage': 37.6},
                {'hashtag': '#aninbakrie',
                'total_mentions': 79,
                'dominant_sentiment': 'positive',
                'dominant_sentiment_count': 72,
                'dominant_sentiment_percentage': 91.1},
                {'hashtag': '#indonesiamaju',
                'total_mentions': 57,
                'dominant_sentiment': 'positive',
                'dominant_sentiment_count': 55,
                'dominant_sentiment_percentage': 96.5},
                {'hashtag': '#prabowo',
                'total_mentions': 55,
                'dominant_sentiment': 'positive',
                'dominant_sentiment_count': 28,
                'dominant_sentiment_percentage': 50.9},
                {'hashtag': '#pengukuhanpenguruskadinindonesia',
                'total_mentions': 49,
                'dominant_sentiment': 'positive',
                'dominant_sentiment_count': 48,
                'dominant_sentiment_percentage': 98.0},
                {'hashtag': '#ketuaumumkadinindonesia',
                'total_mentions': 48,
                'dominant_sentiment': 'positive',
                'dominant_sentiment_count': 47,
                'dominant_sentiment_percentage': 97.9},
                {'hashtag': '#ekonomimaju',
                'total_mentions': 48,
                'dominant_sentiment': 'positive',
                'dominant_sentiment_count': 47,
                'dominant_sentiment_percentage': 97.9},
                {'hashtag': '#kiprahkadinjktdiapresiasi',
                'total_mentions': 46,
                'dominant_sentiment': 'positive',
                'dominant_sentiment_count': 46,
                'dominant_sentiment_percentage': 100.0},
                {'hashtag': '#bakriegroup',
                'total_mentions': 40,
                'dominant_sentiment': 'negative',
                'dominant_sentiment_count': 26,
                'dominant_sentiment_percentage': 65.0},
                {'hashtag': '#mengungkaptabir',
                'total_mentions': 39,
                'dominant_sentiment': 'negative',
                'dominant_sentiment_count': 38,
                'dominant_sentiment_percentage': 97.4},
                {'hashtag': '#kupastanpabasabasi',
                'total_mentions': 39,
                'dominant_sentiment': 'negative',
                'dominant_sentiment_count': 38,
                'dominant_sentiment_percentage': 97.4},
                {'hashtag': '#kritistanpatakut',
                'total_mentions': 39,
                'dominant_sentiment': 'negative',
                'dominant_sentiment_count': 38,
                'dominant_sentiment_percentage': 97.4},
                {'hashtag': '#indonesia',
                'total_mentions': 36,
                'dominant_sentiment': 'positive',
                'dominant_sentiment_count': 21,
                'dominant_sentiment_percentage': 58.3},
                {'hashtag': '#viral',
                'total_mentions': 33,
                'dominant_sentiment': 'positive',
                'dominant_sentiment_count': 16,
                'dominant_sentiment_percentage': 48.5},
                {'hashtag': '#danantara',
                'total_mentions': 32,
                'dominant_sentiment': 'positive',
                'dominant_sentiment_count': 19,
                'dominant_sentiment_percentage': 59.4},
                {'hashtag': '#fouryou',
                'total_mentions': 31,
                'dominant_sentiment': 'negative',
                'dominant_sentiment_count': 30,
                'dominant_sentiment_percentage': 96.8},
                {'hashtag': '#ekonomiindonesia',
                'total_mentions': 29,
                'dominant_sentiment': 'positive',
                'dominant_sentiment_count': 26,
                'dominant_sentiment_percentage': 89.7}]


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
