from fastapi import APIRouter, HTTPException
from models import DashboardRequest
from analysis.trending_link import get_trending_links
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

@router.post("/api/analysis_overview", tags=["Analysis Menu"])
async def get_analysis_overview(request: DashboardRequest,    openapi_examples={
        "default": {
            "summary": "Contoh Dasar",
            "description": "Pencarian dengan keyword, sentiment dan filter dasar",
            "value": {
                "keyword": ["kadin", "indonesia", "ekonomi"],
                "sentiment": ["positive", "neutral"],
                "date_filter": "last 30 days",
                "channel": ["twitter", "instagram", "news"],
                "importance": "all mentions"
            }
        },
        "custom_date": {
            "summary": "Contoh dengan Tanggal Kustom",
            "description": "Pencarian dengan rentang tanggal khusus",
            "value": {
                "keyword": ["investasi"],
                "sentiment": ["positive", "negative", "neutral"],
                "date_filter": "custom",
                "custom_start_date": "2025-01-01",
                "custom_end_date": "2025-01-31",
                "channel": ["news"],
                "importance": "important mentions"
            }
        },
        "complete": {
            "summary": "Contoh Lengkap",
            "description": "Contoh dengan semua filter yang digunakan",
            "value": {
                "keyword": ["ekonomi", "investasi", "bisnis"],
                "sentiment": ["positive", "negative", "neutral"],
                "date_filter": "last 3 months",
                "custom_start_date": None,
                "custom_end_date": None,
                "channel": ["tiktok", "news", "instagram", "twitter", "linkedin", "reddit", "youtube"],
                "influence_score_min": 50,
                "influence_score_max": 100,
                "region": ["Jakarta", "Bandung", "Surabaya", "Medan"],
                "language": ["id", "en", "jv"],
                "importance": "important mentions",
                "domain": ["kompas.com", "detik.com", "cnbcindonesia.com"]
            }
        }
    }):
    dummy_data = [{'metric_name': 'negative_mentions',
            'current_value': 279.0,
            'previous_value': 55.0,
            'growth_percent': 407.27},
            {'metric_name': 'non_social_media_mentions',
            'current_value': 1578.0,
            'previous_value': 234.0,
            'growth_percent': 574.36},
            {'metric_name': 'non_social_media_reach',
            'current_value': 3286.0499999999993,
            'previous_value': 679.7996975323473,
            'growth_percent': 383.39},
            {'metric_name': 'positive_mentions',
            'current_value': 2107.0,
            'previous_value': 417.0,
            'growth_percent': 405.28},
            {'metric_name': 'presence_score',
            'current_value': 23.13,
            'previous_value': 20.18,
            'growth_percent': 14.62},
            {'metric_name': 'social_media_comments',
            'current_value': 42181.0,
            'previous_value': 49187.0,
            'growth_percent': -14.24},
            {'metric_name': 'social_media_mentions',
            'current_value': 1544.0,
            'previous_value': 416.0,
            'growth_percent': 271.15},
            {'metric_name': 'social_media_reach',
            'current_value': 12007.363724177814,
            'previous_value': 5579.121837390629,
            'growth_percent': 115.22},
            {'metric_name': 'social_media_reactions',
            'current_value': 3576494.0,
            'previous_value': 1598051.0,
            'growth_percent': 123.8},
            {'metric_name': 'social_media_shares',
            'current_value': 34054.0,
            'previous_value': 82032.0,
            'growth_percent': -58.49},
            {'metric_name': 'total_mentions',
            'current_value': 3122.0,
            'previous_value': 650.0,
            'growth_percent': 380.31},
            {'metric_name': 'total_reach',
            'current_value': 15293.413724177819,
            'previous_value': 6258.921534922978,
            'growth_percent': 144.35},
            {'metric_name': 'total_social_media_interactions',
            'current_value': 1004152.0,
            'previous_value': 1678660.0,
            'growth_percent': -40.18}]

    return {
                "status": "success",
                "data": dummy_data
            }
    try:
        result = get_trending_links(
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
