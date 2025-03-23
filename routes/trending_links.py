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

@router.post("/api/trending-links", tags=["Analysis Menu"])
async def get_trending_links_analysis(request: DashboardRequest):
    dummy_data =  [{'split_link': 'https://www.antaranews.com/berita', 'total_mentions': 255},
        {'split_link': 'https://x.com/anindyabakrie', 'total_mentions': 136},
        {'split_link': 'https://www.instagram.com/p', 'total_mentions': 93},
        {'split_link': 'https://www.jpnn.com/news', 'total_mentions': 53},
        {'split_link': 'https://www.youtube.com', 'total_mentions': 50},
        {'split_link': 'https://money.kompas.com/read', 'total_mentions': 43},
        {'split_link': 'https://news.detik.com/berita', 'total_mentions': 39},
        {'split_link': 'https://barometer99.com/2025', 'total_mentions': 38},
        {'split_link': 'https://www.linkedin.com', 'total_mentions': 37},
        {'split_link': 'https://www.tiktok.com/@rekankadin', 'total_mentions': 32},
        {'split_link': 'https://www.liputan6.com/bisnis', 'total_mentions': 30},
        {'split_link': 'https://www.tiktok.com/@belakanglayar777',
        'total_mentions': 29},
        {'split_link': 'https://www.tiktok.com/@anindya.bakrie',
        'total_mentions': 28},
        {'split_link': 'https://ekonomi.bisnis.com/read', 'total_mentions': 28},
        {'split_link': 'https://www.tiktok.com/@kabarkita45', 'total_mentions': 27},
        {'split_link': 'https://www.tiktok.com/@merahhhmenyalaaa',
        'total_mentions': 27},
        {'split_link': 'https://papuabarat.antaranews.com/berita',
        'total_mentions': 26},
        {'split_link': 'https://www.tiktok.com/@dibaliklayar757575',
        'total_mentions': 23},
        {'split_link': 'https://www.viva.co.id/bisnis', 'total_mentions': 22},
        {'split_link': 'https://megapolitan.antaranews.com/berita',
        'total_mentions': 21}]
    
    return {"status": "success", "data": dummy_data}
    
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
