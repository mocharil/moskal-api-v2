from fastapi import APIRouter, HTTPException
from models import DashboardRequest
from analysis.presence_score import get_presence_score
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

@router.post("/api/presence-score", tags=["Analysis Menu"])
async def get_presence_score_analysis(request: DashboardRequest):
    return {
            "status": "success",
            "data": [
                {
                "date": "2025-01-01",
                "presence_score": 2.71
                },
                {
                "date": "2025-01-02",
                "presence_score": 4.24
                },
                {
                "date": "2025-01-03",
                "presence_score": 0.76
                },
                {
                "date": "2025-01-04",
                "presence_score": 0.82
                },
                {
                "date": "2025-01-06",
                "presence_score": 1.44
                },
                {
                "date": "2025-01-07",
                "presence_score": 0.79
                },
                {
                "date": "2025-01-08",
                "presence_score": 1.49
                },
                {
                "date": "2025-01-09",
                "presence_score": 7.1
                },
                {
                "date": "2025-01-10",
                "presence_score": 3
                },
                {
                "date": "2025-01-11",
                "presence_score": 0.73
                },
                {
                "date": "2025-01-13",
                "presence_score": 5.26
                },
                {
                "date": "2025-01-14",
                "presence_score": 2.47
                },
                {
                "date": "2025-01-15",
                "presence_score": 4.58
                },
                {
                "date": "2025-01-16",
                "presence_score": 92.2
                },
                {
                "date": "2025-01-17",
                "presence_score": 55.37
                },
                {
                "date": "2025-01-18",
                "presence_score": 6.24
                },
                {
                "date": "2025-01-19",
                "presence_score": 1.55
                },
                {
                "date": "2025-01-20",
                "presence_score": 4.81
                },
                {
                "date": "2025-01-21",
                "presence_score": 7.39
                },
                {
                "date": "2025-01-22",
                "presence_score": 11.34
                },
                {
                "date": "2025-01-23",
                "presence_score": 12.31
                },
                {
                "date": "2025-01-24",
                "presence_score": 4.73
                },
                {
                "date": "2025-01-25",
                "presence_score": 3.8
                },
                {
                "date": "2025-01-26",
                "presence_score": 6.75
                },
                {
                "date": "2025-01-27",
                "presence_score": 4.13
                },
                {
                "date": "2025-01-28",
                "presence_score": 5.08
                },
                {
                "date": "2025-01-29",
                "presence_score": 2.97
                },
                {
                "date": "2025-01-30",
                "presence_score": 28.14
                },
                {
                "date": "2025-01-31",
                "presence_score": 4.82
                },
                {
                "date": "2025-02-02",
                "presence_score": 6.95
                },
                {
                "date": "2025-02-03",
                "presence_score": 5.79
                },
                {
                "date": "2025-02-04",
                "presence_score": 2.18
                },
                {
                "date": "2025-02-05",
                "presence_score": 7.76
                },
                {
                "date": "2025-02-06",
                "presence_score": 11.03
                },
                {
                "date": "2025-02-07",
                "presence_score": 7.82
                },
                {
                "date": "2025-02-08",
                "presence_score": 3.23
                },
                {
                "date": "2025-02-09",
                "presence_score": 6.25
                },
                {
                "date": "2025-02-10",
                "presence_score": 5.52
                },
                {
                "date": "2025-02-11",
                "presence_score": 4.66
                },
                {
                "date": "2025-02-12",
                "presence_score": 17.11
                },
                {
                "date": "2025-02-13",
                "presence_score": 10.21
                },
                {
                "date": "2025-02-14",
                "presence_score": 4.22
                },
                {
                "date": "2025-02-15",
                "presence_score": 1.16
                },
                {
                "date": "2025-02-16",
                "presence_score": 8.62
                },
                {
                "date": "2025-02-17",
                "presence_score": 8.09
                },
                {
                "date": "2025-02-18",
                "presence_score": 34.47
                },
                {
                "date": "2025-02-19",
                "presence_score": 26.02
                },
                {
                "date": "2025-02-20",
                "presence_score": 24.2
                },
                {
                "date": "2025-02-21",
                "presence_score": 9.61
                },
                {
                "date": "2025-02-22",
                "presence_score": 2.78
                },
                {
                "date": "2025-02-23",
                "presence_score": 2.34
                },
                {
                "date": "2025-02-24",
                "presence_score": 26.01
                },
                {
                "date": "2025-02-25",
                "presence_score": 13.44
                },
                {
                "date": "2025-02-26",
                "presence_score": 13.19
                },
                {
                "date": "2025-02-27",
                "presence_score": 7.97
                },
                {
                "date": "2025-02-28",
                "presence_score": 15.65
                },
                {
                "date": "2025-03-01",
                "presence_score": 5.29
                }
            ]
            }
    try:
        result = get_presence_score(
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
