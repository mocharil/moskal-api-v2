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

@router.post("/api/most_followers", tags=["Analysis Menu"])
async def get_trending_links_analysis(request: DashboardRequest):
    return {"status":"success","data":[{'username': '@officialinews',
            'channel': 'tiktok',
            'followers': 8700000,
            'influence_score': 0.7867586206896551,
            'total_mentions': 3,
            'total_reach': 107.62337500000001},
            {'username': '@metro_tv',
            'channel': 'tiktok',
            'followers': 7600000,
            'influence_score': 0.7877105263157894,
            'total_mentions': 6,
            'total_reach': 138.03559475806452},
            {'username': '@liputan6.sctv',
            'channel': 'tiktok',
            'followers': 5900000,
            'influence_score': 0.8107966101694914,
            'total_mentions': 1,
            'total_reach': 40.0},
            {'username': '@gibran_rakabuming',
            'channel': 'tiktok',
            'followers': 5600000,
            'influence_score': 0.0,
            'total_mentions': 1,
            'total_reach': 40.0},
            {'username': 'detikcom',
            'channel': 'instagram',
            'followers': 4800000,
            'influence_score': 1.0,
            'total_mentions': 1,
            'total_reach': 0.10888198757763977},
            {'username': '@kompascom',
            'channel': 'tiktok',
            'followers': 4500000,
            'influence_score': 0.7859772727272727,
            'total_mentions': 2,
            'total_reach': 44.44466019417476},
            {'username': '@tribuncirebon.com',
            'channel': 'tiktok',
            'followers': 3500000,
            'influence_score': 0.7343714285714286,
            'total_mentions': 1,
            'total_reach': 3.238030303030303},
            {'username': '@kumparan',
            'channel': 'tiktok',
            'followers': 2900000,
            'influence_score': 0.8228928571428571,
            'total_mentions': 3,
            'total_reach': 120.0},
            {'username': '@sindonews',
            'channel': 'tiktok',
            'followers': 2600000,
            'influence_score': 0.7206538461538461,
            'total_mentions': 2,
            'total_reach': 16.284512195121952},
            {'username': 'presidenrepublikindonesia',
            'channel': 'instagram',
            'followers': 2500000,
            'influence_score': 0.7402,
            'total_mentions': 1,
            'total_reach': 22.707500000000003},
            {'username': '@menujuindonesiamaju',
            'channel': 'tiktok',
            'followers': 2500000,
            'influence_score': 0.8907999999999999,
            'total_mentions': 3,
            'total_reach': 120.0},
            {'username': '@akuratco',
            'channel': 'tiktok',
            'followers': 2000000,
            'influence_score': 0.7781499999999999,
            'total_mentions': 1,
            'total_reach': 40.0},
            {'username': '@idntimes',
            'channel': 'tiktok',
            'followers': 1800000,
            'influence_score': 0.7935,
            'total_mentions': 2,
            'total_reach': 50.12058823529412},
            {'username': '@tribunkaltim.co',
            'channel': 'tiktok',
            'followers': 1700000,
            'influence_score': 0.7824117647058824,
            'total_mentions': 5,
            'total_reach': 111.25051948051949},
            {'username': '@merdekacom',
            'channel': 'tiktok',
            'followers': 1700000,
            'influence_score': 0.7476470588235293,
            'total_mentions': 1,
            'total_reach': 40.0},
            {'username': '@tempo.co',
            'channel': 'tiktok',
            'followers': 1300000,
            'influence_score': 0.7477499999999999,
            'total_mentions': 1,
            'total_reach': 40.0},
            {'username': '@officialokezone',
            'channel': 'tiktok',
            'followers': 1300000,
            'influence_score': 0.7447692307692307,
            'total_mentions': 1,
            'total_reach': 29.279125},
            {'username': '@tvrinasional',
            'channel': 'tiktok',
            'followers': 1200000,
            'influence_score': 0.7142499999999999,
            'total_mentions': 2,
            'total_reach': 49.60225409836065},
            {'username': '@idxchannel',
            'channel': 'tiktok',
            'followers': 1100000,
            'influence_score': 0.0,
            'total_mentions': 1,
            'total_reach': 40.0},
            {'username': '@warta_ekonomi',
            'channel': 'tiktok',
            'followers': 983500,
            'influence_score': 0.7218327183271832,
            'total_mentions': 7,
            'total_reach': 148.37418482985595}]}
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
