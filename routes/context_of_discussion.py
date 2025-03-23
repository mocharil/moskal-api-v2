from fastapi import APIRouter, HTTPException
from models import DashboardRequest
from dashboard_menu.context_of_discussion import context_of_discussion

router = APIRouter()

@router.post("/api/context-of-discussion", tags=["Dashboard Menu"])
async def get_context_of_discussion(request: DashboardRequest):
    dummy_data = [{'word': 'indonesia', 'dominant_sentiment': 'positive', 'total_data': 5883.0},
                {'word': 'program', 'dominant_sentiment': 'positive', 'total_data': 3555.0},
                {'word': 'ekonomi', 'dominant_sentiment': 'positive', 'total_data': 2916.0},
                {'word': 'kadin', 'dominant_sentiment': 'positive', 'total_data': 3258.0},
                {'word': 'pemerintah',
                'dominant_sentiment': 'positive',
                'total_data': 2837.0},
                {'word': 'masyarakat',
                'dominant_sentiment': 'positive',
                'total_data': 2124.0},
                {'word': 'jakarta', 'dominant_sentiment': 'positive', 'total_data': 2155.0},
                {'word': 'industri', 'dominant_sentiment': 'positive', 'total_data': 1900.0},
                {'word': 'mendukung', 'dominant_sentiment': 'positive', 'total_data': 1435.0},
                {'word': 'strategis', 'dominant_sentiment': 'positive', 'total_data': 1661.0},
                {'word': 'nasional', 'dominant_sentiment': 'positive', 'total_data': 1705.0},
                {'word': 'kerja', 'dominant_sentiment': 'positive', 'total_data': 1379.0},
                {'word': 'sektor', 'dominant_sentiment': 'positive', 'total_data': 1258.0},
                {'word': 'meningkatkan',
                'dominant_sentiment': 'positive',
                'total_data': 1240.0},
                {'word': 'daerah', 'dominant_sentiment': 'positive', 'total_data': 1651.0},
                {'word': 'pembangunan',
                'dominant_sentiment': 'positive',
                'total_data': 1181.0},
                {'word': 'presiden', 'dominant_sentiment': 'positive', 'total_data': 1823.0},
                {'word': 'umkm', 'dominant_sentiment': 'positive', 'total_data': 1012.0},
                {'word': 'usaha', 'dominant_sentiment': 'positive', 'total_data': 977.0},
                {'word': 'memiliki', 'dominant_sentiment': 'positive', 'total_data': 966.0},
                {'word': 'pertumbuhan',
                'dominant_sentiment': 'positive',
                'total_data': 963.0},
                {'word': 'ketua', 'dominant_sentiment': 'positive', 'total_data': 1530.0},
                {'word': 'menteri', 'dominant_sentiment': 'positive', 'total_data': 1166.0},
                {'word': 'kota', 'dominant_sentiment': 'positive', 'total_data': 895.0},
                {'word': 'pangan', 'dominant_sentiment': 'negative', 'total_data': 198.0},
                {'word': 'proyek', 'dominant_sentiment': 'negative', 'total_data': 191.0},
                {'word': 'kebijakan', 'dominant_sentiment': 'neutral', 'total_data': 445.0},
                {'word': 'negara', 'dominant_sentiment': 'neutral', 'total_data': 475.0},
                {'word': 'anggaran', 'dominant_sentiment': 'negative', 'total_data': 168.0},
                {'word': 'hutan', 'dominant_sentiment': 'negative', 'total_data': 162.0},
                {'word': 'lahan', 'dominant_sentiment': 'negative', 'total_data': 158.0},
                {'word': 'prabowo', 'dominant_sentiment': 'neutral', 'total_data': 635.0},
                {'word': 'juta', 'dominant_sentiment': 'negative', 'total_data': 137.0},
                {'word': 'dewan', 'dominant_sentiment': 'neutral', 'total_data': 335.0},
                {'word': 'hashim', 'dominant_sentiment': 'neutral', 'total_data': 309.0},
                {'word': 'bidang', 'dominant_sentiment': 'neutral', 'total_data': 274.0},
                {'word': 'jokowi', 'dominant_sentiment': 'neutral', 'total_data': 273.0},
                {'word': 'djojohadikusumo',
                'dominant_sentiment': 'neutral',
                'total_data': 248.0},
                {'word': 'bakrie', 'dominant_sentiment': 'neutral', 'total_data': 248.0},
                {'word': 'danantara', 'dominant_sentiment': 'neutral', 'total_data': 243.0}]

    return {
            "status": "success",
            "data": dummy_data
        }

    try:
        # Get word frequency analysis
        result = context_of_discussion(
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

        return {
            "status": "success",
            "data": result
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
