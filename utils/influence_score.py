import math
from datetime import datetime

# Daftar media dengan reputasi tinggi
WHITELIST_MEDIA = {
    'antaranews.com', 'apnews.com', 'argumen.id', 'asianews.network', 'barometer99.com',
    'beritaind.com', 'bisnis.com', 'bola.com', 'breitbart.com', 'channel9.id', 'cnbcindonesia.com',
    'cnnindonesia.com', 'dawn.com', 'detik.com', 'era.id', 'fajar.co.id', 'fimela.com', 'genpi.co',
    'grid.id', 'harianhaluan.com', 'harianterbit.com', 'idntimes.com', 'idxchannel.com',
    'indowarta.com', 'industry.co.id', 'inews.id', 'insertlive.com', 'jabarekspres.com',
    'jakarta365.net', 'jakartadaily.id', 'jpnn.com', 'kabarbaik.co', 'kabaroto.com',
    'kalimantanpost.com', 'kalteng.co', 'katadata.co.id', 'keuangannews.id', 'kompas.com',
    'kompasiana.com', 'koran-jakarta.com', 'krjogja.com', 'lampost.co', 'liputan6.com', 'medcom.id',
    'mediaindonesia.com', 'melintas.id', 'merahputih.com', 'moeslimchoice.com', 'ntvnews.id',
    'okezone.com', 'pikiran-rakyat.com', 'poskota.co.id', 'republika.co.id', 'riau24.com',
    'rmoljabar.id', 'suara.com', 'suarabaru.id', 'suarantb.com', 'suarasurabaya.net',
    'tangselxpress.com', 'telisik.id', 'thestar.com.my', 'tirto.id', 'tribune.com.pk',
    'tribunnews.com', 'tvonenews.com', 'viva.co.id', 'voi.id', 'wahananews.co', 'wartaekonomi.co.id',
    'wartajatim.co.id', 'waspada.id', 'tempo.co', 'kumparan.com'
}

# Normalisasi nilai metrik sosial
def normalize(x, max_val=500):
    return math.log1p(x) / math.log1p(max_val)

# Hitung influence dari konten media sosial
def influence_score_social(platform, metrics):
    platform = platform.lower()

    weights = {
        "twitter": {
            "engagement": {"likes": 0.4, "replies": 0.3, "retweets": 0.3},
            "reach": {"views": 1.0}
        },
        "linkedin": {
            "engagement": {"likes": 0.5, "comments": 0.3},
            "reach": {"reposts": 0.2}
        },
        "tiktok": {
            "engagement": {"likes": 0.4, "comments": 0.3, "favorites": 0.1},
            "reach": {"shares": 0.2}
        },
        "reddit": {
            "engagement": {"votes": 0.6},
            "reach": {"comments": 0.4}
        },
        "youtube": {
            "engagement": {"likes": 0.4, "comments": 0.2},
            "reach": {"views": 0.4}
        }
    }

    # Case khusus: Instagram
    if platform == "instagram":
        if "views" in metrics:
            setting = {
                "engagement": {"likes": 0.5, "comments": 0.3},
                "reach": {"views": 0.2}
            }
        else:
            setting = {
                "engagement": {"likes": 0.6, "comments": 0.4},
                "reach": {}
            }
    else:
        if platform not in weights:
            raise ValueError("Unsupported platform")
        setting = weights[platform]

    E = sum(normalize(metrics.get(m, 0)) * w for m, w in setting["engagement"].items())
    R = sum(normalize(metrics.get(m, 0)) * w for m, w in setting.get("reach", {}).items())
    final_score = 10 * min(1, 0.6 * E + 0.4 * R)
    return round(final_score, 2)

# Hitung influence dari berita (tanpa teks)
def influence_score_news(news_item):
    A = 1 if news_item.get("username") in WHITELIST_MEDIA else 0

    media_links = news_item.get("post_media_link", "")
    M = 1 if media_links and "http" in media_links else 0

    created_at = datetime.strptime(news_item.get("post_created_at"), "%Y-%m-%d %H:%M:%S")
    days_old = (datetime.now() - created_at).days
    R = max(0, 1 - days_old / 30)

    Q = 1 if news_item.get("list_quotes") and "quotes" in news_item.get("list_quotes").lower() else 0

    score = 0.6 * A + 0.2 * M + 0.2 * Q
    return round(min(score * 10, 10), 2)

# Fungsi utama: deteksi channel dan hitung skor
def get_influence_score(item):
    if item.get("channel", "").lower() == "news":
        return influence_score_news(item)
    return influence_score_social(item["channel"], item)