from utils.functions import About_BQ
import os
from dotenv import load_dotenv
import os

load_dotenv()
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_CREDS_LOCATION = os.getenv("BQ_CREDS_LOCATION")

BQ = About_BQ(project_id = BQ_PROJECT_ID, credentials_loc = BQ_CREDS_LOCATION)
def kol_to_watch(
    keyword=None,
    sentiment=None,  # Can be a string or list of strings
    date_filter="all time",
    custom_start_date=None,
    custom_end_date=None,
    channel=None,  # Can be a string or list of strings
    influence_score_min=None,
    influence_score_max=None,
    region=None,  # Can be a string or list of strings
    language=None,  # Can be a string or list of strings
    importance="all mentions",
    domain=None,  # Can be a string or list of strings
    sort_by='popular_first',
    limit=10,
    offset=0
   ):
    """
    Get a list of mentions with multiple filters.
    
    Parameters:
    - keyword (str): Search term for post captions
    - sentiment (str/list): Filter by sentiment ('positive', 'negative', 'neutral') or list of multiple sentiments
    - date_filter (str): Predefined date ranges ('yesterday', 'this week', 'last 7 days', etc.)
    - custom_start_date (str): Start date in YYYY-MM-DD format when date_filter is 'custom'
    - custom_end_date (str): End date in YYYY-MM-DD format when date_filter is 'custom'
    - channel (str/list): Filter by channel or list of channels
    - influence_score_min (float): Minimum influence score
    - influence_score_max (float): Maximum influence score
    - region (str/list): Filter by region or list of regions
    - language (str/list): Filter by language or list of languages
    - importance (str): Either 'all mentions' or 'important mentions' (top 10% viral scores)
    - domain (str/list): Domain name filter for link_post
    - sort_by (str): 'popular_first' or 'recent_first'
    - limit (int): Number of results to return
    - offset (int): Offset for pagination
    
    Returns:
    - List of dictionaries with mention details
    """
    # Construct the date filter clause
    date_clause = ""
    if date_filter == "yesterday":
        date_clause = "DATE(CAST(a.post_created_at AS TIMESTAMP)) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)"
    elif date_filter == "this week":
        date_clause = "DATE(CAST(a.post_created_at AS TIMESTAMP)) BETWEEN DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) AND CURRENT_DATE()"
    elif date_filter == "last 7 days":
        date_clause = "DATE(CAST(a.post_created_at AS TIMESTAMP)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
    elif date_filter == "last 30 days":
        date_clause = "DATE(CAST(a.post_created_at AS TIMESTAMP)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
    elif date_filter == "last 3 months":
        date_clause = "DATE(CAST(a.post_created_at AS TIMESTAMP)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)"
    elif date_filter == "this year":
        date_clause = "EXTRACT(YEAR FROM CAST(a.post_created_at AS TIMESTAMP)) = EXTRACT(YEAR FROM CURRENT_DATE())"
    elif date_filter == "last year":
        date_clause = "EXTRACT(YEAR FROM CAST(a.post_created_at AS TIMESTAMP)) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1"
    elif date_filter == "custom" and custom_start_date and custom_end_date:
        date_clause = f"DATE(CAST(a.post_created_at AS TIMESTAMP)) BETWEEN '{custom_start_date}' AND '{custom_end_date}'"
    else:  # all time - no filter
        date_clause = "1=1"  # Always true condition

    # Build the filter conditions
    filter_conditions = []
    
    # Keyword filter
    if keyword:
        filter_conditions.append(f"""SEARCH(a.post_caption, "{keyword}")""")
    
    # Date filter
    filter_conditions.append(date_clause)
    
    # Channel filter
    if channel:
        if isinstance(channel, list):
            channel_condition = f"""a.channel IN ({', '.join([f'"' + c + '"' for c in channel])})"""
        else:
            channel_condition = f"a.channel = \"{channel}\""
        filter_conditions.append(channel_condition)
    
    # Influence score filter
    if influence_score_min is not None:
        filter_conditions.append(f"influence_score*10 >= {influence_score_min}")
    if influence_score_max is not None:
        filter_conditions.append(f"influence_score*10 <= {influence_score_max}")
    
    # Domain filter
    if domain:
        if isinstance(domain, list):
            domain_conditions = []
            for d in domain:
                domain_conditions.append(f"a.link_post LIKE '%{d}%'")
            filter_conditions.append(f"({' OR '.join(domain_conditions)})")
        else:
            filter_conditions.append(f"a.link_post LIKE '%{domain}%'")
    
    # Construct the sentiment filter for post_category table
    category_filter_conditions = []
    
    if sentiment:
        if isinstance(sentiment, list):
            sentiment_condition = f"""c.sentiment IN ({', '.join([f'"' + s + '"' for s in sentiment])})"""
        else:
            sentiment_condition = f"""c.sentiment = '{sentiment}'"""
        category_filter_conditions.append(sentiment_condition)
    
    # Region filter with LOWER and LIKE
    if region:
        if isinstance(region, list):
            region_conditions = []
            for r in region:
                region_conditions.append(f"LOWER(c.region) LIKE '%{r.lower()}%'")
            region_condition = f"({' OR '.join(region_conditions)})"
        else:
            region_condition = f"LOWER(c.region) LIKE '%{region.lower()}%'"
        category_filter_conditions.append(region_condition)
    
    # Language filter with LOWER and LIKE
    if language:
        if isinstance(language, list):
            language_conditions = []
            for l in language:
                if l.lower() == 'id':
                    language_conditions.append("LOWER(c.language) LIKE '%indonesia%'")
                elif l.lower() == 'en':
                    language_conditions.append("LOWER(c.language) LIKE '%english%'")
                else:
                    language_conditions.append(f"LOWER(c.language) LIKE '%{l.lower()}%'")
            language_condition = f"({' OR '.join(language_conditions)})"
        else:
            if language.lower() == 'id':
                language_condition = "LOWER(c.language) LIKE '%indonesia%'"
            elif language.lower() == 'en':
                language_condition = "LOWER(c.language) LIKE '%english%'"
            else:
                language_condition = f"LOWER(c.language) LIKE '%{language.lower()}%'"
        category_filter_conditions.append(language_condition)
    
    # Important mentions filter (using viral_score)
    importance_subquery = ""
    if importance == "important mentions":
        
        filter_channel = ''
        if channel:
            filter_channel = f"""and channel IN ({', '.join([f'"' + c + '"' for c in channel])})"""
        
        
        importance_subquery = f"""
         viral_threshold AS (
            SELECT PERCENTILE_CONT(viral_score, 0.8) OVER() AS threshold
            FROM `medsos.post_analysis`
            WHERE viral_score IS NOT NULL
            {filter_channel}
            LIMIT 1
        ),
        """
        filter_conditions.append("a.viral_score >= (SELECT threshold FROM viral_threshold)")
    
    # Combine all filter conditions
    where_clause = " AND ".join(filter_conditions)
    category_where_clause = " AND ".join(category_filter_conditions) if category_filter_conditions else "1=1"
    
    dummy_news = [{'channel': 'news',
                    'username': 'kalteng.co',
                    'viral_score': 0.0,
                    'reach_score': 1.9,
                    'influence_score': 3.3499999999999996,
                    'total_post': 1,
                    'total_negative': 0,
                    'total_positive': 1,
                    'total_neutral': 0,
                    'issue': ['PAN Kalteng holds Ramadan gathering'],
                    'weighted_influence': 0.008505559914720346,
                    'unified_issues': ['Government Policies & Programs',
                    'Social & Community Development'],
                    'followings': "",
                    'followers': "",
                    'user_category': 'media',
                    'references': [{'channel': 'news',
                        'link_post': 'https://kalteng.co/politika/dpw-pan-kalteng-gelar-buka-puasa-bersama/'}],
                    'is_negative_driver': False},
                    {'channel': 'news',
                    'username': 'vlix.id',
                    'viral_score': 0.0,
                    'reach_score': 6.25,
                    'influence_score': 1.4878571428571428,
                    'total_post': 7,
                    'total_negative': 0,
                    'total_positive': 2,
                    'total_neutral': 5,
                    'issue': ['Vice President Gibran attends Kadin event',
                    'Kadin Indonesia Buka Puasa Bersama',
                    "Hashim Djojohadikusumo's meeting with Jokowi",
                    'Anindya Bakrie re-elected as PB Akuatik chairman',
                    "Prabowo's meeting with business leaders",
                    "Inauguration of Kadin Indonesia's board",
                    "Kadin's meeting with Immigration Minister"],
                    'weighted_influence': 0.006833240951516918,
                    'unified_issues': ['Kadin & Business Leaders',
                    'Government Policies & Programs',
                    'Economic Development & Investment',
                    'Social & Community Development',
                    'Sports & Recreation',
                    'International Relations & Cooperation'],
                    'followings': "",
                    'followers': "",
                    'user_category': 'media',
                    'references': [{'channel': 'instagram',
                        'link_post': 'https://www.instagram.com/p/DG3Lxa1zMaz/'},
                    {'channel': 'news',
                        'link_post': 'https://kabar24.bisnis.com/read/20250309/15/1859548/pengamat-soroti-urgensi-pertemuan-hashim-djojohadikusumo-dan-jokowi'},
                    {'channel': 'tiktok',
                        'link_post': 'https://www.tiktok.com/@momentum7446/video/7479010609833479429'},
                    {'channel': 'twitter',
                        'link_post': 'https://x.com/kompascom/status/1897873256654463156'},
                    {'channel': 'youtube',
                        'link_post': 'https://www.youtube.com/watch?v=HQ4IT1ZmDPo'}],
                    'is_negative_driver': False},
                    {'channel': 'news',
                    'username': 'bantentv.com',
                    'viral_score': 0.0,
                    'reach_score': 4.3,
                    'influence_score': 3.7729999999999997,
                    'total_post': 2,
                    'total_negative': 0,
                    'total_positive': 1,
                    'total_neutral': 1,
                    'issue': ["Prabowo's nephews appointed to Kadin",
                    'Sekolah Rakyat program for underprivileged children'],
                    'weighted_influence': 0.010488538825086978,
                    'unified_issues': ['Kadin & Business Leaders',
                    'Government Policies & Programs',
                    'Education & Employment'],
                    'followings': "",
                    'followers': "",
                    'user_category': 'media',
                    'references': [{'channel': 'news',
                        'link_post': 'https://www.tempo.co/ekonomi/profil-dua-keponakan-prabowo-yang-baru-dilantik-jadi-pengurus-kadin-1220407'},
                    {'channel': 'news',
                        'link_post': 'https://bantentv.com/inhouse/dua-ponakan-prabowo-baru-dilantik-jadi-pengurus-kadin-berikut-profilnya/'},
                    {'channel': 'news',
                        'link_post': 'https://www.melintas.id/pendidikan/345790891/sekolah-rakyat-program-pendidikan-gratis-dan-berkualitas-untuk-anak-anak-miskin-ekstrem-di-indonesia'},
                    {'channel': 'tiktok',
                        'link_post': 'https://www.tiktok.com/@birosuararakyat/video/7479760806016552213'}],
                    'is_negative_driver': False},
                    {'channel': 'news',
                    'username': 'tempo.co',
                    'viral_score': 0.0,
                    'reach_score': 3.35,
                    'influence_score': 5.896,
                    'total_post': 1,
                    'total_negative': 0,
                    'total_positive': 1,
                    'total_neutral': 0,
                    'issue': ['Indonesian Aquatic Awards'],
                    'weighted_influence': 0.014971690312598956,
                    'unified_issues': ['Sports & Recreation', 'Awards & Recognition'],
                    'followings': "",
                    'followers': "",
                    'user_category': 'media',
                    'references': [{'channel': 'news',
                        'link_post': 'https://en.tempo.co/read/1987056/list-of-athletes-coaches-who-received-2025-indonesian-aquatic-awards'}],
                    'is_negative_driver': False},
                    {'channel': 'news',
                    'username': 'konstruksimedia.com',
                    'viral_score': 0.0,
                    'reach_score': 2.95,
                    'influence_score': 4.954000000000001,
                    'total_post': 1,
                    'total_negative': 0,
                    'total_positive': 0,
                    'total_neutral': 1,
                    'issue': ["Insannul Kamil's profile and role in KADIN"],
                    'weighted_influence': 0.01262260486983671,
                    'unified_issues': ['Kadin & Business Leaders',
                    'Government Policies & Programs'],
                    'followings': "",
                    'followers': "",
                    'user_category': 'media',
                    'references': [{'channel': 'news',
                        'link_post': 'https://konstruksimedia.com/insannul-kamil-kembali-masuk-kabinet-kadin-indonesia-sebagai-wakil-ketua-umum-ini-profil-singkatnya/2/'}],
                    'is_negative_driver': False},
                    {'channel': 'news',
                    'username': 'ngopibareng.id',
                    'viral_score': 0.0,
                    'reach_score': 4.6,
                    'influence_score': 8.335999999999999,
                    'total_post': 1,
                    'total_negative': 1,
                    'total_positive': 0,
                    'total_neutral': 0,
                    'issue': ['Kadin Jatim opposes cargo transport restrictions'],
                    'weighted_influence': 0.021124280672525624,
                    'unified_issues': ['Government Policies & Programs', 'Industry & Business'],
                    'followings': "",
                    'followers': "",
                    'user_category': 'media',
                    'references': [{'channel': 'news',
                        'link_post': 'https://www.ngopibareng.id/read/kadin-jatim-dan-5-asosiasi-kepelabuhanan-tolak-skb-pembatasan-operasional-angkutan-barang'}],
                    'is_negative_driver': True},
                    {'channel': 'news',
                    'username': 'mongabay.co.id',
                    'viral_score': 0.05042016806722689,
                    'reach_score': 6.0,
                    'influence_score': 11.033,
                    'total_post': 1,
                    'total_negative': 1,
                    'total_positive': 0,
                    'total_neutral': 0,
                    'issue': ["Challenges in Indonesia's energy transition"],
                    'weighted_influence': 0.027933608856938276,
                    'unified_issues': ['Energy & Environment', 'Government Policies & Programs'],
                    'followings': "",
                    'followers': "",
                    'user_category': 'media',
                    'references': [{'channel': 'news',
                        'link_post': 'https://www.mongabay.co.id/2025/03/16/jetp-mandeg-bagaimana-strategi-kelanjutan-transisi-energi-di-indonesia/'}],
                    'is_negative_driver': True},
                    {'channel': 'news',
                    'username': 'mediakawasan.co.id',
                    'viral_score': 0.10344827586206896,
                    'reach_score': 2.95,
                    'influence_score': 5.632,
                    'total_post': 1,
                    'total_negative': 0,
                    'total_positive': 1,
                    'total_neutral': 0,
                    'issue': ["Ina Cookies' success story with Shopee"],
                    'weighted_influence': 0.014227847623150493,
                    'unified_issues': ['Industry & Business', 'Digitalization & Technology'],
                    'followings': "",
                    'followers': "",
                    'user_category': 'media',
                    'references': [{'channel': 'news',
                        'link_post': 'https://mediakawasan.co.id/48019/perempuan-hebat-dalam-ramadan-berkah-kisah-manis-kesuksesan-bisnis-lokal-legendaris-ina-cookies-bersama-shopee/'},
                    {'channel': 'news',
                        'link_post': 'https://beritadiy.pikiran-rakyat.com/ekonomi/pr-709147875/cerita-perempuan-hebat-dalam-ramadan-berkah-ina-cookies-sukses-buka-lapangan-kerja-bersama-shopee-di-bandung?page=all'}],
                    'is_negative_driver': False},
                    {'channel': 'news',
                    'username': 'ruangenergi.com',
                    'viral_score': 0.11764705882352941,
                    'reach_score': 3.45,
                    'influence_score': 6.308,
                    'total_post': 1,
                    'total_negative': 0,
                    'total_positive': 1,
                    'total_neutral': 0,
                    'issue': ['Nuclear power investment trend'],
                    'weighted_influence': 0.015982020240002993,
                    'unified_issues': ['Energy & Environment', 'Government Policies & Programs'],
                    'followings': "",
                    'followers': "",
                    'user_category': 'media',
                    'references': [{'channel': 'news',
                        'link_post': 'https://www.ruangenergi.com/tren-investasi-pembangkit-nuklir-dunia-melonjak-kadin-tak-mau-ketinggalan/'}],
                    'is_negative_driver': False},
                    {'channel': 'news',
                    'username': 'legion-news.com',
                    'viral_score': 0.14035087719298245,
                    'reach_score': 2.9,
                    'influence_score': 5.084,
                    'total_post': 1,
                    'total_negative': 0,
                    'total_positive': 0,
                    'total_neutral': 1,
                    'issue': ["Kadin Indonesia's new leadership"],
                    'weighted_influence': 0.012921395243998875,
                    'unified_issues': ['Kadin & Business Leaders',
                    'Government Policies & Programs'],
                    'followings': "",
                    'followers': "",
                    'user_category': 'media',
                    'references': [{'channel': 'linkedin',
                        'link_post': 'https://www.linkedin.com/posts/537b9480-197c-54c2-8807-fc8d02067333'},
                    {'channel': 'news',
                        'link_post': 'https://finance.detik.com/berita-ekonomi-bisnis/d-7824200/daftar-lengkap-pengurus-kadin-ada-keponakan-prabowo'},
                    {'channel': 'tiktok',
                        'link_post': 'https://www.tiktok.com/@tvoneofficial/video/7443422807968550151'},
                    {'channel': 'twitter',
                        'link_post': 'https://x.com/Taufiq_BossKopi/status/1900509348595650614'},
                    {'channel': 'youtube',
                        'link_post': 'https://www.youtube.com/watch?v=XpH2gBM77Y0'}],
                    'is_negative_driver': False}]
    dummy_sosmed = [{'channel': 'instagram',
                    'username': 'anindyabakrie',
                    'viral_score': 0.0,
                    'reach_score': 40.0,
                    'influence_score': 56.15730263865861,
                    'total_post': 1,
                    'total_negative': 0,
                    'total_positive': 1,
                    'total_neutral': 0,
                    'issue': ["KADIN's programs and initiatives"],
                    'weighted_influence': 0.1451691894460895,
                    'unified_issues': ["Kadin Indonesia's Activities and Programs"],
                    'followings': "",
                    'user_image_url': "",
                    'followers': "",
                    'user_category': '-',
                    'references': [{'channel': 'instagram',
                        'link_post': 'https://www.instagram.com/p/DHH38lQT89f/'}],
                    'is_negative_driver': False},
                    {'channel': 'twitter',
                    'username': '@jorgianaaa',
                    'viral_score': 0.0,
                    'reach_score': 80.0,
                    'influence_score': 43.60755305356718,
                    'total_post': 2,
                    'total_negative': 2,
                    'total_positive': 0,
                    'total_neutral': 0,
                    'issue': ['Unsolved murder case of Lambang Babar Purnomo'],
                    'weighted_influence': 0.12848774020485762,
                    'unified_issues': ['Crime and Justice'],
                    'followings': 0,
                    'user_image_url': '',
                    'followers': 0,
                    'user_category': 'Human',
                    'references': [{'channel': 'twitter',
                        'link_post': 'https://x.com/jorgianaaa/status/1891484683654885535'},
                    {'channel': 'twitter',
                        'link_post': 'https://x.com/jorgianaaa/status/1891482956646011379'}],
                    'is_negative_driver': True},
                    {'channel': 'instagram',
                    'username': 'donnymayawardhana',
                    'viral_score': 0.0,
                    'reach_score': 40.0,
                    'influence_score': 43.27686121585661,
                    'total_post': 1,
                    'total_negative': 0,
                    'total_positive': 1,
                    'total_neutral': 0,
                    'issue': ['Growth and challenges of postal industry'],
                    'weighted_influence': 0.11478536854530878,
                    'unified_issues': ['Business and Industry Analysis'],
                    'followings': "",
                    'user_image_url': "",
                    'followers': "",
                    'user_category': '-',
                    'references': [{'channel': 'instagram',
                        'link_post': 'https://www.instagram.com/p/DHNVWSahIpG/'}],
                    'is_negative_driver': False},
                    {'channel': 'twitter',
                    'username': '@AnKiiim_',
                    'viral_score': 0.0,
                    'reach_score': 40.0,
                    'influence_score': 41.87093748755223,
                    'total_post': 1,
                    'total_negative': 1,
                    'total_positive': 0,
                    'total_neutral': 0,
                    'issue': ["Allegations of Hashim Djojohadikusumo's involvement in Lambang Babar Purnomo's death"],
                    'weighted_influence': 0.1114689189486873,
                    'unified_issues': ['Crime and Justice'],
                    'followings': 0,
                    'user_image_url': None,
                    'followers': 8649,
                    'user_category': 'influencer',
                    'references': [{'channel': 'twitter',
                        'link_post': 'https://x.com/AnKiiim_/status/1891681328086577321'}],
                    'is_negative_driver': True},
                    {'channel': 'instagram',
                    'username': 'henansekuritas',
                    'viral_score': 0.0,
                    'reach_score': 40.0,
                    'influence_score': 42.25833130589691,
                    'total_post': 1,
                    'total_negative': 0,
                    'total_positive': 0,
                    'total_neutral': 1,
                    'issue': ["Danantara's impact on FDI"],
                    'weighted_influence': 0.11238274666687538,
                    'unified_issues': ['Business and Investment Analysis'],
                    'followings': "",
                    'user_image_url': "",
                    'followers': "",
                    'user_category': '-',
                    'references': [{'channel': 'instagram',
                        'link_post': 'https://www.instagram.com/p/DGz2hMpJSgd/'}],
                    'is_negative_driver': False},
                    {'channel': 'instagram',
                    'username': 'waoderabia',
                    'viral_score': 0.0,
                    'reach_score': 40.0,
                    'influence_score': 45.8429149156003,
                    'total_post': 1,
                    'total_negative': 0,
                    'total_positive': 1,
                    'total_neutral': 0,
                    'issue': ['Kadin Indonesia leadership inauguration'],
                    'weighted_influence': 0.12083846202832989,
                    'unified_issues': ["Kadin Indonesia's Leadership and Structure"],
                    'followings': "",
                    'user_image_url': "",
                    'followers': "",
                    'user_category': '-',
                    'references': [{'channel': 'instagram',
                        'link_post': 'https://www.instagram.com/p/DHLIFgGBrGf/'},
                    {'channel': 'news',
                        'link_post': 'https://www.antaranews.com/berita/4711069/kadin-indonesia-kukuhkan-jajaran-pengurus-masa-bakti-2024-2029'},
                    {'channel': 'tiktok',
                        'link_post': 'https://www.tiktok.com/@anindya.bakrie/video/7481808846810385671'},
                    {'channel': 'youtube',
                        'link_post': 'https://www.youtube.com/watch?v=NXKQnIvT7MY'}],
                    'is_negative_driver': False},
                    {'channel': 'twitter',
                    'username': '@ainurohman',
                    'viral_score': 0.0,
                    'reach_score': 40.0,
                    'influence_score': 71.8123057134211,
                    'total_post': 1,
                    'total_negative': 1,
                    'total_positive': 0,
                    'total_neutral': 0,
                    'issue': ["Jonatan Christie's loss at All England 2025"],
                    'weighted_influence': 0.182097955628179,
                    'unified_issues': ['Sports and Entertainment'],
                    'followings': "",
                    'user_image_url': "",
                    'followers': "",
                    'user_category': '-',
                    'references': [{'channel': 'news',
                        'link_post': 'https://www.liputan6.com/bola/read/5960157/jonatan-christie-ungkap-penyebab-tersingkir-di-babak-16-besar-all-england-2025'},
                    {'channel': 'news',
                        'link_post': 'https://sport.detik.com/raket/d-7822518/jonatan-kalah-salahkan-kondisi-lapangan'},
                    {'channel': 'twitter',
                        'link_post': 'https://x.com/ainurohman/status/1900189018551414806'}],
                    'is_negative_driver': True},
                    {'channel': 'twitter',
                    'username': '@RajaJuliAntoni',
                    'viral_score': 0.0,
                    'reach_score': 40.0,
                    'influence_score': 46.36228513032033,
                    'total_post': 1,
                    'total_negative': 0,
                    'total_positive': 1,
                    'total_neutral': 0,
                    'issue': ['Carbon trading for forest conservation'],
                    'weighted_influence': 0.12206361038056861,
                    'unified_issues': ['Environmental Sustainability and Conservation'],
                    'followings': "",
                    'user_image_url': "",
                    'followers': "",
                    'user_category': '-',
                    'references': [{'channel': 'twitter',
                        'link_post': 'https://x.com/RajaJuliAntoni/status/1899456925370351805'},
                    {'channel': 'tiktok',
                        'link_post': 'https://www.tiktok.com/@rajajuliantoni/video/7480547407252524293'}],
                    'is_negative_driver': False},
                    {'channel': 'twitter',
                    'username': '@Franken_blues',
                    'viral_score': 0.0,
                    'reach_score': 40.0,
                    'influence_score': 46.70853194013368,
                    'total_post': 1,
                    'total_negative': 0,
                    'total_positive': 1,
                    'total_neutral': 0,
                    'issue': ["Gibran and Anindya's manners"],
                    'weighted_influence': 0.12288037594872776,
                    'unified_issues': ['Personal and Social Commentary'],
                    'followings': "",
                    'user_image_url': "",
                    'followers': "",
                    'user_category': '-',
                    'references': [{'channel': 'twitter',
                        'link_post': 'https://x.com/Franken_blues/status/1900825637939802615'}],
                    'is_negative_driver': False},
                    {'channel': 'twitter',
                    'username': '@KemenperinRI',
                    'viral_score': 0.0,
                    'reach_score': 40.0,
                    'influence_score': 43.85848994361519,
                    'total_post': 1,
                    'total_negative': 0,
                    'total_positive': 1,
                    'total_neutral': 0,
                    'issue': ['Hope for Hashim Djojohadikusumo'],
                    'weighted_influence': 0.11615737921752695,
                    'unified_issues': ['Political and Business Support'],
                    'followings': "",
                    'user_image_url': "",
                    'followers': "",
                    'user_category': '-',
                    'references': [{'channel': 'twitter',
                        'link_post': 'https://x.com/KemenperinRI/status/1900404928700350594'}],
                    'is_negative_driver': False}]          
    return {"kol_sosmed":dummy_sosmed, "kol_news":dummy_news}
