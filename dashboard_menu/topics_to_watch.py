from utils.functions import About_BQ
import os
BQ = About_BQ(project_id="inlaid-sentinel-444404-f8", credentials_loc='./utils/inlaid-sentinel-444404-f8-be06a73c1031.json')

def topics_to_watch(
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
    
    dummy_data = [{'unified_issue': 'Kadin Indonesia Leadership and Inauguration',
        'description': "The posts discuss the recent inauguration of the new leadership of Kadin Indonesia, led by Anindya Novyan Bakrie. The new leadership aims to strengthen the role of Kadin in advancing the Indonesian economy and business world. The inauguration was attended by various key figures, including the Vice President, ministers, and Kadin officials. The new leadership emphasizes collaboration and synergy to drive economic growth and achieve Indonesia's economic goals.",
        'total_issue': 61,
        'total_viral_score': 68.27721966063578,
        'total_reach_score': 556.2225556782806,
        'percentage_negative': 4.92,
        'percentage_positive': 75.41,
        'percentage_neutral': 19.67,
        'list_issue': ['Kadin Indonesia new leadership',
        "Kadin Indonesia's new leadership",
        'Kadin Indonesia leadership inauguration',
        'Kadin Indonesia leadership change',
        'Kadin Indonesia leadership inauguration',
        "Kadin Indonesia's appreciation for Kadin Jakarta's efforts",
        "Kadin Indonesia's appreciation for Kadin Jakarta",
        "Kadin Indonesia's role in economic growth",
        "Kadin Indonesia's Ramadan gathering",
        "Kadin's leadership under Anindya Bakrie",
        "Kadin Indonesia's role in economic growth",
        "KADIN's role in economic growth"],
        'share_of_voice': 0.30198019801980197,
        'references': [{'channel': 'instagram',
            'link_post': 'https://www.instagram.com/p/DHLIFgGBrGf/'},
        {'channel': 'linkedin',
            'link_post': 'https://www.linkedin.com/posts/537b9480-197c-54c2-8807-fc8d02067333'},
        {'channel': 'news',
            'link_post': 'https://finance.detik.com/berita-ekonomi-bisnis/d-7824200/daftar-lengkap-pengurus-kadin-ada-keponakan-prabowo'},
        {'channel': 'tiktok',
            'link_post': 'https://www.tiktok.com/@anindya.bakrie/video/7481808846810385671'},
        {'channel': 'twitter',
            'link_post': 'https://x.com/Taufiq_BossKopi/status/1900509348595650614'},
        {'channel': 'youtube',
            'link_post': 'https://www.youtube.com/shorts/Yn4Q4Zi2njo'}]},
        {'unified_issue': "Hashim Djojohadikusumo's Meeting with Jokowi",
        'description': "The posts discuss the meeting between Hashim Djojohadikusumo, the brother of President Prabowo Subianto, and former President Joko Widodo in Solo. The meeting, which lasted for two hours, focused on exchanging ideas about the nation and state, particularly regarding economic conditions and the need for injections from various angles to achieve the government's economic growth targets. Both parties emphasized that the meeting was not politically motivated.",
        'total_issue': 37,
        'total_viral_score': 13.800706463327586,
        'total_reach_score': 195.80661146817695,
        'percentage_negative': 5.41,
        'percentage_positive': 0.0,
        'percentage_neutral': 94.59,
        'list_issue': ["Hashim Djojohadikusumo's meeting with Jokowi",
        'Hashim Djojohadikusumo meeting with Jokowi',
        'Jokowi-Hashim Djojohadikusumo meeting',
        "Jokowi's meeting with Hashim Djojohadikusumo",
        'Hashim Djojohadikusumo meets Jokowi'],
        'share_of_voice': 0.18316831683168316,
        'references': [{'channel': 'news',
            'link_post': 'https://kabar24.bisnis.com/read/20250309/15/1859548/pengamat-soroti-urgensi-pertemuan-hashim-djojohadikusumo-dan-jokowi'},
        {'channel': 'tiktok',
            'link_post': 'https://www.tiktok.com/@idntimes/video/7478958465659014408'},
        {'channel': 'twitter',
            'link_post': 'https://x.com/kompascom/status/1897873256654463156'},
        {'channel': 'youtube',
            'link_post': 'https://www.youtube.com/watch?v=m1VTMsNQKgo'}]},
        {'unified_issue': "Danantara's Role in Economic Growth",
        'description': "The posts highlight the importance of Danantara, a new investment management agency, in driving economic growth in Indonesia. Kadin Indonesia and Kadin NTT express their full support for Danantara, emphasizing its potential to attract investment, create jobs, empower local businesses, and strengthen the national economy. The agency is seen as a key player in achieving Indonesia's economic goals and contributing to the country's overall prosperity.",
        'total_issue': 49,
        'total_viral_score': 15.488781431241652,
        'total_reach_score': 82.39000219347646,
        'percentage_negative': 0.0,
        'percentage_positive': 100.0,
        'percentage_neutral': 0.0,
        'list_issue': ["Danantara's role in economic growth",
        "Danantara's role in strengthening national economy",
        'Kadin supports Danantara for economic growth',
        "Kadin NTT's support for Danantara",
        'Kadin NTT supports Danantara Indonesia',
        "Kadin NTT's support for Danantara as an economic driver",
        "Danantara's role in economic progress",
        "Kadin's support for MBG program"],
        'share_of_voice': 0.24257425742574257,
        'references': [{'channel': 'linkedin',
            'link_post': 'https://www.linkedin.com/posts/b5dc3853-c912-5960-af55-5105323e32c1'},
        {'channel': 'news',
            'link_post': 'https://www.viva.co.id/bisnis/1806948-anindya-bakrie-targetkan-kadin-bangun-100-dapur-mbg-sebelum-17-agustus-2025'},
        {'channel': 'tiktok',
            'link_post': 'https://www.tiktok.com/@anindya.bakrie/video/7481488103409011986'},
        {'channel': 'twitter',
            'link_post': 'https://x.com/tvOneNews/status/1893930384251134018'},
        {'channel': 'youtube',
            'link_post': 'https://www.youtube.com/shorts/rHfHhUYSlI8'}]},
        {'unified_issue': "Kadin DKI Jakarta's Contributions and Recognition",
        'description': 'The posts highlight the significant contributions of Kadin DKI Jakarta, led by Diana Dewi, to the national economy. The organization has received recognition from both the Governor of DKI Jakarta and the Chairman of Kadin Indonesia for its efforts in driving economic growth and development in the region. Kadin DKI Jakarta is seen as a barometer of the national economy and plays a crucial role in shaping the economic landscape of Indonesia.',
        'total_issue': 16,
        'total_viral_score': 0.0,
        'total_reach_score': 0.0,
        'percentage_negative': 0.0,
        'percentage_positive': 100.0,
        'percentage_neutral': 0.0,
        'list_issue': ["Kadin DKI Jakarta's contributions and recognition",
        "Kadin Jakarta's role in national economy",
        "Appreciation for KADIN Jakarta's efforts",
        "Appreciation for KADIN Jakarta's efforts"],
        'share_of_voice': 0.07920792079207921,
        'references': [{'channel': 'twitter',
            'link_post': 'https://x.com/QobilKabil/status/1899340660773355779'},
        {'channel': 'twitter',
            'link_post': 'https://x.com/Incesku175121/status/1899339340024799707'},
        {'channel': 'twitter',
            'link_post': 'https://x.com/Sabrina20421200/status/1899341290900496745'}]},
        {'unified_issue': "Prabowo's Meeting with Business Leaders",
        'description': "The posts discuss President Prabowo Subianto's meeting with prominent business leaders in Jakarta. The meeting focused on strategic discussions about national economic development and key government programs, including the Makan Bergizi Gratis program, infrastructure development, textile industry strengthening, food and energy self-sufficiency, and investment management through Danantara. The President expressed appreciation for the business leaders' support in implementing government policies aimed at improving public welfare.",
        'total_issue': 5,
        'total_viral_score': 0.4150472521313706,
        'total_reach_score': 90.05888888888887,
        'percentage_negative': 0.0,
        'percentage_positive': 60.0,
        'percentage_neutral': 40.0,
        'list_issue': ["Prabowo's meeting with business leaders"],
        'share_of_voice': 0.024752475247524754,
        'references': [{'channel': 'instagram',
            'link_post': 'https://www.instagram.com/p/DG3Lxa1zMaz/'},
        {'channel': 'news',
            'link_post': 'https://www.liputan6.com/news/read/5952719/4-fakta-presiden-prabowo-kumpulkan-pengusaha-di-istana-ajak-atasi-kemiskinan-dan-buka-lapangan-pekerjaan'},
        {'channel': 'tiktok',
            'link_post': 'https://www.tiktok.com/@prabowopresidengemoy/video/7478813708181884165'},
        {'channel': 'twitter',
            'link_post': 'https://x.com/mediaindonesia/status/1899769615359873280'},
        {'channel': 'youtube',
            'link_post': 'https://www.youtube.com/watch?v=HQ4IT1ZmDPo'}]},
        {'unified_issue': 'Koperasi Desa Merah Putih Program',
        'description': "The posts discuss the government's initiative to establish Koperasi Desa Merah Putih (Red and White Village Cooperatives) across Indonesia. The program aims to empower rural communities, improve their economic well-being, and address economic challenges in rural areas. The cooperatives are expected to provide access to fair financing, protect farmers and villagers from unfair economic practices, and contribute to the overall economic growth of the country.",
        'total_issue': 4,
        'total_viral_score': 1.4677146720757268,
        'total_reach_score': 6.699999999999999,
        'percentage_negative': 0.0,
        'percentage_positive': 100.0,
        'percentage_neutral': 0.0,
        'list_issue': ['Koperasi Desa Merah Putih program'],
        'share_of_voice': 0.019801980198019802,
        'references': [{'channel': 'news',
            'link_post': 'https://www.antaranews.com/berita/4699065/khofifah-koperasi-desa-merah-putih-dorong-pertumbuhan-ekonomi'},
        {'channel': 'news',
            'link_post': 'https://www.antaranews.com/berita/4700381/berharap-sejahtera-dari-koperasi-desa-merah-putih'},
        {'channel': 'news',
            'link_post': 'https://channel9.id/video-ket-pers-koperasi-desa-merah-putih-untuk-memutus-renternir/'},
        {'channel': 'tiktok',
            'link_post': 'https://www.tiktok.com/@kumparan/video/7480038852279815479'}]},
        {'unified_issue': "Anindya Bakrie's Leadership in Aquatics",
        'description': "The posts highlight Anindya Bakrie's re-election as the Chairman of the Indonesian Aquatics Association (PB AI) for a third term. Anindya's leadership is characterized by a focus on achieving international success in aquatics, particularly in the Youth Olympic Games 2026 and the Olympic Games 2028. He also emphasizes the importance of promoting swimming as a healthy lifestyle for the Indonesian people and ensuring the well-being of athletes and coaches.",
        'total_issue': 15,
        'total_viral_score': 1.1080748605748607,
        'total_reach_score': 39.065666666666665,
        'percentage_negative': 0.0,
        'percentage_positive': 60.0,
        'percentage_neutral': 40.0,
        'list_issue': ['Anindya Bakrie re-elected as PB AI chairman',
        "Anindya Bakrie's leadership of Akuatik Indonesia",
        "Anindya Bakrie's leadership in Aquatics"],
        'share_of_voice': 0.07425742574257425,
        'references': [{'channel': 'news',
            'link_post': 'https://www.kompas.id/artikel/kembali-pimpin-akuatik-indonesia-anindya-bakrie-dinanti-sejumlah-pekerjaan-rumah'},
        {'channel': 'news',
            'link_post': 'https://sports.okezone.com/read/2025/03/15/43/3122790/resmi-anindya-bakrie-terpilih-jadi-ketum-pb-akuatik-indonesia'},
        {'channel': 'tiktok',
            'link_post': 'https://www.tiktok.com/@rekankadin/photo/7482697218240171272'},
        {'channel': 'tiktok',
            'link_post': 'https://www.tiktok.com/@belakanglayar777/photo/7481198965287898375'},
        {'channel': 'twitter',
            'link_post': 'https://x.com/Dibaliklayar45/status/1900456322250006764'},
        {'channel': 'twitter',
            'link_post': 'https://x.com/detiksport/status/1897696534504128697'}]},
        {'unified_issue': "Government's Palm Oil Downstreaming Strategy",
        'description': "The posts discuss the government's strategy to promote palm oil downstreaming in Indonesia. The strategy involves four key stages: strengthening the industrialization ecosystem, increasing production capacity for domestic needs, enhancing industry competitiveness for global expansion, and achieving net exports. The government believes that palm oil downstreaming will contribute to sustainable and high economic growth in Indonesia.",
        'total_issue': 4,
        'total_viral_score': 0.8275862068965517,
        'total_reach_score': 6.0,
        'percentage_negative': 0.0,
        'percentage_positive': 100.0,
        'percentage_neutral': 0.0,
        'list_issue': ["Government's palm oil downstreaming strategy"],
        'share_of_voice': 0.019801980198019802,
        'references': [{'channel': 'news',
            'link_post': 'https://www.antaranews.com/berita/4703273/pemerintah-mendorong-hilirisasi-sawit-melalui-empat-tahapan'},
        {'channel': 'news',
            'link_post': 'https://www.antaranews.com/berita/4703273/pemerintah-mendorong-hilirisasi-sawit-melalui-empat-tahapan'},
        {'channel': 'news',
            'link_post': 'https://www.antaranews.com/berita/4703273/pemerintah-mendorong-hilirisasi-sawit-melalui-empat-tahapan'}]},
        {'unified_issue': "ANTARA Papua Barat's Partnership with YPMAK",
        'description': "The posts discuss the partnership between ANTARA Papua Barat, a national news agency, and YPMAK, a non-profit organization managing partnership funds from PT Freeport Indonesia. The partnership aims to disseminate information about YPMAK's activities in education, health, and economic empowerment of the Amungme and Kamoro tribes in Mimika, Papua Tengah, to a wider audience in Papua and nationally. The collaboration is expected to contribute to the improvement of the quality of life and well-being of the local community.",
        'total_issue': 4,
        'total_viral_score': 1.3061224489795917,
        'total_reach_score': 10.4,
        'percentage_negative': 0.0,
        'percentage_positive': 100.0,
        'percentage_neutral': 0.0,
        'list_issue': ["ANTARA Papua Barat's partnership with YPMAK"],
        'share_of_voice': 0.019801980198019802,
        'references': [{'channel': 'news',
            'link_post': 'https://papuabarat.antaranews.com/berita/59409/antara-papua-barat-teken-kerja-sama-dengan-ypmak'},
        {'channel': 'news',
            'link_post': 'https://papuabarat.antaranews.com/berita/59409/antara-papua-barat-teken-kerja-sama-dengan-ypmak'},
        {'channel': 'news',
            'link_post': 'https://papuabarat.antaranews.com/berita/59409/antara-papua-barat-teken-kerja-sama-dengan-ypmak'}]},
        {'unified_issue': 'Indonesia Green Energy Investment Dialogue 2025',
        'description': "The posts highlight the Indonesia Green Energy Investment Dialogue 2025, a high-level forum organized by Kadin Indonesia and Katadata Green. The forum aims to facilitate discussions, strategic collaborations, and knowledge sharing on building a robust green energy ecosystem in Indonesia. The event emphasizes Indonesia's commitment to achieving energy resilience through clean and renewable energy sources.",
        'total_issue': 4,
        'total_viral_score': 0.0,
        'total_reach_score': 0.1013793103448276,
        'percentage_negative': 0.0,
        'percentage_positive': 75.0,
        'percentage_neutral': 25.0,
        'list_issue': ['Indonesia Green Energy Investment Dialogue 2025'],
        'share_of_voice': 0.019801980198019802,
        'references': [{'channel': 'twitter',
            'link_post': 'https://x.com/KATADATAcoid/status/1891296359741042787'},
        {'channel': 'twitter',
            'link_post': 'https://x.com/Katadatagreen/status/1891320086285173037'},
        {'channel': 'twitter',
            'link_post': 'https://x.com/KATADATAcoid/status/1899657135698677822'}]},
        {'unified_issue': 'Allegations Against Bakrie Group',
        'description': "The posts express criticism and allegations against the Bakrie Group, highlighting concerns about the company's business practices and potential negative impacts on individuals and the environment. The posts raise questions about the company's financial transparency, its handling of past controversies, and its overall impact on society.",
        'total_issue': 3,
        'total_viral_score': 5.918715789366541,
        'total_reach_score': 0.7844460227272727,
        'percentage_negative': 100.0,
        'percentage_positive': 0.0,
        'percentage_neutral': 0.0,
        'list_issue': ['Allegations against Bakrie Group'],
        'share_of_voice': 0.01485148514851485,
        'references': [{'channel': 'tiktok',
            'link_post': 'https://www.tiktok.com/@merahhhmenyalaaa/video/7471333142431911174'},
        {'channel': 'tiktok',
            'link_post': 'https://www.tiktok.com/@merahhhmenyalaaa/video/7475392056429481221'},
        {'channel': 'tiktok',
            'link_post': 'https://www.tiktok.com/@merahhhmenyalaaa/video/7475152512027348229'}]}]
            
    # data = BQ.to_pull_data(query)
    
    return dummy_data
