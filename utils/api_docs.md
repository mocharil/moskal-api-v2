# Dokumentasi Fungsi Utilitas Moskal AI

Dokumentasi ini menjelaskan input dan output untuk setiap fungsi utilitas.


INPUT PARAMETER : `CommonParams`

| Nama Parameter | Tipe Data | Default | Deskripsi | Contoh |
|---|---|---|---|---|
| `keywords` | list of str (optional) | `None` | Keywords to search for | `['prabowo', 'gibran']` |
| `search_keyword` | list of str (optional) | `None` | Search for exact phrases in addition to keywords | `['prabowo gibran']` |
| `search_exact_phrases` | bool | `False` | Whether to search for exact phrases | `False` |
| `case_sensitive` | bool | `False` | Whether the search is case sensitive | `False` |
| `sentiment` | list of str (optional) | `None` | Sentiment filters | `['positive', 'negative', 'neutral']` |
| `start_date` | str (optional) | `None` | Start date for filtering | `None` |
| `end_date` | str (optional) | `None` | End date for filtering | `None` |
| `date_filter` | str | `last 30 days` | Date filter preset | `last 30 days` |
| `custom_start_date` | str (optional) | `None` | Custom start date (if date_filter is 'custom') | `2025-04-01` |
| `custom_end_date` | str (optional) | `None` | Custom end date (if date_filter is 'custom') | `2025-04-20` |
| `channels` | list of str (optional) | `None` | Channel filters | `['tiktok', 'instagram', 'news', 'reddit', 'facebook', 'twitter', 'linkedin', 'youtube']` |
| `importance` | str | `all mentions` | Importance filter | `important mentions` |
| `influence_score_min` | float (optional) | `None` | Minimum influence score | `0` |
| `influence_score_max` | float (optional) | `None` | Maximum influence score | `100` |
| `region` | list of str (optional) | `None` | Region filters | `['bandung', 'jakarta']` |
| `language` | list of str (optional) | `None` | Language filters | `['indonesia', 'english']` |
| `domain` | list of str (optional) | `None` | Domain filters | `['kumparan.com', 'detik.com']` |

## Fungsi: `utils.analysis_sentiment_mentions.handler_function`

- **Deskripsi:** Analisis sentimen dan breakdown mention.

### Input (Parameters)
Menerima dictionary `params` yang mengikuti struktur `CommonParams`.

### Output (Return Value)
{
  "mentions_by_category": {
    "total": 23753,
    "categories": [
      {
        "name": "news",
        "count": 17473,
        "percentage": 73.56
      },
      {
        "name": "tiktok",
        "count": 6005,
        "percentage": 25.28
      },
      {
        "name": "youtube",
        "count": 164,
        "percentage": 0.69
      },
      {
        "name": "instagram",
        "count": 103,
        "percentage": 0.43
      },
      {
        "name": "twitter",
        "count": 8,
        "percentage": 0.03
      }
    ]
  },
  "sentiment_by_category": {
    "categories": [
      {
        "name": "news",
        "total": 17473,
        "positive": 3102,
        "neutral": 11321,
        "negative": 3028
      },
      {
        "name": "tiktok",
        "total": 6005,
        "positive": 1508,
        "neutral": 3782,
        "negative": 694
      },
      {
        "name": "youtube",
        "total": 164,
        "positive": 14,
        "neutral": 140,
        "negative": 10
      },
      {
        "name": "instagram",
        "total": 103,
        "positive": 23,
        "neutral": 79,
        "negative": 1
      },
      {
        "name": "twitter",
        "total": 8,
        "positive": 3,
        "neutral": 3,
        "negative": 2
      }
    ],
    "sentiments": [
      "positive",
      "neutral",
      "negative"
    ]
  },
  "sentiment_breakdown": {
    "positive": 4650,
    "negative": 3735,
    "neutral": 15325
  }
}

## Fungsi: `utils.analysis_overview.handler_function`

- **Deskripsi:** Gambaran umum analisis.

### Input (Parameters)
Menerima dictionary `params` yang mengikuti struktur `CommonParams`.

### Output (Return Value)
{
  "total_mentions": {
    "value": 23753,
    "display": "23.8K",
    "growth_value": 3774,
    "growth_display": "+3.8K",
    "growth_percentage": 18.889834326042347,
    "growth_percentage_display": "+19%"
  },
  "total_reach": {
    "value": 166798.69592063263,
    "display": "166.8K",
    "growth_value": 55850.74302923336,
    "growth_display": "+55.9K",
    "growth_percentage": 50.33958858520131,
    "growth_percentage_display": "+50%"
  },
  "positive_mentions": {
    "value": 4650,
    "display": "4.7K",
    "growth_value": -2343,
    "growth_display": "-2343",
    "growth_percentage": -33.504933504933504,
    "growth_percentage_display": "-34%"
  },
  "negative_mentions": {
    "value": 3735,
    "display": "3.7K",
    "growth_value": -815,
    "growth_display": "-815",
    "growth_percentage": -17.912087912087912,
    "growth_percentage_display": "-18%"
  },
  "neutral_mentions": {
    "value": 15325,
    "display": "15.3K"
  },
  "presence_score": {
    "value": 12.707423453030925,
    "display": "12",
    "growth_value": 3.3238280187271982,
    "growth_display": "+3",
    "growth_percentage": 35.42168928741577,
    "growth_percentage_display": "+35%"
  },
  "social_media_reach": {
    "value": 135827.8959391354,
    "display": "135.8K",
    "growth_value": 50430.69304129138,
    "growth_display": "+50.4K",
    "growth_percentage": 59.054267973646446,
    "growth_percentage_display": "+59%"
  },
  "social_media_mentions": {
    "value": 6280,
    "display": "6.3K",
    "growth_value": -191,
    "growth_display": "-191",
    "growth_percentage": -2.951630350795859,
    "growth_percentage_display": "-3%"
  },
  "social_media_reactions": {
    "value": 12468327,
    "display": "12.5M",
    "growth_value": -12746746,
    "growth_display": "-12746746",
    "growth_percentage": -50.552088427425936,
    "growth_percentage_display": "-51%"
  },
  "social_media_comments": {
    "value": 1079125,
    "display": "1.1M",
    "growth_value": -366,
    "growth_display": "-366",
    "growth_percentage": -0.03390486812766387,
    "growth_percentage_display": "-0%"
  },
  "social_media_shares": {
    "value": 810668,
    "display": "810.7K",
    "growth_value": -720332,
    "growth_display": "-720332",
    "growth_percentage": -47.04977139124755,
    "growth_percentage_display": "-47%"
  },
  "non_social_media_reach": {
    "value": 30970.799981497228,
    "display": "31.0K",
    "growth_value": 5420.04998794198,
    "growth_display": "+5.4K",
    "growth_percentage": 21.212880206291786,
    "growth_percentage_display": "+21%"
  },
  "non_social_media_mentions": {
    "value": 17473,
    "display": "17.5K",
    "growth_value": 3965,
    "growth_display": "+4.0K",
    "growth_percentage": 29.352976014213798,
    "growth_percentage_display": "+29%"
  },
  "total_social_media_interactions": {
    "value": 15080401,
    "display": "15.1M",
    "growth_value": -13755079,
    "growth_display": "-13755079",
    "growth_percentage": -47.70192485091284,
    "growth_percentage_display": "-48%"
  },
  "channels": {
    "news": {
      "mentions": 17473,
      "reach": 30970.799981497228
    },
    "tiktok": {
      "mentions": 6005,
      "reach": 130774.85312452912
    },
    "youtube": {
      "mentions": 164,
      "reach": 4417.635257170943
    },
    "instagram": {
      "mentions": 103,
      "reach": 635.0075574219227
    },
    "twitter": {
      "mentions": 8,
      "reach": 0.4000000134110451
    }
  },
  "period": {
    "current": {
      "start_date": "2025-04-19",
      "end_date": "2025-05-19"
    },
    "previous": {
      "start_date": "2025-03-19",
      "end_date": "2025-04-18"
    }
  }
}



## Fungsi: `utils.list_of_mentions.handler_function`

- **Deskripsi:** Mendapatkan list mentions.

### Input (Parameters)
Menerima dictionary `params` yang mengikuti struktur `CommonParams` dengan tambahan parameter berikut:
| Nama Parameter | Tipe Data | Default | Deskripsi | Contoh |
|---|---|---|---|---|
| `sort_type` | str | `recent` | Sort type: 'popular', 'recent', or 'relevant' | `recent` |
| `sort_order` | str | `desc` | Sort order: 'desc' or 'asc' | `desc` |

### Output (Return Value)

{
  "data": [
    {
      "pinned": "",
      "channel": "tiktok",
      "thumbnail_url": "None",
      "updated_at": "2025-05-19 01:07:51",
      "post_type": "video",
      "worker": 8,
      "user_last_data_updated": "2025-05-17 06:58:36",
      "like_count": 18,
      "user_influence_score": 0.03145090846681922,
      "user_likes": 5139,
      "post_caption": "#bismillahrame  #fyp  #fyppppppppppppppppppppppp  #xybca  #PINJAMAN  #pinjamanonline  #PINJAMANDANA  #koperasi  #pinjamanuang  #pinjamancepat  #foryou  #semuabisaditiktok  #DANA  #danatunai  #pinjamantanpajaminan  #plecit  #plecithitz  #pinjamankoperasi  #koperasimingguan  #koperasiharian  #pinjamanamanah  #ojk  #syariah  #mekar  #makmur  #pegawai  #fypage  #fypシ゚viral🖤tiktok  #fypシ゚  created by hana hanifah with 𝙂𝘼𝙉𝘿𝙀𝙉 𝘼𝙐𝘿𝙄𝙊’s suara asli - 𝙂𝘼𝙉𝘿𝙀𝙉 𝘼𝙐𝘿𝙄𝙊",
      "link_post": "https://www.tiktok.com/@hana.hanifah378/video/7505264428917591351",
      "user_link_user": "https://www.tiktok.com/@hana.hanifah378",
      "post_created_at": "2025-05-18",
      "user_engagement_rate": 143.72997711670482,
      "user_viral_potential": 0.028745995423340966,
      "user_followers": 3933,
      "user_link_attached": null,
      "user_category": "Spam",
      "user_bio": "Open Dana Pinjaman",
      "name": "hana hanifah",
      "user_followings": 41,
      "username": "@hana.hanifah378",
      "profile_link": "https://www.tiktok.com/@hana.hanifah378",
      "sentiment": "positive",
      "emotions": "hope",
      "issue": "Not Specified",
      "topic": "Not Specified",
      "language": "Indonesian",
      "region": "Not Specified",
      "intent": "hoping",
      "favorites": 4,
      "user_image_url": "https://p16-sign-sg.tiktokcdn.com/tos-alisg-avt-0068/6ef88a932411d395fa73873fd9563fa4~tplv-tiktokx-cropcenter:100:100.jpeg?dr=14579&refresh_token=ef310e4a&x-expires=1747774800&x-signature=mI5LPFeLDlvFqNRXzBBGpqo%2BOI4%3D&t=4d5b0474&ps=13740610&shp=a5d48078&shcp=81f88b70&idc=my",
      "comments": 20,
      "viral_score": 2.3157894736842106,
      "influence_score": 2.26,
      "engagement_rate": 226.3157894736842,
      "shares": 1,
      "post_id": "7505264428917591351",
      "post_music": "suara asli  - 𝙂𝘼𝙉𝘿𝙀𝙉 𝘼𝙐𝘿𝙄𝙊",
      "reach_score": 14.5,
      "likes": 18
    }
  ]
}

## Fungsi: `utils.presence_score.presence_score_analysis`

- **Deskripsi:** Analisis skor kehadiran.

### Input (Parameters)
Menerima dictionary `params` yang mengikuti struktur `CommonParams` dengan tambahan parameter berikut:
- `interval`: "day", "week", "month"
- `compare_with_topics`: true/false
- `num_topics_to_compare`: jumlah topik untuk dibandingkan

### Output (Return Value)

{
  "current_presence_score": 12.709951659824668,
  "presence_over_time": [
    {
      "date": "2025-04-14",
      "score": 6.975342115146164
    },
    {
      "date": "2025-04-21",
      "score": 13.862159240492083
    }
  ]
}
---
## Fungsi: `utils.share_of_voice.handler_function`

- **Deskripsi:** Analisis porsi suara yang paling banyak.

### Input (Parameters)
Menerima dictionary `params` yang mengikuti struktur `CommonParams` dan parameter spesifik dari `ShareOfVoiceRequest` (detail parameter `ShareOfVoiceRequest` perlu dijabarkan jika berbeda dari `CommonParams` atau jika ada tambahan).

### Output (Return Value)

{
  "data": [
    {
      "channel": "news",
      "username": "kompas.tv",
      "total_mentions": 1021,
      "total_reach": 267.5499999523163,
      "percentage_share_of_voice": 4.3,
      "user_image_url": "https://logo.clearbit.com/kompas.tv"
    },
    {
      "channel": "news",
      "username": "antaranews.com",
      "total_mentions": 872,
      "total_reach": 1655.8499972224236,
      "percentage_share_of_voice": 3.67,
      "user_image_url": "https://logo.clearbit.com/antaranews.com"
    }
  ]
}
---
## Fungsi: `utils.most_followers.handler_function`

- **Deskripsi:** Analisis pengikut terbanyak.

### Input (Parameters)
Menerima dictionary `params` yang mengikuti struktur `CommonParams`.

### Output (Return Value)
{
  "data": [
    {
      "channel": "youtube",
      "username": "@kompastv",
      "followers": 18700000,
      "influence_score": 0,
      "total_mentions": 12,
      "total_reach": 260.04699028097093,
      "user_image_url": "https://i.ytimg.com/an/5BMIWZe9isJXLZZWPWvBlg/featured_channel.jpg?v=66da9c94""
    },
    {
      "channel": "youtube",
      "username": "@tvOneNews",
      "followers": 15300000,
      "influence_score": 0,
      "total_mentions": 16,
      "total_reach": 328.3223146945238,
      "user_image_url": "https://i.ytimg.com/vi/8BlVoA3e6-0/hqdefault.jpg?sqp=-oaymwEiCKgBEF5IWvKriqkDFQgBFQAAAAAYASUAAMhCPQCAokN4AQ==\u0026rs=AOn4CLBUHnhcu3_X2AXJWopb6zmZaoKyRQ""
    },
  ]
}
---
## Fungsi: `utils.trending_hashtags.handler_function`

- **Deskripsi:** Analisis tren hashtag.

### Input (Parameters)
Menerima dictionary `params` yang mengikuti struktur `CommonParams` dan parameter spesifik dari `HashtagsRequest` (detail parameter `HashtagsRequest` perlu dijabarkan jika berbeda dari `CommonParams` atau jika ada tambahan).

### Output (Return Value)

{
  "data": [
    {
      "hashtag": "#prabowo",
      "total_mentions": 2727,
      "dominant_sentiment": "neutral",
      "dominant_sentiment_count": 1680,
      "dominant_sentiment_percentage": 61.6
    },
    {
      "hashtag": "#gibran",
      "total_mentions": 1316,
      "dominant_sentiment": "neutral",
      "dominant_sentiment_count": 717,
      "dominant_sentiment_percentage": 54.5
    }
  ]
}
---
## Fungsi: `utils.trending_links.handler_function`

- **Deskripsi:** Analisis tren tautan.

### Input (Parameters)
Menerima dictionary `params` yang mengikuti struktur `CommonParams`.
    
### Output (Return Value)
{
  "data": [
    {
      "link_post": "https://www.antaranews.com/berita",
      "total_mentions": 786
    },
    {
      "link_post": "https://nasional.kompas.com/read",
      "total_mentions": 546
    }
  ]
}
---
## Fungsi: `utils.popular_emojis.handler_function`

- **Deskripsi:** Analisis emoji populer.

### Input (Parameters)
Menerima dictionary `params` yang mengikuti struktur `CommonParams` dan parameter spesifik dari `EmojisRequest` (detail parameter `EmojisRequest` perlu dijabarkan jika berbeda dari `CommonParams` atau jika ada tambahan).

### Output (Return Value)
{
  "data": [
    {
      "emoji": "🔥",
      "total_mentions": 202
    },
    {
      "emoji": "❤",
      "total_mentions": 118
    }
  ]
}
---
## Fungsi: `utils.summary_stats.handler_function`

- **Deskripsi:** Analisis ringkasan statistik.

### Input (Parameters)
Menerima dictionary `params` yang mengikuti struktur `CommonParams`.

### Output (Return Value)
{
  "non_social_mentions": {
    "value": 17473,
    "display": "17.5K",
    "growth": 3965,
    "growth_display": "+4.0K",
    "growth_percentage": 29,
    "growth_percentage_display": "+29%"
  },
  "social_media_mentions": {
    "value": 6280,
    "display": "6.3K",
    "growth": -191,
    "growth_display": "-191",
    "growth_percentage": -3,
    "growth_percentage_display": "-3%"
  },
  "video_mentions": {
    "value": 6169,
    "display": "6.2K",
    "growth": 3396,
    "growth_display": "+3.4K",
    "growth_percentage": 122,
    "growth_percentage_display": "+122%"
  },
  "social_media_shares": {
    "value": 810683,
    "display": "810.7K",
    "growth": -720317,
    "growth_display": "-720.3K",
    "growth_percentage": -47,
    "growth_percentage_display": "-47%"
  },
  "social_media_likes": {
    "value": 12468725,
    "display": "12.5M",
    "growth": -12433849,
    "growth_display": "-12.4M",
    "growth_percentage": -50,
    "growth_percentage_display": "-50%"
  },
  "period": {
    "current": {
      "start_date": "2025-04-19",
      "end_date": "2025-05-19"
    },
    "previous": {
      "start_date": "2025-03-19",
      "end_date": "2025-04-18"
    }
  },
  "time_series": {
    "non_social_mentions": [
      {
        "date": "2025-04-19",
        "value": 326
      },
      {
        "date": "2025-04-20",
        "value": 417
      }
    ],
    "social_media_mentions": [
      {
        "date": "2025-04-19",
        "value": 34,
        "likes": 126818,
        "shares": 7835
      },
      {
        "date": "2025-04-20",
        "value": 38,
        "likes": 352871,
        "shares": 27034
      }
    ],
    "video_mentions": [
      {
        "date": "2025-04-19",
        "value": 34
      },
      {
        "date": "2025-04-20",
        "value": 33
      }
    ],
    "social_media_shares": [
      {
        "date": "2025-04-19",
        "value": 7835
      },
      {
        "date": "2025-04-20",
        "value": 27034
      }
    ],
    "social_media_likes": [
      {
        "date": "2025-04-19",
        "value": 126818
      },
      {
        "date": "2025-04-20",
        "value": 352871
      }
    ]
  }
}
---
## Fungsi: `utils.intent_emotions_region.handler_function`

- **Deskripsi:** Analisis niat, emosi, dan wilayah.

### Input (Parameters)
Menerima dictionary `params` yang mengikuti struktur `CommonParams`.

### Output (Return Value)
{
  "intents_share": [
    {
      "name": "informing",
      "percentage": 74
    },
    {
      "name": "reporting",
      "percentage": 7
    }
  ],
  "emotions_share": [
    {
      "name": "neutral",
      "percentage": 53
    },
    {
      "name": "concern",
      "percentage": 7
    }
  ],
  "regions_share": [
    {
      "name": "Indonesia",
      "percentage": 64
    },
    {
      "name": "Jakarta",
      "percentage": 25
    }
  ]
}
---
## Fungsi: `utils.topics_overview.handler_function`

- **Deskripsi:** Gambaran umum topik.

### Input (Parameters)
Menerima dictionary `params` yang mengikuti struktur `CommonParams` dengan tambahan parameter spesifik berikut:
| Nama Parameter | Tipe Data | Default | Deskripsi | Contoh |
|---|---|---|---|---|
| `owner_id` | str | `N/A` | Owner ID | `5` |
| `project_name` | str | `N/A` | Project name | `gibran raka` |

### Output (Return Value)
[
  {
    "unified_issue": "Policy and Regulation in Specific Sectors",
    "description": "This category includes discussions related to specific policy and regulation.",
    "list_issue": [
      "Hoping for the enactment of PeraturanM"
    ],
    "total_posts": 1,
    "viral_score": 96.03789520263672,
    "reach_score": 0,
    "positive": 1,
    "negative": 0,
    "neutral": 0,
    "share_of_voice": 0.04854368932038835
  }
]
---
## Fungsi: `utils.kol_overview.handler_function`

- **Deskripsi:** Gambaran umum Key Opinion Leaders.

### Input (Parameters)
Menerima dictionary `params` yang mengikuti struktur `CommonParams` dengan tambahan parameter spesifik berikut:
| Nama Parameter | Tipe Data | Default | Deskripsi | Contoh |
|---|---|---|---|---|
| `owner_id` | str | `N/A` | Owner ID | `5` |
| `project_name` | str | `N/A` | Project name | `gibran raka` |

### Output (Return Value)
[
  {
    "link_user": "https://www.instagram.com/zul.hasan",
    "link_post": 3,
    "viral_score": 799.2308072933989,
    "reach_score": 80,
    "channel": "instagram",
    "username": "zul.hasan",
    "user_image_url": "https://scontent-cgk2-2.cdninstagram.com/v/t51.2885-19/285389060_873832920676284_3135284620600762955_n.jpg?stp=dst-jpg_s150x150_tt6&_nc_ht=scontent-cgk2-2.cdninstagram.com&_nc_cat=100&_nc_oc=Q6cZ2QFQnsBcadiwcLifAU-lqr9wfOB0mcgVJ31qfzw1KeZI_rOuAQwCxsWEBPIVrNn4Z58&_nc_ohc=K5-eZRxH2KkQ7kNvwFks0Ne&_nc_gid=tcJFwNyfcNIomwGHE1qHKQ&edm=ABmJApABAAAA&ccb=7-5&oh=00_AfF3XW4XwkXXeC2NgfDyrqlwHSu2Mo2MzXGVaueg2m_vSQ&oe=681BD5A5&_nc_sid=b41fef",
    "user_followers": 614000,
    "engagement_rate": 13.888087087044429,
    "user_category": "Politician",
    "user_influence_score": 7.253333333333334,
    "sentiment_negative": 0,
    "sentiment_neutral": 1,
    "sentiment_positive": 2,
    "is_negative_driver": false,
    "unified_issue": [
      "Hanging out with the Governor",
      "Collaboration of clerics and government",
      "Inspirational Indonesia Menanam movement"
    ],
    "most_viral": 7.253333333333334,
    "share_of_voice": 0.03
  }
]
---
## Fungsi: `utils.keyword_trends.handler_function`

- **Deskripsi:** Analisis tren kata kunci.

### Input (Parameters)
Menerima dictionary `params` yang mengikuti struktur `CommonParams`.

### Output (Return Value)
[
  {
    "post_date": "2025-04-19 00:00:00",
    "total_mentions": 360,
    "total_reach": 1295.628,
    "total_positive": 154,
    "total_negative": 50,
    "total_neutral": 154
  },
  {
    "post_date": "2025-04-20 00:00:00",
    "total_mentions": 455,
    "total_reach": 1691.873,
    "total_positive": 183,
    "total_negative": 74,
    "total_neutral": 197
  }
]
---
## Fungsi: `utils.context_of_disccusion.handler_function`

- **Deskripsi:** Analisis konteks diskusi.

### Input (Parameters)
Menerima dictionary `params` yang mengikuti struktur `CommonParams`.

### Output (Return Value)
{
  "data": [
    {
      "hashtag": "prabowo",
      "total_mentions": 19267,
      "dominant_sentiment": "neutral",
      "dominant_sentiment_count": 12731,
      "dominant_sentiment_percentage": 66.1
    },
    {
      "hashtag": "presiden",
      "total_mentions": 18473,
      "dominant_sentiment": "neutral",
      "dominant_sentiment_count": 12346,
      "dominant_sentiment_percentage": 66.8
    }
  ]
}
---
