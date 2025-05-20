# Dokumentasi API Endpoints

Dokumentasi ini menjelaskan input dan output untuk setiap endpoint API.

## Endpoint: `/api/v2/keyword-trends`

- **Metode:** `POST`
- **Deskripsi:** Analisis tren kata kunci.

Digunakan di menu:
- Dashboard
- Topics pada bagian Occurences
- Summary
- Analysis
- Comparison
- **Fungsi Handler:** `keyword_trends_analysis`
- **Tags:** Dashboard Menu

### Input (Request Body)

Menggunakan model: `CommonParams`

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

### Output (Response Body)

Struktur output ditentukan oleh fungsi `get_keyword_trends`.
Detail spesifik output perlu diperiksa pada implementasi fungsi tersebut di direktori `utils/`.

---

## Endpoint: `/api/v2/context-of-discussion`

- **Metode:** `POST`
- **Deskripsi:** Analisis konteks diskusi.

Digunakan di menu:
- Dashboard
- Topics
- Analysis
- Comparison
- **Fungsi Handler:** `context_analysis`
- **Tags:** Dashboard Menu

### Input (Request Body)

Menggunakan model: `CommonParams`

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

### Output (Response Body)

Struktur output ditentukan oleh fungsi `get_context_of_discussion`.
Detail spesifik output perlu diperiksa pada implementasi fungsi tersebut di direktori `utils/`.

---

## Endpoint: `/api/v2/list-of-mentions`

- **Metode:** `POST`
- **Deskripsi:** Mendapatkan daftar mentions.

Digunakan di menu:
- Dashboard
- Topics
- Summary
- Analysis
- Comparison -> Most Viral gunakan sort_type = "popular"

Parameter tambahan:
- sort_type: 'popular', 'recent', atau 'relevant'
- sort_order: desc atau asc
- page: halaman yang ditampilkan
- page_size: jumlah data per halaman
- **Fungsi Handler:** `get_mentions_list`
- **Tags:** Dashboard Menu

### Input (Request Body)

Menggunakan model: `MentionsRequest`

**Parameter Umum (dari `CommonParams`):**

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

**Parameter Spesifik untuk `MentionsRequest`:**

| Nama Parameter | Tipe Data | Default | Deskripsi | Contoh |
|---|---|---|---|---|
| `sort_type` | str | `recent` | Sort type: 'popular', 'recent', or 'relevant' | `recent` |
| `sort_order` | str | `desc` | Sort order: 'desc' or 'asc' | `desc` |
| `page` | int | `1` | Page number | `1` |
| `page_size` | int | `10` | Number of items per page | `10` |
| `source` | list of str (optional) | `None` | Source filters | `None` |

### Output (Response Body)

Struktur output ditentukan oleh fungsi `get_mentions`.
Detail spesifik output perlu diperiksa pada implementasi fungsi tersebut di direktori `utils/`.

---

## Endpoint: `/api/v2/analysis-overview`

- **Metode:** `POST`
- **Deskripsi:** Gambaran umum analisis.

Digunakan di menu:
- Analysis -> Overview
- Summary -> Summary
- Comparison -> Overview
- **Fungsi Handler:** `analysis_overview`
- **Tags:** Analysis Menu

### Input (Request Body)

Menggunakan model: `CommonParams`

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

### Output (Response Body)

Struktur output ditentukan oleh fungsi `get_social_media_matrix`.
Detail spesifik output perlu diperiksa pada implementasi fungsi tersebut di direktori `utils/`.

---

## Endpoint: `/api/v2/mention-sentiment-breakdown`

- **Metode:** `POST`
- **Deskripsi:** Analisis sentimen dan breakdown mention.

Digunakan di menu:
- Analysis : 
    1. Mention by categories
    2. Sentiment by categories
    3. Sentiment breakdown
- Topics:
    1. Channels share -> gunakan Mention by categories
    2. Overall sentiments -> gunakan Sentiment Breakdown
- Comparison:
    1. Sentiment breakdown
    2. Channels share -> gunakan Mention by categories
- **Fungsi Handler:** `analysis_sentiment`
- **Tags:** Analysis Menu

### Input (Request Body)

Menggunakan model: `CommonParams`

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

### Output (Response Body)

Struktur output ditentukan oleh fungsi `get_category_analytics`.
Detail spesifik output perlu diperiksa pada implementasi fungsi tersebut di direktori `utils/`.

---

## Endpoint: `/api/v2/presence-score`

- **Metode:** `POST`
- **Deskripsi:** Analisis skor kehadiran.

Digunakan pada menu:
- Analysis : 
    Presence Score
- Summary:
    Presence Score -> gunakan score nya saja

Parameter tambahan:
- interval: "day", "week", "month"
- compare_with_topics: true/false
- num_topics_to_compare: jumlah topik untuk dibandingkan
- **Fungsi Handler:** `presence_score_analysis`
- **Tags:** Analysis Menu

### Input (Request Body)

Menggunakan model: `PresenceRequest`

**Parameter Umum (dari `CommonParams`):**

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

**Parameter Spesifik untuk `PresenceRequest`:**

| Nama Parameter | Tipe Data | Default | Deskripsi | Contoh |
|---|---|---|---|---|
| `interval` | str | `week` | Interval: 'day', 'week', or 'month' | `week` |
| `compare_with_topics` | bool | `True` | Compare with topics | `True` |
| `num_topics_to_compare` | int | `10` | Number of topics to compare | `10` |

### Output (Response Body)

Struktur output ditentukan oleh fungsi `get_presence_score`.
Detail spesifik output perlu diperiksa pada implementasi fungsi tersebut di direktori `utils/`.

---

## Endpoint: `/api/v2/most-share-of-voice`

- **Metode:** `POST`
- **Deskripsi:** Analisis porsi suara yang paling banyak.

Digunakan pada Menu:
- Analysis -> Most Share of Voice

Parameter tambahan:
- limit: batas jumlah data
- page: halaman yang ditampilkan
- page_size: jumlah data per halaman
- include_total_count: true/false untuk menampilkan total data
- **Fungsi Handler:** `share_of_voice_analysis`
- **Tags:** Analysis Menu

### Input (Request Body)

Menggunakan model: `ShareOfVoiceRequest`

**Parameter Umum (dari `CommonParams`):**

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

**Parameter Spesifik untuk `ShareOfVoiceRequest`:**

| Nama Parameter | Tipe Data | Default | Deskripsi | Contoh |
|---|---|---|---|---|
| `limit` | int | `10` | Limit of results | `10` |
| `page` | int | `1` | Page number | `1` |
| `page_size` | int | `10` | Number of items per page | `10` |
| `include_total_count` | bool | `True` | Include total count in response | `True` |

### Output (Response Body)

Struktur output ditentukan oleh fungsi `get_share_of_voice`.
Detail spesifik output perlu diperiksa pada implementasi fungsi tersebut di direktori `utils/`.

---

## Endpoint: `/api/v2/most-followers`

- **Metode:** `POST`
- **Deskripsi:** Analisis pengikut terbanyak.

Digunakan pada Menu:
- Analysis -> Most Followers

Parameter tambahan:
- limit: batas jumlah data
- page: halaman yang ditampilkan
- page_size: jumlah data per halaman
- include_total_count: true/false untuk menampilkan total data
- **Fungsi Handler:** `most_followers_analysis`
- **Tags:** Analysis Menu

### Input (Request Body)

Menggunakan model: `FollowersRequest`

**Parameter Umum (dari `CommonParams`):**

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

**Parameter Spesifik untuk `FollowersRequest`:**

| Nama Parameter | Tipe Data | Default | Deskripsi | Contoh |
|---|---|---|---|---|
| `limit` | int | `10` | Limit of results | `10` |
| `page` | int | `1` | Page number | `1` |
| `page_size` | int | `10` | Number of items per page | `10` |
| `include_total_count` | bool | `True` | Include total count in response | `True` |

### Output (Response Body)

Struktur output ditentukan oleh fungsi `get_most_followers`.
Detail spesifik output perlu diperiksa pada implementasi fungsi tersebut di direktori `utils/`.

---

## Endpoint: `/api/v2/trending-hashtags`

- **Metode:** `POST`
- **Deskripsi:** Analisis tren hashtag.

Digunakan pada Menu:
- Analysis -> Trending hashtags

Parameter tambahan:
- limit: batas jumlah data
- page: halaman yang ditampilkan
- page_size: jumlah data per halaman
- sort_by: cara pengurutan data
- **Fungsi Handler:** `trending_hashtags_analysis`
- **Tags:** Analysis Menu

### Input (Request Body)

Menggunakan model: `HashtagsRequest`

**Parameter Umum (dari `CommonParams`):**

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

**Parameter Spesifik untuk `HashtagsRequest`:**

| Nama Parameter | Tipe Data | Default | Deskripsi | Contoh |
|---|---|---|---|---|
| `limit` | int | `100` | Limit of results | `100` |
| `page` | int | `1` | Page number | `1` |
| `page_size` | int | `10` | Number of items per page | `10` |
| `sort_by` | str | `mentions` | Sort by field | `mentions` |

### Output (Response Body)

Struktur output ditentukan oleh fungsi `get_trending_hashtags`.
Detail spesifik output perlu diperiksa pada implementasi fungsi tersebut di direktori `utils/`.

---

## Endpoint: `/api/v2/trending-links`

- **Metode:** `POST`
- **Deskripsi:** Analisis tren tautan.

Digunakan pada Menu:
- Analysis -> Trending links

Parameter tambahan:
- limit: batas jumlah data
- page: halaman yang ditampilkan
- page_size: jumlah data per halaman
- **Fungsi Handler:** `trending_links_analysis`
- **Tags:** Analysis Menu

### Input (Request Body)

Menggunakan model: `LinksRequest`

**Parameter Umum (dari `CommonParams`):**

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

**Parameter Spesifik untuk `LinksRequest`:**

| Nama Parameter | Tipe Data | Default | Deskripsi | Contoh |
|---|---|---|---|---|
| `limit` | int | `10000` | Limit of results | `1000` |
| `page` | int | `1` | Page number | `1` |
| `page_size` | int | `10` | Number of items per page | `10` |

### Output (Response Body)

Struktur output ditentukan oleh fungsi `get_trending_links`.
Detail spesifik output perlu diperiksa pada implementasi fungsi tersebut di direktori `utils/`.

---

## Endpoint: `/api/v2/popular-emojis`

- **Metode:** `POST`
- **Deskripsi:** Analisis emoji populer.

Digunakan pada Menu:
- Analysis -> Popular Emojis

Parameter tambahan:
- limit: batas jumlah data
- page: halaman yang ditampilkan
- page_size: jumlah data per halaman
- **Fungsi Handler:** `popular_emojis_analysis`
- **Tags:** Analysis Menu

### Input (Request Body)

Menggunakan model: `EmojisRequest`

**Parameter Umum (dari `CommonParams`):**

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

**Parameter Spesifik untuk `EmojisRequest`:**

| Nama Parameter | Tipe Data | Default | Deskripsi | Contoh |
|---|---|---|---|---|
| `limit` | int | `100` | Limit of results | `100` |
| `page` | int | `1` | Page number | `1` |
| `page_size` | int | `10` | Number of items per page | `10` |

### Output (Response Body)

Struktur output ditentukan oleh fungsi `get_popular_emojis`.
Detail spesifik output perlu diperiksa pada implementasi fungsi tersebut di direktori `utils/`.

---

## Endpoint: `/api/v2/stats`

- **Metode:** `POST`
- **Deskripsi:** Analisis ringkasan statistik.

Digunakan pada Menu:
- Summary -> Stats

Parameter tambahan:
- compare_with_previous: true/false untuk membandingkan dengan periode sebelumnya
- **Fungsi Handler:** `stats_summary_analysis`
- **Tags:** Summary Menu

### Input (Request Body)

Menggunakan model: `StatsRequest`

**Parameter Umum (dari `CommonParams`):**

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

**Parameter Spesifik untuk `StatsRequest`:**

| Nama Parameter | Tipe Data | Default | Deskripsi | Contoh |
|---|---|---|---|---|
| `compare_with_previous` | bool | `True` | Compare with previous period | `True` |

### Output (Response Body)

Struktur output ditentukan oleh fungsi `get_stats_summary`.
Detail spesifik output perlu diperiksa pada implementasi fungsi tersebut di direktori `utils/`.

---

## Endpoint: `/api/v2/intent-emotions-region`

- **Metode:** `POST`
- **Deskripsi:** Analisis niat, emosi, dan wilayah.

Digunakan pada Menu:
Untuk Parameter Keyword, gunakan list issue yang didapat ketika mendapat Topics
- Topics:
    1. Intent Shares
    2. Emotions Shares
    3. Top Regions
- **Fungsi Handler:** `intent_emotions_analysis`
- **Tags:** Topics Menu

### Input (Request Body)

Menggunakan model: `CommonParams`

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

### Output (Response Body)

Struktur output ditentukan oleh fungsi `get_intents_emotions_region_share`.
Detail spesifik output perlu diperiksa pada implementasi fungsi tersebut di direktori `utils/`.

---

## Endpoint: `/api/v2/topics-sentiment`

- **Metode:** `POST`
- **Deskripsi:** Analisis sentimen topik.

Digunakan pada Menu:
- Topics:
    Overall Sentiment -> di description per sentiment
- **Fungsi Handler:** `topics_sentiment_analysis`
- **Tags:** Topics Menu

### Input (Request Body)

Menggunakan model: `CommonParams`

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

### Output (Response Body)

Struktur output ditentukan oleh fungsi `get_topics_sentiment_analysis`.
Detail spesifik output perlu diperiksa pada implementasi fungsi tersebut di direktori `utils/`.

---

## Endpoint: `/api/v2/topics-overview`

- **Metode:** `POST`
- **Deskripsi:** Gambaran umum topik.

Digunakan pada Menu:
- Dashboard:
    Topics to Watch
- Topics:
    Overview
- Comparison
    Most viral topics

Parameter tambahan:
- owner_id: ID pemilik project
- project_name: Nama project
- **Fungsi Handler:** `topics_overview_analysis`
- **Tags:** Topics Menu

### Input (Request Body)

Menggunakan model: `TopicsOverviewRequest`

**Parameter Umum (dari `CommonParams`):**

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

**Parameter Spesifik untuk `TopicsOverviewRequest`:**

| Nama Parameter | Tipe Data | Default | Deskripsi | Contoh |
|---|---|---|---|---|
| `owner_id` | str | `N/A` | Owner ID | `5` |
| `project_name` | str | `N/A` | Project name | `gibran raka` |

### Output (Response Body)

Struktur output ditentukan oleh fungsi `topic_overviews`.
Detail spesifik output perlu diperiksa pada implementasi fungsi tersebut di direktori `utils/`.

---

## Endpoint: `/api/v2/kol-overview`

- **Metode:** `POST`
- **Deskripsi:** Gambaran umum Key Opinion Leaders.

Digunakan pada Menu:
- Dashboard:
    KOL to Watch
- Summary:
    Influencers
- Comparison:
    Kol to Watch
- KOL:
    Key Opinion Leaders Overview

Parameter tambahan:
- owner_id: ID pemilik project
- project_name: Nama project
- **Fungsi Handler:** `kol_overview_analysis`
- **Tags:** KOL Menu

### Input (Request Body)

Menggunakan model: `KolOverviewRequest`

**Parameter Umum (dari `CommonParams`):**

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

**Parameter Spesifik untuk `KolOverviewRequest`:**

| Nama Parameter | Tipe Data | Default | Deskripsi | Contoh |
|---|---|---|---|---|
| `owner_id` | str | `N/A` | Owner ID | `5` |
| `project_name` | str | `N/A` | Project name | `gibran raka` |

### Output (Response Body)

Struktur output ditentukan oleh fungsi `search_kol`.
Detail spesifik output perlu diperiksa pada implementasi fungsi tersebut di direktori `utils/`.

---
