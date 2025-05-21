from utils.gemini import call_gemini
import os
from utils.es_client import get_elasticsearch_client
import re
import importlib

# Buat koneksi Elasticsearch
es = get_elasticsearch_client()

with open('utils/api_query.md') as f:
    self_query = f.read()

true = True
false = False
def execute_utility_function(endpoint_name: str, params: dict, project_name_arg: str = None, owner_id_arg: str = None):
    """
    Executes a local utility function based on the endpoint name.
    Dynamically imports the module and calls the specified function.
    """
    # IMPORTANT: This map assumes a generic function name like 'handler_function' in most utility modules.
    # You MUST update this map if your actual function names or module paths differ.
    # 'presence_score_analysis' is an example of a specific function name derived from your api_docs.md.
    # Module names are based on files in your 'utils/' directory.
    endpoint_to_module_details = {
        "mention-sentiment-breakdown": ("utils.analysis_sentiment_mentions", "get_category_analytics"),
        "analysis-overview": ("utils.analysis_overview", "get_social_media_matrix"),
        "list-of-mentions": ("utils.list_of_mentions", "get_mentions"),
        "presence-score": ("utils.presence_score", "get_presence_score"),
        "most-share-of-voice": ("utils.share_of_voice", "get_share_of_voice"),
        "most-followers": ("utils.most_followers", "get_most_followers"),
        "trending-hashtags": ("utils.trending_hashtags", "get_trending_hashtags"),
        "trending-links": ("utils.trending_links", "get_trending_links"),
        "popular-emojis": ("utils.popular_emojis", "get_popular_emojis"),
        "stats": ("utils.summary_stats", "get_stats_summary"), # Assumes module is summary_stats.py for 'stats'
        "intent-emotions-region": ("utils.intent_emotions_region", "get_intents_emotions_region_share"),
        "topics-overview": ("utils.topics_overview", "topic_overviews"),
        "kol-overview": ("utils.kol_overview", "search_kol"),
        "keyword-trends": ("utils.keyword_trends", "get_keyword_trends"),
        "context-of-discussion": ("utils.context_of_disccusion", "get_context_of_discussion") # Note: module name 'context_of_disccusion.py'
    }

    module_path, function_name = None, None

    if endpoint_name in endpoint_to_module_details:
        module_path, function_name = endpoint_to_module_details[endpoint_name]
    else:
        # Attempt to parse endpoint_name if it's not a direct key
        # e.g., "utils.module_name.function_name"
        parts = endpoint_name.split('.')
        if len(parts) > 1: # Basic check for "a.b" structure
            potential_function_name = parts[-1]
            potential_module_path = ".".join(parts[:-1])

            # Check if the parsed module_path exists in our map.
            # If it does, we prioritize the function name from our map,
            # especially if the incoming function name is a generic 'handler_function'.
            for mapped_module_path, mapped_function_name in endpoint_to_module_details.values():
                if mapped_module_path == potential_module_path:
                    # Found the module in our map.
                    if mapped_function_name == potential_function_name:
                        # The incoming function name matches the specific one in our map.
                        module_path = potential_module_path
                        function_name = potential_function_name
                        break  # Found a direct match for parsed module and function
                    elif potential_function_name == "handler_function":
                        # Incoming is generic 'handler_function', map has a specific one. Use the specific one.
                        module_path = mapped_module_path # or potential_module_path
                        function_name = mapped_function_name # Use the specific function from the map
                        break # Resolved using specific function from map
            # If after the loop, module_path and function_name are still None,
            # it means the parsed endpoint_name (module or function) didn't match anything
            # or couldn't be resolved appropriately against the map.
            # The error check below (if not module_path or not function_name) will handle this.
        # If not parsable into parts (len(parts) <= 1), or if the loop above didn't set module_path/function_name,
        # they remain None and will be caught by the error check.

    if not module_path or not function_name:
        print(f"Error: Endpoint '{endpoint_name}' is not recognized. It's not a defined alias and couldn't be resolved to a known utility function. Please check the endpoint name or update 'endpoint_to_module_details'.")
        return {"error": f"Unrecognized or unmapped endpoint: '{endpoint_name}'."}

    # Prepare the parameters for the local function call.
    # This replicates the logic from the original call_api where project_name and owner_id were added to the payload.
    current_params = params.copy()
    if project_name_arg:
        current_params['project_name'] = project_name_arg
    current_params['owner_id'] = owner_id_arg if owner_id_arg else "5" # Default owner_id to "5"

    print(f"Attempting to call local function: {module_path}.{function_name}")
    print(f"With parameters: {current_params}")

    try:
        module = importlib.import_module(module_path)
        func_to_call = getattr(module, function_name)
        
        # Assuming the local utility function accepts a single dictionary of parameters
        result = func_to_call(current_params)
        print(f"Result from {module_path}.{function_name}: {result}")
        return result

    except ImportError:
        print(f"Error: Could not import module '{module_path}'. Ensure the file exists and is correctly named (e.g., '{module_path.replace('.', '/')}.py').")
        return {"error": f"Module {module_path} not found."}
    except AttributeError:
        print(f"Error: Function '{function_name}' not found in module '{module_path}'. Ensure the function is defined in the module and the name in 'endpoint_to_module_details' is correct.")
        return {"error": f"Function '{function_name}' not found in {module_path}."}
    except Exception as e:
        print(f"Error executing function {function_name} from {module_path}: {e}")
        import traceback
        traceback.print_exc() # Print full traceback for debugging
        return {"error": f"Exception occurred in {module_path}.{function_name}: {str(e)}"}

def read_api_docs(file_path="utils/api_docs.md") -> str:
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

def generate_mcp_prompt(user_query: str, extracted_keywords: list[str] = None) -> str:
    api_docs = read_api_docs()

    extracted_keywords_str = (
        f"Keywords yang diekstrak sebelumnya: {extracted_keywords}" if extracted_keywords else "Tidak ada keyword yang diekstrak."
    )

    return f"""
Kamu adalah asisten AI yang bertugas memilih endpoint API yang paling sesuai berdasarkan permintaan pengguna.

Berikut adalah dokumentasi endpoint API:
{api_docs}

--- 

### Petunjuk:

1. Ambil _intent_ utama dari kalimat user berikut: "{user_query}"
2. Gunakan kata kunci berikut sebagai filter post_caption: {extracted_keywords_str}
3. Jika User tidak menuliskan tanggal, maka gunakan "last 30 days" di post_created_at
4. Tambahkan Kata kunci jika diperlukan untuk menangkap hasil lebih luas
5. Temukan maksimal 3 endpoint yang paling relevan dari daftar.
6. Tambahkan param seperti: `keywords`, `channels`, `sentiment`, `date_filter` jika terlihat dari permintaan user.
7. Tandai salah satu endpoint sebagai `"primary": true` jika dia paling penting untuk menjawab user query.
8. Jika tidak ada endpoint yang cocok, balas: `{{ "error": "no matching endpoint" }}`

---

### Format Jawaban dalam JSON tanpa penjelasan:

[
  {{
    "endpoint": "topics-overview",
    "primary": true,
    "params": {{
      "keywords": ["politik", "prabowo"],
      "channels": ["twitter"],
      "date_filter": "last 7 days"
    }}
  }},
  {{
    "endpoint": "list-of-mentions",
    "params": {{
      "keywords": ["politik"],
      "sort_type": "recent",
      "date_filter": "last 7 days"
    }}
  }}
]

"""

def generate_mcp_call(user_query: str, extracted_keywords: list[str] = None) -> str:
    prompt = generate_mcp_prompt(user_query, extracted_keywords)
    result = call_gemini(prompt)
    return result

def pipeline_ai(user_query, extracted_keywords):
    mcp_result = generate_mcp_call(user_query, extracted_keywords)

    # true = True # This line was not needed and has been removed.
    
    list_api = []
    list_api_str_match = re.findall(r'\[.*\]', mcp_result, flags=re.I | re.S)
    if not list_api_str_match:
        print(f"Warning: Could not parse function call list from MCP result: {mcp_result}")
        # Depending on desired behavior, you might want to return an error or proceed with an empty list_api
    else:
        try:
            list_api = eval(list_api_str_match[0])
            if not isinstance(list_api, list): # Add type check
                print(f"Warning: Parsed API list is not a list: {list_api}. MCP Result: {mcp_result}")
                list_api = [] 
        except Exception as e:
            print(f"Error evaluating API list string: '{list_api_str_match[0]}'. Error: {e}. MCP Result: {mcp_result}")
            # Depending on desired behavior, you might want to return an error or proceed with an empty list_api
    
    all_data = []
    
    prompt = f"""Kamu adalah AI Assistant, tugasmu membuat query untuk mencari data di Elasticsearch berdasarkan pertanyaan yang diajukan oleh user.

    Berikut adalah panduan untuk membuat query Elasticsearch:

    1.  **Filter Tanggal (post_created_at)**:
        *   Jika pengguna TIDAK menyebutkan rentang tanggal spesifik dalam "Pertanyaan User", maka secara OTOMATIS gunakan filter untuk 30 hari terakhir dari sekarang (misalnya, dalam format Elasticsearch: "now-30d/d" to "now/d").
        *   Jika pengguna MENYEBUTKAN rentang tanggal, gunakan rentang tanggal yang disebutkan tersebut. Pastikan formatnya sesuai untuk Elasticsearch.

    2.  **Filter Konten (post_caption)**:
        *   [WAJIB] Selalu gunakan `extracted_keywords` (yaitu: {extracted_keywords}) sebagai dasar untuk memfilter field `post_caption`. Buat query `match` atau `terms` atau `match_phrase` untuk ini, tergantung mana yang paling sesuai.
        *   Analisa "Pertanyaan User": "{user_query}". Jika terdapat keyword tambahan yang relevan dan dapat memperluas pencarian (misalnya user menyebut "cari juga tentang X"), tambahkan keyword tersebut ke dalam filter `post_caption`.
        *   Gabungkan semua keyword ini (baik dari `extracted_keywords` maupun keyword tambahan dari "Pertanyaan User") dalam query boolean clause (misalnya, `should` jika ingin OR, atau `must` jika ingin AND, tergantung kebutuhan).

    Gunakan informasi dari `{self_query}` sebagai referensi struktur query dan field yang tersedia saat membangun query Elasticsearch.

    Pertanyaan User: {user_query}

    Output yang diharapkan dalam format Query Elasticsearch JSON yang valid dan lengkap, tanpa penjelasan tambahan di luar JSON. Pastikan JSON tersebut adalah objek tunggal yang dimulai dengan `{{` dan diakhiri dengan `}}`.
    """

    query = call_gemini(prompt)

    #remove .keyword
    for i in ['region']:
        query = query.replace(i+'.keyword',i )


    query_es = eval(re.findall(r'\{.*\}',query, flags=re.I|re.S)[0])
    
    index = 'reddit_data,youtube_data,linkedin_data,twitter_data,tiktok_data,instagram_data,facebook_data,news_data,threads_data'

    
    result = es.search(index = index,
              body = query_es)


    all_data.append(result)    
    
    
    for function_call_info in list_api:
        print(f"Processing function call info: {function_call_info}")
        if not isinstance(function_call_info, dict) or 'endpoint' not in function_call_info or 'params' not in function_call_info:
            print(f"Warning: Skipping invalid function call info (must be a dict with 'endpoint' and 'params'): {function_call_info}")
            continue
        
        # 'koperasi' is passed as project_name_arg. 
        # owner_id_arg is not specified, so it will default to "5" inside execute_utility_function.
        data = execute_utility_function(
            endpoint_name=function_call_info['endpoint'],
            params=function_call_info['params'],
            project_name_arg='koperasi' 
        )

        all_data.append({"endpoint": function_call_info['endpoint'], 'data': data})

    #final answered
    answer = call_gemini(f"""
    Kamu adalah Moskal AI, asisten AI yang bertugas untuk menjawab pertanyaan user berdasarkan data yang telah disediakan.

    🎯 TUJUAN:
    - Jawaban harus ringkas, jelas, dan informatif.
    - Format jawaban harus berupa JSON agar bisa dirender di UI (bukan HTML mentah).

    📌 RULES FORMAT OUTPUT:
    - Format JSON dengan struktur: 
    {{
        "response_type": "mixed",
        "components": [
        {{
            "type": "text",
            "content": "<penjelasan singkat jawaban>"
        }},
        {{
            "type": "table",
            "title": "<judul tabel jika ada>",
            "headers": [...],
            "rows": [...]
        }},
        {{
            "type": "chart",
            "chart_type": "bar" | "line" | "pie",
            "title": "<judul chart>",
            "x_axis": "...",
            "y_axis": "...",
            "data": [{{...}}, ...]
        }}
        ],
        "footnotes": [
        {{
            "content": "Referensi: https://example.com/post/abc123"
        }}
        ]
    }}

    📌 ATURAN TAMBAHAN:
    - Jangan menyebut nama Endpoint manapun dalam jawaban.
    - Jika terdapat entri `link_post` dalam data, jadikan itu sebagai footnote referensi.
    - Jika tidak ada `link_post`, `footnotes` boleh dikosongkan.
    - Hapus entri yang mengandung kata 'unspecified' dari data.
    - Semua teks penjelasan boleh dalam bahasa Indonesia.
    - Jika hanya berupa teks (tanpa data tabel/grafik), gunakan type: "text" saja.

    📦 INPUT:
    Pertanyaan User:
    {user_query}

    Data yang tersedia:
    {all_data}

    🎯 OUTPUT:
    Berikan output dalam format JSON sesuai spesifikasi di atas.
    """)

        
        
    print('-------------------------- ANSWER -------------------')
    print(answer)

    print('--------------------------QUERY----------------')
    print(query)
    return eval(re.findall(r'\{.*\}',answer, flags=re.I|re.S)[0])

##contoh penggunaan

"""
user_query = "siapa budi arie? dan hubungannya dengan koperasi apa?"
extracted_keywords = ["politik", "gibran"]  # hasil NER atau keyword extraction, opsional

pipeline_ai(user_query, extracted_keywords)
"""
