from utils.gemini import call_gemini
import os
from elasticsearch import Elasticsearch
import re
import importlib
# Buat koneksi ke Elasticsearch
es = Elasticsearch(
    "http://34.101.178.71:9200/",
    basic_auth=("elastic", "elasticpassword")  # Sesuaikan dengan kredensial Anda
)

with open('utils/api_query.md') as f:
    self_query = f.read()


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
        "mention-sentiment-breakdown": ("utils.analysis_sentiment_mentions", "handler_function"),
        "analysis-overview": ("utils.analysis_overview", "handler_function"),
        "list-of-mentions": ("utils.list_of_mentions", "handler_function"),
        "presence-score": ("utils.presence_score", "presence_score_analysis"),
        "most-share-of-voice": ("utils.share_of_voice", "handler_function"),
        "most-followers": ("utils.most_followers", "handler_function"),
        "trending-hashtags": ("utils.trending_hashtags", "handler_function"),
        "trending-links": ("utils.trending_links", "handler_function"),
        "popular-emojis": ("utils.popular_emojis", "handler_function"),
        "stats": ("utils.summary_stats", "handler_function"), # Assumes module is summary_stats.py for 'stats'
        "intent-emotions-region": ("utils.intent_emotions_region", "handler_function"),
        "topics-overview": ("utils.topics_overview", "handler_function"),
        "kol-overview": ("utils.kol_overview", "handler_function"),
        "keyword-trends": ("utils.keyword_trends", "handler_function"),
        "context-of-discussion": ("utils.context_of_disccusion", "handler_function") # Note: module name 'context_of_disccusion.py'
    }

    if endpoint_name not in endpoint_to_module_details:
        print(f"Error: Endpoint '{endpoint_name}' not found in mapping. Please update 'endpoint_to_module_details' in 'execute_utility_function'.")
        return {"error": f"Endpoint '{endpoint_name}' not mapped to a local function."}

    module_path, function_name = endpoint_to_module_details[endpoint_name]

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
2. Gunakan kata kunci berikut jika tersedia: {extracted_keywords_str}
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
    
    prompt = f"""Kamu adalah AI Assistant, tugasmu membuat query untuk mencari data di elastisearch berdasarkan pertanyaan yang diajukan oleh user

    berikut adalah list keyword yang digunakan user untuk analisis {extracted_keywords}


    {self_query}

    Pertanyaan User: {user_query}


    Output yang diharapkan dalam format Query Elasticsearch tanpa penjelasan
    """

    query = call_gemini(prompt)

    query_es = eval(re.findall(r'\{.*\}',query.replace('.keyword',''), flags=re.I|re.S)[0])
    
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
    Kamu adalah Moskal AI, asisten AI yang bertugas untuk menjawab pertanyaan user berdasarkan data yang telah di provide.
    RULES:
    - Berikan referensi jawaban dengan menyematkan link post sebagai footnote.
    - Gunakan format referensi di dalam teks seperti ini: "jawaban AI tuh gini" [1]
    - Di bagian akhir jawaban, buat daftar referensi dengan format:
      [1]: <url_link>
    - Jangan menyebutkan nama Endpoint pada jawaban.
    - berikan output dalam format markdown

    Pertanyaan User:
    {user_query}

    Data yang tersedia:
    {all_data}
    """)
        
        
    return answer

##contoh penggunaan

"""
user_query = "siapa budi arie? dan hubungannya dengan koperasi apa?"
extracted_keywords = ["politik", "gibran"]  # hasil NER atau keyword extraction, opsional

pipeline_ai(user_query, extracted_keywords)
"""
