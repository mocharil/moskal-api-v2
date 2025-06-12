from utils.gemini import call_gemini
import os
import re
import json
from typing import Dict, List, Any, Optional, AsyncGenerator
import asyncio
from enum import Enum

false = False
true = True
null = ''

class StreamStepType(Enum):
    """Enum untuk tipe step dalam streaming response"""
    INIT = "init"
    ANALYSIS = "analysis" 
    STRATEGY = "strategy"
    QUERY_GENERATION = "query_generation"
    DATA_SEARCH = "data_search"
    DATA_PROCESSING = "data_processing"
    RESPONSE_GENERATION = "response_generation"
    COMPLETED = "completed"
    ERROR = "error"

class StreamingResponse:
    """Class untuk streaming response structure"""
    def __init__(self, step: StreamStepType, message: str, data: Dict = None, progress: int = 0):
        self.step = step.value
        self.message = message
        self.data = data or {}
        self.progress = progress
        self.timestamp = asyncio.get_event_loop().time()
    
    def to_dict(self) -> Dict:
        return {
            "step": self.step,
            "message": self.message,
            "data": self.data,
            "progress": self.progress,
            "timestamp": self.timestamp
        }
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

mcp_config = {
  "mcpServers": {
    "elasticsearch": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-elasticsearch"],
      "env": {
        "ELASTICSEARCH_URL": os.getenv("ELASTICSEARCH_URL", "http://localhost:9200"),
        "ELASTICSEARCH_USERNAME": os.getenv("ELASTICSEARCH_USERNAME"), 
        "ELASTICSEARCH_PASSWORD": os.getenv("ELASTICSEARCH_PASSWORD"),
        "ELASTICSEARCH_INDEX_PATTERN": "reddit_data,youtube_data,linkedin_data,twitter_data,tiktok_data,instagram_data,facebook_data,news_data,threads_data"
      }
    }
  },
  "tools": {
    "allowed": [
      "elasticsearch_search",
      "elasticsearch_aggregate", 
      "elasticsearch_get"
    ]
  }
}

# MCP Elasticsearch Client (unchanged)
class MCPElasticsearchClient:
    """MCP Elasticsearch Client untuk berinteraksi dengan ES melalui MCP"""
    def __init__(self, mcp_server_config: Dict = None):
        self.mcp_config = mcp_server_config or {
            "server": "elasticsearch",
            "url": os.getenv("ELASTICSEARCH_URL", "http://localhost:9200"),
            "username": os.getenv("ELASTICSEARCH_USERNAME"),
            "password": os.getenv("ELASTICSEARCH_PASSWORD")
        }
    
    async def search(self, index: str, body: Dict, size: int = 1000) -> Dict:
        try:
            query_body = body.copy()
            if "size" not in query_body:
                query_body["size"] = size
            
            mcp_query = {
                "index": index,
                "body": query_body
            }
            
            result = await self._execute_mcp_search(mcp_query)
            return result
            
        except Exception as e:
            print(f"Error executing MCP ES search: {e}")
            return {"error": str(e), "hits": {"hits": [], "total": {"value": 0}}}
    
    async def aggregate(self, index: str, body: Dict) -> Dict:
        try:
            query_body = body.copy()
            query_body["size"] = 0
            
            mcp_query = {
                "index": index,
                "body": query_body
            }
            
            result = await self._execute_mcp_search(mcp_query)
            return result
            
        except Exception as e:
            print(f"Error executing MCP ES aggregation: {e}")
            return {"error": str(e), "aggregations": {}}
    
    async def _execute_mcp_search(self, query: Dict) -> Dict:
        try:
            # TODO: Implementasi actual MCP call
            # Untuk sekarang, gunakan fallback ke ES langsung
            from utils.es_client import get_elasticsearch_client
            es = get_elasticsearch_client()
            
            result = es.search(
                index=query["index"], 
                body=query["body"]
            )
            return result
            
        except Exception as e:
            print(f"MCP search execution error: {e}")
            raise

# Inisialisasi MCP ES Client
mcp_es = MCPElasticsearchClient()

with open('utils/api_query.md') as f:
    self_query = f.read()

def read_api_docs(file_path="utils/api_docs.md") -> str:
    """Membaca dokumentasi query patterns untuk ES"""
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

async def stream_generate_search_strategy(user_query: str, extracted_keywords: List[str] = None) -> AsyncGenerator[Dict, None]:
    """Generate search strategy dengan streaming response"""
    
    yield {
        "type": "stream",
        "step": StreamStepType.ANALYSIS.value,
        "message": "Menganalisis pertanyaan dan menentukan strategi pencarian...",
        "progress": 10
    }
    
    api_docs = read_api_docs()
    extracted_keywords_str = (
        f"Keywords yang diekstrak: {extracted_keywords}" if extracted_keywords else "Tidak ada keyword yang diekstrak."
    )

    prompt = f"""
Kamu adalah AI yang bertugas menentukan strategi pencarian data di Elasticsearch berdasarkan pertanyaan user.

Dokumentasi query patterns:
{api_docs}

Pertanyaan User: "{user_query}"

Tentukan strategi pencarian yang dibutuhkan:

1. **Jenis Query yang Dibutuhkan:**
   - "search": untuk mencari dokumen spesifik
   - "aggregation": untuk analisis statistik, trending, summary
   - "both": kombinasi search + aggregation

2. **Parameters:**
   - keywords: list keyword untuk filter
   - date_range: rentang tanggal (default: last 30 days jika tidak disebutkan)
   - channels: platform medsos spesifik jika disebutkan
   - sentiment: filter sentiment jika relevan
   - sort_by: urutan hasil (recent, popular, relevance)
   - analysis_type: jenis analisis (sentiment, trend, overview, comparison)

Format output JSON:
{{
    "query_type": "search|aggregation|both",
    "analysis_type": "sentiment|trend|overview|comparison|mentions",
    "parameters": {{
        "keywords": [...],
        "date_range": "last 30 days",
        "channels": [...],
        "sentiment": null,
        "sort_by": "recent",
        "limit": 100
    }}
}}

<HARD RULES>:
Jika pertanyaan user tidak membutuhkan data media social dari Elasticsearch, maka:
1. gunakan persona bahwa anda adalah Moskal AI yang akan membantu menjawab pertanyaan terkait project anda di media social, mulai dari ...
2. gunakan nada informatif dan friendly
3. jangan memberitahu terkait penggunaan database
3. gunakan format ouput JSON seperti berikut:
{{
    "query_type": "general_question",
    "answer": "<your answer>"
}}
"""

    yield {
        "type": "stream",
        "step": StreamStepType.STRATEGY.value,
        "message": "Menghasilkan strategi pencarian berdasarkan analisis...",
        "progress": 20
    }

    result = call_gemini(prompt)
    
    try:
        strategy_match = re.findall(r'\{.*\}', result, flags=re.I | re.S)
        if strategy_match:
            strategy = eval(strategy_match[0])
            
            yield {
                "type": "stream",
                "step": StreamStepType.STRATEGY.value,
                "message": "Strategi pencarian berhasil dibuat",
                "data": {"strategy": strategy},
                "progress": 30
            }
            
            # Return hasil sebagai yield dengan type result
            yield {
                "type": "result",
                "data": strategy
            }
        else:
            # Default strategy
            default_strategy = {
                "query_type": "search",
                "analysis_type": "mentions",
                "parameters": {
                    "keywords": extracted_keywords or [],
                    "date_range": "last 30 days",
                    "channels": [],
                    "sentiment": None,
                    "sort_by": "recent",
                    "limit": 100
                }
            }
            
            yield {
                "type": "stream",
                "step": StreamStepType.STRATEGY.value,
                "message": "Menggunakan strategi default",
                "data": {"strategy": default_strategy},
                "progress": 30
            }
            
            yield {
                "type": "result",
                "data": default_strategy
            }
            
    except Exception as e:
        yield {
            "type": "stream",
            "step": StreamStepType.ERROR.value,
            "message": f"Error parsing search strategy: {e}",
            "progress": 30
        }
        
        yield {
            "type": "result",
            "data": {
                "query_type": "search",
                "analysis_type": "mentions", 
                "parameters": {
                    "keywords": extracted_keywords or [],
                    "date_range": "last 30 days",
                    "channels": [],
                    "sentiment": None,
                    "sort_by": "recent",
                    "limit": 100
                }
            }
        }

async def stream_generate_elasticsearch_query(strategy: Dict, user_query: str,extracted_keywords: List[str] = [] ) -> AsyncGenerator[Dict, None]:
    """Generate Elasticsearch query dengan streaming response"""
    
    yield {
        "type": "stream",
        "step": StreamStepType.QUERY_GENERATION.value,
        "message": "Thinking...",
        "progress": 40
    }
    
    query_type = strategy.get("query_type", "search")
    params = strategy.get("parameters", {})
    if params:
        print('------------')
        print()
        print('------------')

        keywords = params["keywords"]
        if keywords and type(keywords) == str:
            params["keywords"] = keywords.split(',')
        elif not keywords:
            params["keywords"] = extracted_keywords
        else:
            params["keywords"].extend(extracted_keywords)
    else:
        params = {
            "keywords": extracted_keywords,
            "date_range": "last 30 days",
            "channels": [],
            "sentiment": ["positive","negative","neutral"],
            "sort_by": "recent",
            "limit": 100
        }

    limit = params.get("limit", 100)
    
    prompt = f"""
Buat query Elasticsearch berdasarkan strategy berikut:

Query Type: {query_type}
Analysis Type: {strategy.get("analysis_type", "mentions")}
Parameters: {params}
User Query: {user_query}

Referensi struktur query: {self_query}

Aturan:
1. Gunakan keywords dari parameters untuk filter post_caption
2. Gunakan date_range untuk filter post_created_at
3. Jika query_type="aggregation", buat aggregation query untuk analisis
4. Jika query_type="both", gabungkan search + aggregation
5. Jika channels disebutkan, filter berdasarkan platform
6. WAJIB: Sertakan "size": {limit} di dalam query body
7. Untuk aggregation, tetap sertakan "size": 0

Output harus berupa valid Elasticsearch query JSON dengan field "size" di dalam body.
"""

    query_response = call_gemini(prompt)
    


    # Clean up .keyword references
    for field in ['region', 'channel', 'platform']:
        query_response = query_response.replace(f'{field}.keyword', field)

    try:
        query_match = re.findall(r'\{.*\}', query_response, flags=re.I | re.S)
        if query_match:
            query_body = eval(query_match[0])
            
            # Pastikan size ada di body
            if "size" not in query_body:
                if query_type == "aggregation":
                    query_body["size"] = 0



                else:
                    query_body["size"] = limit
            
            yield {
                "type": "stream",
                "step": StreamStepType.QUERY_GENERATION.value,
                "message": "Query berhasil dibuat",
                "data": {"elasticsearch_query": query_body},
                "progress": 50
            }
            
            yield {
                "type": "result",
                "data": query_body
            }
        else:
            default_query = {
                "size": limit,
                "query": {"match_all": {}}
            }
            
            yield {
                "type": "stream",
                "step": StreamStepType.QUERY_GENERATION.value,
                "message": "Menggunakan query default",
                "data": {"elasticsearch_query": default_query},
                "progress": 50
            }
            
            yield {
                "type": "result",
                "data": default_query
            }
            
    except Exception as e:
        yield {
            "type": "stream",
            "step": StreamStepType.ERROR.value,
            "message": f"Error parsing: {e}",
            "progress": 50
        }
        
        yield {
            "type": "result",
            "data": {
                "size": limit,
                "query": {"match_all": {}}
            }
        }

async def stream_search_elasticsearch_data(strategy: Dict, query_es: Dict) -> AsyncGenerator[Dict, None]:
    """Search Elasticsearch dengan streaming response"""
    
    yield {
        "type": "stream",
        "step": StreamStepType.DATA_SEARCH.value,
        "message": "Mencari data...",
        "progress": 60
    }
    
    index = 'reddit_data,youtube_data,linkedin_data,twitter_data,tiktok_data,instagram_data,facebook_data,news_data,threads_data'
    query_type = strategy.get("query_type", "search")
    
    print(json.dumps(query_es, indent=4))


    try:
        if query_type == "aggregation":
            result = await mcp_es.aggregate(index=index, body=query_es)
        else:
            result = await mcp_es.search(index=index, body=query_es)
        
        total_hits = result.get("hits", {}).get("total", {}).get("value", 0) if "hits" in result else 0
        
        yield {
            "type": "stream",
            "step": StreamStepType.DATA_SEARCH.value,
            "message": f"Data berhasil ditemukan: {total_hits} dokumen",
            "data": {"total_documents": total_hits},
            "progress": 70
        }
        
        yield {
            "type": "result",
            "data": result
        }
        
    except Exception as e:
        yield {
            "type": "stream",
            "step": StreamStepType.ERROR.value,
            "message": f"Error searching: {e}",
            "progress": 70
        }
        
        yield {
            "type": "result",
            "data": {"error": str(e), "hits": {"hits": [], "total": {"value": 0}}}
        }

def process_elasticsearch_results(es_result: Dict, strategy: Dict) -> Dict:
    """Process hasil ES dan extract informasi yang relevan (unchanged)"""
    analysis_type = strategy.get("analysis_type", "mentions")
    
    if "error" in es_result:
        return {"error": es_result["error"], "processed_data": []}
    
    processed_data = {
        "total_hits": 0,
        "documents": [],
        "aggregations": {},
        "analysis_type": analysis_type
    }
    
    # Extract hits
    if "hits" in es_result and "hits" in es_result["hits"]:
        processed_data["total_hits"] = es_result["hits"]["total"]["value"]
        
        for hit in es_result["hits"]["hits"]:
            source = hit.get("_source", {})
            doc = {
                "id": hit.get("_id"),
                "platform": source.get("platform", "unknown"),
                "caption": source.get("post_caption", ""),
                "created_at": source.get("post_created_at"),
                "author": source.get("author_name", ""),
                "link": source.get("link_post", ""),
                "sentiment": source.get("sentiment"),
                "engagement": {
                    "likes": source.get("likes_count", 0),
                    "comments": source.get("comments_count", 0),
                    "shares": source.get("shares_count", 0)
                }
            }
            processed_data["documents"].append(doc)
    
    # Extract aggregations
    if "aggregations" in es_result:
        processed_data["aggregations"] = es_result["aggregations"]
    
    return processed_data

async def stream_generate_final_response(processed_data: Dict, strategy: Dict, user_query: str) -> AsyncGenerator[Dict, None]:
    """Generate final response dengan streaming"""
    
    yield {
        "type": "stream",
        "step": StreamStepType.RESPONSE_GENERATION.value,
        "message": "Menganalisis data dan membuat response...",
        "progress": 80
    }
    
    answer = call_gemini(f"""
Kamu adalah Moskal AI, asisten AI yang bertugas menjawab pertanyaan user berdasarkan data media sosial dari Elasticsearch.

🎯 TUJUAN:
- Jawaban berdasarkan data media sosial yang tersedia
- Format JSON untuk rendering di UI
- Analisis yang insight dan actionable

📌 RULES SAMPLE FORMAT OUTPUT:
{{
    "response_type": "mixed",
    "data_source": "elasticsearch_social_media",
    "total_documents_analyzed": "<jumlahkan seluruh data yang ada>",
    "components": [
        {{
            "type": "text",
            "content": "<insight utama dari analisis>"
        }},
        {{ #if needed
            "type": "table",
            "title": "Top Mentions",
            "headers": ["Column Name 1", "Column Name 2", "Column Name 3"],
            "rows": [...]
        }},
        {{ #if needed
            "type": "chart",
            "chart_type": "bar|line|pie",
            "title": "<judul chart>",
            "data": [...]
        }}
    ],
    "insights": [ #if needed
        "Insight 1: ...",
        "Insight 2: ..."
    ],
    "footnotes": [ #if needed
        {{
            "content": "URL dari field link_post jika ada"
        }}
    ]
}}

📦 INPUT:
Pertanyaan User: {user_query}
Strategy: {strategy}
Data Analysis Type: {processed_data['analysis_type']}
Processed Data: {processed_data}

🎯 OUTPUT:
JSON response dengan analisis mendalam data media sosial.
""")
    
    try:
        answer_match = re.findall(r'\{.*\}', answer, flags=re.I | re.S)
        if answer_match:
            result = eval(answer_match[0])
            
            # Ensure required fields
            if "data_source" not in result:
                result["data_source"] = "elasticsearch_social_media"
            if "total_documents_analyzed" not in result:
                result["total_documents_analyzed"] = processed_data['total_hits']
            
            yield {
                "type": "stream",
                "step": StreamStepType.RESPONSE_GENERATION.value,
                "message": "Response berhasil dibuat",
                "data": {"response_preview": result.get("components", [])[:1]},
                "progress": 90
            }
            
            yield {
                "type": "result",
                "data": result
            }
        else:
            default_response = {
                "response_type": "text",
                "data_source": "elasticsearch_social_media",
                "total_documents_analyzed": processed_data['total_hits'],
                "components": [{"type": "text", "content": "Data media sosial ditemukan, namun gagal memproses analisis."}],
                "footnotes": []
            }
            
            yield {
                "type": "stream",
                "step": StreamStepType.RESPONSE_GENERATION.value,
                "message": "Menggunakan response default",
                "progress": 90
            }
            
            yield {
                "type": "result",
                "data": default_response
            }
            
    except Exception as e:
        yield {
            "type": "stream",
            "step": StreamStepType.ERROR.value,
            "message": f"Error parsing final answer: {e}",
            "progress": 90
        }
        
        yield {
            "type": "result",
            "data": {
                "response_type": "text",
                "data_source": "elasticsearch_social_media", 
                "total_documents_analyzed": processed_data['total_hits'],
                "components": [{"type": "text", "content": f"Error dalam analisis: {str(e)}"}],
                "footnotes": []
            }
        }

async def pipeline_ai_streaming(user_query: str, extracted_keywords: List[str] = None) -> AsyncGenerator[Dict, None]:
    """
    Main streaming pipeline untuk QnA dengan step-by-step progress
    """
    
    # Step 1: Initialize
    yield {
        "type": "stream",
        "step": StreamStepType.INIT.value,
        "message": f"Memulai analisis untuk: {user_query}",
        "data": {"query": user_query, "keywords": extracted_keywords},
        "progress": 0
    }
    
    try:
        # Step 2-3: Generate search strategy
        strategy = None
        async for stream_response in stream_generate_search_strategy(user_query, extracted_keywords):
            if stream_response["type"] == "stream":
                yield stream_response
            elif stream_response["type"] == "result":
                strategy = stream_response["data"]
        
        if not strategy:
            yield {
                "type": "stream",
                "step": StreamStepType.ERROR.value,
                "message": "Gagal membuat strategi pencarian",
                "progress": 30
            }
            return
        
        # Check for general question
        if strategy.get('query_type') == 'general_question':
            yield {
                "type": "final",
                "step": StreamStepType.COMPLETED.value,
                "message": "Pertanyaan umum dijawab",
                "data": {
                    "final_response": {
                        "response_type": "text",
                        "data_source": "general_question", 
                        "total_documents_analyzed": 0,
                        "components": [{"type": "text", "content": strategy["answer"]}],
                        "footnotes": []
                    }
                },
                "progress": 100
            }
            return
        
        # Step 4: Generate Elasticsearch query
        query_es = None
        async for stream_response in stream_generate_elasticsearch_query(strategy, user_query, extracted_keywords):
            if stream_response["type"] == "stream":
                yield stream_response
            elif stream_response["type"] == "result":
                query_es = stream_response["data"]
        
        if not query_es:
            yield {
                "type": "stream",
                "step": StreamStepType.ERROR.value,
                "message": "Gagal mengambil data",
                "progress": 50
            }
            return
        
        # Step 5: Search Elasticsearch
        es_result = None
        async for stream_response in stream_search_elasticsearch_data(strategy, query_es):
            if stream_response["type"] == "stream":
                yield stream_response
            elif stream_response["type"] == "result":
                es_result = stream_response["data"]
        
        if not es_result:
            yield {
                "type": "stream",
                "step": StreamStepType.ERROR.value,
                "message": "Gagal mendapatkan data",
                "progress": 70
            }
            return
        
        # Step 6: Process data
        yield {
            "type": "stream",
            "step": StreamStepType.DATA_PROCESSING.value,
            "message": "Memproses dan menganalisis data...",
            "progress": 75
        }
        
        processed_data = process_elasticsearch_results(es_result, strategy)
        
        yield {
            "type": "stream",
            "step": StreamStepType.DATA_PROCESSING.value,
            "message": f"Data berhasil diproses: {processed_data['total_hits']} dokumen",
            "data": {"processed_summary": {
                "total_hits": processed_data['total_hits'],
                "analysis_type": processed_data['analysis_type']
            }},
            "progress": 80
        }
        
        # Step 7: Generate final response
        final_response = None
        async for stream_response in stream_generate_final_response(processed_data, strategy, user_query):
            if stream_response["type"] == "stream":
                yield stream_response
            elif stream_response["type"] == "result":
                final_response = stream_response["data"]
        
        if not final_response:
            yield {
                "type": "stream",
                "step": StreamStepType.ERROR.value,
                "message": "Gagal membuat response akhir",
                "progress": 90
            }
            return
        
        # Step 8: Completed
        yield {
            "type": "final",
            "step": StreamStepType.COMPLETED.value,
            "message": "Analisis selesai",
            "data": {"final_response": final_response},
            "progress": 100
        }
        
    except Exception as e:
        yield {
            "type": "stream",
            "step": StreamStepType.ERROR.value,
            "message": f"Error dalam pipeline: {str(e)}",
            "progress": 0
        }

# Usage Example untuk FastAPI atau framework lain
async def stream_response_example():
    """
    Contoh penggunaan streaming response
    """
    user_query = "Apa sentimen publik tentang produk X?"
    keywords = ["produk X", "sentimen"]
    
    async for response in pipeline_ai_streaming(user_query, keywords):
        # Kirim response ke client (WebSocket, SSE, dll)
        print(f"Type: {response['type']}")
        print(f"Step: {response['step']}")
        print(f"Message: {response['message']}")
        print(f"Progress: {response['progress']}%")
        if 'data' in response:
            print(f"Data: {response['data']}")
        print("---")
        
        # Untuk SSE (Server-Sent Events):
        # yield f"data: {json.dumps(response)}\n\n"
        
        # Untuk WebSocket:
        # await websocket.send_text(json.dumps(response))

# Backward compatibility wrappers
async def pipeline_ai_async(user_query: str, extracted_keywords: List[str] = None) -> Dict:
    """Async wrapper yang mengembalikan hasil final saja"""
    final_result = None
    async for response in pipeline_ai_streaming(user_query, extracted_keywords):
        if response["type"] == "final" and response["step"] == StreamStepType.COMPLETED.value:
            final_result = response["data"].get("final_response")
            break
    return final_result or {"error": "No final result received"}

def pipeline_ai_sync(user_query: str, extracted_keywords: List[str] = None) -> Dict:
    """Synchronous wrapper yang mengembalikan hasil final saja"""
    import asyncio
    
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, pipeline_ai_async(user_query, extracted_keywords))
                return future.result()
        else:
            return loop.run_until_complete(pipeline_ai_async(user_query, extracted_keywords))
    except RuntimeError:
        return asyncio.run(pipeline_ai_async(user_query, extracted_keywords))