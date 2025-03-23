import requests
import os, re
from elasticsearch import Elasticsearch, helpers
import json
from datetime import datetime, timedelta


####################### Tiktok Helpers ########################

def download_tiktok_video(url, output_filename='tiktok_video.mp4'):
    try:
        # Custom headers to mimic a browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'video/webm,video/ogg,video/*;q=0.9,application/ogg;q=0.7,audio/*;q=0.6,*/*;q=0.5',
            'Accept-Language': 'en-US,en;q=0.5',
            'Range': 'bytes=0-',
            'Referer': 'https://www.tiktok.com/',
            'Connection': 'keep-alive',
        }
        
        # Send GET request with headers
        response = requests.get(url, headers=headers, stream=True, allow_redirects=True)
        response.raise_for_status()
        
        # Open file in binary write mode
        with open(output_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        print(f"Video successfully downloaded as {output_filename}")
        print(f"File size: {os.path.getsize(output_filename) / (1024*1024):.2f} MB")
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading video: {e}")
        
        # Additional error information
        if hasattr(e.response, 'status_code'):
            print(f"Status code: {e.response.status_code}")
        if hasattr(e.response, 'text'):
            print(f"Response text: {e.response.text[:200]}")  # Print first 200 chars of response
            
    except IOError as e:
        print(f"Error saving file: {e}")
    
def generate_tiktok_search_url(search_query: str) -> str:
    # Replace spaces with %20 for URL encoding
    encoded_query = search_query.replace(" ", "%20")
    
    # Construct the TikTok search URL
    tiktok_url = f"https://www.tiktok.com/search?q={encoded_query}"
    
    return tiktok_url
    

######################## Converter ###########################

def convert_string_to_int(s: str) -> int:
    if not re.search(r'\d',s):
        return 0
    s = s.upper().replace(",", "")  # Handle uppercase and remove commas if any
    
    if s.endswith("K"):
        return int(float(s[:-1]) * 1_000)
    elif s.endswith("M"):
        return int(float(s[:-1]) * 1_000_000)
    elif s.endswith("B"):
        return int(float(s[:-1]) * 1_000_000_000)
    else:
        return int(float(s))  # Handle cases without suffix
    
def convert_string_to_date(date_str: str) -> str:
    """
    Convert various date string formats into a standardized Y-m-d format.
    
    Args:
        date_str (str): Input date string.
    
    Returns:
        str: Standardized date format (Y-m-d).
    """
    today = datetime.today()
    
    if "m ago" in date_str:
        min_ago = int(date_str.split("m")[0])
        return (today - timedelta(minutes=min_ago)).strftime("%Y-%m-%d")
    
    elif "h ago" in date_str:
        hours_ago = int(date_str.split("h")[0])
        return (today - timedelta(hours=hours_ago)).strftime("%Y-%m-%d")
    
    elif "d ago" in date_str:
        days_ago = int(date_str.split("d")[0])
        return (today - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    elif "w ago" in date_str:
        weeks_ago = int(date_str.split("w")[0])
        return (today - timedelta(weeks=weeks_ago)).strftime("%Y-%m-%d")
    elif "m ago" in date_str:
        months_ago = int(date_str.split("m")[0])
        return (today.replace(month=today.month - months_ago)).strftime("%Y-%m-%d")
    elif "y ago" in date_str:
        years_ago = int(date_str.split("y")[0])
        return (today.replace(year=today.year - years_ago)).strftime("%Y-%m-%d")
    elif "-" in date_str:
        parts = date_str.split("-")
        if len(parts) == 3:  # Format: YYYY-M-D
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
        elif len(parts) == 2:  # Format: M-D
            return datetime(today.year, int(parts[0]), int(parts[1])).strftime("%Y-%m-%d")
    return "Invalid date format"

######################## ES #####################

def generate_bulk_data(data, index_name, id_ = None):
    for record in data:
        # Each record is wrapped in an action dictionary for Elasticsearch bulk API
        if id_:
            yield {
                "_index": index_name,
                "_source": record,
                "_id":record[id_]
            }

        else:
            yield {
                "_index": index_name,
                "_source": record
            }

def ingest_to_es(es, data, index_name, id_ = None):
    # Ingest the data into Elasticsearch
    try:
        response = helpers.bulk(es, generate_bulk_data(data, index_name, id_ = id_))
        print("Data successfully ingested to Elasticsearch:", response)

    except Exception as e:
        print("Error ingesting data:", e)
        
        
######################## Extraction ###################

def get_mentions(text):
    return re.findall(r'(@\w+)', text)
def get_hashtags(text):
    return re.findall(r'(#\w+)', text)


from google.cloud import bigquery
import pandas as pd
from google.oauth2 import service_account
import uuid

class About_BQ:
    def __init__(self, project_id: str, credentials_loc: str):
        """
        Inisialisasi koneksi ke BigQuery.
        
        :param project_id: ID proyek Google Cloud.
        :param credentials_loc: Path ke file kredensial JSON.
        """
        self.project_id = project_id
        self.credentials = service_account.Credentials.from_service_account_file(credentials_loc)
        self.client = bigquery.Client(credentials=self.credentials, project=self.project_id)

    def table_exists(self, dataset: str, table_name: str) -> bool:
        """
        Mengecek apakah tabel sudah ada di BigQuery.
        
        :param dataset: Nama dataset di BigQuery.
        :param table_name: Nama tabel yang akan dicek.
        :return: True jika tabel ada, False jika tidak ada.
        """
        full_table_id = f"{self.project_id}.{dataset}.{table_name}"
        try:
            self.client.get_table(full_table_id)  # Coba mengambil metadata tabel
            return True  # Jika berhasil, tabel ada
        except Exception:
            return False  # Jika error, berarti tabel tidak ada

    def to_pull_data(self, query: str) -> pd.DataFrame:
        """
        Menjalankan query dan mengambil data dari BigQuery sebagai Pandas DataFrame.

        :param query: Query SQL yang akan dijalankan di BigQuery.
        :return: DataFrame hasil query.
        """
        try:
            print("⏳ Menjalankan query ke BigQuery...")
            query_job = self.client.query(query)  # Eksekusi query
            result_df = query_job.to_dataframe()  # Konversi hasil ke DataFrame
            print(f"✅ Query selesai! {len(result_df)} baris data diambil.")
            return result_df
        except Exception as e:
            print(f"❌ Terjadi kesalahan saat mengambil data: {str(e)}")
            return pd.DataFrame()  # Kembalikan DataFrame kosong jika terjadi error
    
    
    def to_push_data(self, df: pd.DataFrame, dataset: str, table_name: str, if_exist: str = "append"):
        """
        Mengunggah DataFrame ke BigQuery.

        :param df: DataFrame yang akan diunggah.
        :param dataset: Nama dataset di BigQuery.
        :param table_name: Nama tabel di BigQuery.
        :param if_exist: Metode upload ("replace", "append", atau "fail").
        """
        full_table_id = f"{self.project_id}.{dataset}.{table_name}"

        if if_exist == "replace":
            write_disposition = "WRITE_TRUNCATE"
        elif if_exist == "append":
            write_disposition = "WRITE_APPEND"
        elif if_exist == "fail":
            write_disposition = "WRITE_EMPTY"
        else:
            raise ValueError("if_exist harus salah satu dari 'replace', 'append', atau 'fail'.")

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=True
        )

        job = self.client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
        job.result()

        print(f"✅ Data berhasil diunggah ke BigQuery: {full_table_id} (Mode: {if_exist})")

    def merge_data_post(self, df: pd.DataFrame, dataset: str, table_name: str, primary_key: str):
        """
        Menggabungkan data ke BigQuery dengan aturan:
        - Jika tabel belum ada, langsung insert.
        - Jika tabel ada:
            - Jika primary_key sudah ada, data diupdate.
            - Jika primary_key belum ada, data ditambahkan.

        :param df: DataFrame yang akan diunggah dan di-merge.
        :param dataset: Nama dataset di BigQuery.
        :param table_name: Nama tabel utama di BigQuery.
        :param primary_key: Nama kolom yang digunakan sebagai unique key untuk merge.
        """
        full_table = f"{self.project_id}.{dataset}.{table_name}"

        # 🔹 Cek apakah tabel sudah ada
        if not self.table_exists(dataset, table_name):
            print(f"⚠️ Tabel {full_table} belum ada, melakukan INSERT biasa...")
            self.to_push_data(df, dataset, table_name, if_exist="append")
            return

        # Jika tabel sudah ada, lanjutkan dengan MERGE
        temp_table_name = f"temp_{table_name}_{uuid.uuid4().hex[:8]}"
        temp_table = f"{self.project_id}.{dataset}.{temp_table_name}"

        # 🔹 Upload data ke tabel sementara
        print(f"⏳ Mengunggah data ke tabel sementara: {temp_table}")
        self.to_push_data(df, dataset, temp_table_name, if_exist="replace")

        # 🔹 Pastikan `updated_at` tidak ada di kolom untuk `UPDATE SET`
        df_columns = df.columns.tolist()
        update_columns = [col for col in df_columns if col not in [primary_key, "updated_at"]]

        # 🔹 Jika `updated_at` belum ada di df, tambahkan dalam `INSERT`
        insert_columns = df_columns[:]
        if "updated_at" not in insert_columns:
            insert_columns.append("updated_at")

        # 🔹 Query MERGE untuk replace jika `primary_key` sudah ada, dan append jika belum ada
        merge_query = f"""
        MERGE `{full_table}` AS target
        USING `{temp_table}` AS source
        ON target.{primary_key} = source.{primary_key}
        WHEN MATCHED THEN
            UPDATE SET
                {", ".join([f"target.{col} = source.{col}" for col in update_columns])},
                target.updated_at = CAST(CURRENT_TIMESTAMP() AS STRING)
        WHEN NOT MATCHED THEN
            INSERT ({", ".join(insert_columns)})
            VALUES ({", ".join([f"source.{col}" for col in insert_columns])});
        """

        # 🔹 Jalankan MERGE query
        print(f"⏳ Menjalankan MERGE ke {full_table} berdasarkan {primary_key}...")
        query_job = self.client.query(merge_query)
        query_job.result()
        print(f"✅ Data berhasil di-merge ke BigQuery: {full_table}")

        # 🔹 Hapus tabel sementara setelah digunakan
        print(f"🗑️ Menghapus tabel sementara: {temp_table}")
        self.client.delete_table(temp_table, not_found_ok=True)
        print("✅ Tabel sementara berhasil dihapus.")

        
        
