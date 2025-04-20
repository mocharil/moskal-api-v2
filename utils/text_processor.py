"""
Text Processing Utilities

This module provides functions for text processing, including
stopwords handling and text preprocessing.
"""

import re
import string
import nltk
from nltk.corpus import stopwords

def get_stopwords():
    """
    Load NLTK stopwords for Indonesian and English
    
    Returns:
    --------
    set
        Combined set of Indonesian and English stopwords
    """
    try:
        nltk.data.find('corpora/stopwords')
    except LookupError:
        nltk.download('stopwords', quiet=True)
    
    # Combine Indonesian and English stopwords
    return set(stopwords.words('indonesian') + stopwords.words('english'))

def get_indonesian_stopwords():
    """
    Get common Indonesian stopwords
    
    Returns:
    --------
    list
        List of common Indonesian stopwords
    """
    return [
        "yang", "dan", "di", "ke", "dari", "untuk", "pada", "dengan", "ini", "itu",
        "atau", "juga", "saya", "kamu", "kami", "mereka", "dia", "kita", "akan", "tidak",
        "sudah", "telah", "bisa", "dapat", "harus", "ya", "tak", "ada", "jika", "kalau",
        "agar", "supaya", "adalah", "ialah", "bahwa", "oleh", "karena", "sebab", "apa", 
        "kenapa", "mengapa", "bagaimana", "siapa", "kapan", "dimana", "mana", "sejak", 
        "setelah", "sebelum", "ketika", "saat", "waktu", "selama", "hingga", "sampai",
        "tentang", "mengenai", "terhadap", "melalui", "berdasarkan", "sekitar", "antara",
        "nya", "lah", "pun", "kah", "tah", "ku", "mu", "yg", "utk", "dlm", "dg", "dgn",
        "pd", "dr", "sbg", "spt", "tgl", "bln", "thn", "bs", "sdh", "sdg", "blm", "byk",
        "krn", "tsb", "jd", "jgn", "org", "tdk", "mjd", "trs", "sblm", "stlh", "bbrp",
        "msh", "tp", "ttp", "ga", "gak", "tapi", "tetapi", "namun", "melainkan", "padahal",
        "kendatipun", "sedangkan", "maupun"
    ]

def get_english_stopwords():
    """
    Get common English stopwords
    
    Returns:
    --------
    list
        List of common English stopwords
    """
    return [
        "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
        "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers",
        "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves",
        "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are",
        "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does",
        "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until",
        "while", "of", "at", "by", "for", "with", "about", "against", "between", "into",
        "through", "during", "before", "after", "above", "below", "to", "from", "up", "down",
        "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here",
        "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more",
        "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so",
        "than", "too", "very", "s", "t", "can", "will", "just", "don", "don't", "should",
        "now"
    ]

def preprocess_text(text, stopwords_list=None, min_word_length=3):
    """
    Preprocess text for text analysis
    
    Parameters:
    -----------
    text : str
        Text to preprocess
    stopwords_list : list or set, optional
        List of stopwords to remove
    min_word_length : int, optional
        Minimum word length
        
    Returns:
    --------
    list
        List of preprocessed words
    """
    if not text or not isinstance(text, str):
        return []
    
    # Lowercase
    text = text.lower()
    
    # Remove punctuation
    text = re.sub(f'[{string.punctuation}]', ' ', text)
    
    # Remove numbers
    text = re.sub(r'\d+', '', text)
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Tokenize
    words = text.split()
    
    # Remove stopwords and short words
    if stopwords_list:
        words = [word for word in words if word not in stopwords_list and len(word) >= min_word_length]
    else:
        words = [word for word in words if len(word) >= min_word_length]
    
    return words