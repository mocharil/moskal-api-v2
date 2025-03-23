import io, re
from google.oauth2 import service_account
import vertexai
from mimetypes import guess_type

from vertexai.generative_models import (
    GenerationConfig,
    GenerativeModel,
    HarmCategory,
    HarmBlockThreshold,
    Image
)
from dotenv import load_dotenv
import os

print('V2')
# Vertex AI configuration

load_dotenv()
BQ_CREDS_LOCATION = os.getenv("BQ_CREDS_LOCATION")

project_id = os.getenv("GEMINI_PROJECT_ID")
credentials_file_path = os.getenv("GEMINI_CREDS_LOCATION")
credentials = service_account.Credentials.from_service_account_file(credentials_file_path)
vertexai.init(project=project_id, credentials=credentials)

# Cache for model instances to avoid reconnection
model_cache = {}

# Default model
DEFAULT_MODEL = "gemini-1.5-flash"

# Safety configuration
safety_config = {
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
}

# Generation Config
config = GenerationConfig(temperature=0.0, top_p=1, top_k=32)

def get_model(model_name=DEFAULT_MODEL):
    """
    Get a model instance from cache or create a new one if not exists
    
    Args:
        model_name (str): Name of the Gemini model to use
        
    Returns:
        GenerativeModel: The model instance
    """
    if model_name not in model_cache:
        model_cache[model_name] = GenerativeModel(model_name)
    return model_cache[model_name]

def call_gemini(prompt, model_name=DEFAULT_MODEL):
    """
    Generate content using Gemini model
    
    Args:
        prompt: The prompt to send to Gemini
        model_name (str): Name of the Gemini model to use (default: gemini-1.5-flash)
        
    Returns:
        str: The generated content
    """
    # Get the model instance (from cache if available)
    model = get_model(model_name)
    
    # Generate content using the model
    responses = model.generate_content([prompt],
                                      safety_settings=safety_config,
                                      generation_config=config,
                                      stream=True)
    
    # Collect the full result
    full_result = ''
    for response in responses:
        full_result += response.text
    
    return full_result.strip()
