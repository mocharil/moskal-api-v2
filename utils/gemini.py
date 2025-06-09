import io, re
from google.oauth2 import service_account
import vertexai
from mimetypes import guess_type
import time, os
import random
from typing import Any, AsyncGenerator
import logging
import asyncio

from vertexai.generative_models import (
    GenerationConfig,
    GenerativeModel,
    HarmCategory,
    HarmBlockThreshold,
    Image
)

from dotenv import load_dotenv

load_dotenv() 
print('✅ Moskal AI Gemini v2.0 - Streaming Enabled')

# Vertex AI configuration
project_id = os.getenv("GEMINI_PROJECT_ID")
credentials_file_path = os.getenv("GEMINI_CREDS_LOCATION")
print(f'📁 Credentials: {credentials_file_path}')
print(f'🆔 Project ID: {project_id}')
true = True
false = False
null = ''
credentials = service_account.Credentials.from_service_account_file(credentials_file_path)
vertexai.init(project=project_id, credentials=credentials)
model = os.getenv("GEMINI_DEFAULT_MODEL", "gemini-pro")
multimodal_model = GenerativeModel(model)

safety_config = {
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
}

# Generation Config
config = GenerationConfig(temperature=0.0, top_p=1, top_k=32)

def call_gemini(prompt, 
                max_retries=2, 
                initial_backoff=1.0, 
                max_backoff=60.0, 
                backoff_factor=2.0, 
                jitter=0.1):
    """
    Non-streaming version - collect all chunks into single response
    """
    retries = 0
    backoff = initial_backoff
    last_exception = None
    
    while retries < max_retries:
        try:
            # Generate content using the multimodal model
            responses = multimodal_model.generate_content(
                [prompt],
                safety_settings=safety_config, 
                generation_config=config, 
                stream=True
            )
            
            # Collect the full result
            full_result = ''
            for response in responses:
                full_result += response.text
            
            return full_result.strip()
            
        except Exception as e:
            last_exception = e
            
            # Log the error
            if logging:
                logging.warning(f"Gemini API error on attempt {retries+1}/{max_retries}: {str(e)}")
            else:
                print(f"Gemini API error on attempt {retries+1}/{max_retries}: {str(e)}")
            
            # Check if we should retry based on the error type
            if "ServiceUnavailable: 503 Connection reset" in str(e) or "Connection reset" in str(e):
                # Calculate backoff with jitter
                jitter_value = backoff * jitter * random.random()
                wait_time = min(backoff + jitter_value, max_backoff)
                
                if logging:
                    logging.info(f"Retrying in {wait_time:.2f} seconds...")
                else:
                    print(f"Retrying in {wait_time:.2f} seconds...")
                
                time.sleep(wait_time)
                # Increase backoff exponentially
                backoff = min(backoff * backoff_factor, max_backoff)
                retries += 1
            else:
                # For other errors, raise immediately
                raise
    
    # If all retries failed
    error_msg = f"Failed to call Gemini API after {max_retries} attempts. Last error: {last_exception}"
    if logging:
        logging.error(error_msg)
    else:
        print(error_msg)
    
    raise Exception(error_msg)

async def call_gemini_stream(prompt: str, 
                           max_retries: int = 2, 
                           initial_backoff: float = 1.0, 
                           max_backoff: float = 60.0, 
                           backoff_factor: float = 2.0, 
                           jitter: float = 0.1) -> AsyncGenerator[str, None]:
    """
    NEW: Async streaming version that yields chunks as they arrive
    """
    retries = 0
    backoff = initial_backoff
    last_exception = None
    
    while retries < max_retries:
        try:
            # Generate content using the multimodal model with streaming
            responses = multimodal_model.generate_content(
                [prompt],
                safety_settings=safety_config, 
                generation_config=config, 
                stream=True
            )
            
            # Yield each chunk as it arrives
            for response in responses:
                if response.text:
                    yield response.text
                    # Small delay to prevent overwhelming the client
                    await asyncio.sleep(0.01)
            
            return  # Success, exit the retry loop
            
        except Exception as e:
            last_exception = e
            
            # Log the error
            if logging:
                logging.warning(f"Gemini streaming API error on attempt {retries+1}/{max_retries}: {str(e)}")
            else:
                print(f"Gemini streaming API error on attempt {retries+1}/{max_retries}: {str(e)}")
            
            # Check if we should retry based on the error type
            if "ServiceUnavailable: 503 Connection reset" in str(e) or "Connection reset" in str(e):
                # Calculate backoff with jitter
                jitter_value = backoff * jitter * random.random()
                wait_time = min(backoff + jitter_value, max_backoff)
                
                if logging:
                    logging.info(f"Retrying streaming in {wait_time:.2f} seconds...")
                else:
                    print(f"Retrying streaming in {wait_time:.2f} seconds...")
                
                await asyncio.sleep(wait_time)
                # Increase backoff exponentially
                backoff = min(backoff * backoff_factor, max_backoff)
                retries += 1
            else:
                # For other errors, try to yield error and break
                yield f"Error in streaming: {str(e)}"
                return
    
    # If all retries failed, yield error message
    error_msg = f"Failed to call Gemini streaming API after {max_retries} attempts. Last error: {last_exception}"
    if logging:
        logging.error(error_msg)
    else:
        print(error_msg)
    
    yield f"Streaming error: {error_msg}"

# Compatibility function
def call_gemini_sync_stream(prompt: str) -> str:
    """
    Synchronous version that collects all streaming chunks
    (for backward compatibility)
    """
    try:
        responses = multimodal_model.generate_content(
            [prompt],
            safety_settings=safety_config, 
            generation_config=config, 
            stream=True
        )
        
        full_result = ''
        for response in responses:
            full_result += response.text
        
        return full_result.strip()
        
    except Exception as e:
        print(f"Error in sync streaming: {e}")
        # Fallback to regular call_gemini
        return call_gemini(prompt)