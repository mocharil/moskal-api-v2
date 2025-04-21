FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Salin requirements.txt terlebih dahulu untuk memanfaatkan caching Docker
COPY requirements.txt .

# Install dependensi Python 
RUN pip install --no-cache-dir -r requirements.txt

# Secara spesifik upgrade google-cloud-aiplatform ke versi terbaru
RUN pip install --no-cache-dir --upgrade google-cloud-aiplatform

# Salin project files
COPY . .

# Buat file wrapper untuk menangani import error
RUN echo 'try:\n    from vertexai.generative_models import *\nexcept ImportError:\n    print("Warning: vertexai.generative_models tidak tersedia")\n    # Mock classes disini jika diperlukan' > /app/utils/gemini_wrapper.py

# Expose port
EXPOSE 8080

# Menjalankan aplikasi dengan log level debug
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--log-level", "debug"]
