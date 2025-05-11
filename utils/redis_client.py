import os
import json
import logging
from redis import Redis
from redis.exceptions import ConnectionError, RedisError
from typing import Optional, Any, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedisClient:
    def __init__(self):
        try:
            self.redis_client = Redis(
                host=os.getenv('REDIS_HOST', 'localhost'),
                port=int(os.getenv('REDIS_PORT', 6379)),
                decode_responses=True,
                socket_connect_timeout=5  # 5 seconds timeout for connection
            )
            # Test connection
            self.redis_client.ping()
        except ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis_client = None
        except Exception as e:
            logger.error(f"Unexpected error while connecting to Redis: {e}")
            self.redis_client = None

    def is_connected(self) -> bool:
        """
        Check if Redis connection is alive
        """
        if not self.redis_client:
            return False
        try:
            self.redis_client.ping()
            return True
        except (ConnectionError, RedisError):
            return False
        
    def set_with_ttl(self, key: str, value: Any, ttl_seconds: int = 600) -> None:
        """
        Set a key with TTL (Time To Live)
        Default TTL is 10 minutes (600 seconds)
        Returns: None - Silently fails if Redis is unavailable
        """
        if not self.is_connected():
            logger.warning("Redis connection is not available, skipping cache set")
            return
        
        try:
            self.redis_client.setex(
                name=key,
                time=ttl_seconds,
                value=json.dumps(value)
            )
        except (ConnectionError, RedisError) as e:
            logger.warning(f"Redis error while setting key: {e}")
        except Exception as e:
            logger.error(f"Unexpected error setting Redis key: {e}")

    def get(self, key: str) -> Optional[Any]:
        """
        Get value from Redis
        Returns: Optional[Any] - Returns None if key doesn't exist or if Redis is unavailable
        """
        if not self.is_connected():
            logger.warning("Redis connection is not available, skipping cache get")
            return None
        
        try:
            value = self.redis_client.get(key)
            if value:
                return json.loads(value)
            return None
        except (ConnectionError, RedisError) as e:
            logger.warning(f"Redis error while getting key: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting Redis key: {e}")
            return None

    def generate_cache_key(self, prefix: str, **kwargs) -> str:
        """
        Generate a cache key based on prefix and query parameters
        """
        # Sort kwargs by key to ensure consistent key generation
        sorted_params = sorted(kwargs.items())
        # Join parameters into a string
        params_str = '_'.join(f"{k}:{v}" for k, v in sorted_params)
        return f"{prefix}:{params_str}"

# Create a singleton instance
redis_client = RedisClient()
