"""
Buffer Service Implementation
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends, status, BackgroundTasks
from redis.asyncio import Redis, from_url
import httpx
import logging
import json
from datetime import datetime, UTC
from typing import AsyncGenerator, Dict, Any, Optional, List, Literal, Tuple
from pydantic import BaseModel, HttpUrl, Field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Pydantic Models ---
class WebhookConfig(BaseModel):
    url: HttpUrl
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "POST"
    headers: Dict[str, str] = {}

class BufferConfig(BaseModel):
    key: str
    wait_time: int = Field(15, gt=0)
    aggregate_field: str
    max_size: int = Field(10, gt=0)

class BufferRequest(BaseModel):
    webhook: WebhookConfig
    buffer: BufferConfig
    payload: Dict[str, Any]

class BufferMetadata(BaseModel):
    total_messages: int
    first_message: str
    last_message: str
    processed_at: str
    wait_time_used: int
    buffer_key: str

class BufferResponse(BaseModel):
    buffer_key: str
    messages: List[Dict[str, Any]]
    aggregated_content: str
    metadata: BufferMetadata

# --- Redis Connection ---
redis_pool: Optional[Redis] = None

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Lifespan context manager for FastAPI app."""
    global redis_pool
    try:
        redis_pool = await from_url(
            "redis://localhost:6379",
            decode_responses=True,
            encoding="utf-8"
        )
        logger.info("Redis connection established")
        yield
    finally:
        if redis_pool:
            await redis_pool.close()
            redis_pool = None
            logger.info("Redis connection closed")

async def get_redis() -> Redis:
    """Get Redis connection from pool."""
    if redis_pool is None:
        raise RuntimeError("Redis connection not initialized")
    return redis_pool

# Redis dependency
get_redis_dependency = Depends(get_redis)

# --- Helper Functions ---
async def get_by_path(data: Dict[str, Any], path: str) -> Optional[Any]:
    """Get value from nested dictionary using dot notation."""
    keys = path.split(".")
    current = data
    for key in keys:
        try:
            if isinstance(current, dict):
                current = current[key]
            elif isinstance(current, list):
                current = current[int(key)]
            else:
                return None
        except (KeyError, IndexError, ValueError, TypeError):
            return None
    return current

async def aggregate_messages(messages: List[Dict[str, Any]], path: str) -> str:
    """Aggregate message content using specified path."""
    contents = []
    for msg in messages:
        if content := await get_by_path(msg, path):
            contents.append(str(content))
    return "\n".join(contents)

# --- Buffer Manager ---
class BufferManager:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def _get_config_key(self, buffer_key: str) -> str:
        """Get Redis key for buffer configuration."""
        return f"buffer:{buffer_key}:config"

    async def _get_message_list_key(self, buffer_key: str) -> str:
        """Get Redis key for buffer messages list."""
        return f"buffer:{buffer_key}:messages"

    async def _get_last_update_key(self, buffer_key: str) -> str:
        """Get Redis key for last update timestamp."""
        return f"buffer:{buffer_key}:last_update"

    async def _get_last_message_timestamp_key(self, buffer_key: str) -> str:
        """Get Redis key for last message timestamp."""
        return f"buffer:{buffer_key}:last_message_timestamp"
    
    # Dentro de la clase BufferManager en src/buffer_service.py
    async def _get_keys(self, buffer_key: str) -> Tuple[str, str]:
        """
        Genera las claves para la lista de mensajes y la configuración del buffer.

        Args:
            buffer_key: La clave base del buffer.

        Returns:
            Una tupla con la clave de la lista de mensajes y la clave de configuración.
        """
        message_list_key = f"{buffer_key}:messages"
        config_key = f"{buffer_key}:config"
        return message_list_key, config_key

    async def save_config(self, buffer_config: BufferConfig) -> None:
        """Save buffer configuration to Redis."""
        config_key = await self._get_config_key(buffer_config.key)
        await self.redis.set(
            config_key,
            buffer_config.model_dump_json(),
            ex=buffer_config.wait_time * 2,
        )
        logger.info(f"Saved config for buffer: {buffer_config.key}")

    async def get_config(self, buffer_key: str) -> Optional[BufferConfig]:
        """Retrieve buffer configuration from Redis."""
        config_key = await self._get_config_key(buffer_key)
        config_data = await self.redis.get(config_key)
        if config_data:
            logger.info(f"Retrieved config for buffer: {buffer_key}")
            return BufferConfig.model_validate_json(config_data)
        logger.warning(f"No config found for buffer: {buffer_key}")
        return None

    async def add_message(self, request: BufferRequest) -> bool:
        """Añade un mensaje al buffer y retorna True si debe procesarse."""
        message_list_key, config_key = await self._get_keys(request.buffer.key) # CORREGIDO - Desempaquetar tupla
        last_message_timestamp_key = await self._get_last_message_timestamp_key(request.buffer.key)
        last_update_key = await self._get_last_update_key(request.buffer.key)

        message = request.payload.copy()
        message["timestamp"] = datetime.now(UTC).isoformat()

        async with self.redis.pipeline() as pipe:
            await self.save_config(request.buffer)
            await pipe.rpush(message_list_key, json.dumps(message))
            await pipe.set(last_update_key, datetime.now(UTC).timestamp(), ex=request.buffer.wait_time * 2)
            await pipe.set(last_message_timestamp_key, datetime.now(UTC).timestamp(), ex=request.buffer.wait_time * 2)
            
            # Save config - config_key ahora está definida correctamente
            await pipe.set( 
                config_key, 
                request.buffer.model_dump_json(),
                ex=request.buffer.wait_time * 2
            )
            await pipe.execute()

        logger.info(f"Added message to buffer: {request.buffer.key}")

        # Verificar si se debe procesar (debounce)
        buffer_config = await self.get_config(request.buffer.key)
        if not buffer_config:
            return False

        list_length = await self.redis.llen(message_list_key)
        if list_length >= buffer_config.max_size:
            logger.info(f"Buffer {request.buffer.key} reached max size.")
            return True

        last_message_timestamp = await self.redis.get(last_message_timestamp_key)
        if last_message_timestamp is None: # type: ignore
            return False
        elapsed_debounce = datetime.now(UTC).timestamp() - float(last_message_timestamp) # NUEVO - Calcular tiempo desde el último mensaje
        if elapsed_debounce >= buffer_config.wait_time: # NUEVO - Usar elapsed_debounce para el timeout
            logger.info(f"Buffer {request.buffer.key} timed out (debounce).")
            return True

        return False

    async def get_messages(self, buffer_key: str) -> List[Dict[str, Any]]:
        """Get all messages in buffer."""
        message_list_key = await self._get_message_list_key(buffer_key)
        messages = await self.redis.lrange(message_list_key, 0, -1)
        return [json.loads(msg) for msg in messages]

    async def get_and_clear_buffer(
        self,
        buffer_key: str
    ) -> tuple[List[Dict[str, Any]], Optional[BufferConfig]]:
        """Atomically get and clear buffer from Redis."""
        message_list_key = await self._get_message_list_key(buffer_key)
        config_key = await self._get_config_key(buffer_key)
        last_message_timestamp_key = await self._get_last_message_timestamp_key(buffer_key)
        last_update_key = await self._get_last_update_key(buffer_key)

        try:
            async with self.redis.pipeline() as pipe:
                # Watch keys for transactionality
                pipe.watch(message_list_key, config_key, last_update_key, last_message_timestamp_key)

                # Get data
                buffer_config_json = await self.redis.get(config_key)
                messages = await self.redis.lrange(message_list_key, 0, -1)

                # Clear all buffer data
                pipe.multi()
                pipe.delete(message_list_key, config_key, last_update_key, last_message_timestamp_key)
                await pipe.execute()

                # Parse config
                if buffer_config_json:
                    buffer_config = BufferConfig.model_validate_json(buffer_config_json)
                    logger.info(f"Retrieved and cleared buffer: {buffer_key}")
                    return [json.loads(msg) for msg in messages], buffer_config
                else:
                    logger.warning(f"No config found for buffer when clearing: {buffer_key}")
                    return [], None
        except Exception as e:
            logger.error(f"Error getting and clearing buffer: {e}")
            return [], None


    async def clear_buffer(self, buffer_key: str) -> None:
        """Clear all buffer data from Redis."""
        message_list_key = await self._get_message_list_key(buffer_key)
        config_key = await self._get_config_key(buffer_key)
        last_update_key = await self._get_last_update_key(buffer_key)
        last_message_timestamp_key = await self._get_last_message_timestamp_key(buffer_key)
        async with self.redis.pipeline() as pipe:
            await pipe.delete(message_list_key, config_key, last_update_key, last_message_timestamp_key)
            await pipe.execute()
        logger.info(f"Cleared buffer data for: {buffer_key}")


async def process_buffer(
    messages: List[Dict[str, Any]],
    config: BufferConfig,
    webhook: WebhookConfig
) -> BufferResponse:
    """Process messages, aggregate content, and send webhook."""
    aggregated_content = await aggregate_messages(messages, config.aggregate_field)

    response = BufferResponse(
        buffer_key=config.key,
        messages=messages,
        aggregated_content=aggregated_content,
        metadata=BufferMetadata(
            total_messages=len(messages),
            first_message=messages[0].get("timestamp", "") if messages else "",
            last_message=messages[-1].get("timestamp", "") if messages else "",
            processed_at=datetime.now(UTC).isoformat(),
            wait_time_used=config.wait_time,
            buffer_key=config.key
        )
    )

    async with httpx.AsyncClient() as client:
        try:
            webhook_response = await client.request(
                method=webhook.method,
                url=str(webhook.url),
                json=response.model_dump(mode='json'),
                headers=webhook.headers,
                timeout=10.0
            )
            webhook_response.raise_for_status()
            return response
        except httpx.HTTPStatusError as e:
            logger.error(f"Webhook failed: {e.response.status_code} - {e.response.text}")
            raise HTTPException(
                status_code=e.response.status_code,
                detail=f"Webhook error: {e.response.text}"
            )
        except httpx.RequestError as e:
            logger.error(f"Webhook request failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Webhook connection error: {e}"
            )


# --- FastAPI App ---
app = FastAPI(
    title="Buffer Service",
    lifespan=lifespan,
    description="""
    ## Buffer Service API

    This service buffers messages and forwards them to a webhook in batches or after a timeout.

    ### Endpoints

    - **/message**: Receives messages and adds them to the buffer.

    ### Configuration

    The service uses Redis for buffering and is configured via environment variables or default values.

    ### Tests

    For testing instructions, refer to the `README.md` file in the `buffer_service` directory.
    """,
    version="0.2.0"
)

@app.post(
    "/message",
    status_code=status.HTTP_202_ACCEPTED,
    description="Receives and buffers messages. Messages are processed based on configured wait time or buffer size, then sent to a webhook."
)
async def receive_message(
    request: BufferRequest,
    background_tasks: BackgroundTasks,
    redis: Redis = get_redis_dependency,
) -> Dict[str, Any]:
    """Endpoint to receive messages and enqueue buffer processing."""
    buffer_manager = BufferManager(redis)

    should_process = await buffer_manager.add_message(request)
    if should_process:
        messages, config = await buffer_manager.get_and_clear_buffer(request.buffer.key)
        if messages and config:
            background_tasks.add_task(
                process_buffer,
                messages,
                config,
                request.webhook
            )
            return {
                "status": "processed",
                "message_count": len(messages),
                "reason": "buffer_full or timeout", # Updated reason
            }

    return {"status": "buffered"}