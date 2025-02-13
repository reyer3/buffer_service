"""
Buffer Service Implementation
"""
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, HttpUrl
from typing import Dict, Any, Optional, List
from redis.asyncio import Redis
import aiohttp
from datetime import datetime, UTC
import json
from functools import reduce
import operator
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Modelos Pydantic
class WebhookConfig(BaseModel):
    url: HttpUrl
    method: str = "POST"
    headers: Dict[str, str] = {}

class BufferConfig(BaseModel):
    key: str
    wait_time: int = 15
    aggregate_field: str
    max_size: int = 10
    paths: Optional[Dict[str, str]] = None

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

# Redis connection
async def get_redis() -> Redis:
    """Get Redis connection"""
    if not hasattr(get_redis, "redis_client"):
        get_redis.redis_client = Redis(
            host='localhost',
            port=6379,
            decode_responses=True,
            encoding='utf-8'
        )
    return get_redis.redis_client

class BufferManager:
    def __init__(self, redis: Redis):
        self.redis = redis

    async def save_config(self, buffer_key: str, config: BufferConfig) -> None:
        """Guarda la configuración del buffer"""
        await self.redis.set(
            f"buffer:{buffer_key}:config",
            config.model_dump_json(),
            ex=config.wait_time * 2
        )

    async def get_config(self, buffer_key: str) -> Optional[BufferConfig]:
        """Recupera la configuración del buffer"""
        config_data = await self.redis.get(f"buffer:{buffer_key}:config")
        if config_data:
            return BufferConfig.model_validate_json(config_data)
        return None

    async def add_message(self, request: BufferRequest) -> bool:
        """Añade un mensaje al buffer y retorna True si el buffer está listo para ser procesado"""
        # Obtenemos el tamaño actual antes de añadir el mensaje
        current_size = await self.redis.llen(f"buffer:{request.buffer.key}:messages")
        
        # Si ya alcanzamos max_size, no añadimos más mensajes
        if current_size >= request.buffer.max_size:
            return True

        # Preparar el mensaje
        message = request.payload.copy()
        if "timestamp" not in message:
            message["timestamp"] = datetime.now(UTC).isoformat()

        # Guardamos todo usando pipeline
        async with self.redis.pipeline() as pipe:
            # Guardar configuración
            await pipe.set(
                f"buffer:{request.buffer.key}:config",
                request.buffer.model_dump_json(),
                ex=request.buffer.wait_time * 2
            )
            
            # Guardar mensaje
            await pipe.lpush(f"buffer:{request.buffer.key}:messages", json.dumps(message))
            
            # Actualizar timestamp
            await pipe.set(
                f"buffer:{request.buffer.key}:last_update",
                datetime.now(UTC).timestamp(),
                ex=request.buffer.wait_time * 2
            )
            
            await pipe.execute()

        # Verificar si alcanzamos max_size después de añadir
        new_size = await self.redis.llen(f"buffer:{request.buffer.key}:messages")
        return new_size >= request.buffer.max_size

    async def should_process_buffer(self, buffer_key: str) -> tuple[bool, Optional[BufferConfig]]:
        """Verifica si el buffer debe ser procesado"""
        config = await self.get_config(buffer_key)
        if not config:
            return False, None

        last_update = await self.redis.get(f"buffer:{buffer_key}:last_update")
        if not last_update:
            return False, config
        
        elapsed = datetime.now(UTC).timestamp() - float(last_update)
        return elapsed >= config.wait_time, config

    async def get_and_clear_buffer(self, buffer_key: str) -> tuple[List[Dict[str, Any]], Optional[BufferConfig]]:
        """Obtiene y limpia el buffer"""
        async with self.redis.pipeline() as pipe:
            # Obtener configuración y mensajes
            config = await self.get_config(buffer_key)
            messages = await self.redis.lrange(f"buffer:{buffer_key}:messages", 0, -1)
            
            # Limpiar todo
            await pipe.delete(f"buffer:{buffer_key}:messages")
            await pipe.delete(f"buffer:{buffer_key}:last_update")
            await pipe.delete(f"buffer:{buffer_key}:config")
            await pipe.execute()

        return [json.loads(msg) for msg in messages], config

async def get_by_path(obj: dict, path: str) -> Any:
    """Obtiene un valor de un diccionario usando dot notation"""
    try:
        return reduce(operator.getitem, path.split('.'), obj)
    except (KeyError, TypeError):
        logger.warning(f"Path {path} not found in object")
        return None

async def process_buffer(messages: List[Dict[str, Any]], config: BufferConfig, webhook: WebhookConfig) -> BufferResponse:
    """Procesa el buffer y envía los resultados al webhook"""
    try:
        # Extraer y agregar contenido según el path especificado
        aggregated = []
        for msg in messages:
            content = await get_by_path(msg, config.aggregate_field)
            if content:
                aggregated.append(str(content))

        # Crear respuesta
        response = BufferResponse(
            buffer_key=config.key,
            messages=messages,
            aggregated_content="\n".join(aggregated),
            metadata=BufferMetadata(
                total_messages=len(messages),
                first_message=messages[0].get("timestamp", ""),
                last_message=messages[-1].get("timestamp", ""),
                processed_at=datetime.now(UTC).isoformat(),
                wait_time_used=config.wait_time,
                buffer_key=config.key
            )
        )

        # Enviar al webhook
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method=webhook.method,
                url=str(webhook.url),
                json=response.model_dump(mode='json'),
                headers=webhook.headers
            ) as webhook_response:
                if webhook_response.status >= 400:
                    raise HTTPException(
                        status_code=webhook_response.status,
                        detail=f"Webhook request failed: {await webhook_response.text()}"
                    )
                
        return response

    except Exception as e:
        logger.error(f"Error processing buffer: {str(e)}")
        raise

# FastAPI app
app = FastAPI(title="Buffer Service")

@app.post("/message")
async def receive_message(
    request: BufferRequest,
    redis: Redis = Depends(get_redis)
) -> Dict[str, Any]:
    """Endpoint para recibir mensajes"""
    buffer_manager = BufferManager(redis)
    
    should_process = await buffer_manager.add_message(request)
    
    if should_process:
        messages, config = await buffer_manager.get_and_clear_buffer(request.buffer.key)
        if messages and config:
            await process_buffer(messages, config, request.webhook)
            return {
                "status": "processed",
                "message_count": len(messages),
                "reason": "max_size_reached"
            }
    
    return {"status": "buffered"}