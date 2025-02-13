import asyncio
from asyncio import Task
from contextlib import asynccontextmanager
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
import json

import httpx
from fastapi import HTTPException, status
from pydantic import BaseModel, Field, HttpUrl, ValidationError

import redis.asyncio as redis
from redis.commands.core import AsyncScript

UTC = timezone.utc
logger = logging.getLogger(__name__)


class WebhookConfig(BaseModel):
    """Configuration for the webhook."""
    url: HttpUrl
    method: str = Field(default="POST")
    headers: Dict[str, str] = Field(default={})


class BufferConfig(BaseModel):
    """Configuration for the buffer."""
    key: str
    wait_time: int = Field(gt=0)
    aggregate_field: str
    max_size: int = Field(gt=0)


class BufferRequest(BaseModel):
    """Request to add message to buffer."""
    webhook: WebhookConfig
    buffer: BufferConfig
    payload: Dict[str, Any]


class BufferMetadata(BaseModel):
    """Metadata about the processed buffer."""
    total_messages: int
    first_message: str
    last_message: str
    processed_at: str
    wait_time_used: int
    buffer_key: str


class BufferResponse(BaseModel):
    """Response from processing the buffer."""
    buffer_key: str
    messages: List[Dict[str, Any]]
    aggregated_content: str
    metadata: BufferMetadata


async def aggregate_messages(messages: List[Dict[str, Any]], aggregate_field: str) -> str:
    """Aggregate messages based on the configured field."""
    aggregated_list: List[str] = []
    for message in messages:
        try:
            value = message
            for key in aggregate_field.split('.'):
                if isinstance(value, dict):
                    value = value.get(key)
                elif isinstance(value, list):
                    try:
                        key_as_int = int(key)
                        value = value[key_as_int]
                    except (ValueError, IndexError):
                        value = None
                        break
                else:
                    value = None
                    break
            if value is not None:
                aggregated_list.append(str(value))
        except (KeyError, TypeError):
            logger.warning(f"Error accessing field {aggregate_field} in message")
            return ""
    return ", ".join(aggregated_list)


async def _prepare_response(
    messages: List[Dict[str, Any]],
    config: BufferConfig
) -> BufferResponse:
    """Prepara la respuesta de forma asíncrona."""
    aggregated_content = await aggregate_messages(messages, config.aggregate_field)
    
    return BufferResponse(
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


async def _send_webhook_request(
    client: httpx.AsyncClient,
    webhook: WebhookConfig,
    response: BufferResponse
) -> BufferResponse:
    """Envía la request al webhook con mejor manejo de errores."""
    try:
        webhook_response = await client.request(
            method=webhook.method,
            url=str(webhook.url),
            json=response.model_dump(mode='json'),
            headers=webhook.headers
        )
        webhook_response.raise_for_status()
        return response
        
    except httpx.TimeoutException as e:
        logger.error(f"Webhook timeout: {e}")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"Webhook timeout: {str(e)}"
        )
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
            detail=f"Webhook connection error: {str(e)}"
        )


async def process_buffer(
    messages: List[Dict[str, Any]],
    config: BufferConfig,
    webhook: WebhookConfig
) -> BufferResponse:
    """Process messages with improved concurrency handling."""
    try:
        response = await _prepare_response(messages, config)
        
        timeout = httpx.Timeout(
            connect=5.0,    # Tiempo para conectar
            read=10.0,      # Tiempo para leer la respuesta
            write=5.0,      # Tiempo para escribir el request
            pool=2.0        # Tiempo para obtener una conexión del pool
        )

        # Usar AsyncClient con mejor configuración
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            http2=True,
            limits=httpx.Limits(max_keepalive_connections=5)
        ) as client:
            return await _send_webhook_request(client, webhook, response)
    except Exception as e:
        logger.error(f"Error processing buffer: {str(e)}")
        raise


class BufferManager:
    """Manages the buffer using Redis."""
    _get_and_clear_script: Optional[str] = None
    _processing_tasks: Dict[str, Task] = {}  # Tracking de tareas por buffer_key
    _active_buffers: Set[str] = set()  # Control de buffers activos
    
    GET_AND_CLEAR_BUFFER_SCRIPT = """
    local messages = redis.call('LRANGE', KEYS[1], 0, -1)
    if #messages > 0 then
        redis.call('DEL', KEYS[1])
        return messages
    else
        return {}
    end
    """

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        """Initialize the BufferManager with Redis connection."""
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None
        self._lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._get_and_clear_script: Optional[str] = None  # Instancia
        self._processing_tasks: Dict[str, Task] = {}       # Instancia
        self._active_buffers: Set[str] = set()             # Instancia


    @asynccontextmanager
    async def get_redis(self):
        """Manejo seguro de la conexión Redis."""
        if self.redis is None:
            self.redis = redis.from_url(self.redis_url)
        try:
            yield self.redis
        except redis.RedisError as e:
            logger.error(f"Redis error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

    async def load_scripts(self) -> None:
        """Loads the Lua scripts into Redis."""
        if self._get_and_clear_script is None:
            async with self.get_redis() as redis:
                self._get_and_clear_script = await redis.script_load(
                    self.GET_AND_CLEAR_BUFFER_SCRIPT
                )

    async def save_config(self, config: BufferConfig) -> None:
        """Saves the buffer configuration."""
        try:
            config_key = f"config:{config.key}"
            config_data = config.model_dump_json().encode()
            async with self.get_redis() as redis:
                await redis.set(config_key, config_data)
        except Exception as e:
            logger.error(f"Error saving config: {str(e)}")
            raise

    async def get_config(self, key: str) -> Optional[BufferConfig]:
        """Retrieves the buffer configuration."""
        try:
            config_key = f"config:{key}"
            async with self.get_redis() as redis:
                config_data = await redis.get(config_key)
            if config_data:
                return BufferConfig.model_validate_json(config_data.decode())
            return None
        except ValidationError:
            logger.warning(f"Invalid config data for key {key}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving config: {str(e)}")
            return None

    async def add_message(self, request: BufferRequest) -> None:
        print("Starting add_message")
        try:
            async with self.get_redis() as redis_conn:
                # Usamos el pipeline para agrupar las operaciones
                async with redis_conn.pipeline() as pipe:
                    pipe.set(
                        f"config:{request.buffer.key}",
                        request.buffer.model_dump_json().encode()
                    )
                    message_data = {
                        "payload": request.payload,
                        "timestamp": datetime.now(UTC).isoformat(),
                    }
                    pipe.rpush(
                        request.buffer.key,
                        json.dumps(message_data).encode()
                    )
                    pipe.llen(request.buffer.key)
                    # Ejecutamos el pipeline y obtenemos los resultados
                    results = await pipe.execute()
                    # results = [resultado_set, resultado_rpush, current_size]
                    current_size = results[2]
                    print(f"ADD_MESSAGE: current_size: {current_size}")

                    if current_size >= request.buffer.max_size:
                        print("ADD_MESSAGE: Max size condition TRUE - calling process_and_send")
                        await self.process_and_send(request.buffer.key)
                        return
                    else:
                        print("ADD_MESSAGE: Max size condition FALSE - not calling process_and_send")
        except Exception as e:
            print(f"Error in add_message: {str(e)}")
            raise
        finally:
            print("Exiting add_message")

        
    def _cleanup_task(self, buffer_key: str, task: Task) -> None:
        """Limpieza de recursos cuando una tarea termina."""
        self._processing_tasks.pop(buffer_key, None)
        try:
            if exc := task.exception():
                logger.error(f"Task for buffer {buffer_key} failed: {exc}")
        except asyncio.CancelledError:
            pass

    async def schedule_processing(self, buffer_key: str, wait_time: int) -> None:
        """Programa el procesamiento del buffer de forma segura."""
        async with self._lock:
            if buffer_key in self._active_buffers:
                # Ya hay un procesamiento programado
                return

            self._active_buffers.add(buffer_key)
            
        try:
            task = asyncio.create_task(
                self._delayed_processing(buffer_key, wait_time)
            )
            self._processing_tasks[buffer_key] = task
            
            # Asegurar limpieza cuando la tarea termine
            task.add_done_callback(
                lambda t: self._cleanup_task(buffer_key, t)
            )
        except Exception as e:
            logger.error(f"Error scheduling processing: {e}")
            self._active_buffers.discard(buffer_key)
            raise

    async def _delayed_processing(self, buffer_key: str, wait_time: int) -> None:
        """Manejo mejorado del procesamiento retrasado."""
        try:
            # Usar wait_for para manejar cancelación
            await asyncio.wait_for(
                asyncio.sleep(wait_time),
                timeout=wait_time + 1  # Pequeño margen
            )

            async with self.get_redis() as redis:
                # Verificar condiciones atomicamente
                exists = await redis.exists(buffer_key)
                size = await redis.llen(buffer_key) if exists else 0
                
                if exists and size > 0:
                    await self.process_and_send(buffer_key)
                
        except asyncio.CancelledError:
            logger.info(f"Processing cancelled for buffer {buffer_key}")
            raise
        except asyncio.TimeoutError:
            logger.error(f"Processing timeout for buffer {buffer_key}")
        except Exception as e:
            logger.error(f"Error in delayed processing: {e}")
        finally:
            async with self._lock:
                self._active_buffers.discard(buffer_key)

    async def process_and_send(self, buffer_key: str) -> None:
        # Adquirir el lock para realizar la verificación de forma atómica.
        async with self._lock:
            if buffer_key in self._active_buffers:
                return  # Ya se está procesando este buffer.
            self._active_buffers.add(buffer_key)
        try:
            config = await self.get_config(buffer_key)
            if not config:
                return

            messages = await self.get_and_clear_buffer(buffer_key)
            if not messages:
                return

            if task := self._processing_tasks.get(buffer_key):
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            async with httpx.AsyncClient(timeout=10.0) as client:
                await self._send_to_webhook(messages, config, client)
        except Exception as e:
            logger.error(f"Error processing buffer: {e}")
            raise
        finally:
            async with self._lock:
                self._active_buffers.discard(buffer_key)


    async def get_and_clear_buffer(self, buffer_key: str) -> Optional[List[bytes]]:
        """Atomically gets and clears the buffer using a Lua script."""
        try:
            await self.load_scripts()
            if self._get_and_clear_script:
                async with self.get_redis() as redis:
                    messages = await redis.evalsha(
                        self._get_and_clear_script,
                        keys=[buffer_key]
                    )
                    return messages if messages else []
            return None
        except Exception as e:
            logger.error(f"Error getting and clearing buffer: {str(e)}")
            raise

    async def clear_buffer(self, buffer_key: str) -> None:
        """Clears the buffer and its configuration."""
        try:
            async with self.get_redis() as redis:
                async with redis.pipeline() as pipe:
                    await pipe.delete(buffer_key)
                    await pipe.delete(f"config:{buffer_key}")
                    await pipe.execute()
        except Exception as e:
            logger.error(f"Error clearing buffer: {str(e)}")
            raise

    async def shutdown(self) -> None:
        """Limpieza ordenada de recursos."""
        self._shutdown_event.set()
        
        # Cancelar todas las tareas pendientes
        tasks = list(self._processing_tasks.values())
        for task in tasks:
            task.cancel()
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Cerrar conexión Redis
        if self.redis:
            await self.redis.close()
            self.redis = None