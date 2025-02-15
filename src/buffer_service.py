from enum import Enum
import os
import asyncio
import json
import logging
from asyncio import Task
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
import random

from prometheus_client import Counter, Gauge, generate_latest
from fastapi.responses import JSONResponse, PlainTextResponse
import httpx
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ConfigDict, BaseModel, Field, HttpUrl, ValidationError
from dotenv import load_dotenv
import tracemalloc

# Cargar variables del .env
load_dotenv()

# Constantes y configuración de logging
UTC = timezone.utc
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Iniciar tracemalloc para debug de memoria
tracemalloc.start(25)  # Capturar 25 frames

# =============================
# Modelos Pydantic
# =============================
class WebhookConfig(BaseModel):
    """Configuración para el webhook."""
    url: HttpUrl
    method: str = Field(default="POST")
    headers: Dict[str, str] = Field(default={})


class BufferConfig(BaseModel):
    """Configuración para el buffer."""
    key: str = Field(..., min_length=1, max_length=255)
    wait_time: int = Field(gt=0, le=3600)  # máximo 1 hora
    aggregate_field: str = Field(..., min_length=1, max_length=255)
    max_size: int = Field(gt=0, le=10000)  # máximo 10000 mensajes
    max_message_size: int = Field(default=1048576)  # 1MB por defecto
    webhook: Optional[WebhookConfig] = None
    webhook_timeout: int = Field(default=30, gt=0, le=300)  # timeout en segundos, máximo 5 minutos
    webhook_retries: int = Field(default=3, ge=0, le=5)  # número de reintentos, máximo 5
    model_config = ConfigDict(json_schema_extra={
        "example": {
            "key": "my_buffer",
            "wait_time": 60,
            "aggregate_field": "data.message",
            "max_size": 100,
            "max_message_size": 1048576,
            "webhook_timeout": 30,
            "webhook_retries": 3
        }
    })


class BufferRequest(BaseModel):
    """Request para añadir un mensaje al buffer."""
    webhook: WebhookConfig
    buffer: BufferConfig
    payload: Dict[str, Any]


class BufferMetadata(BaseModel):
    """Metadata sobre el buffer procesado."""
    total_messages: int
    first_message: str
    last_message: str
    processed_at: str
    wait_time_used: int
    buffer_key: str


class BufferResponse(BaseModel):
    """Respuesta del procesamiento del buffer."""
    buffer_key: str
    messages: List[Dict[str, Any]]
    aggregated_content: str
    metadata: BufferMetadata

class ErrorCode(Enum):
    BUFFER_FULL = "BUFFER_FULL"
    WEBHOOK_FAILED = "WEBHOOK_FAILED"
    REDIS_ERROR = "REDIS_ERROR"
    INVALID_CONFIG = "INVALID_CONFIG"

class DetailedHTTPException(HTTPException):
    def __init__(
        self,
        status_code: int,
        error_code: ErrorCode,
        detail: str,
        headers: Optional[dict] = None
    ):
        super().__init__(
            status_code=status_code,
            detail={
                "error_code": error_code.value,
                "message": detail
            },
            headers=headers
        )


# =============================
# Funciones auxiliares
# =============================
async def aggregate_messages(messages: List[Dict[str, Any]], aggregate_field: str) -> str:
    """Agrega los mensajes según el campo configurado."""
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
            logger.warning(f"Error accediendo al campo {aggregate_field} en el mensaje")
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
    response: BufferResponse,
    config: BufferConfig
) -> BufferResponse:
    """Envía la request al webhook con manejo de errores y reintentos."""
    max_retries = config.webhook_retries
    base_delay = 1  # Delay base de 1 segundo

    for attempt in range(max_retries + 1):
        try:
            webhook_response = await client.request(
                method=webhook.method,
                url=str(webhook.url),
                json=response.model_dump(mode="json"),
                headers=webhook.headers,
                timeout=config.webhook_timeout
            )
            result = webhook_response.raise_for_status()
            if hasattr(result, '__await__'):
                await result
            MESSAGES_PROCESSED.inc()
            return response
        except httpx.TimeoutException as e:
            logger.error(f"Webhook timeout (attempt {attempt + 1}/{max_retries + 1}): {e}")
            WEBHOOK_FAILURES.inc()
            if attempt == max_retries:
                raise DetailedHTTPException(
                    status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                    error_code=ErrorCode.WEBHOOK_FAILED,
                    detail=f"Webhook timeout after {max_retries + 1} attempts: {str(e)}"
                )
        except httpx.HTTPStatusError as e:
            logger.error(f"Webhook failed (attempt {attempt + 1}/{max_retries + 1}): {e.response.status_code} - {e.response.text}")
            WEBHOOK_FAILURES.inc()
            if attempt == max_retries:
                raise DetailedHTTPException(
                    status_code=e.response.status_code,
                    error_code=ErrorCode.WEBHOOK_FAILED,
                    detail=f"Webhook error after {max_retries + 1} attempts: {e.response.text}"
                )
        except httpx.RequestError as e:
            logger.error(f"Webhook request failed (attempt {attempt + 1}/{max_retries + 1}): {e}")
            WEBHOOK_FAILURES.inc()
            if attempt == max_retries:
                raise DetailedHTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    error_code=ErrorCode.WEBHOOK_FAILED,
                    detail=f"Webhook connection error after {max_retries + 1} attempts: {str(e)}"
                )
        
        # Backoff exponencial con jitter
        if attempt < max_retries:
            delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), 60)  # máximo 60 segundos
            logger.info(f"Retrying webhook in {delay:.2f} seconds...")
            await asyncio.sleep(delay)


async def process_buffer(
    messages: List[Dict[str, Any]],
    config: BufferConfig,
    webhook: WebhookConfig,
    client: Optional[httpx.AsyncClient] = None
) -> BufferResponse:
    """
    Procesa los mensajes, prepara la respuesta y envía el webhook.
    Si se provee un cliente httpx, se utiliza; de lo contrario, se crea uno.
    """
    response = await _prepare_response(messages, config)
    timeout = httpx.Timeout(
        connect=5.0,
        read=10.0,
        write=5.0,
        pool=2.0
    )
    if client is None:
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            http2=True,
            limits=httpx.Limits(max_keepalive_connections=5)
        ) as client:
            return await _send_webhook_request(client, webhook, response, config)
    else:
        return await _send_webhook_request(client, webhook, response, config)


# =============================
# Clase BufferManager
# =============================
class BufferManager:
    """Maneja el buffer utilizando Redis."""
    GET_AND_CLEAR_BUFFER_SCRIPT = """
        local key = KEYS[1]
        local backup_key = KEYS[2]
        local messages = redis.call('LRANGE', key, 0, -1)
        if #messages > 0 then
            redis.call('DEL', backup_key)  -- Limpiar backup anterior si existe
            redis.call('RPUSH', backup_key, unpack(messages))  -- Crear backup
            redis.call('PEXPIRE', backup_key, 300000)  -- Expira en 5 minutos
            redis.call('DEL', key)  -- Borrar buffer original
            return messages
        else
            return {}
        end
        """

    COMMIT_BACKUP_SCRIPT = """
        local backup_key = KEYS[1]
        redis.call('DEL', backup_key)
        return true
        """

    RESTORE_FROM_BACKUP_SCRIPT = """
        local backup_key = KEYS[1]
        local original_key = KEYS[2]
        local messages = redis.call('LRANGE', backup_key, 0, -1)
        if #messages > 0 then
            redis.call('DEL', original_key)
            redis.call('RPUSH', original_key, unpack(messages))
            redis.call('DEL', backup_key)
            return messages
        else
            return {}
        end
        """

    def __init__(self, redis_url: Optional[str] = None):
        if redis_url is None:
            redis_host = os.getenv("REDIS_HOST", "localhost")
            redis_port = os.getenv("REDIS_PORT", "6379")
            redis_db = os.getenv("REDIS_DB", "0")
            redis_password = os.getenv("REDIS_PASSWORD", "")
            redis_ssl = os.getenv("REDIS_SSL", "false").lower() == "true"
            scheme = "rediss" if redis_ssl else "redis"
            if redis_password:
                redis_url = f"{scheme}://:{redis_password}@{redis_host}:{redis_port}/{redis_db}"
            else:
                redis_url = f"{scheme}://{redis_host}:{redis_port}/{redis_db}"
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None
        self._lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._get_and_clear_script: Optional[str] = None
        self._commit_backup_script: Optional[str] = None
        self._restore_from_backup_script: Optional[str] = None
        self._processing_tasks: Dict[str, Task] = {}
        self._active_buffers: Set[str] = set()
        self._scheduled_buffers: Set[str] = set()
        self._processing_buffers: Set[str] = set()

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
        """Carga los scripts Lua en Redis."""
        if self._get_and_clear_script is None:
            async with self.get_redis() as redis_conn:
                self._get_and_clear_script = await redis_conn.script_load(
                    self.GET_AND_CLEAR_BUFFER_SCRIPT
                )
                self._commit_backup_script = await redis_conn.script_load(
                    self.COMMIT_BACKUP_SCRIPT
                )
                self._restore_from_backup_script = await redis_conn.script_load(
                    self.RESTORE_FROM_BACKUP_SCRIPT
                )

    async def save_config(self, config: BufferConfig) -> None:
        """Guarda la configuración del buffer."""
        try:
            config_key = f"config:{config.key}"
            config_data = config.model_dump_json().encode()
            async with self.get_redis() as redis_conn:
                await redis_conn.set(config_key, config_data)
        except Exception as e:
            logger.error(f"Error saving config: {str(e)}")
            raise

    async def get_config(self, key: str) -> Optional[BufferConfig]:
        """Recupera la configuración del buffer."""
        try:
            config_key = f"config:{key}"
            async with self.get_redis() as redis_conn:
                config_data = await redis_conn.get(config_key)
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
        """Añade un mensaje al buffer con validación de tamaño."""
        logger.info("Starting add_message")
        try:
            # Validar tamaño del mensaje
            message_size = len(json.dumps(request.payload).encode())
            if message_size > request.buffer.max_message_size:
                BUFFER_ERRORS.labels(error_type="message_too_large").inc()
                raise DetailedHTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    error_code=ErrorCode.INVALID_CONFIG,
                    detail=f"Message size ({message_size} bytes) exceeds maximum allowed size ({request.buffer.max_message_size} bytes)"
                )

            MESSAGE_SIZE.inc(message_size)
            BUFFER_OPERATIONS.labels(operation="add_message").inc()

            async with self.get_redis() as redis_conn:
                async with redis_conn.pipeline() as pipe:
                    # Guardar configuración (incluyendo webhook, si se provee)
                    await pipe.set(
                        f"config:{request.buffer.key}",
                        request.buffer.model_dump_json().encode()
                    )
                    message_data = {
                        "payload": request.payload,
                        "timestamp": datetime.now(UTC).isoformat(),
                        "size": message_size
                    }
                    await pipe.rpush(
                        request.buffer.key,
                        json.dumps(message_data).encode()
                    )
                    await pipe.llen(request.buffer.key)
                    results = await pipe.execute()
                    current_size = results[2]
                    logger.info(f"ADD_MESSAGE: current_size: {current_size}")
                    
                    # Actualizar la métrica por key
                    ACTIVE_BUFFERS.labels(buffer_key=request.buffer.key).set(current_size)
                    
                    if current_size >= request.buffer.max_size:
                        logger.info("ADD_MESSAGE: Tamaño máximo alcanzado - llamando process_and_send")
                        await self.process_and_send(request.buffer.key)
                        return
                    else:
                        # Programar procesamiento si no está ya programado
                        await self.schedule_processing(request.buffer.key, request.buffer.wait_time)
                        logger.info("ADD_MESSAGE: Procesamiento programado")
        except redis.RedisError as e:
            BUFFER_ERRORS.labels(error_type="redis_error").inc()
            logger.error(f"Redis error in add_message: {e}")
            raise DetailedHTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                error_code=ErrorCode.REDIS_ERROR,
                detail=f"Redis error: {str(e)}"
            )
        except Exception as e:
            BUFFER_ERRORS.labels(error_type="unknown").inc()
            logger.error(f"Error in add_message: {str(e)}")
            raise
        finally:
            logger.info("Exiting add_message")

    def _cleanup_task(self, buffer_key: str, task: Task) -> None:
        """Limpieza de recursos cuando una tarea termina."""
        try:
            if not task.cancelled():
                exc = task.exception()
                if exc:
                    logger.error(f"Task for buffer {buffer_key} failed: {exc}")
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            pass
        finally:
            # Crear una tarea asíncrona para la limpieza bajo lock
            asyncio.create_task(self._async_cleanup(buffer_key))
            logger.info(f"Iniciada limpieza asíncrona para buffer {buffer_key}")

    async def _async_cleanup(self, buffer_key: str) -> None:
        """Limpieza asíncrona de recursos bajo lock."""
        async with self._lock:
            self._processing_tasks.pop(buffer_key, None)
            self._scheduled_buffers.discard(buffer_key)
            self._processing_buffers.discard(buffer_key)
            self._active_buffers.discard(buffer_key)
            logger.info(f"Completada limpieza asíncrona para buffer {buffer_key}")

    async def schedule_processing(self, buffer_key: str, wait_time: int) -> None:
        """Programa el procesamiento del buffer de forma segura."""
        logger.info(f"Intentando programar procesamiento para buffer {buffer_key}")
        async with self._lock:
            # Verificar si ya está programado o en procesamiento
            if buffer_key in self._scheduled_buffers:
                logger.info(f"Buffer {buffer_key} ya está programado")
                return
            if buffer_key in self._processing_buffers:
                logger.info(f"Buffer {buffer_key} está siendo procesado")
                return
            
            # Marcar como programado y activo
            self._scheduled_buffers.add(buffer_key)
            self._active_buffers.add(buffer_key)
            logger.info(f"Buffer {buffer_key} marcado como programado y activo")

        try:
            if self._shutdown_event.is_set():
                logger.info("No se programa procesamiento - apagado en progreso")
                return

            task = asyncio.create_task(
                self._delayed_processing(buffer_key, wait_time)
            )
            self._processing_tasks[buffer_key] = task
            task.add_done_callback(lambda t: self._cleanup_task(buffer_key, t))
            logger.info(f"Tarea de procesamiento creada para buffer {buffer_key}")
        except Exception as e:
            logger.error(f"Error al programar procesamiento: {e}")
            async with self._lock:
                self._scheduled_buffers.discard(buffer_key)
                self._active_buffers.discard(buffer_key)
            raise

    async def _delayed_processing(self, buffer_key: str, wait_time: int) -> None:
        """Manejo del procesamiento retrasado."""
        try:
            if self._shutdown_event.is_set():
                logger.info(f"Omitiendo procesamiento retrasado para {buffer_key} - apagado en progreso")
                return

            logger.info(f"Iniciando espera de {wait_time} segundos para buffer {buffer_key}")
            try:
                await asyncio.sleep(wait_time)
            except (asyncio.CancelledError, GeneratorExit):
                logger.info(f"Espera cancelada o finalizada para buffer {buffer_key}")
                return

            if self._shutdown_event.is_set():
                return

            # Remover de scheduled_buffers bajo lock
            async with self._lock:
                self._scheduled_buffers.discard(buffer_key)
                logger.info(f"Buffer {buffer_key} removido de scheduled_buffers")

            # Verificar si el buffer existe y tiene mensajes
            try:
                async with self.get_redis() as redis_conn:
                    exists = await redis_conn.exists(buffer_key)
                    if exists and exists > 0:  # Redis devuelve un entero, no un booleano
                        size = await redis_conn.llen(buffer_key)
                        if size > 0:
                            logger.info(f"Buffer {buffer_key} tiene {size} mensajes, iniciando procesamiento")
                            await self.process_and_send(buffer_key)
                        else:
                            logger.info(f"Buffer {buffer_key} está vacío")
                    else:
                        logger.info(f"Buffer {buffer_key} no existe")
            except redis.RedisError as e:
                logger.error(f"Error de Redis al verificar buffer {buffer_key}: {e}")
                raise
        except Exception as e:
            logger.error(f"Error en procesamiento retrasado: {e}")
        finally:
            self._processing_tasks.pop(buffer_key, None)
            self._active_buffers.discard(buffer_key)
            logger.info(f"Limpieza final completada para buffer {buffer_key}")

    async def process_and_send(self, buffer_key: str) -> None:
        """Procesa el buffer y envía la información al webhook con manejo de backup."""
        logger.info(f"Iniciando process_and_send para buffer {buffer_key}")
        
        async with self._lock:
            if buffer_key in self._processing_buffers:
                logger.info(f"Buffer {buffer_key} ya está siendo procesado")
                return
            self._processing_buffers.add(buffer_key)
            self._active_buffers.add(buffer_key)
            logger.info(f"Buffer {buffer_key} marcado como en procesamiento")
            
        try:
            config = await self.get_config(buffer_key)
            if not config:
                logger.warning(f"No se encontró configuración para buffer {buffer_key}")
                return

            messages = await self.get_and_clear_buffer(buffer_key)
            if not messages:
                logger.info(f"No se encontraron mensajes en buffer {buffer_key}")
                return

            try:
                async with httpx.AsyncClient(timeout=config.webhook_timeout) as client:
                    await self._send_to_webhook(messages, config, client)
                await self.commit_backup(buffer_key)
                ACTIVE_BUFFERS.labels(buffer_key=buffer_key).set(0)
                MESSAGES_PROCESSED.inc(len(messages))
                logger.info(f"Procesamiento exitoso para buffer {buffer_key}")
            except Exception as e:
                logger.error(f"Error procesando mensajes, intentando restaurar: {e}")
                await self.restore_from_backup(buffer_key)
                async with self.get_redis() as redis_conn:
                    size = await redis_conn.llen(buffer_key)
                    ACTIVE_BUFFERS.labels(buffer_key=buffer_key).set(size)
                WEBHOOK_FAILURES.inc()
                raise
        except Exception as e:
            logger.error(f"Error procesando buffer: {e}")
            raise
        finally:
            async with self._lock:
                self._processing_buffers.discard(buffer_key)
                self._active_buffers.discard(buffer_key)
            logger.info(f"Finalizado process_and_send para buffer {buffer_key}")

    async def get_and_clear_buffer(self, buffer_key: str) -> Optional[List[bytes]]:
        """Recupera y limpia atómicamente el buffer usando un script Lua con backup."""
        try:
            await self.load_scripts()
            if self._get_and_clear_script:
                async with self.get_redis() as redis_conn:
                    backup_key = f"backup:{buffer_key}"
                    messages = await redis_conn.evalsha(
                        self._get_and_clear_script,
                        2,  # número de keys
                        buffer_key,  # la key original
                        backup_key   # la key de backup
                    )
                    return messages if messages else []
            return None
        except Exception as e:
            logger.error(f"Error getting and clearing buffer: {str(e)}")
            raise

    async def commit_backup(self, buffer_key: str) -> None:
        """Confirma que el procesamiento fue exitoso y elimina el backup."""
        try:
            if self._commit_backup_script:
                async with self.get_redis() as redis_conn:
                    backup_key = f"backup:{buffer_key}"
                    await redis_conn.evalsha(
                        self._commit_backup_script,
                        1,  # número de keys
                        backup_key
                    )
        except Exception as e:
            logger.error(f"Error committing backup: {str(e)}")

    async def restore_from_backup(self, buffer_key: str) -> Optional[List[bytes]]:
        """Restaura el buffer desde el backup en caso de error."""
        try:
            if self._restore_from_backup_script:
                async with self.get_redis() as redis_conn:
                    backup_key = f"backup:{buffer_key}"
                    messages = await redis_conn.evalsha(
                        self._restore_from_backup_script,
                        2,  # número de keys
                        backup_key,
                        buffer_key
                    )
                    return messages if messages else []
            return None
        except Exception as e:
            logger.error(f"Error restoring from backup: {str(e)}")
            return None

    async def _send_to_webhook(
        self, messages: List[bytes], config: BufferConfig, client: httpx.AsyncClient
    ) -> None:
        """
        Decodifica los mensajes y llama a process_buffer usando el cliente provisto.
        Se requiere que en la configuración esté definido el webhook.
        """
        if not config.webhook:
            logger.error("No hay configuración de webhook en el buffer")
            return
        try:
            decoded_messages = [json.loads(m.decode()) for m in messages]
            response = await process_buffer(decoded_messages, config, config.webhook, client)
            if hasattr(response, 'raise_for_status'):
                result = response.raise_for_status()
                if hasattr(result, '__await__'):
                    await result
        except Exception as e:
            logger.error(f"Error enviando al webhook: {e}")
            raise

    async def clear_buffer(self, buffer_key: str) -> None:
        """Limpia el buffer y su configuración."""
        try:
            async with self.get_redis() as redis_conn:
                async with redis_conn.pipeline() as pipe:
                    await pipe.delete(buffer_key)
                    await pipe.delete(f"config:{buffer_key}")
                    await pipe.execute()
                    # Actualizar la métrica cuando se limpia el buffer
                    ACTIVE_BUFFERS.labels(buffer_key=buffer_key).set(0)
        except Exception as e:
            logger.error(f"Error clearing buffer: {str(e)}")
            raise

    async def shutdown(self) -> None:
        """Limpieza ordenada de recursos."""
        logger.info("Starting shutdown...")
        self._shutdown_event.set()
        
        # Cancelar todas las tareas pendientes
        tasks = list(self._processing_tasks.values())
        if tasks:
            for task in tasks:
                if not task.done():
                    task.cancel()
            
            # Esperar a que todas las tareas se completen o cancelen
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                logger.error(f"Error waiting for tasks during shutdown: {e}")
            finally:
                self._processing_tasks.clear()
                self._active_buffers.clear()

        # Cerrar la conexión Redis
        if self.redis:
            try:
                await self.redis.close()
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")
            finally:
                self.redis = None
        
        logger.info("Shutdown complete")


# Instancia global de BufferManager
buffer_manager = BufferManager()


# =============================
# FastAPI: Lifespan y Endpoints
# =============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Contexto de vida de la aplicación."""
    logger.info("Iniciando Buffer Service...")
    await buffer_manager.load_scripts()
    yield  # La aplicación se ejecuta aquí
    logger.info("Cerrando Buffer Service...")
    await buffer_manager.shutdown()


app = FastAPI(
    title="Buffer Service",
    description="Servicio de buffering para mensajes con agregación y webhooks",
    version="1.0.0",
    lifespan=lifespan
)

# Configuración de CORS (en producción, limitar los orígenes)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(DetailedHTTPException)
async def detailed_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": "error",
            "error": exc.detail
        }
    )

@app.post("/message", response_model=Dict[str, Any])
async def add_message(request: BufferRequest) -> Dict[str, Any]:
    """
    Añade un mensaje al buffer.
    El mensaje se procesa cuando se alcanza el tamaño máximo o transcurre el tiempo configurado.
    """
    try:
        await buffer_manager.add_message(request)
        return {
            "status": "success",
            "message": "Mensaje añadido al buffer",
            "buffer_key": request.buffer.key
        }
    except Exception as e:
        logger.error(f"Error al añadir mensaje: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get("/health")
async def health_check() -> Dict[str, str]:
    """Endpoint para health check."""
    return {"status": "healthy"}

# Métricas Prometheus mejoradas
MESSAGES_PROCESSED = Counter('buffer_messages_processed_total', 'Total de mensajes procesados')
ACTIVE_BUFFERS = Gauge('buffer_messages_count', 'Número de mensajes en el buffer', ['buffer_key'])
PROCESSING_TIME = Counter('buffer_processing_time_seconds', 'Tiempo de procesamiento', ['buffer_key'])
WEBHOOK_FAILURES = Counter('buffer_webhook_failures_total', 'Total de fallos en webhooks')
MESSAGE_SIZE = Counter('buffer_message_size_bytes', 'Tamaño de los mensajes en bytes')
BUFFER_OPERATIONS = Counter('buffer_operations_total', 'Total de operaciones del buffer', ['operation'])
BUFFER_ERRORS = Counter('buffer_errors_total', 'Total de errores', ['error_type'])

@app.get("/metrics")
async def metrics() -> PlainTextResponse:
    """Endpoint mejorado para métricas en formato Prometheus."""
    # Actualizar las métricas de buffers activos
    async with buffer_manager.get_redis() as redis_conn:
        # Obtener todas las keys que son buffers (excluyendo las de configuración y backup)
        keys = await redis_conn.keys("*")
        buffer_keys = [k.decode() for k in keys if not k.decode().startswith(("config:", "backup:"))]
        
        # Actualizar el tamaño de cada buffer
        for key in buffer_keys:
            size = await redis_conn.llen(key)
            ACTIVE_BUFFERS.labels(buffer_key=key).set(size)
    
    return PlainTextResponse(generate_latest())

@app.post("/log-level")
async def set_log_level(level: str) -> Dict[str, str]:
    """Cambiar el nivel de logging en tiempo de ejecución."""
    try:
        level = level.upper()
        logging.getLogger().setLevel(getattr(logging, level))
        return {"status": "success", "message": f"Nivel de logging cambiado a {level}"}
    except (AttributeError, ValueError) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Nivel de logging inválido. Use: DEBUG, INFO, WARNING, ERROR, o CRITICAL"
        )