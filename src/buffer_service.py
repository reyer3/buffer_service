from enum import Enum
import os
import asyncio
import json
import logging
from asyncio import Task
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from prometheus_client import Counter, Gauge, generate_latest
from fastapi.responses import PlainTextResponse
import httpx
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, HttpUrl, ValidationError
from dotenv import load_dotenv

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
    key: str
    wait_time: int = Field(gt=0)
    aggregate_field: str
    max_size: int = Field(gt=0)
    # Opcionalmente se puede incluir el webhook
    webhook: Optional[WebhookConfig] = None


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
    response: BufferResponse
) -> BufferResponse:
    """Envía la request al webhook con manejo de errores."""
    try:
        webhook_response = await client.request(
            method=webhook.method,
            url=str(webhook.url),
            json=response.model_dump(mode="json"),
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
            return await _send_webhook_request(client, webhook, response)
    else:
        return await _send_webhook_request(client, webhook, response)


# =============================
# Clase BufferManager
# =============================
class BufferManager:
    """Maneja el buffer utilizando Redis."""
    GET_AND_CLEAR_BUFFER_SCRIPT = """
    local messages = redis.call('LRANGE', KEYS[1], 0, -1)
    if #messages > 0 then
        redis.call('DEL', KEYS[1])
        return messages
    else
        return {}
    end
    """

    def __init__(self, redis_url: Optional[str] = None):
        # Si no se provee un redis_url, se construye a partir del .env
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
        self._processing_tasks: Dict[str, Task] = {}
        self._active_buffers: Set[str] = set()

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
        logger.info("Starting add_message")
        try:
            async with self.get_redis() as redis_conn:
                async with redis_conn.pipeline() as pipe:
                    # Guardar configuración (incluyendo webhook, si se provee)
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
                    results = await pipe.execute()
                    current_size = results[2]
                    logger.info(f"ADD_MESSAGE: current_size: {current_size}")
                    if current_size >= request.buffer.max_size:
                        logger.info("ADD_MESSAGE: Tamaño máximo alcanzado - llamando process_and_send")
                        await self.process_and_send(request.buffer.key)
                        return
                    else:
                        logger.info("ADD_MESSAGE: Tamaño máximo no alcanzado")
        except Exception as e:
            logger.error(f"Error in add_message: {str(e)}")
            raise
        finally:
            logger.info("Exiting add_message")

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
                return  # Procesamiento ya programado
            self._active_buffers.add(buffer_key)
        try:
            task = asyncio.create_task(
                self._delayed_processing(buffer_key, wait_time)
            )
            self._processing_tasks[buffer_key] = task
            task.add_done_callback(lambda t: self._cleanup_task(buffer_key, t))
        except Exception as e:
            logger.error(f"Error scheduling processing: {e}")
            self._active_buffers.discard(buffer_key)
            raise

    async def _delayed_processing(self, buffer_key: str, wait_time: int) -> None:
        """Manejo del procesamiento retrasado."""
        try:
            await asyncio.wait_for(asyncio.sleep(wait_time), timeout=wait_time + 1)
            async with self.get_redis() as redis_conn:
                exists = await redis_conn.exists(buffer_key)
                size = await redis_conn.llen(buffer_key) if exists else 0
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
        """Procesa el buffer y envía la información al webhook."""
        async with self._lock:
            if buffer_key in self._active_buffers:
                return  # Buffer ya en proceso
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
        """Recupera y limpia atómicamente el buffer usando un script Lua."""
        try:
            await self.load_scripts()
            if self._get_and_clear_script:
                async with self.get_redis() as redis_conn:
                    messages = await redis_conn.evalsha(
                        self._get_and_clear_script,
                        keys=[buffer_key]
                    )
                    return messages if messages else []
            return None
        except Exception as e:
            logger.error(f"Error getting and clearing buffer: {str(e)}")
            raise

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
            await process_buffer(decoded_messages, config, config.webhook, client)
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
        except Exception as e:
            logger.error(f"Error clearing buffer: {str(e)}")
            raise

    async def shutdown(self) -> None:
        """Limpieza ordenada de recursos."""
        self._shutdown_event.set()
        tasks = list(self._processing_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self.redis:
            await self.redis.close()
            self.redis = None


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

# Métricas Prometheus
MESSAGES_PROCESSED = Counter('buffer_messages_processed_total', 'Total de mensajes procesados')
ACTIVE_BUFFERS = Gauge('buffer_active_buffers', 'Número de buffers activos')
PROCESSING_TIME = Counter('buffer_processing_time_seconds', 'Tiempo de procesamiento', ['buffer_key'])
WEBHOOK_FAILURES = Counter('buffer_webhook_failures_total', 'Total de fallos en webhooks')

@app.get("/metrics")
async def metrics() -> PlainTextResponse:
    """Endpoint mejorado para métricas en formato Prometheus."""
    ACTIVE_BUFFERS.set(len(buffer_manager._active_buffers))
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