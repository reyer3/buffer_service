import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import AsyncMock, patch, call, MagicMock

import httpx
import pytest
from aioresponses import aioresponses
from fastapi import HTTPException, status
from pydantic import HttpUrl

from tests import TEST_SETTINGS

from src.buffer_service import (
    BufferConfig,
    BufferManager,
    BufferRequest,
    WebhookConfig,
    aggregate_messages,
    process_buffer,
    redis,  # Ensure redis is imported here for RedisError to be accessible
)
import redis.asyncio as redis_asyncio  # Import redis.asyncio and alias it for clarity

# ---------------------------
# Fixtures
# ---------------------------
@pytest.fixture
def mock_redis() -> AsyncMock:
    """Fixture para un cliente Redis mockeado."""
    mock = AsyncMock(spec=redis_asyncio.Redis)
    
    # Configurar métodos asincrónicos
    mock.set = AsyncMock(return_value=True)
    mock.get = AsyncMock(return_value=None)
    mock.rpush = AsyncMock(return_value=1)
    mock.llen = AsyncMock(return_value=0)
    mock.script_load = AsyncMock(return_value="mocked_script_sha")  # Corregido a AsyncMock
    mock.evalsha = AsyncMock(return_value=[])
    mock.close = AsyncMock()  # Aseguramos que sea AsyncMock

    # Pipeline mocking mejorado
    pipeline_mock = AsyncMock()
    pipeline_mock.execute = AsyncMock(return_value=[True, 1, 2])
    pipeline_mock.set = AsyncMock()
    pipeline_mock.rpush = AsyncMock()
    pipeline_mock.llen = AsyncMock()
    
    mock.pipeline = MagicMock(return_value=pipeline_mock)
    
    return mock

@pytest.fixture
def buffer_manager(mock_redis: AsyncMock) -> BufferManager:
    """Fixture para BufferManager con Redis mockeado."""
    with patch('src.buffer_service.redis.from_url', return_value=mock_redis) as mock_from_url:
        manager = BufferManager(redis_url=TEST_SETTINGS['REDIS_URL'])
        manager.redis = mock_redis
        return manager

@pytest.fixture
def sample_request() -> BufferRequest:
    """Fixture para una request de ejemplo."""
    return BufferRequest(
        webhook=WebhookConfig(url=HttpUrl('http://test.com/webhook')),
        buffer=BufferConfig(
            key='test_buffer',
            wait_time=15,
            aggregate_field='json.input.data.query',
            max_size=2
        ),
        payload={"json": {"input": {"data": {"query": "test message"}}}}
    )

@pytest.fixture
def sample_webhook_config() -> WebhookConfig:
    """Fixture para configuración de webhook."""
    return WebhookConfig(url=HttpUrl('http://test.com/webhook'))

@pytest.fixture
def sample_messages() -> List[Dict[str, Any]]:
    """Fixture para mensajes de ejemplo."""
    current_time = datetime.now(timezone.utc)
    return [
        {
            "json": {"input": {"data": {"query": "message 1"}}},
            "timestamp": current_time.isoformat(),
        },
        {
            "json": {"input": {"data": {"query": "message 2"}}},
            "timestamp": current_time.isoformat(),
        },
    ]

@pytest.fixture
def mock_webhook():
    """Fixture para mock de webhook."""
    with aioresponses() as mocked:
        yield mocked

class AsyncMockWithCancel(AsyncMock):
    """Mock especial para tareas cancelables."""
    async def __call__(self, *args, **kwargs):
        return await super().__call__(*args, **kwargs)

    def cancel(self):
        self._cancelled = True

# ---------------------------
# Tests básicos de BufferManager
# ---------------------------
@pytest.mark.asyncio
async def test_buffer_manager_init():
    """Test inicialización del BufferManager."""
    manager = BufferManager("redis://test:6379")
    assert manager.redis_url == "redis://test:6379"
    assert manager._lock is not None
    assert manager._shutdown_event is not None
    assert not manager._processing_tasks
    assert not manager._active_buffers

@pytest.mark.asyncio
async def test_buffer_manager_redis_context_manager(
    buffer_manager: BufferManager,
    mock_redis: AsyncMock
):
    """Test del context manager de Redis."""
    async with buffer_manager.get_redis() as redis_cm:
        assert redis_cm == mock_redis

    # Test error handling
    mock_redis.get.side_effect = redis.RedisError("Test error")
    with pytest.raises(redis.RedisError):
        async with buffer_manager.get_redis() as redis_cm:
            await redis_cm.get("test")

@pytest.mark.asyncio
async def test_buffer_manager_load_scripts(
    buffer_manager: BufferManager,
    mock_redis: AsyncMock
):
    """Test carga de scripts Lua."""
    mock_redis.script_load.return_value = "test_sha1"
    await buffer_manager.load_scripts()
    mock_redis.script_load.assert_called_once_with(BufferManager.GET_AND_CLEAR_BUFFER_SCRIPT)
    assert buffer_manager._get_and_clear_script == "test_sha1"

@pytest.mark.asyncio
async def test_buffer_manager_save_config(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    mock_redis: AsyncMock
):
    """Test guardar configuración."""
    await buffer_manager.save_config(sample_request.buffer)
    mock_redis.set.assert_called_once_with(
        f"config:{sample_request.buffer.key}",
        sample_request.buffer.model_dump_json().encode()
    )

@pytest.mark.asyncio
async def test_buffer_manager_get_config(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    mock_redis: AsyncMock
):
    """Test obtener configuración."""
    # Test config exists
    config_data = sample_request.buffer.model_dump_json().encode()
    mock_redis.get.return_value = config_data

    config = await buffer_manager.get_config(sample_request.buffer.key)
    assert config == sample_request.buffer
    mock_redis.get.assert_called_with(f"config:{sample_request.buffer.key}")

    # Test config doesn't exist
    mock_redis.get.return_value = None
    config = await buffer_manager.get_config(sample_request.buffer.key)
    assert config is None

# ---------------------------
# Tests de add_message usando pipeline
# ---------------------------
@pytest.mark.asyncio
async def test_buffer_manager_add_message_with_pipeline(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    mock_redis: AsyncMock,
):
    """Test add_message usando pipeline para agrupar comandos."""
    pipeline_mock = AsyncMock()
    pipeline_mock.__aenter__.return_value = pipeline_mock
    pipeline_mock.__aexit__.return_value = None
    # Simulamos que, tras ejecutar el pipeline, el tamaño (llen) es igual al max_size.
    pipeline_mock.execute.return_value = [True, 1, sample_request.buffer.max_size]
    mock_redis.pipeline.return_value = pipeline_mock

    with patch.object(buffer_manager, 'process_and_send') as mock_process:
        await buffer_manager.add_message(sample_request)
        mock_process.assert_called_once_with(sample_request.buffer.key)

    pipeline_mock.set.assert_called_once_with(
        f"config:{sample_request.buffer.key}",
        sample_request.buffer.model_dump_json().encode()
    )
    pipeline_mock.rpush.assert_called_once()
    pipeline_mock.llen.assert_called_once_with(sample_request.buffer.key)

@pytest.mark.asyncio
async def test_buffer_manager_max_size_trigger(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    mock_redis: AsyncMock,
):
    """Test comportamiento cuando se alcanza tamaño máximo usando pipeline."""
    # Configurar el mock del pipeline
    pipeline_mock = mock_redis.pipeline.return_value
    pipeline_mock.__aenter__.return_value = pipeline_mock  # Esto es esencial
    pipeline_mock.__aexit__.return_value = None
    pipeline_mock.execute = AsyncMock(return_value=[True, 1, 2])
    
    with patch.object(buffer_manager, 'process_and_send') as mock_process:
        await buffer_manager.add_message(sample_request)
        mock_process.assert_called_once_with(sample_request.buffer.key)


# ---------------------------
# Test de schedule_processing
# ---------------------------
@pytest.mark.asyncio
async def test_buffer_manager_schedule_processing(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    mock_redis: AsyncMock,
):
    """Test programación de procesamiento."""
    mock_redis.exists = AsyncMock(return_value=True)
    mock_redis.llen = AsyncMock(return_value=1)
    
    process_mock = AsyncMock()
    with patch.object(buffer_manager, 'process_and_send', new=process_mock):
        await buffer_manager.schedule_processing(sample_request.buffer.key, 0.1)
        await asyncio.sleep(0.2)
        process_mock.assert_called_once_with(sample_request.buffer.key)

# ---------------------------
# Test de get_and_clear_buffer
# ---------------------------
@pytest.mark.asyncio
async def test_buffer_manager_get_and_clear_buffer(
    buffer_manager: BufferManager,
    sample_messages: List[Dict[str, Any]],
    mock_redis: AsyncMock,
):
    """Test obtener y limpiar buffer usando evalsha en pipeline."""
    encoded_messages = [json.dumps(msg).encode() for msg in sample_messages]
    mock_redis.evalsha = AsyncMock(return_value=encoded_messages)

    messages = await buffer_manager.get_and_clear_buffer("test_buffer")

    assert messages is not None
    assert len(messages) == len(sample_messages)
    for msg, expected in zip(messages, encoded_messages):
        assert msg == expected

# ---------------------------
# Tests de process_buffer (webhook)
# ---------------------------
@pytest.mark.asyncio
async def test_process_buffer_success(
    mock_webhook,
    sample_messages: List[Dict[str, Any]],
    sample_webhook_config: WebhookConfig,
):
    """Test procesamiento exitoso del buffer."""
    mock_webhook.post(
        str(sample_webhook_config.url),
        status=200,
        payload={"status": "ok"}
    )

    config = BufferConfig(
        key="test_buffer",
        wait_time=15,
        aggregate_field="json.input.data.query",
        max_size=10
    )

    with patch('httpx.AsyncClient', autospec=True) as mock_client:
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = AsyncMock()
        mock_client.return_value.__aenter__.return_value.request = AsyncMock(
            return_value=mock_response
        )

        response = await process_buffer(
            sample_messages,
            config,
            sample_webhook_config
        )

        assert response.buffer_key == config.key
        assert len(response.messages) == len(sample_messages)
        assert response.metadata.total_messages == len(sample_messages)

@pytest.mark.asyncio
async def test_process_buffer_webhook_error(
    mock_webhook,
    sample_messages: List[Dict[str, Any]],
    sample_webhook_config: WebhookConfig,
):
    """Test manejo de error en webhook."""
    from httpx import Request, Response, HTTPStatusError
    from fastapi import HTTPException

    fake_request = Request("POST", str(sample_webhook_config.url))
    fake_response = Response(500, request=fake_request, content=b"Internal Server Error")

    # Usamos MagicMock para simular raise_for_status (sincrónico)
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    mock_response.raise_for_status.side_effect = HTTPStatusError(
        "Server error", request=fake_request, response=fake_response
    )

    with patch('src.buffer_service.httpx.AsyncClient', autospec=True) as mock_client:
        instance = mock_client.return_value.__aenter__.return_value
        instance.request = AsyncMock(return_value=mock_response)

        config = BufferConfig(
            key="test_buffer",
            wait_time=15,
            aggregate_field="json.input.data.query",
            max_size=10
        )

        with pytest.raises(HTTPException) as exc_info:
            await process_buffer(
                sample_messages,
                config,
                sample_webhook_config
            )

        assert exc_info.value.status_code == 500
        assert "Internal Server Error" in exc_info.value.detail

@pytest.mark.asyncio
async def test_process_buffer_webhook_timeout(
    mock_webhook,
    sample_messages: List[Dict[str, Any]],
    sample_webhook_config: WebhookConfig,
):
    """Test timeout en webhook."""
    from httpx import TimeoutException
    with patch('src.buffer_service.httpx.AsyncClient', autospec=True) as mock_client:
        instance = mock_client.return_value.__aenter__.return_value
        instance.request = AsyncMock(side_effect=TimeoutException("Request timed out"))

        config = BufferConfig(
            key="test_buffer",
            wait_time=15,
            aggregate_field="json.input.data.query",
            max_size=10
        )

        with pytest.raises(HTTPException) as exc_info:
            await process_buffer(
                sample_messages,
                config,
                sample_webhook_config
            )

        assert exc_info.value.status_code == status.HTTP_504_GATEWAY_TIMEOUT
        assert "Request timed out" in exc_info.value.detail

# ---------------------------
# Test de shutdown
# ---------------------------
@pytest.mark.asyncio
async def test_buffer_manager_shutdown(
    buffer_manager: BufferManager,
    mock_redis: AsyncMock
):
    """Test shutdown ordenado usando tareas reales."""
    buffer_manager.redis = mock_redis
    await buffer_manager.shutdown()
    mock_redis.close.assert_awaited_once()  # Usar assert_awaited_once en lugar de assert_called_once

# ---------------------------
# Tests de aggregate_messages
# ---------------------------
@pytest.mark.asyncio
async def test_aggregate_messages_success(sample_messages):
    """Test agregación exitosa de mensajes."""
    aggregate_field = "json.input.data.query"
    expected_aggregation = "message 1, message 2"
    aggregated_content = await aggregate_messages(sample_messages, aggregate_field)
    assert aggregated_content == expected_aggregation

@pytest.mark.asyncio
async def test_aggregate_messages_empty(sample_messages):
    """Test agregación con campo inexistente."""
    aggregate_field = "nonexistent.field"
    aggregated_content = await aggregate_messages(sample_messages, aggregate_field)
    assert aggregated_content == ""

@pytest.mark.asyncio
async def test_aggregate_messages_nested_list(buffer_manager: BufferManager):
    """Test agregación con listas anidadas."""
    messages = [
        {
            "data": {
                "items": [
                    {"value": "item1"},
                    {"value": "item2"}
                ]
            }
        },
        {
            "data": {
                "items": [
                    {"value": "item3"},
                    {"value": "item4"}
                ]
            }
        }
    ]
    result = await aggregate_messages(messages, "data.items.0.value")
    assert result == "item1, item3"

# ---------------------------
# Tests adicionales
# ---------------------------
@pytest.mark.asyncio
async def test_schedule_processing_duplicate(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    mock_redis: AsyncMock
):
    """Test que no se programen duplicados."""
    buffer_manager._active_buffers.add(sample_request.buffer.key)

    process_mock = AsyncMock()
    with patch.object(buffer_manager, 'process_and_send', new=process_mock):
        await buffer_manager.schedule_processing(sample_request.buffer.key, 0.1)
        await asyncio.sleep(0.2)
        process_mock.assert_not_called()

@pytest.mark.asyncio
async def test_process_and_send_no_config(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    mock_redis: AsyncMock
):
    """Test process_and_send cuando no hay configuración."""
    # Simulamos que al obtener la configuración se retorna None.
    mock_redis.get.return_value = None
    await buffer_manager.process_and_send(sample_request.buffer.key)
    # Como no hay config, no se debe llamar a evalsha para obtener mensajes.
    mock_redis.evalsha.assert_not_called()

@pytest.mark.asyncio
async def test_process_and_send_no_messages(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    mock_redis: AsyncMock
):
    """Test process_and_send cuando no hay mensajes."""
    config_data = sample_request.buffer.model_dump_json().encode()
    mock_redis.get.return_value = config_data

    # Asignamos el valor de _get_and_clear_script en la instancia.
    buffer_manager._get_and_clear_script = "dummy_sha"
    buffer_manager.redis = mock_redis

    # Parcheamos load_scripts en la instancia para evitar que modifique _get_and_clear_script.
    buffer_manager.load_scripts = AsyncMock(return_value=None)

    # Forzamos que evalsha sea awaitable y retorne [] (sin mensajes).
    mock_redis.evalsha = AsyncMock(return_value=[])

    await buffer_manager.process_and_send(sample_request.buffer.key)

    # Se espera que evalsha se llame cuando no hay mensajes.
    mock_redis.evalsha.assert_called_once_with("dummy_sha", keys=[sample_request.buffer.key])


@pytest.mark.asyncio
async def test_buffer_cleanup_on_error(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    mock_redis: AsyncMock
):
    """Test limpieza del buffer cuando ocurre un error."""
    config_data = sample_request.buffer.model_dump_json().encode()
    mock_redis.get.return_value = config_data

    buffer_manager._get_and_clear_script = "dummy_sha"
    buffer_manager.redis = mock_redis
    buffer_manager.load_scripts = AsyncMock(return_value=None)

    # Configuramos evalsha para que lance una excepción.
    mock_redis.evalsha = AsyncMock(side_effect=Exception("Processing error"))

    # No preagregamos la key a _active_buffers para probar la ejecución normal.
    with pytest.raises(Exception) as exc_info:
        await buffer_manager.process_and_send(sample_request.buffer.key)

    assert "Processing error" in str(exc_info.value)
    assert sample_request.buffer.key not in buffer_manager._active_buffers

@pytest.mark.asyncio
async def test_concurrent_processing(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    mock_redis: AsyncMock
):
    """Test procesamiento concurrente."""
    config_data = sample_request.buffer.model_dump_json().encode()
    mock_redis.get.return_value = config_data

    buffer_manager._get_and_clear_script = "dummy_sha"
    buffer_manager.redis = mock_redis
    buffer_manager.load_scripts = AsyncMock(return_value=None)

    # Usamos un evento para controlar el flujo
    start_event = asyncio.Event()
    processed = False

    async def delayed_evalsha(*args, **kwargs):
        nonlocal processed
        if not processed:
            processed = True
            await start_event.wait()  # Pausar la primera llamada
        return []

    mock_redis.evalsha = AsyncMock(side_effect=delayed_evalsha)

    async def process():
        await buffer_manager.process_and_send(sample_request.buffer.key)

    task1 = asyncio.create_task(process())
    task2 = asyncio.create_task(process())

    # Permitir que task1 inicie y se pause
    await asyncio.sleep(0)
    start_event.set()  # Reanudar task1

    await asyncio.gather(task1, task2)

    # Solo la primera llamada procesa
    assert mock_redis.evalsha.call_count == 1


