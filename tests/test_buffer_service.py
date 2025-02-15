import tracemalloc
import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Awaitable
from unittest.mock import AsyncMock, patch, call, MagicMock

import httpx
import pytest
from aioresponses import aioresponses
from fastapi import HTTPException, status
from pydantic import HttpUrl
from httpx import TimeoutException, Request, Response, HTTPStatusError

from tests import TEST_SETTINGS

from src.buffer_service import (
    BufferConfig,
    BufferManager,
    BufferRequest,
    WebhookConfig,
    DetailedHTTPException,
    ErrorCode,
    aggregate_messages,
    process_buffer,
    redis,  # Ensure redis is imported here for RedisError to be accessible
)
import redis.asyncio as redis_asyncio  # Import redis.asyncio and alias it for clarity

# Iniciar tracemalloc para los tests
tracemalloc.start(25)

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
    mock.exists = AsyncMock(return_value=0)
    mock.script_load = AsyncMock(return_value="mocked_script_sha")  # Corregido a AsyncMock
    mock.evalsha = AsyncMock(return_value=[])
    mock.close = AsyncMock()  # Aseguramos que sea AsyncMock

    # Implementar el context manager
    mock.__aenter__.return_value = mock
    mock.__aexit__.return_value = None

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

@pytest.fixture
def pipeline_mock():
    """Fixture para un pipeline mock que maneja coroutines y context managers."""
    class PipelineMock:
        def __init__(self):
            self.execute = AsyncMock(return_value=[True, 1, 2])
            self.set = AsyncMock()
            self.rpush = AsyncMock()
            self.llen = AsyncMock()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

    return PipelineMock()

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
    
    # Verificar que se cargaron los tres scripts
    assert mock_redis.script_load.call_count == 3
    calls = mock_redis.script_load.call_args_list
    assert calls[0].args[0] == BufferManager.GET_AND_CLEAR_BUFFER_SCRIPT
    assert calls[1].args[0] == BufferManager.COMMIT_BACKUP_SCRIPT
    assert calls[2].args[0] == BufferManager.RESTORE_FROM_BACKUP_SCRIPT

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
    pipeline_mock,
):
    """Test add_message usando pipeline para agrupar comandos."""
    pipeline_mock.execute.return_value = [True, 1, sample_request.buffer.max_size]
    mock_redis.pipeline = MagicMock(return_value=pipeline_mock)

    # Mockear process_and_send
    process_mock = AsyncMock()
    with patch.object(buffer_manager, 'process_and_send', new=process_mock):
        await buffer_manager.add_message(sample_request)
        
        # Verificar que se llamaron los métodos correctos
        assert pipeline_mock.set.called
        assert pipeline_mock.rpush.called
        assert pipeline_mock.llen.called
        assert pipeline_mock.execute.called
        
        # Verificar los argumentos de set
        config_key = f"config:{sample_request.buffer.key}"
        config_data = sample_request.buffer.model_dump_json().encode()
        pipeline_mock.set.assert_called_with(config_key, config_data)
        
        # Verificar que se llamó a process_and_send
        process_mock.assert_called_once_with(sample_request.buffer.key)

@pytest.mark.asyncio
async def test_buffer_manager_max_size_trigger(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    mock_redis: AsyncMock,
    pipeline_mock,
):
    """Test comportamiento cuando se alcanza tamaño máximo usando pipeline."""
    pipeline_mock.execute.return_value = [True, 1, 2]
    mock_redis.pipeline = MagicMock(return_value=pipeline_mock)
    
    process_mock = AsyncMock()
    with patch.object(buffer_manager, 'process_and_send', new=process_mock):
        await buffer_manager.add_message(sample_request)
        
        # Verificar que se ejecutó el pipeline
        assert pipeline_mock.execute.called
        
        # Verificar que se llamó a process_and_send
        process_mock.assert_called_once_with(sample_request.buffer.key)

# ---------------------------
# Test de schedule_processing
# ---------------------------
@pytest.mark.asyncio
async def test_buffer_manager_schedule_processing(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    mock_redis: AsyncMock,
):
    """Test que el procesamiento programado funciona correctamente."""
    # Configurar mocks de Redis correctamente
    mock_redis.exists.return_value = 1
    mock_redis.llen.return_value = 1
    mock_redis.get.return_value = sample_request.buffer.model_dump_json().encode()
    
    # Asegurar que el script Lua está cargado
    buffer_manager._get_and_clear_script = "test_script"
    mock_redis.evalsha.return_value = [json.dumps({"test": "data"}).encode()]
    
    process_mock = AsyncMock()
    
    with patch.object(buffer_manager, 'process_and_send', new=process_mock):
        # Programar procesamiento con tiempo corto para el test
        wait_time = 0.1
        await buffer_manager.schedule_processing(sample_request.buffer.key, wait_time)
        
        # Verificar que la tarea se agregó correctamente
        assert sample_request.buffer.key in buffer_manager._processing_tasks
        assert sample_request.buffer.key in buffer_manager._active_buffers
        
        # Esperar a que se complete el procesamiento
        await asyncio.sleep(wait_time + 0.3)
        
        # Verificar que se llamó a process_and_send y se limpiaron los recursos
        process_mock.assert_called_once_with(sample_request.buffer.key)
        assert sample_request.buffer.key not in buffer_manager._processing_tasks
        assert sample_request.buffer.key not in buffer_manager._active_buffers

@pytest.mark.asyncio
async def test_buffer_manager_concurrent_processing(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    mock_redis: AsyncMock
):
    """Test que el procesamiento concurrente se maneja correctamente."""
    config_data = sample_request.buffer.model_dump_json().encode()
    mock_redis.get.return_value = config_data
    
    # Simular que el buffer está siendo procesado
    async with buffer_manager._lock:
        buffer_manager._processing_buffers.add(sample_request.buffer.key)
        buffer_manager._active_buffers.add(sample_request.buffer.key)
    
    # Intentar procesar el mismo buffer mientras está activo
    await buffer_manager.process_and_send(sample_request.buffer.key)

    # Verificar que, dado que el buffer ya estaba en procesamiento, el método retornó sin realizar procesamiento adicional
    # El buffer debería seguir marcado como activo y en procesamiento
    assert sample_request.buffer.key in buffer_manager._active_buffers
    assert sample_request.buffer.key in buffer_manager._processing_buffers

@pytest.mark.asyncio
async def test_buffer_manager_shutdown_with_pending_tasks(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    mock_redis: AsyncMock
):
    """Test que el shutdown maneja correctamente las tareas pendientes."""
    # Crear una tarea simulada
    async def mock_delayed_processing():
        try:
            await asyncio.sleep(10)  # Tarea larga
        except asyncio.CancelledError:
            # Simular limpieza al ser cancelado
            buffer_manager._active_buffers.discard(sample_request.buffer.key)
            return
    
    task = asyncio.create_task(mock_delayed_processing())
    buffer_manager._processing_tasks[sample_request.buffer.key] = task
    buffer_manager._active_buffers.add(sample_request.buffer.key)
    
    # Ejecutar shutdown
    await buffer_manager.shutdown()
    
    # Verificar que la tarea fue cancelada
    assert task.cancelled()
    # Verificar que los recursos se limpiaron
    assert not buffer_manager._processing_tasks
    assert not buffer_manager._active_buffers
    assert mock_redis.close.called

@pytest.mark.asyncio
async def test_buffer_manager_metrics_update(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    mock_redis: AsyncMock,
    pipeline_mock,
):
    """Test que las métricas se actualizan correctamente."""
    from src.buffer_service import (
        MESSAGES_PROCESSED,
        ACTIVE_BUFFERS,
        MESSAGE_SIZE,
        BUFFER_OPERATIONS,
        WEBHOOK_FAILURES
    )
    
    # Configurar el pipeline mock para simular buffer lleno
    pipeline_mock.execute.return_value = [True, 1, sample_request.buffer.max_size]
    pipeline_mock.llen.return_value = sample_request.buffer.max_size
    mock_redis.pipeline.return_value = pipeline_mock
    
    # Capturar valores iniciales de métricas
    initial_messages = MESSAGES_PROCESSED._value.get()
    initial_size = MESSAGE_SIZE._value.get()
    initial_ops = BUFFER_OPERATIONS.labels(operation="add_message")._value.get()
    
    # Simular procesamiento exitoso que vacía el buffer
    async def mock_process(*args, **kwargs):
        MESSAGES_PROCESSED.inc()
        ACTIVE_BUFFERS.labels(buffer_key=sample_request.buffer.key).set(0)
    
    process_mock = AsyncMock(side_effect=mock_process)
    
    with patch.object(buffer_manager, 'process_and_send', new=process_mock):
        await buffer_manager.add_message(sample_request)
        
        # Verificar incrementos en métricas
        assert MESSAGE_SIZE._value.get() > initial_size
        assert BUFFER_OPERATIONS.labels(operation="add_message")._value.get() > initial_ops
        assert MESSAGES_PROCESSED._value.get() > initial_messages
        
        # Verificar que ACTIVE_BUFFERS refleja el buffer vacío
        buffer_size = ACTIVE_BUFFERS.labels(buffer_key=sample_request.buffer.key)._value.get()
        assert buffer_size == 0  # Debe ser 0 después de procesar
        
        # Verificar que process_and_send fue llamado
        process_mock.assert_called_once_with(sample_request.buffer.key)

@pytest.mark.asyncio
async def test_multiple_buffers_independent_sizes(
    buffer_manager: BufferManager,
    mock_redis: AsyncMock,
):
    """Test que diferentes buffers mantienen sus tamaños de forma independiente."""
    # Crear dos configuraciones diferentes
    config1 = BufferConfig(
        key="buffer1",
        wait_time=15,
        aggregate_field="data.value",
        max_size=5,
        webhook=WebhookConfig(url=HttpUrl('http://test.com/webhook'))
    )
    config2 = BufferConfig(
        key="buffer2",
        wait_time=15,
        aggregate_field="data.value",
        max_size=10,
        webhook=WebhookConfig(url=HttpUrl('http://test.com/webhook'))
    )
    
    # Configurar pipeline mocks para cada buffer
    pipeline_mock1 = AsyncMock()
    pipeline_mock1.execute.return_value = [True, 1, 3]  # buffer1: 3/5
    pipeline_mock1.llen.return_value = 3
    pipeline_mock1.__aenter__ = AsyncMock(return_value=pipeline_mock1)
    pipeline_mock1.__aexit__ = AsyncMock()
    
    pipeline_mock2 = AsyncMock()
    pipeline_mock2.execute.return_value = [True, 1, 10]  # buffer2: 10/10 (lleno)
    pipeline_mock2.llen.return_value = 10
    pipeline_mock2.__aenter__ = AsyncMock(return_value=pipeline_mock2)
    pipeline_mock2.__aexit__ = AsyncMock()
    
    mock_redis.pipeline.side_effect = [pipeline_mock1, pipeline_mock2]
    
    process_mock = AsyncMock()
    with patch.object(buffer_manager, 'process_and_send', new=process_mock):
        # Enviar mensajes a ambos buffers
        await buffer_manager.add_message(BufferRequest(
            webhook=config1.webhook,
            buffer=config1,
            payload={"data": {"value": "test1"}}
        ))
        
        await buffer_manager.add_message(BufferRequest(
            webhook=config2.webhook,
            buffer=config2,
            payload={"data": {"value": "test2"}}
        ))
        
        # Verificar que solo se procesó el buffer que alcanzó su máximo
        assert process_mock.call_count == 1
        process_mock.assert_called_once_with("buffer2")
        
        # Verificar métricas independientes
        assert ACTIVE_BUFFERS.labels(buffer_key="buffer1")._value.get() == 3
        assert ACTIVE_BUFFERS.labels(buffer_key="buffer2")._value.get() == 0  # Procesado
        
        # Verificar que los buffers mantienen su estado independiente
        assert "buffer1" not in buffer_manager._active_buffers  # No en procesamiento
        assert "buffer2" not in buffer_manager._active_buffers  # Ya procesado

@pytest.mark.asyncio
async def test_webhook_retry_with_increasing_backoff(
    mock_webhook,
    sample_messages: List[Dict[str, Any]],
    sample_webhook_config: WebhookConfig,
):
    """Test que el reintento de webhook usa backoff exponencial."""
    from src.buffer_service import WEBHOOK_FAILURES
    
    config = BufferConfig(
        key="test_buffer",
        wait_time=15,
        aggregate_field="json.input.data.query",
        max_size=10,
        webhook=sample_webhook_config,
        webhook_retries=2,
        webhook_timeout=1  # Timeout corto para el test
    )

    retry_times = []
    start_time = None

    async def mock_request(*args, **kwargs):
        nonlocal start_time, retry_times
        current_time = datetime.now(timezone.utc)
        
        if start_time is None:
            start_time = current_time
        else:
            delay = (current_time - start_time).total_seconds()
            retry_times.append(delay)
        
        raise TimeoutException("Connection timeout")

    mock_client = AsyncMock()
    mock_client_instance = AsyncMock()
    mock_client_instance.request = AsyncMock(side_effect=mock_request)
    mock_client.__aenter__.return_value = mock_client_instance

    with patch('httpx.AsyncClient', return_value=mock_client), \
         pytest.raises(DetailedHTTPException) as exc_info:
        await process_buffer(sample_messages, config, sample_webhook_config)
        
        # Verificar que los tiempos entre reintentos son crecientes
        assert len(retry_times) == 2  # Dos reintentos
        assert retry_times[1] > retry_times[0]  # Segundo reintento más largo
        assert retry_times[1] >= retry_times[0] * 2  # Verificar backoff exponencial
        
        # Verificar el error resultante
        assert exc_info.value.status_code == status.HTTP_504_GATEWAY_TIMEOUT
        assert exc_info.value.detail['error_code'] == ErrorCode.WEBHOOK_FAILED.value
        
        # Verificar métricas de fallos
        assert WEBHOOK_FAILURES._value.get() == 3  # Intento inicial + 2 reintentos

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
    config = BufferConfig(
        key="test_buffer",
        wait_time=15,
        aggregate_field="json.input.data.query",
        max_size=10,
        webhook=sample_webhook_config
    )

    # Crear mock manualmente en lugar de usar patch como context manager
    mock_client = AsyncMock()
    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = AsyncMock()
    mock_response.raise_for_status.return_value = None

    mock_client_instance = AsyncMock()
    mock_client_instance.request = AsyncMock(return_value=mock_response)
    mock_client.__aenter__.return_value = mock_client_instance
    mock_client.__aexit__ = AsyncMock()

    with patch('httpx.AsyncClient', return_value=mock_client):
        response = await process_buffer(
            sample_messages,
            config,
            sample_webhook_config
        )
        
        # Verificar que se llamó request con los parámetros correctos
        assert mock_client_instance.request.called
        call_args = mock_client_instance.request.call_args
        assert call_args is not None
        assert call_args.kwargs['method'] == sample_webhook_config.method
        assert call_args.kwargs['url'] == str(sample_webhook_config.url)
        
        # Verificar la respuesta
        assert response.buffer_key == config.key
        assert len(response.messages) == len(sample_messages)
        assert response.metadata.total_messages == len(sample_messages)
        
        # Verificar que raise_for_status fue llamado
        assert mock_response.raise_for_status.called

@pytest.mark.asyncio
async def test_process_buffer_webhook_error(
    mock_webhook,
    sample_messages: List[Dict[str, Any]],
    sample_webhook_config: WebhookConfig,
):
    """Test manejo de error en webhook."""
    fake_request = Request("POST", str(sample_webhook_config.url))
    fake_response = Response(500, request=fake_request, content=b"Internal Server Error")
    
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
            max_size=10,
            webhook_retries=3
        )

        with pytest.raises(DetailedHTTPException) as exc_info:
            await process_buffer(sample_messages, config, sample_webhook_config)

        error_detail = exc_info.value.detail
        assert error_detail['error_code'] == 'WEBHOOK_FAILED'
        assert 'Internal Server Error' in error_detail['message']
        assert exc_info.value.status_code == 500

@pytest.mark.asyncio
async def test_process_buffer_webhook_timeout(
    mock_webhook,
    sample_messages: List[Dict[str, Any]],
    sample_webhook_config: WebhookConfig,
):
    """Test timeout en webhook."""
    with patch('src.buffer_service.httpx.AsyncClient', autospec=True) as mock_client:
        instance = mock_client.return_value.__aenter__.return_value
        instance.request = AsyncMock(side_effect=TimeoutException("Request timed out"))

        config = BufferConfig(
            key="test_buffer",
            wait_time=15,
            aggregate_field="json.input.data.query",
            max_size=10,
            webhook_retries=3
        )

        with pytest.raises(DetailedHTTPException) as exc_info:
            await process_buffer(sample_messages, config, sample_webhook_config)

        error_detail = exc_info.value.detail
        assert error_detail['error_code'] == 'WEBHOOK_FAILED'
        assert 'Request timed out' in error_detail['message']
        assert exc_info.value.status_code == status.HTTP_504_GATEWAY_TIMEOUT

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

    buffer_manager._get_and_clear_script = "dummy_sha"
    buffer_manager.redis = mock_redis
    buffer_manager.load_scripts = AsyncMock(return_value=None)
    mock_redis.evalsha = AsyncMock(return_value=[])

    await buffer_manager.process_and_send(sample_request.buffer.key)

    # Verificar que evalsha se llamó con los parámetros correctos para el backup
    mock_redis.evalsha.assert_called_once_with(
        "dummy_sha",
        2,  # número de keys (original + backup)
        sample_request.buffer.key,  # key original
        f"backup:{sample_request.buffer.key}"  # key de backup
    )

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
async def test_webhook_retry_success_after_failures(
    mock_webhook,
    sample_messages: List[Dict[str, Any]],
    sample_webhook_config: WebhookConfig,
):
    """Test reintento exitoso de webhook después de fallos."""
    config = BufferConfig(
        key="test_buffer",
        wait_time=15,
        aggregate_field="json.input.data.query",
        max_size=10,
        webhook=sample_webhook_config,
        webhook_retries=3
    )

    # Simular 2 fallos y luego éxito
    responses = [
        TimeoutException("Connection timeout"),
        HTTPStatusError(
            "Server error",
            request=Request("POST", str(sample_webhook_config.url)),
            response=Response(500, request=Request("POST", str(sample_webhook_config.url)))
        ),
        None  # Éxito en el tercer intento
    ]
    current_try = 0

    async def mock_request(*args, **kwargs):
        nonlocal current_try
        error = responses[current_try]
        current_try += 1
        if error:
            raise error
        mock_resp = AsyncMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = AsyncMock()
        return mock_resp

    mock_client = AsyncMock()
    mock_client_instance = AsyncMock()
    mock_client_instance.request = AsyncMock(side_effect=mock_request)
    mock_client.__aenter__.return_value = mock_client_instance

    with patch('httpx.AsyncClient', return_value=mock_client):
        response = await process_buffer(
            sample_messages,
            config,
            sample_webhook_config
        )
        
        # Verificar que se hicieron los 3 intentos
        assert mock_client_instance.request.call_count == 3
        assert response is not None
        assert response.buffer_key == config.key

@pytest.mark.asyncio
async def test_process_buffer_with_large_messages(
    mock_webhook,
    sample_webhook_config: WebhookConfig,
):
    """Test procesamiento de mensajes grandes."""
    config = BufferConfig(
        key="test_buffer",
        wait_time=15,
        aggregate_field="data.content",
        max_size=10,
        max_message_size=1024 * 1024,  # 1MB
        webhook=sample_webhook_config
    )

    # Crear mensajes grandes
    large_messages = []
    for i in range(5):
        large_messages.append({
            "data": {
                "content": "x" * 100000,  # 100KB de contenido
                "id": f"msg_{i}"
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

    mock_client = AsyncMock()
    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = AsyncMock()

    mock_client_instance = AsyncMock()
    mock_client_instance.request = AsyncMock(return_value=mock_response)
    mock_client.__aenter__.return_value = mock_client_instance

    with patch('httpx.AsyncClient', return_value=mock_client):
        response = await process_buffer(
            large_messages,
            config,
            sample_webhook_config
        )
        
        # Verificar el procesamiento
        assert response is not None
        assert len(response.messages) == len(large_messages)
        assert response.metadata.total_messages == len(large_messages)
        
        # Verificar que se llamó al webhook con el payload correcto
        call_args = mock_client_instance.request.call_args
        assert call_args is not None
        sent_data = call_args.kwargs['json']
        assert len(sent_data['messages']) == len(large_messages)

@pytest.mark.asyncio
async def test_process_buffer_with_invalid_message_size(
    buffer_manager: BufferManager,
):
    """Test manejo de mensajes que exceden el tamaño máximo."""
    config = BufferConfig(
        key="test_buffer",
        wait_time=15,
        aggregate_field="data.content",
        max_size=10,
        max_message_size=1024,  # 1KB
        webhook=WebhookConfig(url=HttpUrl('http://test.com/webhook'))
    )

    # Crear un mensaje que excede el tamaño máximo
    large_message = {
        "data": {
            "content": "x" * 2048,  # 2KB de contenido
            "id": "large_msg"
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    with pytest.raises(DetailedHTTPException) as exc_info:
        await buffer_manager.add_message(BufferRequest(
            webhook=config.webhook,
            buffer=config,
            payload=large_message
        ))

    assert exc_info.value.status_code == status.HTTP_413_REQUEST_ENTITY_TOO_LARGE
    assert exc_info.value.detail['error_code'] == ErrorCode.INVALID_CONFIG.value

# ---------------------------
# Tests de manejo de errores y recuperación
# ---------------------------
@pytest.mark.asyncio
async def test_restore_from_backup_after_webhook_failure(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    mock_redis: AsyncMock,
):
    """Test restauración desde backup después de fallo en webhook."""
    # Preparar datos de prueba
    config_data = sample_request.buffer.model_dump_json().encode()
    mock_redis.get.return_value = config_data
    
    # Simular mensajes en backup
    backup_messages = [
        json.dumps({"data": "test1", "timestamp": datetime.now(timezone.utc).isoformat()}).encode(),
        json.dumps({"data": "test2", "timestamp": datetime.now(timezone.utc).isoformat()}).encode()
    ]
    
    # Configurar el script de restauración
    buffer_manager._restore_from_backup_script = "restore_sha"
    mock_redis.evalsha.return_value = backup_messages

    # Ejecutar restauración
    restored_messages = await buffer_manager.restore_from_backup(sample_request.buffer.key)
    
    # Verificaciones
    assert restored_messages is not None
    assert len(restored_messages) == 2
    mock_redis.evalsha.assert_called_once_with(
        "restore_sha",
        2,
        f"backup:{sample_request.buffer.key}",
        sample_request.buffer.key
    )

@pytest.mark.asyncio
async def test_multiple_buffers_independent_sizes(
    buffer_manager: BufferManager,
    mock_redis: AsyncMock,
):
    """Test que diferentes buffers mantienen sus tamaños de forma independiente."""
    # Crear dos configuraciones diferentes
    config1 = BufferConfig(
        key="buffer1",
        wait_time=15,
        aggregate_field="data.value",
        max_size=5
    )
    config2 = BufferConfig(
        key="buffer2",
        wait_time=15,
        aggregate_field="data.value",
        max_size=10
    )
    
    # Configurar pipeline mocks para cada buffer
    pipeline_mock1 = AsyncMock()
    pipeline_mock1.execute = AsyncMock(return_value=[True, 1, 3])  # No alcanza max_size
    pipeline_mock1.set = AsyncMock()
    pipeline_mock1.rpush = AsyncMock()
    pipeline_mock1.llen = AsyncMock(return_value=3)
    pipeline_mock1.__aenter__ = AsyncMock(return_value=pipeline_mock1)
    pipeline_mock1.__aexit__ = AsyncMock()
    
    pipeline_mock2 = AsyncMock()
    pipeline_mock2.execute = AsyncMock(return_value=[True, 1, 10])  # Alcanza max_size
    pipeline_mock2.set = AsyncMock()
    pipeline_mock2.rpush = AsyncMock()
    pipeline_mock2.llen = AsyncMock(return_value=10)
    pipeline_mock2.__aenter__ = AsyncMock(return_value=pipeline_mock2)
    pipeline_mock2.__aexit__ = AsyncMock()
    
    # Configurar el mock de Redis para devolver diferentes pipelines
    mock_redis.pipeline = MagicMock(side_effect=[pipeline_mock1, pipeline_mock2])
    
    # Mockear process_and_send
    process_mock = AsyncMock()
    with patch.object(buffer_manager, 'process_and_send', new=process_mock):
        # Enviar mensajes a ambos buffers
        await buffer_manager.add_message(BufferRequest(
            webhook=WebhookConfig(url=HttpUrl('http://test.com/webhook')),
            buffer=config1,
            payload={"data": {"value": "test1"}}
        ))
        
        await buffer_manager.add_message(BufferRequest(
            webhook=WebhookConfig(url=HttpUrl('http://test.com/webhook')),
            buffer=config2,
            payload={"data": {"value": "test2"}}
        ))
        
        # Verificar que solo se procesó el buffer que alcanzó su máximo
        assert process_mock.call_count == 1
        process_mock.assert_called_once_with("buffer2")

@pytest.fixture(autouse=True)
async def cleanup_tracemalloc():
    """Fixture para limpiar tracemalloc después de cada test."""
    yield
    tracemalloc.clear_traces()


