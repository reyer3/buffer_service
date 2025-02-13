"""
Tests for Buffer Service
"""
import pytest
import asyncio
from datetime import datetime, UTC
from unittest.mock import patch, AsyncMock, MagicMock
import fakeredis.aioredis
from fastapi.testclient import TestClient
from httpx import AsyncClient
import pytest_asyncio
from redis.asyncio import Redis

from buffer_service import (
    app, 
    BufferManager, 
    BufferRequest, 
    WebhookConfig, 
    BufferConfig,
    BufferResponse,
    get_redis,
    process_buffer
)

from tests import TEST_SETTINGS

# Override Redis connection for testing
@pytest_asyncio.fixture
async def redis_mock():
    """Crea una instancia de fakeredis"""
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield redis
    await redis.aclose()

@pytest_asyncio.fixture
async def mock_redis_dependency(redis_mock):
    """Override the Redis dependency"""
    app.dependency_overrides[get_redis] = lambda: redis_mock
    yield redis_mock
    app.dependency_overrides.clear()

@pytest_asyncio.fixture
async def buffer_manager(redis_mock):
    """Crea una instancia de BufferManager con redis mock"""
    return BufferManager(redis_mock)

@pytest.fixture
def sample_request():
    """Crea un request de ejemplo"""
    return BufferRequest(
        webhook=WebhookConfig(
            url=TEST_SETTINGS['TEST_WEBHOOK_URL'],
            method="POST"
        ),
        buffer=BufferConfig(
            key="test_user@whatsapp.net",
            wait_time=TEST_SETTINGS['DEFAULT_WAIT_TIME'],
            aggregate_field="json.input.data.query",
            max_size=10
        ),
        payload={
            "json": {
                "input": {
                    "data": {
                        "query": "test message"
                    }
                }
            }
        }
    )

@pytest_asyncio.fixture
async def async_client(mock_redis_dependency):
    """Cliente async para tests de endpoints"""
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client

# Tests unitarios
@pytest.mark.asyncio
async def test_buffer_manager_add_message(buffer_manager, sample_request):
    """Test añadir mensaje al buffer"""
    # Limpiar estado previo
    await buffer_manager.redis.delete(f"buffer:{sample_request.buffer.key}:messages")
    
    should_process = await buffer_manager.add_message(sample_request)
    assert not should_process, "No debería procesar con un solo mensaje"
    
    messages = await buffer_manager.redis.lrange(
        f"buffer:{sample_request.buffer.key}:messages",
        0,
        -1
    )
    assert len(messages) == 1, "Debería haber un mensaje en el buffer"
    
    config = await buffer_manager.get_config(sample_request.buffer.key)
    assert config is not None, "Debería existir la configuración"
    assert config.wait_time == sample_request.buffer.wait_time
    assert config.key == sample_request.buffer.key

@pytest.mark.asyncio
async def test_buffer_manager_max_size(buffer_manager, sample_request):
    """Test procesar buffer cuando alcanza max_size"""
    # Configurar max_size para el test
    sample_request.buffer.max_size = 2
    
    # Limpiar buffer antes de empezar
    key = f"buffer:{sample_request.buffer.key}:messages"
    await buffer_manager.redis.delete(key)
    
    # Primer mensaje
    should_process = await buffer_manager.add_message(sample_request)
    assert not should_process, "No debería procesar con el primer mensaje"
    
    # Segundo mensaje (alcanza max_size)
    should_process = await buffer_manager.add_message(sample_request)
    assert should_process, "Debería procesar al alcanzar max_size"
    
    messages = await buffer_manager.redis.lrange(key, 0, -1)
    assert len(messages) == 2, "Debería haber dos mensajes en el buffer"

@pytest.mark.asyncio
async def test_buffer_manager_timeout(buffer_manager, sample_request):
    """Test timeout del buffer"""
    # Limpiar estado previo
    key = f"buffer:{sample_request.buffer.key}"
    await buffer_manager.redis.delete(f"{key}:messages")
    await buffer_manager.redis.delete(f"{key}:last_update")
    
    await buffer_manager.add_message(sample_request)
    
    # Verificar antes del timeout
    should_process, config = await buffer_manager.should_process_buffer(sample_request.buffer.key)
    assert not should_process, "No debería procesar antes del timeout"
    
    # Simular timeout
    timestamp = datetime.now(UTC).timestamp() - (sample_request.buffer.wait_time + 1)
    await buffer_manager.redis.set(f"{key}:last_update", str(timestamp))
    
    should_process, config = await buffer_manager.should_process_buffer(sample_request.buffer.key)
    assert should_process, "Debería procesar después del timeout"
    assert config is not None, "Debería tener configuración válida"

@pytest.mark.asyncio
async def test_get_by_path():
    """Test extracción de valores usando dot notation"""
    from buffer_service.src.buffer_service import get_by_path
    
    test_obj = {
        "level1": {
            "level2": {
                "value": "test"
            }
        }
    }
    
    result = await get_by_path(test_obj, "level1.level2.value")
    assert result == "test", "Debería obtener el valor correcto"
    
    result = await get_by_path(test_obj, "invalid.path")
    assert result is None, "Debería retornar None para paths inválidos"

@pytest.mark.asyncio
async def test_receive_message_endpoint(async_client, sample_request, mock_redis_dependency):
    """Test endpoint /message"""
    # Convertir request a JSON compatible
    request_data = sample_request.model_dump(mode='json')
    
    response = await async_client.post("/message", json=request_data)
    assert response.status_code == 200, "Debería retornar 200 OK"
    
    data = response.json()
    assert data["status"] == "buffered", "Debería indicar que el mensaje fue bufferizado"

@pytest.mark.asyncio
async def test_process_buffer_with_webhook(sample_request):
    """Test procesamiento completo del buffer con llamada al webhook"""
    messages = [
        {
            "json": {"input": {"data": {"query": "message 1"}}},
            "timestamp": datetime.now(UTC).isoformat()
        },
        {
            "json": {"input": {"data": {"query": "message 2"}}},
            "timestamp": datetime.now(UTC).isoformat()
        }
    ]
    
    mock_response = AsyncMock()
    mock_response.status = 200
    
    # Mock para aiohttp.ClientSession
    mock_session = AsyncMock()
    mock_session.request.return_value.__aenter__.return_value = mock_response
    
    with patch('aiohttp.ClientSession', return_value=mock_session):
        response = await process_buffer(
            messages=messages,
            config=sample_request.buffer,
            webhook=sample_request.webhook
        )
        
        assert isinstance(response, BufferResponse), "Debería retornar una BufferResponse"
        assert response.buffer_key == sample_request.buffer.key
        assert len(response.messages) == 2
        assert "message 1\nmessage 2" in response.aggregated_content
        
        # Verificar llamada al webhook
        call_kwargs = mock_session.request.call_args.kwargs
        assert call_kwargs["method"] == "POST"
        assert str(sample_request.webhook.url) in str(call_kwargs["url"])

@pytest.mark.asyncio
async def test_concurrent_messages(buffer_manager, sample_request):
    """Test manejo concurrente de mensajes"""
    num_messages = 5
    tasks = []
    
    # Limpiar buffer antes de empezar
    key = f"buffer:{sample_request.buffer.key}:messages"
    await buffer_manager.redis.delete(key)
    
    for _ in range(num_messages):
        tasks.append(buffer_manager.add_message(sample_request))
    
    await asyncio.gather(*tasks)
    
    messages = await buffer_manager.redis.llen(key)
    assert messages == num_messages, f"Debería haber exactamente {num_messages} mensajes"

if __name__ == "__main__":
    pytest.main(["-v"])