"""Tests for Buffer Service."""
import sys
print(sys.path) # Debug: Print sys.path
import aioresponses
import pytest
import asyncio
from datetime import datetime, UTC
from unittest.mock import AsyncMock, patch
import fakeredis.aioredis
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
import pytest_asyncio
from redis.asyncio import Redis
from aioresponses import aioresponses
import httpx
from typing import Any, AsyncGenerator, Dict, Generator, List
import contextlib

from buffer_service.src.buffer_service import (
    app,
    BufferManager,
    BufferRequest,
    WebhookConfig,
    BufferConfig,
    BufferResponse,
    BufferMetadata,
    process_buffer,
    aggregate_messages,
    get_by_path,
    get_redis_dependency
)

from tests import TEST_SETTINGS

# --- Test Data ---
TEST_USER = "test_user@whatsapp.net"
TEST_WEBHOOK_URL = "http://test.com/webhook"

# --- Fixtures ---
@pytest.fixture(autouse=True)
def mock_redis_pool():
    """Mock the global Redis pool."""
    with patch("buffer_service.src.buffer_service.redis_pool", None):
        yield

# tests/test_buffer_service.py (CORRECTED redis_mock fixture)
@pytest_asyncio.fixture
async def redis_mock() -> AsyncGenerator[Redis, None]:
    """Create a FakeRedis instance."""
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True) # Removed connection_pool argument
    yield redis
    await redis.aclose()

@pytest_asyncio.fixture
async def patch_redis(redis_mock: Redis) -> AsyncGenerator[None, None]:
    """Override the Redis dependency globally."""
    async def mock_get_redis() -> Redis:
        return redis_mock

    original_dependency = app.dependency_overrides.get(get_redis_dependency)
    app.dependency_overrides[get_redis_dependency] = mock_get_redis
    yield
    if original_dependency:
        app.dependency_overrides[get_redis_dependency] = original_dependency
    else:
        del app.dependency_overrides[get_redis_dependency]

@pytest.fixture
def test_client(patch_redis: None) -> Generator[TestClient, None, None]:
    """Create a TestClient with mocked Redis."""
    with TestClient(app) as client:
        yield client

@pytest_asyncio.fixture
async def buffer_manager(redis_mock: Redis) -> BufferManager:
    """Create a BufferManager instance with mocked Redis."""
    return BufferManager(redis_mock)

@pytest.fixture
def mock_httpx_client():
    """Mock httpx client for webhook calls."""
    mock_response = httpx.Response(200, json={"status": "ok"})
    
    with patch("httpx.AsyncClient") as mock_client:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value.request.return_value = mock_response
        mock_client.return_value = mock_instance
        yield mock_instance

@pytest.fixture
def sample_webhook_config() -> WebhookConfig:
    """Create a sample webhook config."""
    return WebhookConfig(
        url=TEST_WEBHOOK_URL,
        method="POST",
        headers={}
    )

@pytest.fixture
def sample_buffer_config() -> BufferConfig:
    """Create a sample buffer config."""
    return BufferConfig(
        key=TEST_USER,
        wait_time=15,
        aggregate_field="json.input.data.query",
        max_size=2
    )

@pytest.fixture
def sample_payload() -> dict:
    """Create a sample message payload."""
    return {
        "json": {
            "input": {
                "data": {
                    "query": "test message"
                }
            }
        }
    }

@pytest.fixture
def sample_request(
    sample_webhook_config: WebhookConfig,
    sample_buffer_config: BufferConfig,
    sample_payload: dict
) -> BufferRequest:
    """Create a complete sample request."""
    return BufferRequest(
        webhook=sample_webhook_config,
        buffer=sample_buffer_config,
        payload=sample_payload
    )

@pytest.fixture
def sample_messages() -> list[dict]:
    """Create sample messages for testing."""
    return [
        {
            "json": {"input": {"data": {"query": "message 1"}}},
            "timestamp": datetime.now(UTC).isoformat()
        },
        {
            "json": {"input": {"data": {"query": "message 2"}}},
            "timestamp": datetime.now(UTC).isoformat()
        }
    ]

# --- Tests ---
@pytest.mark.asyncio
async def test_buffer_manager_add_message(
    buffer_manager: BufferManager,
    sample_request: BufferRequest
) -> None:
    """Test adding a single message to buffer."""
    await buffer_manager.clear_buffer(sample_request.buffer.key)

    should_process = await buffer_manager.add_message(sample_request)
    assert not should_process, "Single message should not trigger processing"

    messages = await buffer_manager.get_messages(sample_request.buffer.key)
    assert len(messages) == 1, "Should have exactly one message"

# aioresponses fixture (session-scoped and autouse)
@pytest.fixture(scope="session", autouse=True)  # <-- scope="session", autouse=True is important for webhook tests!
def mock_webhook():
    with aioresponses() as m:
        yield m


@pytest.mark.asyncio
async def test_buffer_manager_max_size(
    buffer_manager: BufferManager,
    sample_request: BufferRequest
) -> None:
    """Test buffer max size behavior."""
    await buffer_manager.clear_buffer(sample_request.buffer.key)

    # First message should not trigger processing
    should_process = await buffer_manager.add_message(sample_request)
    assert not should_process, "First message should not trigger processing"
    messages = await buffer_manager.get_messages(sample_request.buffer.key)
    assert len(messages) == 1, "Should have one message after first add"

    # Second message should trigger processing (max_size = 2)
    should_process = await buffer_manager.add_message(sample_request)
    assert should_process, "Max size reached should trigger processing"
    messages = await buffer_manager.get_messages(sample_request.buffer.key)
    assert len(messages) == 2, "Should have two messages before processing"


@pytest.mark.asyncio
async def test_buffer_manager_get_and_clear_buffer(
    buffer_manager: BufferManager,
    sample_request: BufferRequest,
    sample_messages: List[Dict[str, Any]]
) -> None:
    """Test getting and clearing buffer atomically."""
    await buffer_manager.clear_buffer(sample_request.buffer.key)

    # Add messages
    for msg in sample_messages:
        sample_request.payload = msg
        await buffer_manager.add_message(sample_request)

    # Get and clear
    messages, config = await buffer_manager.get_and_clear_buffer(sample_request.buffer.key)

    assert len(messages) == len(sample_messages), "Should retrieve all messages"
    assert config is not None, "Should retrieve valid config"
    assert config.key == sample_request.buffer.key, "Config should match request"

    # Verify buffer is cleared
    remaining_messages = await buffer_manager.get_messages(sample_request.buffer.key)
    assert len(remaining_messages) == 0, "Buffer should be empty after clear"


@pytest.mark.asyncio
async def test_buffer_manager_clear_buffer(
    buffer_manager: BufferManager,
    sample_request: BufferRequest
) -> None:
    """Test explicit buffer clearing."""
    # Add a message
    await buffer_manager.add_message(sample_request)

    # Clear buffer
    await buffer_manager.clear_buffer(sample_request.buffer.key)

    # Verify everything is cleared
    messages = await buffer_manager.get_messages(sample_request.buffer.key)
    assert len(messages) == 0, "Messages should be cleared"


# tests/test_buffer_service.py (CORRECTED webhook tests to use aioresponses fixture)

@pytest.mark.asyncio
async def test_process_buffer_with_webhook(
    mock_webhook, # Use mock_webhook fixture
    sample_messages: List[Dict[str, Any]],
    sample_buffer_config: BufferConfig,
    sample_webhook_config: WebhookConfig
) -> None:
    """Test processing buffer and sending to webhook."""
    mock_webhook.post(TEST_SETTINGS['TEST_WEBHOOK_URL'], status=200, payload={"status": "ok"}) # Mock webhook response with aioresponses

    response = await process_buffer(
        sample_messages,
        sample_buffer_config,
        sample_webhook_config
    )

    # Verify response
    assert isinstance(response, BufferResponse), "Should return BufferResponse"
    assert response.buffer_key == sample_buffer_config.key, "Response key should match config"
    assert len(response.messages) == len(sample_messages), "Should include all messages"
    assert "message 1\nmessage 2" in response.aggregated_content, "Should aggregate messages"

    # Verify webhook call (using aioresponses assertion)
    assert mock_webhook.call_count == 1
    request_info = mock_webhook.requests[0]
    assert request_info.method == "POST"
    assert str(sample_webhook_config.url) in str(request_info.url)

@pytest.mark.asyncio
async def test_process_buffer_webhook_fail(
    mock_webhook, # Use mock_webhook fixture
    sample_messages: List[Dict[str, Any]],
    sample_buffer_config: BufferConfig,
    sample_webhook_config: WebhookConfig
) -> None:
    """Test webhook failure handling."""
    # Setup mock to fail with aioresponses
    mock_webhook.post(TEST_SETTINGS['TEST_WEBHOOK_URL'], status=500, body="Internal Server Error")

    with pytest.raises(HTTPException) as exc_info:
        await process_buffer(
            sample_messages,
            sample_buffer_config,
            sample_webhook_config
        )
    assert exc_info.value.status_code == 500

@pytest.mark.asyncio
async def test_process_buffer_webhook_timeout(
    mock_webhook, # Use mock_webhook fixture
    sample_messages: List[Dict[str, Any]],
    sample_buffer_config: BufferConfig,
    sample_webhook_config: WebhookConfig
) -> None:
    """Test webhook timeout handling."""
    # Setup mock to timeout with aioresponses
    mock_webhook.post(TEST_SETTINGS['TEST_WEBHOOK_URL'], exception=asyncio.TimeoutError())

    with pytest.raises(HTTPException) as exc_info:
        await process_buffer(
            sample_messages,
            sample_buffer_config,
            sample_webhook_config
        )
    assert exc_info.value.status_code == 500
    assert "Webhook connection error" in exc_info.value.detail
    mock_webhook.assert_called_once() # Verify aioresponses mock was called

@pytest.mark.asyncio
async def test_receive_message_endpoint(
    test_client: TestClient,
    sample_request: BufferRequest
) -> None:
    """Test the message receiving endpoint."""
    response = test_client.post(
        "/message",
        json=sample_request.model_dump(mode='json')
    )

    assert response.status_code == 202, "Should accept message"
    assert response.json()["status"] == "buffered", "Should indicate message is buffered"


@pytest.mark.asyncio
async def test_receive_message_endpoint_process_immediately(
    test_client: TestClient,
    sample_request: BufferRequest,
    mock_httpx_client: AsyncMock
) -> None:
    """Test endpoint when buffer should be processed immediately."""
    # Modify request to trigger immediate processing
    sample_request.buffer.max_size = 1

    response = test_client.post(
        "/message",
        json=sample_request.model_dump(mode='json')
    )

    assert response.status_code == 202, "Should accept message"
    data = response.json()
    assert data["status"] == "processed", "Should indicate message is processed"
    assert data["message_count"] == 1, "Should process one message"
    assert data["reason"] == "buffer_full", "Should indicate buffer was full"


@pytest.mark.asyncio
async def test_concurrent_messages(
    buffer_manager: BufferManager,
    sample_request: BufferRequest
) -> None:
    """Test concurrent message handling."""
    await buffer_manager.clear_buffer(sample_request.buffer.key)

    # Send multiple messages concurrently
    num_messages = 5
    tasks = [
        buffer_manager.add_message(sample_request)
        for _ in range(num_messages)
    ]

    results = await asyncio.gather(*tasks)
    messages = await buffer_manager.get_messages(sample_request.buffer.key)

    expected_size = min(num_messages, sample_request.buffer.max_size)
    assert len(messages) == expected_size, f"Should have {expected_size} messages"
    assert any(results), "At least one message should trigger processing"


if __name__ == "__main__":
    pytest.main(["-v"])