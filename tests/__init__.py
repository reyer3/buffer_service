"""
Buffer Service Tests
------------------

Test suite for the Buffer Service.
Includes unit tests, integration tests, and performance tests.

Usage:
    pytest tests/ -v
    pytest tests/ -v -k "test_buffer_manager"  # Run specific tests
    pytest tests/ --cov=buffer_service  # Run with coverage
"""

import pytest

# Configuración global para los tests
pytest.register_assert_rewrite('tests.test_buffer_service')

# Variables de configuración para tests
TEST_SETTINGS = {
    "TEST_WEBHOOK_URL": "http://test.com/webhook",
    "TEST_BUFFER_KEY": "test_user@whatsapp.net",
    "TEST_AGGREGATE_FIELD": "json.input.data.query",
    "TEST_WAIT_TIME": 15,
    "TEST_MAX_SIZE": 2,
    "REDIS_URL": "redis://localhost:6379",
}