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
    'REDIS_HOST': 'localhost',
    'REDIS_PORT': 6379,
    'DEFAULT_WAIT_TIME': 15,
    'MAX_BUFFER_SIZE': 10,
    'TEST_WEBHOOK_URL': 'http://test.com/webhook'
}