from setuptools import setup, find_packages

setup(
    name='buffer_service',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'fastapi',
        'uvicorn',
        'redis',
        'aiohttp',
        'pydantic',
        'python-dotenv'
    ],
)
