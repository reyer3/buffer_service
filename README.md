# Buffer Service

Servicio de buffer para mensajes con tiempo de espera configurable.

## Instalación

```bash
python3 -m venv venv
source venv/bin/activate
pip install -e .
```

## Desarrollo

Instalar dependencias de desarrollo:

```bash
pip install -r test-requirements.txt
```

## Ejecutar

```bash
python -m uvicorn buffer_service.src.buffer_service:app --reload
```

## Tests

```bash
pytest tests/ -v
```

## API

POST /message

Request:
```json
{
  "webhook": {
    "url": "https://callback.example.com/webhook",
    "method": "POST",
    "headers": {}
  },
  "buffer": {
    "key": "user123@whatsapp.net",
    "wait_time": 20,
    "aggregate_field": "json.input.data.query",
    "max_size": 5
  },
  "payload": {
    "json": {
      "input": {
        "data": {
          "query": "mensaje"
        }
      }
    }
  }
}
```

