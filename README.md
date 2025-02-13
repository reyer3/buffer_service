# Buffer Service 📬

<div align="center">

![Python Version](https://img.shields.io/badge/python-3.12-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.109.0-009688.svg)
![Redis](https://img.shields.io/badge/redis-7.2-red.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

*Un servicio de buffering asíncrono con capacidades de agregación y webhooks*

[Características](#características) •
[Instalación](#instalación) •
[Uso](#uso) •
[API](#api) •
[Configuración](#configuración) •
[Desarrollo](#desarrollo)

</div>

## 🌟 Características

- **Buffering Inteligente**
  - 🔄 Agregación configurable de mensajes
  - ⏱️ Tiempos de espera personalizables
  - 📊 Límites de tamaño ajustables
  - 🔁 Reintentos automáticos

- **Integración Flexible**
  - 🎯 Soporte para webhooks
  - 🔌 API REST completa
  - 📡 Eventos en tiempo real
  - 🔐 Autenticación configurable

- **Alto Rendimiento**
  - ⚡ Operaciones asíncronas
  - 📈 Escalabilidad horizontal
  - 🚀 Optimizado para latencia baja
  - 💾 Persistencia en Redis

## 🚀 Instalación

### Prerrequisitos

- Python 3.12+
- Redis 7.2+
- Poetry (recomendado)

### Usando Poetry (Recomendado)

```bash
# Instalar dependencias
poetry install

# Activar entorno virtual
poetry shell
```

### Usando pip

```bash
# Crear entorno virtual
python -m venv venv

# Activar entorno
source venv/bin/activate  # Linux/macOS
venv\Scripts\activate     # Windows

# Instalar dependencias
pip install -r requirements.txt
```

### Usando Docker 🐳

```bash
# Construir imagen
docker build -t buffer-service .

# Ejecutar contenedor
docker run -p 8000:8000 buffer-service
```

## 💡 Uso

### Iniciar el Servicio

```bash
# Desarrollo
uvicorn src.buffer_service:app --reload

# Producción
uvicorn src.buffer_service:app --host 0.0.0.0 --port 8000
```

### Docker Compose

```bash
docker-compose up -d
```

## 🔧 API

### Enviar Mensaje al Buffer

\`\`\`http
POST /api/v1/message
Content-Type: application/json
\`\`\`

```json
{
  "webhook": {
    "url": "https://api.example.com/webhook",
    "method": "POST",
    "headers": {
      "Authorization": "Bearer token123"
    }
  },
  "buffer": {
    "key": "user123@domain.net",
    "wait_time": 20,
    "aggregate_field": "json.input.data.query",
    "max_size": 5
  },
  "payload": {
    "json": {
      "input": {
        "data": {
          "query": "mensaje de ejemplo"
        }
      }
    }
  }
}
```

### Respuesta Exitosa

```json
{
  "status": "success",
  "message_id": "msg_123456",
  "buffer_info": {
    "current_size": 3,
    "max_size": 5,
    "time_remaining": 15
  }
}
```

## ⚙️ Configuración

### Variables de Entorno

```bash
# Redis
REDIS_URL=redis://localhost:6379
REDIS_PASSWORD=secure_password

# Aplicación
APP_NAME=buffer-service
LOG_LEVEL=INFO
DEBUG=False

# Seguridad
SECRET_KEY=your_secret_key
ALLOWED_ORIGINS=["https://example.com"]

# Webhooks
WEBHOOK_TIMEOUT=10
MAX_RETRIES=3
```

## 🛠️ Desarrollo

### Estructura del Proyecto

```
buffer-service/
├── src/
│   ├── buffer_service/
│   │   ├── __init__.py
│   │   ├── core/
│   │   ├── api/
│   │   └── models/
│   └── tests/
├── docker/
├── docs/
├── poetry.lock
├── pyproject.toml
└── README.md
```

### Pruebas

```bash
# Ejecutar tests
pytest

# Con cobertura
pytest --cov=src

# Tests específicos
pytest tests/test_buffer_service.py -v
```

### Integración Continua

- GitHub Actions para CI/CD
- Pruebas automáticas en PR
- Análisis de código estático
- Generación de documentación

## 📊 Monitoreo

- Métricas de Prometheus exposición en `/metrics`
- Integración con Grafana
- Logs estructurados
- Trazabilidad distribuida

## 🔐 Seguridad

- Autenticación JWT
- Rate limiting
- CORS configurables
- Validación de entrada

## 📖 Documentación

- API docs: `/docs` (Swagger UI)
- ReDoc: `/redoc`
- Arquitectura: `/docs/architecture.md`
- Guía de contribución: `/docs/CONTRIBUTING.md`

## 🤝 Contribuir

1. Fork el repositorio
2. Crear rama (`git checkout -b feature/amazing`)
3. Commit cambios (`git commit -am 'Add amazing feature'`)
4. Push a la rama (`git push origin feature/amazing`)
5. Abrir Pull Request

## 📝 Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para detalles.

---

<div align="center">

**¿Necesitas ayuda?** [Abrir un issue](https://github.com/user/buffer-service/issues/new) • [Documentación](docs/) • [Discusiones](https://github.com/user/buffer-service/discussions)

</div>