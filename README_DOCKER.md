# Docker Setup para ETL Star Wars

Este proyecto esta compuesto por Dockerfiles separados para cada fase del ETL y un docker-compose para orquestar todo el proceso.

## Estructura

### Dockerfiles
- `Dockerfile.mongo` - Contenedor para MongoDB (base de datos)
- `Dockerfile.extract` - Contenedor creado para la fase de extracción
- `Dockerfile.transform` - Contenedor creado para la fase de transformación
- `Dockerfile.load` - Contenedor creado para la fase de carga

### Docker Compose
- `docker-compose.yml` - Orquestación completa (MongoDB + ETL)

## Uso

### Ejecutar todo el ETL (MongoDB + ETL)

```bash
# Ejecutar todo el pipeline (MongoDB + ETL)
docker-compose up

# Ejecutar en segundo plano
docker-compose up -d

# Ver logs
docker-compose logs -f

# Ver logs de un servicio específico
docker-compose logs -f mongodb
docker-compose logs -f extract
docker-compose logs -f transform
docker-compose logs -f load

# Detener todo
docker-compose down
```

### Ejecutar solo MongoDB

```bash
# Levantar solo MongoDB
docker-compose up mongodb

# En segundo plano
docker-compose up -d mongodb

# Ver logs de MongoDB
docker-compose logs -f mongodb
```

### Ejecutar solo el setup de MongoDB

```bash
# Configurar colecciones e índices
docker-compose up mongo_setup
```

### Ejecutar cada fase del ETL individualmente

Si quieres ejecutar solo una fase específica:

#### Extract
```bash
docker build -f Dockerfile.extract -t harry-potter-extract .
docker run -v $(pwd)/data:/app/data harry-potter-extract
```

#### Transform
```bash
docker build -f Dockerfile.transform -t harry-potter-transform .
docker run -v $(pwd)/data:/app/data harry-potter-transform
```

#### Load
```bash
# Primero asegúrate de que MongoDB esté corriendo
docker run -d --name mongodb -p 27017:27017 mongo:7

# Ejecutar setup de MongoDB
docker build -f Dockerfile.transform -t harry-potter-setup .
docker run --link mongodb:mongodb harry-potter-setup python -c "from mongo_setup import MongoSetup; MongoSetup('mongodb://mongodb:27017/').setup_all()"

# Ejecutar load
docker build -f Dockerfile.load -t harry-potter-load .
docker run -v $(pwd)/data:/app/data --link mongodb:mongodb -e MONGO_CONNECTION=mongodb://mongodb:27017/ harry-potter-load
```

## Datos

Los datos intermedios se guardan en el directorio `./data`:
- `1.raw_data.json` - Datos extraídos de SWAPI
- `2.transformed_data.json` - Datos transformados

## MongoDB

MongoDB se ejecuta en un contenedor separado y está disponible en `localhost:27017`.

### Acceder a MongoDB

```bash
# Conectarse a MongoDB
docker exec -it star_wars_mongo mongosh

# O desde fuera del contenedor
mongosh mongodb://localhost:27017/star_wars
```

### Setup inicial de MongoDB

El setup de MongoDB se ejecuta automáticamente cuando levantas el ETL completo. Si quieres ejecutarlo manualmente:

```bash
docker-compose up mongo_setup
```

## Limpieza

```bash
# Detener y eliminar contenedores
docker-compose down

# Eliminar también volúmenes (¡CUIDADO! Borra los datos de MongoDB)
docker-compose down -v

# Eliminar imágenes
docker rmi harry-potter-extract harry-potter-transform harry-potter-load

# Limpiar todo (contenedores, imágenes, volúmenes)
docker system prune -a --volumes
```

## Arquitectura

```
┌─────────────┐
│   MongoDB   │  ← Base de datos (Dockerfile.mongo)
└─────────────┘
       ↑
       │
┌─────────────┐
│   Extract   │  ← Fase 1: Extracción (Dockerfile.extract)
└─────────────┘
       ↓
┌─────────────┐
│  Transform  │  ← Fase 2: Transformación (Dockerfile.transform)
└─────────────┘
       ↓
┌─────────────┐
│    Load     │  ← Fase 3: Carga (Dockerfile.load)
└─────────────┘
       ↓
┌─────────────┐
│   MongoDB   │
└─────────────┘
```

