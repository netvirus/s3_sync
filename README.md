
# S3 Sync and Mark Utility

Этот репозиторий содержит две утилиты для работы с S3 бакетами:

1. `s3_sync.py`: Утилита для синхронизации объектов между парами S3 бакетов.
2. `s3_mark.py`: Утилита для сверки двух S3 бакетов и обновления метаданных на бакете-источнике.

## Описание

### `s3_sync.py`

Этот сервис используется для синхронизации объектов между исходным и целевым бакетами. Если объект в исходном бакете не помечен метаданными `synced-to-backup`, утилита скопирует его в целевой бакет и обновит метаданные исходного объекта.
Сервис запускается и остается в памяти производя новые и новые итерации через заданный интервал времени.
Обрабатываться будут бакеты у которых в конфиге выставлено ```enabled: true```

### `s3_mark.py`

Эта утилита проверяет наличие объектов в исходном и целевом бакетах. Если объект присутствует в обоих бакетах, но не помечен метаданными `synced-to-backup` в исходном бакете, утилита обновит метаданные объекта. У каждого бакета есть флаг `managed_s3_mark`, который определяет, будет ли утилита `s3_mark.py` обрабатывать этот бакет.

## Конфигурационный файл

Конфигурация для обеих утилит хранится в файле `config.yaml`. Пример файла конфигурации:

```yaml
telegram:
  bot_token: YOUR_TELEGRAM_BOT_TOKEN
  chat_id: YOUR_TELEGRAM_CHAT_ID
  enabled: true

sync:
  interval: 60  # Интервал в секундах между циклами синхронизации
  check_bucket_exists: true
  metadata_key: synced-to-backup
  metadata_value: '1'

buckets:
  pair:
    example_pair:
      - name: source_bucket
        access-key: YOUR_SOURCE_ACCESS_KEY
        secret-key: YOUR_SOURCE_SECRET_KEY
        endpoint-url: https://your-source-endpoint.com
        bucket-name: your-source-bucket-name
        port: 443
        enabled: true
        managed_s3_mark: false
      - name: target_bucket
        access-key: YOUR_TARGET_ACCESS_KEY
        secret-key: YOUR_TARGET_SECRET_KEY
        endpoint-url: https://your-target-endpoint.com
        bucket-name: your-target-bucket-name
        port: 443
        enabled: true
        managed_s3_mark: false
```

## Запуск

### Запуск `s3_sync.py`

Для запуска утилиты синхронизации используйте следующую команду:

```bash
python3 s3_sync.py
```

Для тестового запуска (без фактической синхронизации) используйте:

```bash
python3 s3_sync.py --dry-run
```

### Запуск `s3_mark.py`

Для запуска утилиты проверки используйте следующую команду:

```bash
python3 s3_mark.py
```

Если в конфиге у бакетов выставлено значение managed_s3_mark: false то s3_mark.py проигноритует эти бакеты.


## Запуск в Docker

Для удобства использования, обе утилиты можно запускать в Docker контейнере. В репозитории уже подготовлены `Dockerfile` и `docker-compose.yml` для этой цели.

### `Dockerfile`

Пример `Dockerfile`:

```dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "s3_sync.py"]
```

### `docker-compose.yml`

Пример `docker-compose.yml`:

```yaml
version: '3.8'

services:
  s3sync:
    build: .
    container_name: s3sync
    volumes:
      - ./config.yaml:/app/config.yaml
    environment:
      - PYTHONUNBUFFERED=1
    restart: always
```

### Запуск в Docker compose

Для запуска утилит в Docker выполните следующие шаги:

1. Склонируйте репозиторий и перейдите в его директорию.
2. Создайте `config.yaml` файл с вашей конфигурацией.
3. Соберите Docker образ:

   ```bash
   docker-compose build
   ```

4. Запустите контейнер:

   ```bash
   docker-compose up -d
   ```
   
Контейнер будет автоматически перезапускаться в случае завершения работы или ошибок.

### Подхват изменений в config.yaml
Если в процессе работы контейнера с s3_sync.py внести изменения в config.yaml то при следующей итерации конфиг будет перечитан и новые параметры тут же вступят в силу.


### Заключение

Эти утилиты предназначены для автоматизации задач по синхронизации и проверке состояния объектов в S3 бакетах. Пожалуйста, настройте конфигурационный файл в соответствии с вашими потребностями и используйте их для упрощения управления вашими данными в S3.

### Автор
Yaroslav Tsuprak

yaroslav.tsuprak@gmail.com