FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . .

# Команда запуска по умолчанию (можно запустить как сервис, так и утилиту)
ENTRYPOINT ["sh", "-c"]

# Запуск сервиса по умолчанию
CMD ["python s3_sync.py"]

# Для запуска утилиты используйте:
# docker run --rm -it <image_name> python s3_mark.py