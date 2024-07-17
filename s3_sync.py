import aioboto3
import asyncio
import requests
import yaml
import time
import sys
import hashlib
from datetime import datetime
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

# Функция для вывода сообщений с добавлением даты и времени
def log_message(message):
    print(f"[ {datetime.now().strftime('%d.%m.%Y %H:%M:%S')} ] {message}")

# Загружаем конфигурацию из файла
def load_config(config_path='config.yaml'):
    log_message(f"Loading configuration from {config_path}")
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)

# Отправляем сообщение в Telegram
def send_telegram_message(token, chat_id, message, enabled):
    if not enabled:
        return
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    data = {'chat_id': chat_id, 'text': message}
    try:
        response = requests.post(url, data=data)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        log_message(f"Error sending message to Telegram: {e}")

# Валидируем пары бакетов в конфигурации
def validate_bucket_pairs(config):
    if 'buckets' not in config or 'pair' not in config['buckets']:
        raise ValueError("Configuration must contain 'buckets' and 'pair' keys.")

    pairs = config['buckets']['pair']
    valid_pairs = {}
    for pair_name, pair_buckets in pairs.items():
        if len(pair_buckets) != 2:
            log_message(f"Skipping pair {pair_name} because it must contain exactly two buckets.")
            continue
        if not pair_buckets[0].get('enabled', True) or not pair_buckets[1].get('enabled', True):
            log_message(f"Skipping pair {pair_name} because one or both buckets are disabled.")
            continue
        for bucket in pair_buckets:
            if 'name' not in bucket or 'access-key' not in bucket or 'secret-key' not in bucket or 'endpoint-url' not in bucket or 'port' not in bucket or 'bucket-name' not in bucket:
                log_message(f"Skipping pair {pair_name} because bucket {bucket} is missing required fields.")
                break
        else:
            valid_pairs[pair_name] = pair_buckets

    if not valid_pairs:
        log_message("No valid bucket pairs found in configuration.")

    config['buckets']['pair'] = valid_pairs
    log_message(f"All bucket pairs are correctly specified. Total pairs found: {len(valid_pairs)}")

# Проверяем существование бакета
async def check_bucket_exists(s3_client, bucket_name):
    try:
        await s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            log_message(f"Bucket {bucket_name} does not exist.")
        else:
            log_message(f"Error checking bucket {bucket_name}: {e}")
        return False

# Копируем объект из исходного бакета в целевой
async def copy_object_alternative(s3_client_source, s3_client_target, source_bucket_name, target_bucket_name, key, metadata_key, metadata_value):
    try:
        log_message(f"Copying {key} from {source_bucket_name} to {target_bucket_name}")
        # Загрузка объекта из исходного бакета
        obj = await s3_client_source.get_object(Bucket=source_bucket_name, Key=key)
        object_data = await obj['Body'].read()

        # Загрузка объекта в целевой бакет
        await s3_client_target.put_object(Bucket=target_bucket_name, Key=key, Body=object_data)

        # Обновляем метаданные объекта в исходном бакете, устанавливая 'synced-to-backup' в '1'
        await s3_client_source.copy_object(
            CopySource={'Bucket': source_bucket_name, 'Key': key},
            Bucket=source_bucket_name,
            Key=key,
            Metadata={metadata_key: metadata_value},
            MetadataDirective='REPLACE'
        )
        log_message(f"Successfully copied {key} from {source_bucket_name} to {target_bucket_name} and updated metadata")

    except ClientError as e:
        log_message(f"Error copying {key} from {source_bucket_name} to {target_bucket_name}: {e}")

# Синхронизация пары бакетов
async def sync_bucket_pair(session, source_bucket, target_bucket, check_exists, metadata_key, metadata_value, dry_run=False):
    # Проверка включена ли пара бакетов
    if not source_bucket.get('enabled', True) or not target_bucket.get('enabled', True):
        message = f"Skipping pair {source_bucket['bucket-name']} -> {target_bucket['bucket-name']} due to enabled set to false"
        log_message(message)
        return message

    source_endpoint = f"{source_bucket['endpoint-url']}:{source_bucket['port']}"
    target_endpoint = f"{target_bucket['endpoint-url']}:{target_bucket['port']}"

    async with session.client(
            's3',
            aws_access_key_id=source_bucket['access-key'],
            aws_secret_access_key=source_bucket['secret-key'],
            endpoint_url=source_endpoint,
            verify=False  # Отключение проверки SSL-сертификатов
    ) as s3_client_source, session.client(
        's3',
        aws_access_key_id=target_bucket['access-key'],
        aws_secret_access_key=target_bucket['secret-key'],
        endpoint_url=target_endpoint,
        verify=False  # Отключение проверки SSL-сертификатов
    ) as s3_client_target:

        source_bucket_name = source_bucket['bucket-name']
        target_bucket_name = target_bucket['bucket-name']

        if check_exists:
            if not await check_bucket_exists(s3_client_source, source_bucket_name):
                message = f"Source bucket {source_bucket_name} does not exist."
                log_message(message)
                return message
            if not await check_bucket_exists(s3_client_target, target_bucket_name):
                message = f"Target bucket {target_bucket_name} does not exist."
                log_message(message)
                return message

        try:
            log_message(f"Listing objects in source bucket: {source_bucket_name}")
            response = await s3_client_source.list_objects_v2(Bucket=source_bucket_name)
            objects = response.get('Contents', [])
            log_message(f"Found [{len(objects)}] objects in source bucket: {source_bucket_name}")
        except ClientError as e:
            message = f"Error listing objects in source bucket {source_bucket_name}: {e}"
            log_message(message)
            return message

        if objects:
            tasks = []
            copied_count = 0
            log_message(f"Getting metadata for Objects in source bucket: {source_bucket_name}")
            for obj in objects:
                key = obj['Key']
                try:
                    metadata_response = await s3_client_source.head_object(Bucket=source_bucket_name, Key=key)
                    metadata = metadata_response['Metadata']
                except ClientError as e:
                    message = f"Error getting metadata for {key} in source bucket {source_bucket_name}: {e}"
                    log_message(message)
                    return message

                if metadata.get(metadata_key) != metadata_value:
                    if dry_run:
                        log_message(f"Dry run: Would copy {key} from {source_bucket_name} to {target_bucket_name}")
                        log_message(f"Dry run: Would update metadata of {key} in {source_bucket_name}")
                    else:
                        tasks.append(copy_object_alternative(s3_client_source, s3_client_target, source_bucket_name, target_bucket_name, key, metadata_key, metadata_value))
                        copied_count += 1

            if tasks:
                try:
                    await asyncio.gather(*tasks)
                except ClientError as e:
                    message = f"Error during copying objects from {source_bucket_name} to {target_bucket_name}: {e}"
                    log_message(message)
                    return message
            return f"Synchronization completed for bucket pair: {source_bucket_name} -> {target_bucket_name}: {copied_count} objects copied."
        else:
            return f"No one objects found for synchronization in source bucket: {source_bucket_name}"

# Синхронизация всех пар бакетов
async def sync_buckets(config, dry_run=False):
    session = aioboto3.Session()
    pairs = config['buckets']['pair']
    tasks = []
    metadata_key = config['sync'].get('metadata_key', 'synced-to-backup')
    metadata_value = config['sync'].get('metadata_value', '1')
    for pair_name, pair_buckets in pairs.items():
        check_exists = config['sync'].get('check_bucket_exists', True)
        task = sync_bucket_pair(session, pair_buckets[0], pair_buckets[1], check_exists, metadata_key, metadata_value, dry_run)
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    for result in results:
        telegram_enabled = config['telegram'].get('enabled', True)
        log_message(result)
        send_telegram_message(config['telegram'].get('bot_token'), config['telegram'].get('chat_id'), result, telegram_enabled)


# Рассчитываем хэш сумму файла
def calculate_file_hash(file_path):
    hasher = hashlib.md5()
    with open(file_path, 'rb') as file:
        buffer = file.read()
        hasher.update(buffer)
    return hasher.hexdigest()


# Выводим информацию о конфигурации
def print_config_info(config):
    interval = config['sync']['interval']
    pairs = config['buckets']['pair']
    total_pairs = len(pairs)
    enabled_pairs = sum(1 for pair in pairs.values() if pair[0].get('enabled', True) and pair[1].get('enabled', True))
    disabled_pairs = total_pairs - enabled_pairs
    log_message(f"[ Config reloaded. Interval: {interval} seconds. Total pairs: [{total_pairs}], Enabled pairs: [{enabled_pairs}], Disabled pairs: [{disabled_pairs}] ]")


if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv
    config_path = 'config.yaml'
    config = load_config(config_path)
    config_hash = calculate_file_hash(config_path)

    try:
        validate_bucket_pairs(config)
        log_message(f"Bucket pairs to be synchronized:")
        pairs = config['buckets']['pair']
        for pair_name, pair_buckets in pairs.items():
            log_message(f"[ {pair_buckets[0]['bucket-name']} -> {pair_buckets[1]['bucket-name']} ]")

        while True:
            # Проверка изменения хэш суммы конфигурационного файла
            new_config_hash = calculate_file_hash(config_path)
            if new_config_hash != config_hash:
                log_message(f"Configuration file changed, reloading...")
                config = load_config(config_path)
                validate_bucket_pairs(config)
                config_hash = new_config_hash
                print_config_info(config)  # Вывод информации о конфигурации

            log_message(f"Starting synchronization cycle...")
            asyncio.run(sync_buckets(config, dry_run))
            log_message(f"Synchronization cycle completed.")
            time.sleep(config['sync']['interval'])
    except Exception as e:
        telegram_enabled = config['telegram'].get('enabled', True)
        if 'telegram' in config and 'bot_token' in config['telegram'] and 'chat_id' in config['telegram']:
            send_telegram_message(config['telegram']['bot_token'], config['telegram']['chat_id'], f"Error: {e}", telegram_enabled)
        log_message(f"[ Error: {e} ]")
