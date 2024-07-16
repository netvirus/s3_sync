import aioboto3
import asyncio
import requests
import yaml
import time
import sys
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError


def load_config(config_path='config.yaml'):
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)


def send_telegram_message(token, chat_id, message, enabled):
    if not enabled:
        return
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    data = {'chat_id': chat_id, 'text': message}
    try:
        response = requests.post(url, data=data)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error sending message to Telegram: {e}")


def validate_bucket_pairs(config):
    if 'buckets' not in config or 'pair' not in config['buckets']:
        raise ValueError("Configuration must contain 'buckets' and 'pair' keys.")

    pairs = config['buckets']['pair']
    pair_count = 0
    for pair_name, pair_buckets in pairs.items():
        if len(pair_buckets) != 2:
            raise ValueError(f"Bucket pair {pair_name} must contain exactly two buckets.")
        for bucket in pair_buckets:
            if 'name' not in bucket or 'access-key' not in bucket or 'secret-key' not in bucket or 'endpoint-url' not in bucket or 'port' not in bucket or 'bucket-name' not in bucket:
                raise ValueError(f"Bucket {bucket} in pair {pair_name} is missing required fields.")
        pair_count += 1

    print(f"All bucket pairs are correctly specified. Total pairs found: {pair_count}")


async def check_bucket_exists(s3_client, bucket_name):
    try:
        await s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Bucket {bucket_name} does not exist.")
        else:
            print(f"Error checking bucket {bucket_name}: {e}")
        return False


async def copy_object_alternative(s3_client_source, s3_client_target, source_bucket_name, target_bucket_name, key,
                                  metadata_key, metadata_value):
    try:
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

    except ClientError as e:
        print(f"Error copying {key} from {source_bucket_name} to {target_bucket_name}: {e}")
        raise


async def sync_bucket_pair(session, source_bucket, target_bucket, check_exists, metadata_key, metadata_value,
                           dry_run=False):
    source_endpoint = f"{source_bucket['endpoint-url']}:{source_bucket['port']}"
    target_endpoint = f"{target_bucket['endpoint-url']}:{target_bucket['port']}"

    async with session.client(
            's3',
            aws_access_key_id=source_bucket['access-key'],
            aws_secret_access_key=source_bucket['secret-key'],
            endpoint_url=source_endpoint
    ) as s3_client_source, session.client(
        's3',
        aws_access_key_id=target_bucket['access-key'],
        aws_secret_access_key=target_bucket['secret-key'],
        endpoint_url=target_endpoint
    ) as s3_client_target:

        source_bucket_name = source_bucket['bucket-name']
        target_bucket_name = target_bucket['bucket-name']

        if check_exists:
            if not await check_bucket_exists(s3_client_source, source_bucket_name):
                return f"Source bucket {source_bucket_name} does not exist."
            if not await check_bucket_exists(s3_client_target, target_bucket_name):
                return f"Target bucket {target_bucket_name} does not exist."

        try:
            response = await s3_client_source.list_objects_v2(Bucket=source_bucket_name)
            objects = response.get('Contents', [])
        except ClientError as e:
            return f"Error listing objects in source bucket {source_bucket_name}: {e}"

        if objects:
            tasks = []
            copied_count = 0
            for obj in objects:
                key = obj['Key']
                try:
                    metadata_response = await s3_client_source.head_object(Bucket=source_bucket_name, Key=key)
                    metadata = metadata_response['Metadata']
                except ClientError as e:
                    return f"Error getting metadata for {key} in source bucket {source_bucket_name}: {e}"

                if metadata.get(metadata_key) != metadata_value:
                    if dry_run:
                        print(f"Dry run: Would copy {key} from {source_bucket_name} to {target_bucket_name}")
                        print(f"Dry run: Would update metadata of {key} in {source_bucket_name}")
                    else:
                        tasks.append(copy_object_alternative(s3_client_source, s3_client_target, source_bucket_name,
                                                             target_bucket_name, key, metadata_key, metadata_value))
                        copied_count += 1

            if tasks:
                try:
                    await asyncio.gather(*tasks)
                except ClientError as e:
                    return f"Error during copying objects from {source_bucket_name} to {target_bucket_name}: {e}"
            return f"Synchronization completed for bucket pair: {source_bucket_name} -> {target_bucket_name}: {copied_count} objects copied."
        else:
            return f"No one objects found for synchronization in source bucket: {source_bucket_name}"


async def sync_buckets(config, dry_run=False):
    session = aioboto3.Session()
    pairs = config['buckets']['pair']
    tasks = []
    metadata_key = config['sync'].get('metadata_key', 'synced-to-backup')
    metadata_value = config['sync'].get('metadata_value', '1')
    for pair_name, pair_buckets in pairs.items():
        check_exists = config['sync'].get('check_bucket_exists', True)
        tasks.append(
            sync_bucket_pair(session, pair_buckets[0], pair_buckets[1], check_exists, metadata_key, metadata_value,
                             dry_run))
    results = await asyncio.gather(*tasks)
    for result in results:
        telegram_enabled = config['telegram'].get('enabled', True)
        print(result)
        send_telegram_message(config['telegram']['bot_token'], config['telegram']['chat_id'], result, telegram_enabled)


if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv
    config = load_config()
    try:
        validate_bucket_pairs(config)
        print("Bucket pairs to be synchronized:")
        pairs = config['buckets']['pair']
        for pair_name, pair_buckets in pairs.items():
            print(f"{pair_buckets[0]['bucket-name']} -> {pair_buckets[1]['bucket-name']}")
        while True:
            asyncio.run(sync_buckets(config, dry_run))
            time.sleep(config['sync']['interval'])
    except Exception as e:
        telegram_enabled = config['telegram'].get('enabled', True)
        if 'telegram' in config and 'bot_token' in config['telegram'] and 'chat_id' in config['telegram']:
            send_telegram_message(config['telegram']['bot_token'], config['telegram']['chat_id'], f"Error: {e}",
                                  telegram_enabled)
        print(f"Error: {e}")