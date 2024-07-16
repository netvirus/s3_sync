import aioboto3
import asyncio
import yaml
from botocore.exceptions import ClientError


def load_config(config_path='config.yaml'):
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)


async def check_and_sync_buckets(config):
    session = aioboto3.Session()
    pairs = config['buckets']['pair']
    metadata_key = config['sync'].get('metadata_key', 'synced-to-backup')
    metadata_value = config['sync'].get('metadata_value', '1')

    for pair_name, pair_buckets in pairs.items():
        source_bucket = pair_buckets[0]
        target_bucket = pair_buckets[1]

        if not source_bucket.get('managed_s3_mark', True):
            print(f":: Skipping pair {source_bucket['name']} -> {target_bucket['name']} due to managed_s3_mark set to false ::")
            print(f"=====================================================")
            continue

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

            print(f"[ Checking and syncing bucket pair: {source_bucket_name} -> {target_bucket_name} ]")

            # Получаем список объектов из исходного и целевого бакетов
            try:
                source_objects = await s3_client_source.list_objects_v2(Bucket=source_bucket_name)
                target_objects = await s3_client_target.list_objects_v2(Bucket=target_bucket_name)
                print(f"[ Retrieved object lists from {source_bucket_name} and {target_bucket_name} ]")
            except ClientError as e:
                print(f"Error listing objects in buckets: {e}")
                continue

            source_keys = {obj['Key'] for obj in source_objects.get('Contents', [])}
            target_keys = {obj['Key'] for obj in target_objects.get('Contents', [])}

            # Объекты, которые присутствуют в целевом бакете
            common_keys = source_keys & target_keys
            print(f"[ Found {len(common_keys)} common objects in {target_bucket_name} ]")

            # Объекты, которые отсутствуют в целевом бакете и не имеют метки в исходном бакете
            missing_and_unmarked_count = 0
            for key in source_keys - target_keys:
                try:
                    metadata_response = await s3_client_source.head_object(Bucket=source_bucket_name, Key=key)
                    metadata = metadata_response['Metadata']
                    if metadata.get(metadata_key) != metadata_value:
                        missing_and_unmarked_count += 1
                except ClientError as e:
                    print(f"Error checking metadata for {key} in {source_bucket_name}: {e}")

            print(f"[ Objects not found in target bucket and not marked in source bucket: {missing_and_unmarked_count} ]")

            already_marked_count = 0
            newly_marked_count = 0

            # Помечаем объекты, которые присутствуют в целевом бакете и не имеют метки в исходном бакете
            for key in common_keys:
                try:
                    metadata_response = await s3_client_source.head_object(Bucket=source_bucket_name, Key=key)
                    metadata = metadata_response['Metadata']
                    if metadata.get(metadata_key) != metadata_value:
                        await mark_object(s3_client_source, source_bucket_name, key, metadata_key, metadata_value)
                        newly_marked_count += 1
                    else:
                        already_marked_count += 1
                except ClientError as e:
                    print(f"Error checking metadata for {key} in {source_bucket_name}: {e}")

            print(f"[ Objects already marked: {already_marked_count} ]")
            print(f"[ New objects marked: {newly_marked_count} ]")
            print(f"=====================================================")


async def mark_object(s3_client_source, source_bucket_name, key, metadata_key, metadata_value):
    try:
        await s3_client_source.copy_object(
            CopySource={'Bucket': source_bucket_name, 'Key': key},
            Bucket=source_bucket_name,
            Key=key,
            Metadata={metadata_key: metadata_value},
            MetadataDirective='REPLACE'
        )
        print(f"Marked {key} in {source_bucket_name}")
    except ClientError as e:
        print(f"Error marking {key} in {source_bucket_name}: {e}")


if __name__ == '__main__':
    config = load_config()
    try:
        asyncio.run(check_and_sync_buckets(config))
    except Exception as e:
        print(f"Error: {e}")
