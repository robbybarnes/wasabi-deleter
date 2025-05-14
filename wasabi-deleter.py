import boto3
import botocore
import sys
import time

# === Wasabi Configuration ===
WASABI_ACCESS_KEY = 'your-access-key'
WASABI_SECRET_KEY = 'your-secret-key'

# Use us-east-1 for initial/global operations, regardless of region the data is in
REGION = 'us-east-1'
ENDPOINT_URL = 'https://s3.wasabisys.com'

# === Session Setup ===
session = boto3.session.Session(
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    region_name=REGION
)

# Use this client only for global operations (list_buckets, get_bucket_location)
s3 = session.client('s3', endpoint_url=ENDPOINT_URL)

def get_bucket_region(bucket_name):
    """Fetch the bucket's region."""
    response = s3.get_bucket_location(Bucket=bucket_name)
    # Wasabi returns None or an empty string for us-east-1
    location = response.get('LocationConstraint')
    if not location or location == '':
        return 'us-east-1'
    return location

def get_s3_client_for_bucket(bucket_name):
    """Create an S3 client for the bucket's region and endpoint."""
    region = get_bucket_region(bucket_name)
    # Region-specific endpoint for Wasabi
    if region == 'us-east-1':
        endpoint = 'https://s3.wasabisys.com'
    else:
        endpoint = f'https://s3.{region}.wasabisys.com'
    return session.client('s3', region_name=region, endpoint_url=endpoint)

def list_buckets():
    response = s3.list_buckets()
    return [bucket['Name'] for bucket in response.get('Buckets', [])]

def confirm(prompt):
    ans = input(f"{prompt} (y/N): ").strip().lower()
    return ans == 'y'

def delete_all_objects(bucket_name):
    print(f"\n--- Deleting objects in bucket: {bucket_name} ---")
    s3_bucket = get_s3_client_for_bucket(bucket_name)
    paginator = s3_bucket.get_paginator('list_object_versions')
    deleted_count = 0
    page_num = 0

    try:
        for page in paginator.paginate(Bucket=bucket_name):
            page_num += 1
            print(f"Loaded page {page_num} of object versions")
            objects_to_delete = []

            versions = page.get('Versions', [])
            markers = page.get('DeleteMarkers', [])

            print(f"  Found {len(versions)} versions and {len(markers)} delete markers on this page.")

            for version in versions:
                objects_to_delete.append({
                    'Key': version['Key'],
                    'VersionId': version['VersionId']
                })

            for marker in markers:
                objects_to_delete.append({
                    'Key': marker['Key'],
                    'VersionId': marker['VersionId']
                })

            if objects_to_delete:
                for i in range(0, len(objects_to_delete), 1000):
                    chunk = objects_to_delete[i:i + 1000]
                    # Guard against accidentally sending empty batches
                    if not chunk:
                        continue
                    # Ensure every object has a Key and VersionId
                    valid_chunk = [
                        obj for obj in chunk if 'Key' in obj and 'VersionId' in obj
                    ]
                    if not valid_chunk:
                        continue
                    try:
                        s3_bucket.delete_objects(
                            Bucket=bucket_name,
                            Delete={'Objects': valid_chunk, 'Quiet': True}
                        )
                        deleted_count += len(valid_chunk)
                        print(f"    Deleted so far: {deleted_count}")
                    except botocore.exceptions.ClientError as e:
                        print(f"    Error deleting objects batch: {e}")

        print(f"\nCompleted clearing all objects from bucket '{bucket_name}' ({deleted_count} deleted).")
        return True
    except botocore.exceptions.ClientError as e:
        print(f"\nError clearing bucket '{bucket_name}': {e}")
        return False

def delete_bucket(bucket_name):
    """Delete bucket (using correct regional client)"""
    s3_bucket = get_s3_client_for_bucket(bucket_name)
    try:
        s3_bucket.delete_bucket(Bucket=bucket_name)
        print(f"Deleted bucket: {bucket_name}")
        return True
    except botocore.exceptions.ClientError as e:
        print(f"Failed to delete bucket '{bucket_name}': {e}")
        return False

def main():
    buckets = list_buckets()
    if not buckets:
        print("No buckets found.")
        return

    print("\n=== Buckets Found ===")
    for bucket in buckets:
        print(f"- {bucket}")

    for bucket in buckets:
        if confirm(f"\nDo you want to DELETE ALL OBJECTS and the BUCKET '{bucket}'?"):
            success = delete_all_objects(bucket)
            if success:
                time.sleep(1)  # brief pause to ensure S3 has cleared the state
                if confirm(f"Proceed to DELETE the bucket '{bucket}' now?"):
                    delete_bucket(bucket)
                else:
                    print(f"Skipped bucket deletion: {bucket}")
        else:
            print(f"Skipped: {bucket}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(1)