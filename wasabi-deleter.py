import boto3
import botocore
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def abort_incomplete_uploads(s3_bucket, bucket_name):
    paginator = s3_bucket.get_paginator('list_multipart_uploads')
    for page in paginator.paginate(Bucket=bucket_name):
        uploads = page.get('Uploads', [])
        for upload in uploads:
            try:
                s3_bucket.abort_multipart_upload(
                    Bucket=bucket_name, Key=upload['Key'], UploadId=upload['UploadId']
                )
                print(f"  Aborted multipart upload: {upload['Key']}")
            except botocore.exceptions.ClientError as e:
                print(f"  Error aborting multipart upload {upload['Key']}: {e}")

def delete_object_batch(s3_bucket, bucket_name, objects_to_delete):
    try:
        s3_bucket.delete_objects(
            Bucket=bucket_name,
            Delete={'Objects': objects_to_delete, 'Quiet': True}
        )
        return len(objects_to_delete)
    except botocore.exceptions.ClientError as e:
        print(f"    Error deleting objects batch: {e}")
        return 0

def delete_all_objects(bucket_name, max_workers=4):
    print(f"\n--- Deleting objects in bucket: {bucket_name} ---")
    s3_bucket = get_s3_client_for_bucket(bucket_name)
    total_deleted = 0
    last_deleted = -1

    # Abort incomplete multipart uploads first
    abort_incomplete_uploads(s3_bucket, bucket_name)

    while last_deleted != 0:  # Repeat until no deletions in a pass
        paginator = s3_bucket.get_paginator('list_object_versions')
        objects_batches = []
        last_deleted = 0

        for page in paginator.paginate(Bucket=bucket_name):
            versions = page.get('Versions', [])
            markers = page.get('DeleteMarkers', [])
            objects_to_delete = [
                {"Key": v["Key"], "VersionId": v["VersionId"]} for v in versions
            ] + [
                    {"Key": m["Key"], "VersionId": m["VersionId"]} for m in markers
                ]

            # Batch up to 1000
            for i in range(0, len(objects_to_delete), 1000):
                objects_batches.append(objects_to_delete[i:i + 1000])

        if objects_batches:
            print(f"  Found {sum(len(b) for b in objects_batches)} items to delete in {len(objects_batches)} batches.")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(delete_object_batch, s3_bucket, bucket_name, batch)
                    for batch in objects_batches
                ]
                for future in as_completed(futures):
                    deleted = future.result()
                    total_deleted += deleted
                    last_deleted += deleted
                    print(f"    Deleted so far: {total_deleted}")

        else:
            last_deleted = 0  # no more to delete

        # Also handle incomplete multipart uploads (again, just in case)
        abort_incomplete_uploads(s3_bucket, bucket_name)

    print(f"\nCompleted clearing all objects from bucket '{bucket_name}' ({total_deleted} deleted).")
    return True

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