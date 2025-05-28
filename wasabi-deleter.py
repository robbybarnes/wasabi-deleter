#!/usr/bin/env python3
"""
Wasabi Bucket Cleaner - A robust tool for completely deleting Wasabi S3 buckets and their contents.

This script handles the complexities of deleting versioned objects, delete markers, and buckets
with special characters in object names. It includes automatic retry logic, error handling,
and fallback mechanisms for edge cases.

Author: Robby Barnes
License: MIT
"""

import boto3
import botocore
import sys
import time
import logging
import requests
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from collections import deque
from typing import List, Dict, Optional, Tuple

# === Logging Configuration ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# === Configuration ===
# You can set these as environment variables or pass them as arguments
WASABI_ACCESS_KEY = os.environ.get('WASABI_ACCESS_KEY', '')
WASABI_SECRET_KEY = os.environ.get('WASABI_SECRET_KEY', '')

# Default region and endpoint
REGION = 'us-east-1'
ENDPOINT_URL = 'https://s3.wasabisys.com'

# Performance and reliability settings
MAX_WORKERS = 10  # Number of concurrent threads for deletion
BATCH_SIZE = 500  # Number of objects to delete in a single batch
MAX_RETRIES = 5  # Maximum number of retry attempts
RETRY_DELAY = 1.0  # Initial retry delay in seconds
MAX_RETRY_DELAY = 30.0  # Maximum retry delay in seconds
CONNECTION_POOL_SIZE = 20  # Size of the connection pool
RATE_LIMIT_DELAY = 0.1  # Delay between requests to avoid rate limiting

# Progress tracking
progress_lock = Lock()
stats = {
    'total_deleted': 0,
    'failed_deletes': 0,
    'retried_deletes': 0,
    'server_errors': 0,
    'last_error_time': 0
}


def validate_credentials():
    """Validate that AWS credentials are set."""
    if not WASABI_ACCESS_KEY or not WASABI_SECRET_KEY:
        logger.error("Wasabi credentials not found!")
        logger.error("Please set WASABI_ACCESS_KEY and WASABI_SECRET_KEY environment variables")
        logger.error("Example: export WASABI_ACCESS_KEY='your-access-key'")
        logger.error("         export WASABI_SECRET_KEY='your-secret-key'")
        sys.exit(1)


# === Session and Client Setup ===
def create_session():
    """Create a boto3 session with Wasabi credentials."""
    return boto3.session.Session(
        aws_access_key_id=WASABI_ACCESS_KEY,
        aws_secret_access_key=WASABI_SECRET_KEY,
        region_name=REGION
    )


def create_s3_client(session, endpoint_url=ENDPOINT_URL, config=None):
    """Create an S3 client with the specified configuration."""
    return session.client('s3', endpoint_url=endpoint_url, config=config)


# Initialize session and configuration
validate_credentials()
session = create_session()

# Configure connection pooling and retry logic
config = botocore.config.Config(
    max_pool_connections=CONNECTION_POOL_SIZE,
    retries={
        'max_attempts': MAX_RETRIES,
        'mode': 'adaptive'  # Adaptive retry mode for better handling of throttling
    }
)

# Global S3 client
s3 = create_s3_client(session, config=config)

# Cache for bucket regions and clients to avoid repeated API calls
bucket_region_cache = {}
client_cache = {}


def get_bucket_region(bucket_name: str) -> str:
    """
Determine the region of a Wasabi bucket.

Wasabi buckets can be in different regions, and we need to use
the correct regional endpoint for optimal performance.

Args:
bucket_name: Name of the bucket

Returns:
The region code (e.g., 'us-east-1', 'us-west-1')
    """
    if bucket_name in bucket_region_cache:
        return bucket_region_cache[bucket_name]

    try:
        response = s3.get_bucket_location(Bucket=bucket_name)
        location = response.get('LocationConstraint')
        # Wasabi returns None for us-east-1
        region = 'us-east-1' if not location or location == '' else location
        bucket_region_cache[bucket_name] = region
        return region
    except Exception as e:
        logger.error(f"Failed to get region for bucket {bucket_name}: {e}")
        return 'us-east-1'  # Default fallback


def get_s3_client_for_bucket(bucket_name: str):
    """
Get or create an S3 client for the bucket's specific region.

Using region-specific endpoints improves performance and reliability.

Args:
bucket_name: Name of the bucket

Returns:
Boto3 S3 client configured for the bucket's region
    """
    region = get_bucket_region(bucket_name)

    if region in client_cache:
        return client_cache[region]

    # Construct region-specific endpoint for Wasabi
    if region == 'us-east-1':
        endpoint = 'https://s3.wasabisys.com'
    else:
        endpoint = f'https://s3.{region}.wasabisys.com'

    client = create_s3_client(session, endpoint_url=endpoint, config=config)
    client_cache[region] = client
    return client


def list_buckets() -> List[str]:
    """
List all buckets in the Wasabi account.

Returns:
List of bucket names
    """
    try:
        response = s3.list_buckets()
        return [bucket['Name'] for bucket in response.get('Buckets', [])]
    except Exception as e:
        logger.error(f"Failed to list buckets: {e}")
        return []


def confirm(prompt: str) -> bool:
    """
Get user confirmation for destructive operations.

Args:
prompt: The confirmation prompt to display

Returns:
True if user confirms, False otherwise
    """
    ans = input(f"{prompt} (y/N): ").strip().lower()
    return ans == 'y'


def delete_object_batch_with_retry(s3_bucket, bucket_name: str,
                                   objects_to_delete: List[Dict],
                                   retry_count: int = 0) -> Tuple[int, List[Dict]]:
    """
Delete a batch of objects with retry logic and error handling.

This function handles various error conditions including:
- MalformedXML errors (falls back to individual deletion)
- Server errors (500) with exponential backoff
- Request timeouts
- Rate limiting

Args:
s3_bucket: Boto3 S3 client
bucket_name: Name of the bucket
objects_to_delete: List of objects to delete (with Key and optionally VersionId)
retry_count: Current retry attempt number

Returns:
Tuple of (number of successfully deleted objects, list of failed objects)
    """
    # Add rate limiting to avoid overwhelming the service
    time.sleep(RATE_LIMIT_DELAY)

    try:
        # Log problematic keys for debugging (only in debug mode)
        for obj in objects_to_delete[:5]:  # Just log first 5
            if any(ord(c) < 32 or ord(c) > 126 for c in obj.get('Key', '')):
                logger.debug(f"Object with special chars: {repr(obj.get('Key', ''))}")

        # Attempt batch deletion
        response = s3_bucket.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': objects_to_delete,
                'Quiet': False  # Get detailed response for error handling
            }
        )

        # Check for partial failures
        errors = response.get('Errors', [])
        deleted = response.get('Deleted', [])

        if errors:
            logger.warning(f"Batch delete had {len(errors)} errors")
            if retry_count < MAX_RETRIES:
                # Exponential backoff with jitter
                delay = min(RETRY_DELAY * (2 ** retry_count) + (time.time() % 1), MAX_RETRY_DELAY)
                time.sleep(delay)
                with progress_lock:
                    stats['retried_deletes'] += len(errors)
                # Retry only the failed objects
                failed_objects = [{'Key': err['Key'], 'VersionId': err.get('VersionId')}
                                  for err in errors]
                return delete_object_batch_with_retry(
                    s3_bucket, bucket_name, failed_objects, retry_count + 1
                )
            else:
                with progress_lock:
                    stats['failed_deletes'] += len(errors)
                return len(deleted), errors

        return len(deleted), []

    except botocore.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        status_code = e.response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)

        # Handle MalformedXML errors - usually caused by special characters in keys
        if error_code == 'MalformedXML':
            logger.warning(f"MalformedXML error - attempting to delete objects individually")
            # Try deleting objects one by one
            deleted_count = 0
            failed_objects = []

            for obj in objects_to_delete:
                try:
                    if 'VersionId' in obj:
                        s3_bucket.delete_object(
                            Bucket=bucket_name,
                            Key=obj['Key'],
                            VersionId=obj['VersionId']
                        )
                    else:
                        s3_bucket.delete_object(
                            Bucket=bucket_name,
                            Key=obj['Key']
                        )
                    deleted_count += 1
                except Exception as e2:
                    logger.debug(f"Failed to delete {repr(obj.get('Key', ''))}: {e2}")
                    failed_objects.append(obj)

            return deleted_count, failed_objects

        # Handle 500 errors with more aggressive retry
        elif status_code == 500:
            with progress_lock:
                stats['server_errors'] += 1
                stats['last_error_time'] = time.time()

            if retry_count < MAX_RETRIES:
                # Longer backoff for server errors
                delay = min(RETRY_DELAY * (3 ** retry_count) + (time.time() % 2), MAX_RETRY_DELAY)
                logger.warning(f"Server error (500), retrying in {delay:.1f}s (attempt {retry_count + 1}/{MAX_RETRIES})")
                time.sleep(delay)

                # Split batch in half if it's a server error and batch is large
                if len(objects_to_delete) > 100:
                    mid = len(objects_to_delete) // 2
                    logger.info(f"Splitting batch of {len(objects_to_delete)} objects into two")
                    deleted1, errors1 = delete_object_batch_with_retry(
                        s3_bucket, bucket_name, objects_to_delete[:mid], retry_count + 1
                    )
                    deleted2, errors2 = delete_object_batch_with_retry(
                        s3_bucket, bucket_name, objects_to_delete[mid:], retry_count + 1
                    )
                    return deleted1 + deleted2, errors1 + errors2
                else:
                    return delete_object_batch_with_retry(
                        s3_bucket, bucket_name, objects_to_delete, retry_count + 1
                    )

        # Handle request timeouts
        elif error_code == 'RequestTimeout' and retry_count < MAX_RETRIES:
            delay = min(RETRY_DELAY * (2 ** retry_count), MAX_RETRY_DELAY)
            time.sleep(delay)
            return delete_object_batch_with_retry(
                s3_bucket, bucket_name, objects_to_delete, retry_count + 1
            )

        # Log other errors and mark all objects as failed
        logger.error(f"Error deleting batch: {e}")
        with progress_lock:
            stats['failed_deletes'] += len(objects_to_delete)
        return 0, objects_to_delete


def abort_incomplete_uploads_parallel(s3_bucket, bucket_name: str):
    """
Abort all incomplete multipart uploads in parallel.

Incomplete multipart uploads can prevent bucket deletion and consume storage.

Args:
s3_bucket: Boto3 S3 client
bucket_name: Name of the bucket
    """
    try:
        paginator = s3_bucket.get_paginator('list_multipart_uploads')
        uploads_to_abort = []

        # Collect all incomplete uploads
        for page in paginator.paginate(Bucket=bucket_name):
            uploads = page.get('Uploads', [])
            uploads_to_abort.extend(uploads)

        if not uploads_to_abort:
            return

        logger.info(f"Found {len(uploads_to_abort)} incomplete uploads to abort")

        def abort_upload(upload):
            """Abort a single multipart upload."""
            try:
                s3_bucket.abort_multipart_upload(
                    Bucket=bucket_name,
                    Key=upload['Key'],
                    UploadId=upload['UploadId']
                )
                return True, upload['Key']
            except Exception as e:
                return False, f"{upload['Key']} - {str(e)}"

        # Abort uploads in parallel
        with ThreadPoolExecutor(max_workers=min(10, len(uploads_to_abort))) as executor:
            futures = [executor.submit(abort_upload, upload) for upload in uploads_to_abort]

            aborted = 0
            failed = 0
            for future in as_completed(futures):
                success, key = future.result()
                if success:
                    aborted += 1
                else:
                    failed += 1
                    logger.warning(f"Failed to abort upload: {key}")

            logger.info(f"Aborted {aborted} uploads, {failed} failed")

    except Exception as e:
        logger.error(f"Error listing multipart uploads: {e}")


def check_bucket_versioning(s3_bucket, bucket_name: str) -> bool:
    """
Check if bucket versioning is enabled.

Args:
s3_bucket: Boto3 S3 client
bucket_name: Name of the bucket

Returns:
True if versioning is enabled, False otherwise
    """
    try:
        response = s3_bucket.get_bucket_versioning(Bucket=bucket_name)
        status = response.get('Status', 'Disabled')
        return status == 'Enabled'
    except Exception as e:
        logger.error(f"Error checking versioning status: {e}")
        return False


def disable_bucket_versioning(s3_bucket, bucket_name: str) -> bool:
    """
Disable (suspend) bucket versioning.

Note: This doesn't delete existing versions, but prevents new versions
from being created during the deletion process.

Args:
s3_bucket: Boto3 S3 client
bucket_name: Name of the bucket

Returns:
True if successful, False otherwise
    """
    try:
        s3_bucket.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={'Status': 'Suspended'}
        )
        logger.info(f"✓ Disabled versioning for bucket: {bucket_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to disable versioning: {e}")
        return False


def delete_bucket_policy(s3_bucket, bucket_name: str):
    """
Delete bucket policy if it exists.

Bucket policies might prevent deletion operations.

Args:
s3_bucket: Boto3 S3 client
bucket_name: Name of the bucket
    """
    try:
        s3_bucket.delete_bucket_policy(Bucket=bucket_name)
        logger.info(f"✓ Deleted bucket policy for: {bucket_name}")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchBucketPolicy':
            logger.warning(f"Could not delete bucket policy: {e}")


def delete_bucket_lifecycle(s3_bucket, bucket_name: str):
    """
Delete bucket lifecycle configuration if it exists.

Lifecycle rules might interfere with deletion operations.

Args:
s3_bucket: Boto3 S3 client
bucket_name: Name of the bucket
    """
    try:
        s3_bucket.delete_bucket_lifecycle(Bucket=bucket_name)
        logger.info(f"✓ Deleted lifecycle configuration for: {bucket_name}")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchLifecycleConfiguration':
            logger.warning(f"Could not delete lifecycle configuration: {e}")


def check_object_lock_configuration(s3_bucket, bucket_name: str) -> bool:
    """
Check if bucket has object lock enabled.

Object lock can prevent deletion of objects.

Args:
s3_bucket: Boto3 S3 client
bucket_name: Name of the bucket

Returns:
True if object lock is enabled, False otherwise
    """
    try:
        response = s3_bucket.get_object_lock_configuration(Bucket=bucket_name)
        return 'ObjectLockConfiguration' in response
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ObjectLockConfigurationNotFoundError':
            return False
        logger.error(f"Error checking object lock configuration: {e}")
        return True  # Assume it's enabled if we can't check


def verify_bucket_empty(s3_bucket, bucket_name: str) -> bool:
    """
Verify that a bucket is completely empty.

Checks for:
- Regular objects
- Object versions
- Delete markers

Args:
s3_bucket: Boto3 S3 client
bucket_name: Name of the bucket

Returns:
True if bucket is empty, False otherwise
    """
    try:
        # Check for any regular objects
        response = s3_bucket.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
        if response.get('KeyCount', 0) > 0:
            return False

        # Check for any versions or delete markers
        response = s3_bucket.list_object_versions(Bucket=bucket_name, MaxKeys=1)
        if response.get('Versions', []) or response.get('DeleteMarkers', []):
            return False

        return True
    except Exception as e:
        logger.error(f"Error verifying bucket is empty: {e}")
        return False


def delete_all_objects(bucket_name: str, max_workers: int = MAX_WORKERS) -> bool:
    """
Delete all objects in a bucket, including all versions and delete markers.

This function:
1. Checks and disables versioning if enabled
2. Removes bucket policies and lifecycle rules
3. Aborts incomplete multipart uploads
4. Deletes all objects, versions, and delete markers in parallel
5. Performs a final verification and cleanup

Args:
bucket_name: Name of the bucket to clear
max_workers: Maximum number of concurrent workers

Returns:
True if all objects were deleted successfully, False otherwise
    """
    logger.info(f"\n{'='*50}")
    logger.info(f"Deleting all objects in bucket: {bucket_name}")
    logger.info(f"{'='*50}")

    s3_bucket = get_s3_client_for_bucket(bucket_name)
    start_time = time.time()

    # Check for object lock
    if check_object_lock_configuration(s3_bucket, bucket_name):
        logger.warning(f"⚠️  Bucket '{bucket_name}' has object lock enabled. Some objects may not be deletable.")

    # Check and disable versioning if enabled
    if check_bucket_versioning(s3_bucket, bucket_name):
        logger.info(f"Bucket '{bucket_name}' has versioning enabled. Attempting to suspend...")
        disable_bucket_versioning(s3_bucket, bucket_name)

    # Delete bucket policy and lifecycle rules
    delete_bucket_policy(s3_bucket, bucket_name)
    delete_bucket_lifecycle(s3_bucket, bucket_name)

    # Reset stats for this bucket
    with progress_lock:
        stats['total_deleted'] = 0
        stats['failed_deletes'] = 0
        stats['retried_deletes'] = 0

    # Abort incomplete uploads first
    abort_incomplete_uploads_parallel(s3_bucket, bucket_name)

    iteration = 0
    while True:
        iteration += 1
        logger.info(f"\nIteration {iteration} - Scanning for objects...")

        paginator = s3_bucket.get_paginator('list_object_versions')
        page_config = {
            'PageSize': 1000,  # Max page size for better performance
        }

        # Use a queue for better memory management with large buckets
        batches_queue = deque()
        total_found = 0

        try:
            # Collect all objects, versions, and delete markers
            for page in paginator.paginate(Bucket=bucket_name, PaginationConfig=page_config):
                versions = page.get('Versions', [])
                markers = page.get('DeleteMarkers', [])

                # Create deletion requests for all objects
                objects_to_delete = [
                    {"Key": v["Key"], "VersionId": v["VersionId"]} for v in versions
                ] + [
                        {"Key": m["Key"], "VersionId": m["VersionId"]} for m in markers
                    ]

                # Create batches for parallel processing
                for i in range(0, len(objects_to_delete), BATCH_SIZE):
                    batch = objects_to_delete[i:i + BATCH_SIZE]
                    batches_queue.append(batch)
                    total_found += len(batch)

        except Exception as e:
            logger.error(f"Error listing objects: {e}")
            return False

        if not batches_queue:
            logger.info("No more objects to delete")
            break

        logger.info(f"Found {total_found} objects in {len(batches_queue)} batches")

        # Process batches in parallel with adaptive concurrency
        batch_deleted = 0
        failed_objects = []

        # Adaptive worker count based on server errors
        current_workers = max_workers
        if stats.get('server_errors', 0) > 10:
            current_workers = max(2, max_workers // 2)
            logger.warning(f"Reducing workers to {current_workers} due to server errors")

        with ThreadPoolExecutor(max_workers=current_workers) as executor:
            futures = []

            # Submit batches with flow control
            submitted = 0
            while batches_queue:
                # Check if we should slow down based on recent errors
                with progress_lock:
                    if stats.get('last_error_time', 0) > time.time() - 60:
                        # Had an error in the last minute, slow down
                        time.sleep(0.5)

                batch = batches_queue.popleft()
                future = executor.submit(
                    delete_object_batch_with_retry,
                    s3_bucket,
                    bucket_name,
                    batch
                )
                futures.append(future)
                submitted += 1

                # Limit concurrent submissions
                if submitted % current_workers == 0:
                    time.sleep(0.2)  # Brief pause every batch of workers

            # Process results with progress updates
            completed = 0
            for future in as_completed(futures):
                deleted, errors = future.result()
                batch_deleted += deleted
                failed_objects.extend(errors)

                with progress_lock:
                    stats['total_deleted'] += deleted
                    completed += 1

                # Progress update every 10 batches
                if completed % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = stats['total_deleted'] / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"Progress: {completed}/{len(futures)} batches | "
                        f"Total deleted: {stats['total_deleted']} | "
                        f"Rate: {rate:.1f} objects/sec"
                    )

        if batch_deleted == 0:
            logger.info("No objects were deleted in this iteration")
            break

        logger.info(f"Iteration {iteration} complete: {batch_deleted} objects deleted")

    # Final cleanup of any remaining multipart uploads
    abort_incomplete_uploads_parallel(s3_bucket, bucket_name)

    # Verify bucket is truly empty
    logger.info("Verifying bucket is completely empty...")
    if not verify_bucket_empty(s3_bucket, bucket_name):
        logger.warning("⚠️  Bucket still contains objects or delete markers after deletion attempts")
        # Try one more aggressive cleanup
        logger.info("Attempting final cleanup pass...")
        paginator = s3_bucket.get_paginator('list_object_versions')
        remaining_objects = []

        for page in paginator.paginate(Bucket=bucket_name, PaginationConfig={'PageSize': 1000}):
            versions = page.get('Versions', [])
            markers = page.get('DeleteMarkers', [])

            for v in versions:
                remaining_objects.append({"Key": v["Key"], "VersionId": v["VersionId"]})
            for m in markers:
                remaining_objects.append({"Key": m["Key"], "VersionId": m["VersionId"]})

        if remaining_objects:
            logger.info(f"Found {len(remaining_objects)} remaining objects/markers")
            # Delete in small batches
            for i in range(0, len(remaining_objects), 100):
                batch = remaining_objects[i:i + 100]
                try:
                    s3_bucket.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': batch, 'Quiet': False}
                    )
                except Exception as e:
                    logger.error(f"Final cleanup error: {e}")

    # Final statistics
    elapsed = time.time() - start_time
    logger.info(f"\n{'='*50}")
    logger.info(f"Bucket '{bucket_name}' clearing completed!")
    logger.info(f"Total objects deleted: {stats['total_deleted']}")
    logger.info(f"Failed deletes: {stats['failed_deletes']}")
    logger.info(f"Retried deletes: {stats['retried_deletes']}")
    logger.info(f"Server errors encountered: {stats.get('server_errors', 0)}")
    logger.info(f"Time elapsed: {elapsed:.2f} seconds")
    logger.info(f"Average rate: {stats['total_deleted']/elapsed:.1f} objects/sec")
    logger.info(f"{'='*50}\n")

    # Final empty check
    is_empty = verify_bucket_empty(s3_bucket, bucket_name)
    if is_empty:
        logger.info("✓ Bucket is verified empty")
    else:
        logger.warning("⚠️  Bucket may still contain objects")

    return stats['failed_deletes'] == 0 and is_empty


def delete_bucket_with_force(bucket_name: str, force: bool = False) -> bool:
    """
Delete a bucket, optionally using Wasabi's force_delete parameter.

The force_delete parameter is Wasabi-specific and attempts to delete
all objects in the bucket before deleting the bucket itself.

Args:
bucket_name: Name of the bucket to delete
force: Whether to use force_delete parameter

Returns:
True if deletion was successful, False otherwise
    """
    s3_bucket = get_s3_client_for_bucket(bucket_name)

    # First attempt with force_delete parameter if requested
    if force:
        try:
            # Wasabi-specific force delete using query parameter
            from botocore.awsrequest import AWSRequest
            from botocore.auth import SigV4Auth

            # Get the endpoint for the bucket's region
            region = get_bucket_region(bucket_name)
            if region == 'us-east-1':
                endpoint = 'https://s3.wasabisys.com'
            else:
                endpoint = f'https://s3.{region}.wasabisys.com'

            # Create signed request for force delete
            url = f"{endpoint}/{bucket_name}?force_delete=true"
            request = AWSRequest(method='DELETE', url=url)
            SigV4Auth(session.get_credentials(), 's3', region).add_auth(request)

            prepared_request = request.prepare()
            response = requests.delete(
                prepared_request.url,
                headers=dict(prepared_request.headers),
                timeout=30
            )

            if response.status_code == 204:
                logger.info(f"✓ Successfully force-deleted bucket: {bucket_name}")
                return True
            else:
                logger.warning(f"Force delete returned status {response.status_code}: {response.text}")
        except Exception as e:
            logger.warning(f"Force delete failed: {e}")

    # Regular delete with retries
    for attempt in range(MAX_RETRIES):
        try:
            s3_bucket.delete_bucket(Bucket=bucket_name)
            logger.info(f"✓ Successfully deleted bucket: {bucket_name}")
            return True
        except botocore.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'BucketNotEmpty':
                logger.error(f"Bucket '{bucket_name}' is not empty. Please delete all objects first.")
                return False
            elif error_code == 'NoSuchBucket':
                logger.warning(f"Bucket '{bucket_name}' does not exist.")
                return True
            else:
                logger.error(f"Attempt {attempt + 1} failed to delete bucket: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                else:
                    return False


def delete_bucket(bucket_name: str) -> bool:
    """
Delete a bucket using standard S3 API.

Args:
bucket_name: Name of the bucket to delete

Returns:
True if deletion was successful, False otherwise
    """
    return delete_bucket_with_force(bucket_name, force=False)


def main():
    """
Main function that orchestrates the bucket deletion process.

Provides an interactive interface for selecting and deleting buckets.
    """
    try:
        # List all buckets
        buckets = list_buckets()
        if not buckets:
            logger.info("No buckets found.")
            return

        logger.info("\n=== Buckets Found ===")
        for i, bucket in enumerate(buckets, 1):
            logger.info(f"{i}. {bucket}")

        # Ask if user wants to delete all buckets at once
        if len(buckets) > 1:
            if confirm(f"\nDelete ALL {len(buckets)} buckets and their contents?"):
                for bucket in buckets:
                    logger.info(f"\nProcessing bucket: {bucket}")
                    if delete_all_objects(bucket):
                        time.sleep(0.5)  # Brief pause for S3 consistency
                        if not delete_bucket(bucket):
                            logger.warning(f"Failed to delete bucket. Trying force delete...")
                            delete_bucket_with_force(bucket, force=True)
                return

        # Individual bucket processing
        for bucket in buckets:
            if confirm(f"\nDelete bucket '{bucket}' and ALL its contents?"):
                if delete_all_objects(bucket):
                    time.sleep(0.5)  # Brief pause for S3 consistency
                    if confirm(f"Proceed to DELETE the bucket '{bucket}'?"):
                        if not delete_bucket(bucket):
                            logger.warning(f"Failed to delete bucket. Trying force delete...")
                            delete_bucket_with_force(bucket, force=True)
                    else:
                        logger.info(f"Skipped bucket deletion: {bucket}")
            else:
                logger.info(f"Skipped: {bucket}")

        logger.info("\n✓ Operation completed successfully!")

    except KeyboardInterrupt:
        logger.warning("\n\n⚠️  Operation interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()