#!/usr/bin/env python3
"""
Wasabi Bucket Cleaner - A robust tool for completely deleting Wasabi S3 buckets.

This script handles the complexities of deleting versioned objects, delete markers,
and buckets with special characters in object names. It includes automatic retry
logic, error handling, and fallback mechanisms for edge cases.

Author: Robby Barnes
License: MIT
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from threading import Lock
from typing import TYPE_CHECKING

import boto3
import botocore.config
import botocore.exceptions
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from boto3 import Session


# === Logging Configuration ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


@dataclass
class WasabiConfig:
    """Configuration settings for Wasabi bucket operations."""

    access_key: str
    secret_key: str
    region: str = "us-east-1"
    endpoint_url: str = "https://s3.wasabisys.com"
    max_workers: int = 10
    batch_size: int = 500
    max_retries: int = 5
    retry_delay: float = 1.0
    max_retry_delay: float = 30.0
    connection_pool_size: int = 20
    rate_limit_delay: float = 0.1

    @classmethod
    def from_environment(cls) -> WasabiConfig:
        """Create configuration from environment variables."""
        access_key = os.environ.get("WASABI_ACCESS_KEY", "")
        secret_key = os.environ.get("WASABI_SECRET_KEY", "")

        if not access_key or not secret_key:
            logger.error("Wasabi credentials not found!")
            logger.error(
                "Please set WASABI_ACCESS_KEY and WASABI_SECRET_KEY environment variables"
            )
            logger.error("Example: export WASABI_ACCESS_KEY='your-access-key'")
            logger.error("         export WASABI_SECRET_KEY='your-secret-key'")
            sys.exit(1)

        return cls(access_key=access_key, secret_key=secret_key)


@dataclass
class DeletionStats:
    """Statistics for tracking deletion progress."""

    total_deleted: int = 0
    failed_deletes: int = 0
    retried_deletes: int = 0
    server_errors: int = 0
    last_error_time: float = 0.0
    _lock: Lock = field(default_factory=Lock, repr=False, compare=False)

    def reset(self) -> None:
        """Reset all statistics to zero."""
        with self._lock:
            self.total_deleted = 0
            self.failed_deletes = 0
            self.retried_deletes = 0
            self.server_errors = 0
            self.last_error_time = 0.0

    def increment_deleted(self, count: int = 1) -> None:
        """Thread-safe increment of deleted count."""
        with self._lock:
            self.total_deleted += count

    def increment_failed(self, count: int = 1) -> None:
        """Thread-safe increment of failed count."""
        with self._lock:
            self.failed_deletes += count

    def increment_retried(self, count: int = 1) -> None:
        """Thread-safe increment of retried count."""
        with self._lock:
            self.retried_deletes += count

    def record_server_error(self) -> None:
        """Thread-safe recording of server error."""
        with self._lock:
            self.server_errors += 1
            self.last_error_time = time.time()


class WasabiBucketCleaner:
    """
    A robust tool for completely deleting Wasabi S3 buckets.

    Handles versioned objects, delete markers, multipart uploads, and objects
    with special characters in their names.

    Attributes:
        config: Configuration settings for the cleaner.
        stats: Statistics tracking deletion progress.
    """

    def __init__(self, config: WasabiConfig) -> None:
        """
        Initialize the bucket cleaner with configuration.

        Args:
            config: Wasabi configuration settings.
        """
        self.config = config
        self.stats = DeletionStats()
        self._session: Session | None = None
        self._s3_client: S3Client | None = None
        self._boto_config: botocore.config.Config | None = None
        self._bucket_region_cache: dict[str, str] = {}
        self._client_cache: dict[str, S3Client] = {}

    @property
    def session(self) -> Session:
        """Lazily create and cache boto3 session."""
        if self._session is None:
            self._session = boto3.session.Session(
                aws_access_key_id=self.config.access_key,
                aws_secret_access_key=self.config.secret_key,
                region_name=self.config.region,
            )
        return self._session

    @property
    def boto_config(self) -> botocore.config.Config:
        """Lazily create and cache boto configuration."""
        if self._boto_config is None:
            self._boto_config = botocore.config.Config(
                max_pool_connections=self.config.connection_pool_size,
                retries={"max_attempts": self.config.max_retries, "mode": "adaptive"},
            )
        return self._boto_config

    @property
    def s3_client(self) -> S3Client:
        """Lazily create and cache default S3 client."""
        if self._s3_client is None:
            self._s3_client = self.session.client(
                "s3", endpoint_url=self.config.endpoint_url, config=self.boto_config
            )
        return self._s3_client

    def _get_bucket_region(self, bucket_name: str) -> str:
        """
        Determine the region of a Wasabi bucket.

        Wasabi buckets can be in different regions, and we need to use
        the correct regional endpoint for optimal performance.

        Args:
            bucket_name: Name of the bucket.

        Returns:
            The region code (e.g., 'us-east-1', 'us-west-1').
        """
        if bucket_name in self._bucket_region_cache:
            return self._bucket_region_cache[bucket_name]

        try:
            response = self.s3_client.get_bucket_location(Bucket=bucket_name)
            location = response.get("LocationConstraint")
            region = "us-east-1" if not location or location == "" else location
            self._bucket_region_cache[bucket_name] = region
            return region
        except botocore.exceptions.ClientError as e:
            logger.error(f"Failed to get region for bucket {bucket_name}: {e}")
            return "us-east-1"

    def _get_s3_client_for_bucket(self, bucket_name: str) -> S3Client:
        """
        Get or create an S3 client for the bucket's specific region.

        Using region-specific endpoints improves performance and reliability.

        Args:
            bucket_name: Name of the bucket.

        Returns:
            Boto3 S3 client configured for the bucket's region.
        """
        region = self._get_bucket_region(bucket_name)

        if region in self._client_cache:
            return self._client_cache[region]

        if region == "us-east-1":
            endpoint = "https://s3.wasabisys.com"
        else:
            endpoint = f"https://s3.{region}.wasabisys.com"

        client = self.session.client(
            "s3", endpoint_url=endpoint, config=self.boto_config
        )
        self._client_cache[region] = client
        return client

    def list_buckets(self) -> list[str]:
        """
        List all buckets in the Wasabi account.

        Returns:
            List of bucket names.
        """
        try:
            response = self.s3_client.list_buckets()
            return [bucket["Name"] for bucket in response.get("Buckets", [])]
        except botocore.exceptions.ClientError as e:
            logger.error(f"Failed to list buckets: {e}")
            return []

    def _delete_object_batch_with_retry(
        self,
        s3_client: S3Client,
        bucket_name: str,
        objects_to_delete: list[dict[str, str]],
        retry_count: int = 0,
    ) -> tuple[int, list[dict[str, str]]]:
        """
        Delete a batch of objects with retry logic and error handling.

        Handles various error conditions including MalformedXML errors,
        server errors (500) with exponential backoff, request timeouts,
        and rate limiting.

        Args:
            s3_client: Boto3 S3 client.
            bucket_name: Name of the bucket.
            objects_to_delete: List of objects to delete with Key and optional VersionId.
            retry_count: Current retry attempt number.

        Returns:
            Tuple of (successfully deleted count, list of failed objects).
        """
        time.sleep(self.config.rate_limit_delay)

        try:
            for obj in objects_to_delete[:5]:
                key = obj.get("Key", "")
                if any(ord(c) < 32 or ord(c) > 126 for c in key):
                    logger.debug(f"Object with special chars: {repr(key)}")

            response = s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": objects_to_delete, "Quiet": False},
            )

            errors = response.get("Errors", [])
            deleted = response.get("Deleted", [])

            if errors:
                logger.warning(f"Batch delete had {len(errors)} errors")
                if retry_count < self.config.max_retries:
                    delay = min(
                        self.config.retry_delay * (2**retry_count) + (time.time() % 1),
                        self.config.max_retry_delay,
                    )
                    time.sleep(delay)
                    self.stats.increment_retried(len(errors))
                    failed_objects = []
                    for err in errors:
                        if "Key" in err:
                            obj_identifier = {"Key": err["Key"]}
                            if version_id := err.get("VersionId"):
                                obj_identifier["VersionId"] = version_id
                            failed_objects.append(obj_identifier)
                    return self._delete_object_batch_with_retry(
                        s3_client, bucket_name, failed_objects, retry_count + 1
                    )
                else:
                    self.stats.increment_failed(len(errors))
                    return len(deleted), errors

            return len(deleted), []

        except botocore.exceptions.ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            status_code = e.response.get("ResponseMetadata", {}).get(
                "HTTPStatusCode", 0
            )

            if error_code == "MalformedXML":
                logger.warning(
                    "MalformedXML error - attempting individual object deletion"
                )
                return self._delete_objects_individually(
                    s3_client, bucket_name, objects_to_delete
                )

            if status_code == 500:
                self.stats.record_server_error()
                if retry_count < self.config.max_retries:
                    delay = min(
                        self.config.retry_delay * (3**retry_count) + (time.time() % 2),
                        self.config.max_retry_delay,
                    )
                    logger.warning(
                        f"Server error (500), retrying in {delay:.1f}s "
                        f"(attempt {retry_count + 1}/{self.config.max_retries})"
                    )
                    time.sleep(delay)

                    if len(objects_to_delete) > 100:
                        mid = len(objects_to_delete) // 2
                        logger.info(
                            f"Splitting batch of {len(objects_to_delete)} objects"
                        )
                        deleted1, errors1 = self._delete_object_batch_with_retry(
                            s3_client, bucket_name, objects_to_delete[:mid], retry_count + 1
                        )
                        deleted2, errors2 = self._delete_object_batch_with_retry(
                            s3_client, bucket_name, objects_to_delete[mid:], retry_count + 1
                        )
                        return deleted1 + deleted2, errors1 + errors2
                    else:
                        return self._delete_object_batch_with_retry(
                            s3_client, bucket_name, objects_to_delete, retry_count + 1
                        )

            if error_code == "RequestTimeout" and retry_count < self.config.max_retries:
                delay = min(
                    self.config.retry_delay * (2**retry_count),
                    self.config.max_retry_delay,
                )
                time.sleep(delay)
                return self._delete_object_batch_with_retry(
                    s3_client, bucket_name, objects_to_delete, retry_count + 1
                )

            logger.error(f"Error deleting batch: {e}")
            self.stats.increment_failed(len(objects_to_delete))
            return 0, objects_to_delete

    def _delete_objects_individually(
        self,
        s3_client: S3Client,
        bucket_name: str,
        objects_to_delete: list[dict[str, str]],
    ) -> tuple[int, list[dict[str, str]]]:
        """
        Delete objects one by one as a fallback for batch failures.

        Args:
            s3_client: Boto3 S3 client.
            bucket_name: Name of the bucket.
            objects_to_delete: List of objects to delete.

        Returns:
            Tuple of (successfully deleted count, list of failed objects).
        """
        deleted_count = 0
        failed_objects = []

        for obj in objects_to_delete:
            try:
                delete_kwargs = {"Bucket": bucket_name, "Key": obj["Key"]}
                if "VersionId" in obj and obj["VersionId"]:
                    delete_kwargs["VersionId"] = obj["VersionId"]
                s3_client.delete_object(**delete_kwargs)
                deleted_count += 1
            except botocore.exceptions.ClientError as e:
                logger.debug(f"Failed to delete {repr(obj.get('Key', ''))}: {e}")
                failed_objects.append(obj)

        return deleted_count, failed_objects

    def _abort_incomplete_uploads(self, s3_client: S3Client, bucket_name: str) -> None:
        """
        Abort all incomplete multipart uploads in parallel.

        Incomplete multipart uploads can prevent bucket deletion and consume storage.

        Args:
            s3_client: Boto3 S3 client.
            bucket_name: Name of the bucket.
        """
        try:
            paginator = s3_client.get_paginator("list_multipart_uploads")
            uploads_to_abort = []

            for page in paginator.paginate(Bucket=bucket_name):
                uploads = page.get("Uploads", [])
                uploads_to_abort.extend(uploads)

            if not uploads_to_abort:
                return

            logger.info(f"Found {len(uploads_to_abort)} incomplete uploads to abort")

            def abort_upload(upload: dict) -> tuple[bool, str]:
                try:
                    s3_client.abort_multipart_upload(
                        Bucket=bucket_name,
                        Key=upload["Key"],
                        UploadId=upload["UploadId"],
                    )
                    return True, upload["Key"]
                except botocore.exceptions.ClientError as e:
                    return False, f"{upload['Key']} - {e}"

            worker_count = min(10, len(uploads_to_abort))
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [
                    executor.submit(abort_upload, upload) for upload in uploads_to_abort
                ]

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

        except botocore.exceptions.ClientError as e:
            logger.error(f"Error listing multipart uploads: {e}")

    def _check_bucket_versioning(self, s3_client: S3Client, bucket_name: str) -> bool:
        """
        Check if bucket versioning is enabled.

        Args:
            s3_client: Boto3 S3 client.
            bucket_name: Name of the bucket.

        Returns:
            True if versioning is enabled, False otherwise.
        """
        try:
            response = s3_client.get_bucket_versioning(Bucket=bucket_name)
            status = response.get("Status", "Disabled")
            return status == "Enabled"
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error checking versioning status: {e}")
            return False

    def _disable_bucket_versioning(
        self, s3_client: S3Client, bucket_name: str
    ) -> bool:
        """
        Disable (suspend) bucket versioning.

        This doesn't delete existing versions, but prevents new versions
        from being created during the deletion process.

        Args:
            s3_client: Boto3 S3 client.
            bucket_name: Name of the bucket.

        Returns:
            True if successful, False otherwise.
        """
        try:
            s3_client.put_bucket_versioning(
                Bucket=bucket_name, VersioningConfiguration={"Status": "Suspended"}
            )
            logger.info(f"Disabled versioning for bucket: {bucket_name}")
            return True
        except botocore.exceptions.ClientError as e:
            logger.error(f"Failed to disable versioning: {e}")
            return False

    def _delete_bucket_policy(self, s3_client: S3Client, bucket_name: str) -> None:
        """
        Delete bucket policy if it exists.

        Bucket policies might prevent deletion operations.

        Args:
            s3_client: Boto3 S3 client.
            bucket_name: Name of the bucket.
        """
        try:
            s3_client.delete_bucket_policy(Bucket=bucket_name)
            logger.info(f"Deleted bucket policy for: {bucket_name}")
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchBucketPolicy":
                logger.warning(f"Could not delete bucket policy: {e}")

    def _delete_bucket_lifecycle(self, s3_client: S3Client, bucket_name: str) -> None:
        """
        Delete bucket lifecycle configuration if it exists.

        Lifecycle rules might interfere with deletion operations.

        Args:
            s3_client: Boto3 S3 client.
            bucket_name: Name of the bucket.
        """
        try:
            s3_client.delete_bucket_lifecycle(Bucket=bucket_name)
            logger.info(f"Deleted lifecycle configuration for: {bucket_name}")
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchLifecycleConfiguration":
                logger.warning(f"Could not delete lifecycle configuration: {e}")

    def _check_object_lock(self, s3_client: S3Client, bucket_name: str) -> bool:
        """
        Check if bucket has object lock enabled.

        Object lock can prevent deletion of objects.

        Args:
            s3_client: Boto3 S3 client.
            bucket_name: Name of the bucket.

        Returns:
            True if object lock is enabled, False otherwise.
        """
        try:
            response = s3_client.get_object_lock_configuration(Bucket=bucket_name)
            return "ObjectLockConfiguration" in response
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ObjectLockConfigurationNotFoundError":
                return False
            logger.error(f"Error checking object lock configuration: {e}")
            return True

    def _verify_bucket_empty(self, s3_client: S3Client, bucket_name: str) -> bool:
        """
        Verify that a bucket is completely empty.

        Checks for regular objects, object versions, and delete markers.

        Args:
            s3_client: Boto3 S3 client.
            bucket_name: Name of the bucket.

        Returns:
            True if bucket is empty, False otherwise.
        """
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
            if response.get("KeyCount", 0) > 0:
                return False

            response = s3_client.list_object_versions(Bucket=bucket_name, MaxKeys=1)
            if response.get("Versions", []) or response.get("DeleteMarkers", []):
                return False

            return True
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error verifying bucket is empty: {e}")
            return False

    def delete_all_objects(self, bucket_name: str) -> bool:
        """
        Delete all objects in a bucket, including versions and delete markers.

        This method:
        1. Checks and disables versioning if enabled
        2. Removes bucket policies and lifecycle rules
        3. Aborts incomplete multipart uploads
        4. Deletes all objects, versions, and delete markers in parallel
        5. Performs final verification and cleanup

        Args:
            bucket_name: Name of the bucket to clear.

        Returns:
            True if all objects were deleted successfully, False otherwise.
        """
        logger.info(f"\n{'=' * 50}")
        logger.info(f"Deleting all objects in bucket: {bucket_name}")
        logger.info(f"{'=' * 50}")

        s3_client = self._get_s3_client_for_bucket(bucket_name)
        start_time = time.time()

        if self._check_object_lock(s3_client, bucket_name):
            logger.warning(
                f"Bucket '{bucket_name}' has object lock enabled. "
                "Some objects may not be deletable."
            )

        if self._check_bucket_versioning(s3_client, bucket_name):
            logger.info(
                f"Bucket '{bucket_name}' has versioning enabled. Suspending..."
            )
            self._disable_bucket_versioning(s3_client, bucket_name)

        self._delete_bucket_policy(s3_client, bucket_name)
        self._delete_bucket_lifecycle(s3_client, bucket_name)
        self.stats.reset()
        self._abort_incomplete_uploads(s3_client, bucket_name)

        iteration = 0
        while True:
            iteration += 1
            logger.info(f"\nIteration {iteration} - Scanning for objects...")

            paginator = s3_client.get_paginator("list_object_versions")
            page_config = {"PageSize": 1000}

            batches_queue: deque[list[dict[str, str]]] = deque()
            total_found = 0

            try:
                for page in paginator.paginate(
                    Bucket=bucket_name, PaginationConfig=page_config
                ):
                    versions = page.get("Versions", [])
                    markers = page.get("DeleteMarkers", [])

                    objects_to_delete = [
                        {"Key": v["Key"], "VersionId": v["VersionId"]}
                        for v in versions
                    ] + [
                        {"Key": m["Key"], "VersionId": m["VersionId"]}
                        for m in markers
                    ]

                    for i in range(0, len(objects_to_delete), self.config.batch_size):
                        batch = objects_to_delete[i : i + self.config.batch_size]
                        batches_queue.append(batch)
                        total_found += len(batch)

            except botocore.exceptions.ClientError as e:
                logger.error(f"Error listing objects: {e}")
                return False

            if not batches_queue:
                logger.info("No more objects to delete")
                break

            logger.info(f"Found {total_found} objects in {len(batches_queue)} batches")

            batch_deleted = 0
            current_workers = self.config.max_workers
            if self.stats.server_errors > 10:
                current_workers = max(2, self.config.max_workers // 2)
                logger.warning(
                    f"Reducing workers to {current_workers} due to server errors"
                )

            with ThreadPoolExecutor(max_workers=current_workers) as executor:
                futures = []
                submitted = 0

                while batches_queue:
                    if self.stats.last_error_time > time.time() - 60:
                        time.sleep(0.5)

                    batch = batches_queue.popleft()
                    future = executor.submit(
                        self._delete_object_batch_with_retry,
                        s3_client,
                        bucket_name,
                        batch,
                    )
                    futures.append(future)
                    submitted += 1

                    if submitted % current_workers == 0:
                        time.sleep(0.2)

                completed = 0
                for future in as_completed(futures):
                    deleted, _ = future.result()
                    batch_deleted += deleted
                    self.stats.increment_deleted(deleted)
                    completed += 1

                    if completed % 10 == 0:
                        elapsed = time.time() - start_time
                        rate = (
                            self.stats.total_deleted / elapsed if elapsed > 0 else 0
                        )
                        logger.info(
                            f"Progress: {completed}/{len(futures)} batches | "
                            f"Total deleted: {self.stats.total_deleted} | "
                            f"Rate: {rate:.1f} objects/sec"
                        )

            if batch_deleted == 0:
                logger.info("No objects were deleted in this iteration")
                break

            logger.info(f"Iteration {iteration} complete: {batch_deleted} objects deleted")

        self._abort_incomplete_uploads(s3_client, bucket_name)

        logger.info("Verifying bucket is completely empty...")
        if not self._verify_bucket_empty(s3_client, bucket_name):
            logger.warning(
                "Bucket still contains objects or delete markers after deletion"
            )
            logger.info("Attempting final cleanup pass...")
            self._final_cleanup(s3_client, bucket_name)

        elapsed = time.time() - start_time
        self._log_statistics(bucket_name, elapsed)

        is_empty = self._verify_bucket_empty(s3_client, bucket_name)
        if is_empty:
            logger.info("Bucket is verified empty")
        else:
            logger.warning("Bucket may still contain objects")

        return self.stats.failed_deletes == 0 and is_empty

    def _final_cleanup(self, s3_client: S3Client, bucket_name: str) -> None:
        """
        Perform a final aggressive cleanup of remaining objects.

        Args:
            s3_client: Boto3 S3 client.
            bucket_name: Name of the bucket.
        """
        paginator = s3_client.get_paginator("list_object_versions")
        remaining_objects: list[dict[str, str]] = []

        for page in paginator.paginate(
            Bucket=bucket_name, PaginationConfig={"PageSize": 1000}
        ):
            for v in page.get("Versions", []):
                remaining_objects.append({"Key": v["Key"], "VersionId": v["VersionId"]})
            for m in page.get("DeleteMarkers", []):
                remaining_objects.append({"Key": m["Key"], "VersionId": m["VersionId"]})

        if remaining_objects:
            logger.info(f"Found {len(remaining_objects)} remaining objects/markers")
            for i in range(0, len(remaining_objects), 100):
                batch = remaining_objects[i : i + 100]
                try:
                    s3_client.delete_objects(
                        Bucket=bucket_name, Delete={"Objects": batch, "Quiet": False}
                    )
                except botocore.exceptions.ClientError as e:
                    logger.error(f"Final cleanup error: {e}")

    def _log_statistics(self, bucket_name: str, elapsed: float) -> None:
        """
        Log deletion statistics.

        Args:
            bucket_name: Name of the bucket.
            elapsed: Time elapsed in seconds.
        """
        rate = self.stats.total_deleted / elapsed if elapsed > 0 else 0
        logger.info(f"\n{'=' * 50}")
        logger.info(f"Bucket '{bucket_name}' clearing completed!")
        logger.info(f"Total objects deleted: {self.stats.total_deleted}")
        logger.info(f"Failed deletes: {self.stats.failed_deletes}")
        logger.info(f"Retried deletes: {self.stats.retried_deletes}")
        logger.info(f"Server errors encountered: {self.stats.server_errors}")
        logger.info(f"Time elapsed: {elapsed:.2f} seconds")
        logger.info(f"Average rate: {rate:.1f} objects/sec")
        logger.info(f"{'=' * 50}\n")

    def delete_bucket(self, bucket_name: str, force: bool = False) -> bool:
        """
        Delete a bucket, optionally using Wasabi's force_delete parameter.

        The force_delete parameter is Wasabi-specific and attempts to delete
        all objects in the bucket before deleting the bucket itself.

        Args:
            bucket_name: Name of the bucket to delete.
            force: Whether to use force_delete parameter.

        Returns:
            True if deletion was successful, False otherwise.
        """
        s3_client = self._get_s3_client_for_bucket(bucket_name)

        if force:
            if self._force_delete_bucket(bucket_name):
                return True

        for attempt in range(self.config.max_retries):
            try:
                s3_client.delete_bucket(Bucket=bucket_name)
                logger.info(f"Successfully deleted bucket: {bucket_name}")
                return True
            except botocore.exceptions.ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "BucketNotEmpty":
                    logger.error(
                        f"Bucket '{bucket_name}' is not empty. "
                        "Please delete all objects first."
                    )
                    return False
                elif error_code == "NoSuchBucket":
                    logger.warning(f"Bucket '{bucket_name}' does not exist.")
                    return True
                else:
                    logger.error(f"Attempt {attempt + 1} failed to delete bucket: {e}")
                    if attempt < self.config.max_retries - 1:
                        time.sleep(self.config.retry_delay * (attempt + 1))
                    else:
                        return False
        return False

    def _force_delete_bucket(self, bucket_name: str) -> bool:
        """
        Attempt force deletion of a bucket using Wasabi's special parameter.

        Args:
            bucket_name: Name of the bucket to delete.

        Returns:
            True if successful, False otherwise.
        """
        try:
            region = self._get_bucket_region(bucket_name)
            if region == "us-east-1":
                endpoint = "https://s3.wasabisys.com"
            else:
                endpoint = f"https://s3.{region}.wasabisys.com"

            url = f"{endpoint}/{bucket_name}?force_delete=true"
            request = AWSRequest(method="DELETE", url=url)
            SigV4Auth(self.session.get_credentials(), "s3", region).add_auth(request)

            prepared_request = request.prepare()
            response = requests.delete(
                prepared_request.url,
                headers=dict(prepared_request.headers),
                timeout=30,
            )

            if response.status_code == 204:
                logger.info(f"Successfully force-deleted bucket: {bucket_name}")
                return True
            else:
                logger.warning(
                    f"Force delete returned status {response.status_code}: "
                    f"{response.text}"
                )
        except requests.RequestException as e:
            logger.warning(f"Force delete failed: {e}")
        return False


def confirm(prompt: str) -> bool:
    """
    Get user confirmation for destructive operations.

    Args:
        prompt: The confirmation prompt to display.

    Returns:
        True if user confirms, False otherwise.
    """
    ans = input(f"{prompt} (y/N): ").strip().lower()
    return ans == "y"


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description="Wasabi Bucket Cleaner - Delete Wasabi S3 buckets and their contents",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          Interactive mode - list and select buckets
  %(prog)s --bucket my-bucket       Delete specific bucket
  %(prog)s --bucket my-bucket --yes Skip confirmation prompts
  %(prog)s --list                   List all buckets without deleting

Environment Variables:
  WASABI_ACCESS_KEY    Your Wasabi access key
  WASABI_SECRET_KEY    Your Wasabi secret key
        """,
    )
    parser.add_argument(
        "--bucket", "-b", type=str, help="Specific bucket to delete"
    )
    parser.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompts"
    )
    parser.add_argument(
        "--list", "-l", action="store_true", help="List buckets without deleting"
    )
    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Use Wasabi force_delete if standard delete fails",
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=10,
        help="Number of concurrent workers (default: 10)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )
    return parser.parse_args()


def main() -> None:
    """Main function that orchestrates the bucket deletion process."""
    args = parse_arguments()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    config = WasabiConfig.from_environment()
    config.max_workers = args.workers

    cleaner = WasabiBucketCleaner(config)

    try:
        buckets = cleaner.list_buckets()
        if not buckets:
            logger.info("No buckets found.")
            return

        if args.list:
            logger.info("\n=== Buckets Found ===")
            for i, bucket in enumerate(buckets, 1):
                logger.info(f"{i}. {bucket}")
            return

        if args.bucket:
            if args.bucket not in buckets:
                logger.error(f"Bucket '{args.bucket}' not found.")
                sys.exit(1)
            buckets = [args.bucket]

        logger.info("\n=== Buckets Found ===")
        for i, bucket in enumerate(buckets, 1):
            logger.info(f"{i}. {bucket}")

        if len(buckets) > 1 and not args.bucket:
            if args.yes or confirm(
                f"\nDelete ALL {len(buckets)} buckets and their contents?"
            ):
                for bucket in buckets:
                    logger.info(f"\nProcessing bucket: {bucket}")
                    if cleaner.delete_all_objects(bucket):
                        time.sleep(0.5)
                        if not cleaner.delete_bucket(bucket):
                            if args.force:
                                logger.warning(
                                    "Standard delete failed. Trying force delete..."
                                )
                                cleaner.delete_bucket(bucket, force=True)
                return

        for bucket in buckets:
            if args.yes or confirm(
                f"\nDelete bucket '{bucket}' and ALL its contents?"
            ):
                if cleaner.delete_all_objects(bucket):
                    time.sleep(0.5)
                    if args.yes or confirm(
                        f"Proceed to DELETE the bucket '{bucket}'?"
                    ):
                        if not cleaner.delete_bucket(bucket):
                            if args.force:
                                logger.warning(
                                    "Standard delete failed. Trying force delete..."
                                )
                                cleaner.delete_bucket(bucket, force=True)
                    else:
                        logger.info(f"Skipped bucket deletion: {bucket}")
            else:
                logger.info(f"Skipped: {bucket}")

        logger.info("\nOperation completed successfully!")

    except KeyboardInterrupt:
        logger.warning("\n\nOperation interrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()
