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
import itertools
import logging
import os
import random
import sys
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from threading import Lock
from typing import TYPE_CHECKING, Iterator

import boto3
import botocore.config
import botocore.exceptions

if TYPE_CHECKING:
    from boto3 import Session
    from mypy_boto3_s3 import S3Client


logger = logging.getLogger(__name__)

# S3 error codes that indicate a transient condition worth retrying.
RETRYABLE_ERROR_CODES = frozenset(
    {
        "InternalError",
        "ServiceUnavailable",
        "SlowDown",
        "RequestTimeout",
        "Throttling",
        "ThrottlingException",
    }
)

# S3 allows at most 1000 keys per DeleteObjects request.
MAX_BATCH_SIZE = 1000

# Safety cap on list/delete passes over a bucket.
MAX_PASSES = 20


class ConfigError(Exception):
    """Raised when required configuration is missing or invalid."""


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
            raise ConfigError(
                "Wasabi credentials not found. Set the WASABI_ACCESS_KEY and "
                "WASABI_SECRET_KEY environment variables."
            )

        return cls(access_key=access_key, secret_key=secret_key)


@dataclass
class DeletionStats:
    """Statistics for tracking deletion progress."""

    total_deleted: int = 0
    failed_deletes: int = 0
    retried_deletes: int = 0
    server_errors: int = 0
    _lock: Lock = field(default_factory=Lock, repr=False, compare=False)

    def reset(self) -> None:
        """Reset all statistics to zero."""
        with self._lock:
            self.total_deleted = 0
            self.failed_deletes = 0
            self.retried_deletes = 0
            self.server_errors = 0

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


def endpoint_for_region(region: str) -> str:
    """Return the Wasabi S3 endpoint URL for a region."""
    if region == "us-east-1":
        return "https://s3.wasabisys.com"
    return f"https://s3.{region}.wasabisys.com"


def _add_force_delete_param(request, **_kwargs) -> None:
    """botocore before-sign hook that appends Wasabi's force_delete flag."""
    separator = "&" if "?" in request.url else "?"
    request.url = f"{request.url}{separator}force_delete=true"


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
                "s3",
                endpoint_url=self.config.endpoint_url,
                region_name=self.config.region,
                config=self.boto_config,
            )
        return self._s3_client

    def _new_regional_client(self, region: str) -> S3Client:
        """Create an S3 client that targets and signs for a specific region."""
        return self.session.client(
            "s3",
            endpoint_url=endpoint_for_region(region),
            region_name=region,
            config=self.boto_config,
        )

    def _get_bucket_region(self, bucket_name: str) -> str:
        """
        Determine the region of a Wasabi bucket.

        Wasabi buckets can be in different regions, and we need to use
        the correct regional endpoint (and signing region) for each.
        """
        if bucket_name in self._bucket_region_cache:
            return self._bucket_region_cache[bucket_name]

        try:
            response = self.s3_client.get_bucket_location(Bucket=bucket_name)
            region = response.get("LocationConstraint") or "us-east-1"
        except botocore.exceptions.ClientError as e:
            logger.warning(
                f"Could not determine region for bucket {bucket_name} "
                f"({e}); assuming us-east-1"
            )
            region = "us-east-1"

        self._bucket_region_cache[bucket_name] = region
        return region

    def _get_s3_client_for_bucket(self, bucket_name: str) -> S3Client:
        """Get or create an S3 client for the bucket's specific region."""
        region = self._get_bucket_region(bucket_name)
        if region not in self._client_cache:
            self._client_cache[region] = self._new_regional_client(region)
        return self._client_cache[region]

    def list_buckets(self) -> list[str]:
        """
        List all buckets in the Wasabi account.

        Raises:
            botocore.exceptions.ClientError: If the listing fails.
        """
        response = self.s3_client.list_buckets()
        return [bucket["Name"] for bucket in response.get("Buckets", [])]

    def _backoff(self, attempt: int) -> float:
        """Exponential backoff with jitter, capped at max_retry_delay."""
        base = min(self.config.retry_delay * (2**attempt), self.config.max_retry_delay)
        return base * random.uniform(0.5, 1.0)

    def _delete_object_batch_with_retry(
        self,
        s3_client: S3Client,
        bucket_name: str,
        objects_to_delete: list[dict[str, str]],
        retry_count: int = 0,
    ) -> tuple[int, list[dict[str, str]]]:
        """
        Delete a batch of objects with retry logic and error handling.

        Handles MalformedXML errors (falls back to per-object deletes),
        transient server/throttling errors (exponential backoff, splitting
        large batches), and per-key errors reported in the response.

        Returns:
            Tuple of (successfully deleted count, list of failed objects).
        """
        if not objects_to_delete:
            return 0, []

        time.sleep(self.config.rate_limit_delay)

        try:
            response = s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": objects_to_delete, "Quiet": False},
            )
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

            retryable = status_code >= 500 or error_code in RETRYABLE_ERROR_CODES
            if retryable and retry_count < self.config.max_retries:
                if status_code >= 500:
                    self.stats.record_server_error()
                delay = self._backoff(retry_count)
                logger.warning(
                    f"{error_code or status_code} error, retrying in {delay:.1f}s "
                    f"(attempt {retry_count + 1}/{self.config.max_retries})"
                )
                time.sleep(delay)

                if len(objects_to_delete) > 100:
                    mid = len(objects_to_delete) // 2
                    logger.info(f"Splitting batch of {len(objects_to_delete)} objects")
                    deleted1, failed1 = self._delete_object_batch_with_retry(
                        s3_client, bucket_name, objects_to_delete[:mid], retry_count + 1
                    )
                    deleted2, failed2 = self._delete_object_batch_with_retry(
                        s3_client, bucket_name, objects_to_delete[mid:], retry_count + 1
                    )
                    return deleted1 + deleted2, failed1 + failed2

                return self._delete_object_batch_with_retry(
                    s3_client, bucket_name, objects_to_delete, retry_count + 1
                )

            logger.error(f"Error deleting batch: {e}")
            self.stats.increment_failed(len(objects_to_delete))
            return 0, objects_to_delete

        deleted_count = len(response.get("Deleted", []))
        errors = response.get("Errors", [])
        if not errors:
            return deleted_count, []

        retryable_objs: list[dict[str, str]] = []
        permanent_objs: list[dict[str, str]] = []
        for err in errors:
            obj = {"Key": err.get("Key", "")}
            if version_id := err.get("VersionId"):
                obj["VersionId"] = version_id
            if err.get("Code") in RETRYABLE_ERROR_CODES:
                retryable_objs.append(obj)
            else:
                permanent_objs.append(obj)

        if permanent_objs:
            sample = errors[0]
            logger.warning(
                f"{len(permanent_objs)} object(s) could not be deleted, e.g. "
                f"{sample.get('Key')!r}: {sample.get('Code')} - {sample.get('Message')}"
            )
            self.stats.increment_failed(len(permanent_objs))

        if retryable_objs and retry_count < self.config.max_retries:
            self.stats.increment_retried(len(retryable_objs))
            time.sleep(self._backoff(retry_count))
            more_deleted, still_failed = self._delete_object_batch_with_retry(
                s3_client, bucket_name, retryable_objs, retry_count + 1
            )
            return deleted_count + more_deleted, permanent_objs + still_failed

        if retryable_objs:
            self.stats.increment_failed(len(retryable_objs))
        return deleted_count, permanent_objs + retryable_objs

    def _delete_objects_individually(
        self,
        s3_client: S3Client,
        bucket_name: str,
        objects_to_delete: list[dict[str, str]],
    ) -> tuple[int, list[dict[str, str]]]:
        """
        Delete objects one by one as a fallback for batch failures.

        Returns:
            Tuple of (successfully deleted count, list of failed objects).
        """
        deleted_count = 0
        failed_objects = []

        for obj in objects_to_delete:
            try:
                delete_kwargs = {"Bucket": bucket_name, "Key": obj["Key"]}
                if obj.get("VersionId"):
                    delete_kwargs["VersionId"] = obj["VersionId"]
                s3_client.delete_object(**delete_kwargs)
                deleted_count += 1
            except botocore.exceptions.ClientError as e:
                logger.debug(f"Failed to delete {obj.get('Key', '')!r}: {e}")
                failed_objects.append(obj)

        if failed_objects:
            self.stats.increment_failed(len(failed_objects))
        return deleted_count, failed_objects

    def _abort_incomplete_uploads(self, s3_client: S3Client, bucket_name: str) -> None:
        """
        Abort all incomplete multipart uploads in parallel.

        Incomplete multipart uploads can prevent bucket deletion and consume storage.
        """
        try:
            paginator = s3_client.get_paginator("list_multipart_uploads")
            uploads_to_abort = []
            for page in paginator.paginate(Bucket=bucket_name):
                uploads_to_abort.extend(page.get("Uploads", []))
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error listing multipart uploads: {e}")
            return

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

        aborted = failed = 0
        worker_count = min(self.config.max_workers, len(uploads_to_abort))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            for success, key in executor.map(abort_upload, uploads_to_abort):
                if success:
                    aborted += 1
                else:
                    failed += 1
                    logger.warning(f"Failed to abort upload: {key}")

        logger.info(f"Aborted {aborted} uploads, {failed} failed")

    def _check_bucket_versioning(self, s3_client: S3Client, bucket_name: str) -> bool:
        """Return True if bucket versioning is enabled."""
        try:
            response = s3_client.get_bucket_versioning(Bucket=bucket_name)
            return response.get("Status") == "Enabled"
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error checking versioning status: {e}")
            return False

    def _disable_bucket_versioning(
        self, s3_client: S3Client, bucket_name: str
    ) -> bool:
        """
        Suspend bucket versioning.

        This doesn't delete existing versions, but prevents new versions
        from being created during the deletion process.
        """
        try:
            s3_client.put_bucket_versioning(
                Bucket=bucket_name, VersioningConfiguration={"Status": "Suspended"}
            )
            logger.info(f"Suspended versioning for bucket: {bucket_name}")
            return True
        except botocore.exceptions.ClientError as e:
            logger.error(f"Failed to suspend versioning: {e}")
            return False

    def _delete_bucket_policy(self, s3_client: S3Client, bucket_name: str) -> None:
        """Delete the bucket policy, which might otherwise deny deletions."""
        try:
            s3_client.delete_bucket_policy(Bucket=bucket_name)
            logger.info(f"Deleted bucket policy for: {bucket_name}")
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchBucketPolicy":
                logger.warning(f"Could not delete bucket policy: {e}")

    def _delete_bucket_lifecycle(self, s3_client: S3Client, bucket_name: str) -> None:
        """Delete the lifecycle configuration, which might interfere with deletion."""
        try:
            s3_client.delete_bucket_lifecycle(Bucket=bucket_name)
            logger.info(f"Deleted lifecycle configuration for: {bucket_name}")
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchLifecycleConfiguration":
                logger.warning(f"Could not delete lifecycle configuration: {e}")

    def _check_object_lock(self, s3_client: S3Client, bucket_name: str) -> bool:
        """Return True if the bucket has (or may have) object lock enabled."""
        try:
            response = s3_client.get_object_lock_configuration(Bucket=bucket_name)
            return "ObjectLockConfiguration" in response
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ObjectLockConfigurationNotFoundError":
                return False
            logger.error(f"Error checking object lock configuration: {e}")
            return True

    def _verify_bucket_empty(self, s3_client: S3Client, bucket_name: str) -> bool:
        """Return True if the bucket has no objects, versions, or delete markers."""
        try:
            response = s3_client.list_object_versions(Bucket=bucket_name, MaxKeys=1)
            return not (response.get("Versions") or response.get("DeleteMarkers"))
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error verifying bucket is empty: {e}")
            return False

    def _iter_version_batches(
        self, s3_client: S3Client, bucket_name: str
    ) -> Iterator[list[dict[str, str]]]:
        """Stream every object version and delete marker in batch-sized chunks."""
        paginator = s3_client.get_paginator("list_object_versions")
        batch: list[dict[str, str]] = []
        for page in paginator.paginate(
            Bucket=bucket_name, PaginationConfig={"PageSize": 1000}
        ):
            for item in itertools.chain(
                page.get("Versions", []), page.get("DeleteMarkers", [])
            ):
                batch.append({"Key": item["Key"], "VersionId": item["VersionId"]})
                if len(batch) >= self.config.batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def _delete_pass(
        self, s3_client: S3Client, bucket_name: str, start_time: float
    ) -> tuple[int, int]:
        """
        Make one list-and-delete pass over the bucket.

        Listing and deletion are pipelined: batches are submitted to the worker
        pool as pages arrive, with a bounded number in flight, so memory use
        stays flat regardless of bucket size.

        Returns:
            Tuple of (objects found, objects deleted) in this pass.

        Raises:
            botocore.exceptions.ClientError: If listing the bucket fails.
        """
        workers = self.config.max_workers
        if self.stats.server_errors > 10:
            workers = max(2, workers // 2)
            logger.warning(f"Reducing workers to {workers} due to server errors")

        found = deleted = completed = 0
        in_flight: set[Future] = set()

        def collect(done: set[Future]) -> None:
            nonlocal deleted, completed
            for future in done:
                count, _ = future.result()
                deleted += count
                completed += 1
                self.stats.increment_deleted(count)
                if completed % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = self.stats.total_deleted / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"Progress: {completed} batches | "
                        f"Total deleted: {self.stats.total_deleted} | "
                        f"Rate: {rate:.1f} objects/sec"
                    )

        with ThreadPoolExecutor(max_workers=workers) as executor:
            try:
                for batch in self._iter_version_batches(s3_client, bucket_name):
                    found += len(batch)
                    if len(in_flight) >= workers * 2:
                        done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                        collect(done)
                    in_flight.add(
                        executor.submit(
                            self._delete_object_batch_with_retry,
                            s3_client,
                            bucket_name,
                            batch,
                        )
                    )
            finally:
                done, _ = wait(in_flight)
                collect(done)

        return found, deleted

    def delete_all_objects(self, bucket_name: str) -> bool:
        """
        Delete all objects in a bucket, including versions and delete markers.

        This method:
        1. Checks and suspends versioning if enabled
        2. Removes bucket policies and lifecycle rules
        3. Aborts incomplete multipart uploads
        4. Deletes all objects, versions, and delete markers in parallel,
           re-scanning until nothing is left or no progress is made
        5. Verifies the bucket is empty

        Returns:
            True if the bucket is verified empty, False otherwise.
        """
        logger.info("=" * 50)
        logger.info(f"Deleting all objects in bucket: {bucket_name}")
        logger.info("=" * 50)

        s3_client = self._get_s3_client_for_bucket(bucket_name)
        start_time = time.time()
        self.stats.reset()

        if self._check_object_lock(s3_client, bucket_name):
            logger.warning(
                f"Bucket '{bucket_name}' has object lock enabled. "
                "Some objects may not be deletable."
            )

        if self._check_bucket_versioning(s3_client, bucket_name):
            logger.info(f"Bucket '{bucket_name}' has versioning enabled. Suspending...")
            self._disable_bucket_versioning(s3_client, bucket_name)

        self._delete_bucket_policy(s3_client, bucket_name)
        self._delete_bucket_lifecycle(s3_client, bucket_name)
        self._abort_incomplete_uploads(s3_client, bucket_name)

        for pass_number in range(1, MAX_PASSES + 1):
            logger.info(f"Pass {pass_number} - scanning for objects...")
            try:
                found, deleted = self._delete_pass(s3_client, bucket_name, start_time)
            except botocore.exceptions.ClientError as e:
                logger.error(f"Error listing objects: {e}")
                return False

            if found == 0:
                logger.info("No more objects to delete")
                break

            logger.info(f"Pass {pass_number} complete: {deleted}/{found} deleted")
            if deleted == 0:
                logger.warning("No progress made in this pass; giving up")
                break
        else:
            logger.warning(f"Stopped after {MAX_PASSES} passes")

        # Uploads may have been started while we were deleting.
        self._abort_incomplete_uploads(s3_client, bucket_name)

        self._log_statistics(bucket_name, time.time() - start_time)

        is_empty = self._verify_bucket_empty(s3_client, bucket_name)
        if is_empty:
            logger.info("Bucket is verified empty")
        else:
            logger.warning("Bucket still contains objects or delete markers")
        return is_empty

    def _log_statistics(self, bucket_name: str, elapsed: float) -> None:
        """Log deletion statistics."""
        rate = self.stats.total_deleted / elapsed if elapsed > 0 else 0
        logger.info("=" * 50)
        logger.info(f"Bucket '{bucket_name}' clearing completed")
        logger.info(f"Total objects deleted: {self.stats.total_deleted}")
        logger.info(f"Failed deletes: {self.stats.failed_deletes}")
        logger.info(f"Retried deletes: {self.stats.retried_deletes}")
        logger.info(f"Server errors encountered: {self.stats.server_errors}")
        logger.info(f"Time elapsed: {elapsed:.2f} seconds")
        logger.info(f"Average rate: {rate:.1f} objects/sec")
        logger.info("=" * 50)

    def delete_bucket(self, bucket_name: str, force: bool = False) -> bool:
        """
        Delete a bucket.

        Args:
            bucket_name: Name of the bucket to delete.
            force: Use Wasabi's force_delete parameter, which deletes any
                remaining objects server-side along with the bucket.

        Returns:
            True if deletion was successful (or the bucket no longer exists).
        """
        if force:
            return self._force_delete_bucket(bucket_name)

        s3_client = self._get_s3_client_for_bucket(bucket_name)
        for attempt in range(self.config.max_retries):
            try:
                s3_client.delete_bucket(Bucket=bucket_name)
                logger.info(f"Successfully deleted bucket: {bucket_name}")
                return True
            except botocore.exceptions.ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "BucketNotEmpty":
                    logger.error(f"Bucket '{bucket_name}' is not empty.")
                    return False
                if error_code == "NoSuchBucket":
                    logger.warning(f"Bucket '{bucket_name}' does not exist.")
                    return True
                logger.error(f"Attempt {attempt + 1} failed to delete bucket: {e}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(self.config.retry_delay * (attempt + 1))
        return False

    def _force_delete_bucket(self, bucket_name: str) -> bool:
        """
        Delete a bucket using Wasabi's force_delete query parameter.

        The parameter is injected before signing, so botocore produces a
        correct S3 SigV4 signature (including X-Amz-Content-SHA256).
        """
        region = self._get_bucket_region(bucket_name)
        client = self._new_regional_client(region)
        client.meta.events.register(
            "before-sign.s3.DeleteBucket", _add_force_delete_param
        )
        try:
            client.delete_bucket(Bucket=bucket_name)
            logger.info(f"Successfully force-deleted bucket: {bucket_name}")
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchBucket":
                logger.warning(f"Bucket '{bucket_name}' does not exist.")
                return True
            logger.error(f"Force delete failed: {e}")
            return False


def confirm(prompt: str) -> bool:
    """Ask the user a yes/no question; anything but 'y' (including EOF) is no."""
    try:
        ans = input(f"{prompt} (y/N): ").strip().lower()
    except EOFError:
        return False
    return ans == "y"


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def _batch_size(value: str) -> int:
    number = _positive_int(value)
    if number > MAX_BATCH_SIZE:
        raise argparse.ArgumentTypeError(f"must be at most {MAX_BATCH_SIZE}")
    return number


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Wasabi Bucket Cleaner - Delete Wasabi S3 buckets and their contents",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          Interactive mode - confirm each bucket
  %(prog)s --bucket my-bucket       Delete specific bucket
  %(prog)s -b one -b two            Delete several specific buckets
  %(prog)s --bucket my-bucket --yes Skip confirmation prompts
  %(prog)s --all --yes              Delete EVERY bucket without prompting
  %(prog)s --list                   List all buckets without deleting

Environment Variables:
  WASABI_ACCESS_KEY    Your Wasabi access key
  WASABI_SECRET_KEY    Your Wasabi secret key
        """,
    )
    parser.add_argument(
        "--bucket",
        "-b",
        action="append",
        metavar="BUCKET",
        help="Bucket to delete (repeatable)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Target every bucket in the account (required with --yes "
        "when no --bucket is given)",
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
        help="Use Wasabi force_delete if the bucket cannot be emptied or "
        "standard delete fails",
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=_positive_int,
        default=10,
        help="Number of concurrent workers (default: 10)",
    )
    parser.add_argument(
        "--batch-size",
        type=_batch_size,
        default=500,
        help=f"Objects per delete request, 1-{MAX_BATCH_SIZE} (default: 500)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )
    args = parser.parse_args(argv)

    if args.bucket and args.all:
        parser.error("--bucket and --all are mutually exclusive")
    if args.yes and not args.bucket and not args.all and not args.list:
        parser.error(
            "refusing to delete every bucket with --yes; "
            "pass --bucket NAME, or --all to really delete everything"
        )
    return args


def process_bucket(
    cleaner: WasabiBucketCleaner,
    bucket: str,
    *,
    force: bool,
    confirm_bucket_delete: bool,
) -> bool:
    """
    Empty and delete one bucket.

    Returns:
        True if the bucket was deleted or the user chose to keep it.
    """
    emptied = cleaner.delete_all_objects(bucket)

    if not emptied and not force:
        logger.error(
            f"Bucket '{bucket}' could not be fully emptied; not deleting it. "
            "Re-run, or use --force to let Wasabi delete the remainder."
        )
        return False

    if confirm_bucket_delete and not confirm(f"Proceed to DELETE the bucket '{bucket}'?"):
        logger.info(f"Skipped bucket deletion: {bucket}")
        return True

    if emptied and cleaner.delete_bucket(bucket):
        return True

    if force:
        logger.warning("Trying Wasabi force delete...")
        return cleaner.delete_bucket(bucket, force=True)
    return False


def main(argv: list[str] | None = None) -> int:
    """Orchestrate the bucket deletion process. Returns a process exit code."""
    args = parse_arguments(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        config = WasabiConfig.from_environment()
    except ConfigError as e:
        logger.error(str(e))
        return 1
    config.max_workers = args.workers
    config.batch_size = args.batch_size

    cleaner = WasabiBucketCleaner(config)

    try:
        try:
            buckets = cleaner.list_buckets()
        except (botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError) as e:
            logger.error(f"Failed to list buckets: {e}")
            return 1

        if not buckets:
            logger.info("No buckets found.")
            return 0

        if args.list:
            logger.info("=== Buckets Found ===")
            for i, bucket in enumerate(buckets, 1):
                logger.info(f"{i}. {bucket}")
            return 0

        if args.bucket:
            missing = [b for b in args.bucket if b not in buckets]
            if missing:
                logger.error(f"Bucket(s) not found: {', '.join(missing)}")
                return 1
            buckets = list(dict.fromkeys(args.bucket))

        logger.info("=== Buckets Selected ===")
        for i, bucket in enumerate(buckets, 1):
            logger.info(f"{i}. {bucket}")

        failures: list[str] = []
        delete_everything = False
        if len(buckets) > 1 and not args.bucket:
            delete_everything = args.yes or confirm(
                f"\nDelete ALL {len(buckets)} buckets and their contents?"
            )

        for bucket in buckets:
            if delete_everything or args.yes:
                selected = True
            else:
                selected = confirm(f"\nDelete bucket '{bucket}' and ALL its contents?")
            if not selected:
                logger.info(f"Skipped: {bucket}")
                continue

            logger.info(f"Processing bucket: {bucket}")
            ok = process_bucket(
                cleaner,
                bucket,
                force=args.force,
                confirm_bucket_delete=not (args.yes or delete_everything),
            )
            if not ok:
                failures.append(bucket)

        if failures:
            logger.error(f"Finished with failures: {', '.join(failures)}")
            return 1
        logger.info("Operation completed successfully!")
        return 0

    except KeyboardInterrupt:
        logger.warning("Operation interrupted by user")
        return 130


if __name__ == "__main__":
    sys.exit(main())
