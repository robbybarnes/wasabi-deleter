"""Tests for wasabi-deleter.py. Run with: python -m pytest"""

from __future__ import annotations

import importlib.util
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest import mock

import botocore.exceptions
import pytest
from botocore.stub import Stubber

_SCRIPT = Path(__file__).resolve().parent.parent / "wasabi-deleter.py"
_spec = importlib.util.spec_from_file_location("wasabi_deleter", _SCRIPT)
wd = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(wd)


def make_cleaner(**overrides) -> wd.WasabiBucketCleaner:
    settings = dict(rate_limit_delay=0, retry_delay=0, max_workers=2, batch_size=2)
    settings.update(overrides)
    return wd.WasabiBucketCleaner(wd.WasabiConfig("AK", "SK", **settings))


def client_error(code: str, status: int = 400, op: str = "DeleteObjects"):
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": code},
         "ResponseMetadata": {"HTTPStatusCode": status}},
        op,
    )


class FakeS3:
    """Minimal thread-safe in-memory stand-in for the S3 calls the cleaner makes."""

    def __init__(self, n_objects: int, page_size: int = 3):
        self.objects = {(f"key-{i}", f"v{i}") for i in range(n_objects)}
        self.page_size = page_size
        self.lock = threading.Lock()
        self.fail_next_deletes = 0  # raise a 500 for this many delete calls
        self.undeletable: set[str] = set()  # keys that return AccessDenied

    # --- configuration calls ---
    def get_object_lock_configuration(self, **_):
        raise client_error("ObjectLockConfigurationNotFoundError", 404)

    def get_bucket_versioning(self, **_):
        return {}

    def delete_bucket_policy(self, **_):
        raise client_error("NoSuchBucketPolicy", 404)

    def delete_bucket_lifecycle(self, **_):
        raise client_error("NoSuchLifecycleConfiguration", 404)

    # --- listing ---
    def get_paginator(self, name):
        fake = self

        class Paginator:
            def paginate(self, **_):
                if name == "list_multipart_uploads":
                    yield {"Uploads": []}
                    return
                with fake.lock:
                    items = sorted(fake.objects)
                for i in range(0, len(items), fake.page_size):
                    yield {"Versions": [
                        {"Key": k, "VersionId": v}
                        for k, v in items[i:i + fake.page_size]
                    ]}

        return Paginator()

    def list_object_versions(self, **_):
        with self.lock:
            items = sorted(self.objects)[:1]
        return {"Versions": [{"Key": k, "VersionId": v} for k, v in items]}

    # --- deletion ---
    def delete_objects(self, Delete, **_):
        with self.lock:
            if self.fail_next_deletes:
                self.fail_next_deletes -= 1
                raise client_error("InternalError", 500)
            deleted, errors = [], []
            for obj in Delete["Objects"]:
                if obj["Key"] in self.undeletable:
                    errors.append({**obj, "Code": "AccessDenied", "Message": "no"})
                else:
                    self.objects.discard((obj["Key"], obj["VersionId"]))
                    deleted.append(obj)
            return {"Deleted": deleted, "Errors": errors}


def install_fake(cleaner, fake, bucket="b"):
    cleaner._bucket_region_cache[bucket] = "us-east-1"
    cleaner._client_cache["us-east-1"] = fake


# --- batch retry logic ---

def test_partial_retry_counts_objects_deleted_on_first_attempt():
    cleaner = make_cleaner()
    s3 = cleaner.s3_client
    objs = [{"Key": f"k{i}", "VersionId": "v"} for i in range(3)]
    with Stubber(s3) as st:
        st.add_response("delete_objects", {
            "Deleted": [{"Key": "k0"}, {"Key": "k1"}],
            "Errors": [{"Key": "k2", "VersionId": "v", "Code": "InternalError"}],
        })
        st.add_response("delete_objects", {"Deleted": [{"Key": "k2"}]})
        deleted, failed = cleaner._delete_object_batch_with_retry(s3, "b", objs)
        st.assert_no_pending_responses()
    assert (deleted, failed) == (3, [])


def test_permanent_per_key_errors_are_not_retried():
    cleaner = make_cleaner()
    s3 = cleaner.s3_client
    objs = [{"Key": "k0", "VersionId": "v"}, {"Key": "k1", "VersionId": "v"}]
    with Stubber(s3) as st:
        st.add_response("delete_objects", {
            "Deleted": [{"Key": "k0"}],
            "Errors": [{"Key": "k1", "VersionId": "v", "Code": "AccessDenied"}],
        })
        deleted, failed = cleaner._delete_object_batch_with_retry(s3, "b", objs)
        st.assert_no_pending_responses()  # exactly one call, no retries
    assert deleted == 1
    assert failed == [{"Key": "k1", "VersionId": "v"}]
    assert cleaner.stats.failed_deletes == 1


def test_malformed_xml_falls_back_to_individual_deletes():
    cleaner = make_cleaner()
    s3 = cleaner.s3_client
    objs = [{"Key": "weird\x01key", "VersionId": "v1"}]
    with Stubber(s3) as st:
        st.add_client_error("delete_objects", "MalformedXML", http_status_code=400)
        st.add_response("delete_object", {},
                        {"Bucket": "b", "Key": "weird\x01key", "VersionId": "v1"})
        assert cleaner._delete_object_batch_with_retry(s3, "b", objs) == (1, [])


# --- full bucket emptying ---

def test_empties_bucket_across_multiple_pages():
    cleaner = make_cleaner()
    fake = FakeS3(n_objects=11)
    install_fake(cleaner, fake)
    assert cleaner.delete_all_objects("b") is True
    assert fake.objects == set()
    assert cleaner.stats.total_deleted == 11


def test_transient_failure_does_not_block_bucket_deletion():
    # Previously a single failed batch made delete_all_objects return False
    # even after a later pass emptied the bucket, so it was never deleted.
    cleaner = make_cleaner(max_retries=0)
    fake = FakeS3(n_objects=4)
    fake.fail_next_deletes = 1
    install_fake(cleaner, fake)
    assert cleaner.delete_all_objects("b") is True
    assert fake.objects == set()


def test_undeletable_objects_report_not_empty_and_terminate():
    cleaner = make_cleaner()
    fake = FakeS3(n_objects=4)
    fake.undeletable = {"key-0"}
    install_fake(cleaner, fake)
    assert cleaner.delete_all_objects("b") is False
    assert fake.objects == {("key-0", "v0")}


# --- force delete ---

def test_force_delete_sends_signed_request_with_query_param():
    seen = {}

    class Handler(BaseHTTPRequestHandler):
        def do_DELETE(self):
            seen["path"] = self.path
            seen["headers"] = dict(self.headers)
            self.send_response(204)
            self.end_headers()

        def log_message(self, *_):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        cleaner = make_cleaner()
        cleaner._bucket_region_cache["my-bucket"] = "eu-central-1"
        url = f"http://127.0.0.1:{server.server_port}"
        with mock.patch.object(wd, "endpoint_for_region", return_value=url):
            assert cleaner.delete_bucket("my-bucket", force=True) is True
    finally:
        server.shutdown()

    assert "force_delete=true" in seen["path"]
    headers = {k.lower(): v for k, v in seen["headers"].items()}
    assert "x-amz-content-sha256" in headers
    # Signed for the bucket's own region, not the session default.
    assert "/eu-central-1/s3/aws4_request" in headers["authorization"]


# --- bucket processing / CLI ---

def test_not_emptied_without_force_skips_bucket_delete():
    cleaner = mock.Mock()
    cleaner.delete_all_objects.return_value = False
    assert wd.process_bucket(cleaner, "b", force=False, confirm_bucket_delete=False) is False
    cleaner.delete_bucket.assert_not_called()


def test_not_emptied_with_force_goes_straight_to_force_delete():
    cleaner = mock.Mock()
    cleaner.delete_all_objects.return_value = False
    cleaner.delete_bucket.return_value = True
    assert wd.process_bucket(cleaner, "b", force=True, confirm_bucket_delete=False) is True
    cleaner.delete_bucket.assert_called_once_with("b", force=True)


@pytest.mark.parametrize("argv", [
    ["--yes"],
    ["--all", "--bucket", "x"],
    ["--workers", "0"],
    ["--batch-size", "1001"],
])
def test_rejected_arguments(argv):
    with pytest.raises(SystemExit) as exc:
        wd.parse_arguments(argv)
    assert exc.value.code == 2


def test_accepted_arguments():
    args = wd.parse_arguments(["-b", "one", "-b", "two", "--yes"])
    assert args.bucket == ["one", "two"]
    assert wd.parse_arguments(["--all", "--yes"]).all is True
    assert wd.parse_arguments(["--list", "--yes"]).list is True


def test_missing_credentials_exit_code(monkeypatch):
    monkeypatch.delenv("WASABI_ACCESS_KEY", raising=False)
    monkeypatch.delenv("WASABI_SECRET_KEY", raising=False)
    assert wd.main(["--list"]) == 1
