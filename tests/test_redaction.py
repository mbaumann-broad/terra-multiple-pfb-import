"""Unit tests for log redaction (terra_import_prototype.redaction)."""

import json
import logging

from terra_import_prototype.clients.base import BaseClient
from terra_import_prototype.logging_setup import LOGGER_NAME
from terra_import_prototype.redaction import REDACTED, redact_text, redact_url

# The exact signed S3 PFB URL shape that leaked in the importJob body (values are fabricated).
_SIGNED_URL = (
    "https://s3.amazonaws.com/edu-ucsc-gi-platform-anvil-dev-storage-anvildev.us-east-1/"
    "manifests/05a727e2-739d-5e24-b1d2-97c71be92fa6.avro"
    "?response-content-disposition=attachment%3Bfilename%3D%22anvil-manifest.avro%22"
    "&AWSAccessKeyId=ASIAUHATKQ7OVN5RMSE6"
    "&Signature=abc123DEF456ghi789%2Bxyz%3D"
    "&x-amz-security-token=FwoGZXIvYXdzEABCDEF1234567890token"
    "&Expires=1718000000"
)


def test_redact_signed_url_keeps_structure_drops_secrets():
    redacted = redact_url(_SIGNED_URL)
    # Host, path and non-sensitive params survive.
    assert "s3.amazonaws.com" in redacted
    assert "manifests/05a727e2-739d-5e24-b1d2-97c71be92fa6.avro" in redacted
    assert "Expires=1718000000" in redacted
    # The credential / signature values are gone.
    assert "ASIAUHATKQ7OVN5RMSE6" not in redacted
    assert "abc123DEF456ghi789" not in redacted
    assert "FwoGZXIvYXdzEABCDEF1234567890token" not in redacted
    assert redacted.count(REDACTED) >= 3


def test_redact_importjob_body():
    """The reported leak: the importJob request body logged with an unredacted signed URL."""
    body = json.dumps({"url": _SIGNED_URL, "filetype": "pfb", "options": None})
    redacted = redact_text(body)
    assert "ASIAUHATKQ7OVN5RMSE6" not in redacted
    assert "abc123DEF456ghi789" not in redacted
    assert "FwoGZXIvYXdzEABCDEF1234567890token" not in redacted
    # Still valid JSON with the non-secret fields intact.
    parsed = json.loads(redacted)
    assert parsed["filetype"] == "pfb"
    assert "s3.amazonaws.com" in parsed["url"]


def test_redact_bearer_jwt_and_aws_key():
    jwt = "eyJhbGciOiJFUzI1NiJ9.eyJzdWIiOiJ4In0.c2lnbmF0dXJlX3ZhbHVl"
    text = f"Authorization: Bearer {jwt} key=AKIAIOSFODNN7EXAMPLE"
    redacted = redact_text(text)
    assert jwt not in redacted
    assert "AKIAIOSFODNN7EXAMPLE" not in redacted
    assert "Bearer " + REDACTED in redacted


def test_redact_leaves_clean_text_unchanged():
    clean = "GET https://firecloud.example/api/workspaces/ns/ws/importJob/abc-123"
    assert redact_text(clean) == clean


class _FakeResponse:
    status_code = 202
    reason = "Accepted"
    headers: dict = {}

    def raise_for_status(self):
        pass


def test_base_client_logs_redacted_request(caplog):
    """End-to-end: the importJob request BaseClient logs must not leak the signed URL or token."""
    client = BaseClient("https://fc.example", lambda: "google-bearer-token")
    client._session.request = lambda *a, **k: _FakeResponse()  # no network
    with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
        client.request(
            "POST",
            "/api/workspaces/ns/ws/importJob",
            json_body={"url": _SIGNED_URL, "filetype": "pfb", "options": None},
        )
    assert "ASIAUHATKQ7OVN5RMSE6" not in caplog.text
    assert "abc123DEF456ghi789" not in caplog.text
    assert "FwoGZXIvYXdzEABCDEF1234567890token" not in caplog.text
    assert "google-bearer-token" not in caplog.text  # bearer token redacted in headers
    assert REDACTED in caplog.text
