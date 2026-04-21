"""Tests for ``scripts.dr.*`` — the DR snapshot tooling.

We stay entirely in-memory: moto mocks S3 (R2 speaks S3 API), pg_dump /
pg_restore are replaced by fakes that just write a known blob. The goal
is to verify the encryption + manifest + key-handling logic, not to
re-test pg_dump.
"""

from __future__ import annotations

import asyncio
import json
import secrets
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws
from scripts.dr._core import (
    DRConfigError,
    DRDecryptError,
    SnapshotManifest,
    build_manifest,
    build_object_key,
    decrypt_bytes,
    encrypt_bytes,
    load_encryption_key,
    manifest_key_for,
    parse_snapshot_uri,
)

TEST_BUCKET = "loftly-dr-snapshots-test"


@pytest.fixture
def enc_key_env(monkeypatch: pytest.MonkeyPatch) -> bytes:
    """Populate ``LOFTLY_DR_ENCRYPTION_KEY`` with a fresh random key."""
    key = secrets.token_bytes(32)
    monkeypatch.setenv("LOFTLY_DR_ENCRYPTION_KEY", key.hex())
    return key


@pytest.fixture
def moto_s3() -> Any:
    """Yield a moto-backed boto3 S3 client with a clean test bucket."""
    # moto doesn't implement Object Lock per-PUT retention dates fully, but it
    # accepts the params without error as long as the bucket is created with
    # ObjectLockEnabledForBucket=True.
    with mock_aws():
        client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        client.create_bucket(
            Bucket=TEST_BUCKET,
            ObjectLockEnabledForBucket=True,
        )
        yield client


# ---------------------------------------------------------------------------
# Key loading
# ---------------------------------------------------------------------------


def test_load_encryption_key_rejects_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOFTLY_DR_ENCRYPTION_KEY", raising=False)
    with pytest.raises(DRConfigError, match="unset"):
        load_encryption_key()


def test_load_encryption_key_rejects_bad_hex(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOFTLY_DR_ENCRYPTION_KEY", "not-hex-at-all")
    with pytest.raises(DRConfigError, match="hex"):
        load_encryption_key()


def test_load_encryption_key_rejects_wrong_length(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOFTLY_DR_ENCRYPTION_KEY", "ab" * 16)  # 16 bytes, not 32
    with pytest.raises(DRConfigError, match="32 bytes"):
        load_encryption_key()


def test_load_encryption_key_ok(enc_key_env: bytes) -> None:
    assert load_encryption_key() == enc_key_env


# ---------------------------------------------------------------------------
# Encryption round-trip + tamper detection
# ---------------------------------------------------------------------------


def test_encrypt_decrypt_roundtrip() -> None:
    key = secrets.token_bytes(32)
    plaintext = b"pg_dump_custom_format_blob" * 500
    blob = encrypt_bytes(plaintext, key)
    # Prepended IV means ciphertext is strictly larger than plaintext.
    assert len(blob) == 12 + len(plaintext) + 16  # IV + ct + GCM tag
    assert decrypt_bytes(blob, key) == plaintext


def test_decrypt_detects_tamper() -> None:
    key = secrets.token_bytes(32)
    blob = bytearray(encrypt_bytes(b"sensitive rows", key))
    # Flip a byte in the middle of the ciphertext — GCM tag must reject.
    blob[30] ^= 0x01
    with pytest.raises(DRDecryptError):
        decrypt_bytes(bytes(blob), key)


def test_decrypt_with_wrong_key_fails() -> None:
    plaintext = b"x" * 100
    blob = encrypt_bytes(plaintext, secrets.token_bytes(32))
    with pytest.raises(DRDecryptError):
        decrypt_bytes(blob, secrets.token_bytes(32))


def test_decrypt_rejects_short_blob() -> None:
    with pytest.raises(DRDecryptError, match="too short"):
        decrypt_bytes(b"\x00" * 10, secrets.token_bytes(32))


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def test_manifest_roundtrip() -> None:
    ciphertext = b"ciphertext-goes-here" * 10
    manifest = build_manifest(
        env="staging",
        object_key="staging/2026-04-21/postgres-04-00-00.dump.enc",
        ciphertext=ciphertext,
        row_counts={"users": 10, "cards": 120, "affiliate_clicks": 0},
        now=datetime(2026, 4, 21, 4, 0, 0, tzinfo=UTC),
    )
    rehydrated = SnapshotManifest.from_json(manifest.to_json())
    assert rehydrated == manifest
    assert rehydrated.sha256 == manifest.sha256
    assert rehydrated.size_bytes == len(ciphertext)
    assert rehydrated.row_counts["cards"] == 120
    assert rehydrated.created_at == "2026-04-21T04:00:00+00:00"


def test_manifest_tolerates_extra_keys() -> None:
    """Older restore_db must still parse a manifest with future fields."""
    payload = json.dumps(
        {
            "env": "staging",
            "object_key": "staging/2026-04-21/postgres-04-00-00.dump.enc",
            "sha256": "a" * 64,
            "size_bytes": 123,
            "created_at": "2026-04-21T04:00:00+00:00",
            "row_counts": {"users": 1},
            "pg_dump_format": "custom",
            "schema_version": 1,
            "future_field_we_dont_know": "ignored",
        }
    )
    m = SnapshotManifest.from_json(payload)
    assert m.env == "staging"


# ---------------------------------------------------------------------------
# Key naming
# ---------------------------------------------------------------------------


def test_build_object_key_shape() -> None:
    key = build_object_key("staging", now=datetime(2026, 4, 21, 4, 0, 0, tzinfo=UTC))
    assert key == "staging/2026-04-21/postgres-04-00-00.dump.enc"


def test_manifest_key_for_roundtrip() -> None:
    k = "staging/2026-04-21/postgres-04-00-00.dump.enc"
    assert manifest_key_for(k) == "staging/2026-04-21/postgres-04-00-00.manifest.json"


def test_manifest_key_for_rejects_non_enc() -> None:
    with pytest.raises(DRConfigError):
        manifest_key_for("staging/2026-04-21/postgres-04-00-00.dump")


def test_parse_snapshot_uri_full_uri() -> None:
    bucket, key = parse_snapshot_uri(
        "s3://some-bucket/staging/2026-04-21/postgres-04-00-00.dump.enc",
        env="staging",
    )
    assert bucket == "some-bucket"
    assert key.endswith("postgres-04-00-00.dump.enc")


def test_parse_snapshot_uri_date_shorthand() -> None:
    bucket, key = parse_snapshot_uri("2026-04-21", bucket="b", env="staging")
    assert bucket == "b"
    assert key == "staging/2026-04-21/"


def test_parse_snapshot_uri_rejects_garbage() -> None:
    with pytest.raises(DRConfigError):
        parse_snapshot_uri("not-a-date-or-uri", env="staging")


# ---------------------------------------------------------------------------
# Prod guard
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://loftly:pw@loftly-postgres-prod.internal:5432/loftly",
        "postgresql://user:pw@loftly-api-prod.fly.dev:5432/loftly",
        "postgresql://user:pw@db-prod.loftly.co.th:5432/loftly",
    ],
)
def test_prod_guard_blocks_without_flag(url: str) -> None:
    from scripts.dr.restore_db import _target_looks_like_prod, run

    assert _target_looks_like_prod(url)

    async def _fail_pg_restore(*_a: Any, **_kw: Any) -> None:
        raise AssertionError("pg_restore should never be invoked when guard fires")

    with pytest.raises(DRConfigError, match="really-prod"):
        asyncio.run(
            run(
                snapshot_ref="s3://b/staging/2026-04-21/postgres-00-00-00.dump.enc",
                target_database_url=url,
                really_prod=False,
                skip_verify=True,
                bucket="b",
                env="staging",
                s3=object(),  # never used; guard fires first
                pg_restore_runner=_fail_pg_restore,
            )
        )


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://user:pw@loftly-postgres-staging.internal:5432/loftly",
        "postgresql://user:pw@localhost:5432/loftly_dr_drill_20260421",
    ],
)
def test_prod_guard_allows_non_prod(url: str) -> None:
    from scripts.dr.restore_db import _target_looks_like_prod

    assert not _target_looks_like_prod(url)


# ---------------------------------------------------------------------------
# list_snapshots — moto-backed, including date ordering
# ---------------------------------------------------------------------------


def test_list_snapshots_parses_date_order(moto_s3: Any) -> None:
    from scripts.dr.list_snapshots import list_snapshots

    # Three snapshots across two days; expect newest first.
    keys = [
        "staging/2026-04-20/postgres-04-00-00.dump.enc",
        "staging/2026-04-21/postgres-04-00-00.dump.enc",
        "staging/2026-04-21/postgres-16-30-00.dump.enc",
        # Non-matching suffix — should be ignored.
        "staging/2026-04-21/postgres-04-00-00.manifest.json",
        # Different env — should be ignored when filter=staging.
        "prod/2026-04-21/postgres-04-00-00.dump.enc",
    ]
    for k in keys:
        moto_s3.put_object(Bucket=TEST_BUCKET, Key=k, Body=b"xx")

    records = list_snapshots(moto_s3, bucket=TEST_BUCKET, env="staging")
    returned_keys = [r.key for r in records]
    assert returned_keys == [
        "staging/2026-04-21/postgres-16-30-00.dump.enc",
        "staging/2026-04-21/postgres-04-00-00.dump.enc",
        "staging/2026-04-20/postgres-04-00-00.dump.enc",
    ]
    # Env filter excluded prod row.
    assert all(r.key.startswith("staging/") for r in records)


# ---------------------------------------------------------------------------
# End-to-end snapshot flow through moto
# ---------------------------------------------------------------------------


def test_snapshot_run_end_to_end(
    moto_s3: Any, enc_key_env: bytes, tmp_path: Path
) -> None:
    """Verify the snapshot script writes an encrypted object + manifest we can parse."""
    from scripts.dr import snapshot_db

    # Fake pg_dump just writes a known blob.
    expected_plaintext = b"PGDMP\x00\x00\x00fake-custom-dump" * 64

    def fake_pg_dump(_url: str, out_path: Path) -> None:
        out_path.write_bytes(expected_plaintext)

    async def fake_row_counts(_url: str) -> dict[str, int]:
        return {"users": 3, "cards": 17}

    # Override DEFAULT_BUCKET used by helpers.
    now = datetime(2026, 4, 21, 4, 30, 0, tzinfo=UTC)
    with patch("scripts.dr.snapshot_db.build_object_key", return_value=build_object_key("staging", now=now)):
        object_key, manifest_key = asyncio.run(
            snapshot_db.run(
                database_url="postgresql://ignored",
                env="staging",
                bucket=TEST_BUCKET,
                s3=moto_s3,
                pg_dump_runner=fake_pg_dump,
                row_counter=fake_row_counts,
            )
        )

    assert object_key.startswith("staging/2026-04-21/")
    assert manifest_key.endswith(".manifest.json")

    # Pull the ciphertext back and verify it decrypts to our fake dump.
    blob = moto_s3.get_object(Bucket=TEST_BUCKET, Key=object_key)["Body"].read()
    assert decrypt_bytes(blob, enc_key_env) == expected_plaintext

    # Manifest round-trips and records the row counts.
    raw_manifest = moto_s3.get_object(Bucket=TEST_BUCKET, Key=manifest_key)["Body"].read()
    manifest = SnapshotManifest.from_json(raw_manifest)
    assert manifest.row_counts == {"users": 3, "cards": 17}
    assert manifest.size_bytes == len(blob)
    assert manifest.env == "staging"
