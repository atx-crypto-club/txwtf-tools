"""
Live backup round-trip integration test.

Requires a running Incus cluster with:
  - ``incus`` CLI accessible to the runner user
  - A cached container image aliased ``ubuntu-24.04-container``
  - SFTP access from the runner to ``TXWTF_SFTP_HOST`` (default: star-01)
  - A writable storage directory on the SFTP host

The test exercises the full relay pipeline:
  1. Launch a temp container, write a canary file, stop & publish as image
  2. Export image → local tarball → encrypt → relay to SFTP backup
  3. Delete local tarball, image, and container
  4. Relay from SFTP backup → decrypt → local tarball
  5. Import image, launch restored container, verify canary file
  6. Clean up everything

All resources use the ``txwtf-ci-`` prefix so they cannot collide with
production workloads.

Env vars (all optional, with sane defaults):
  TXWTF_SFTP_HOST      SFTP target hostname          (default: star-01)
  TXWTF_SFTP_USER      SFTP username                 (default: tfx)
  TXWTF_SFTP_DIR       Remote directory for backups   (default: /media/catx-easystore/txwtf-tools-tests)
  TXWTF_INCUS_TARGET   Incus cluster member to target (default: unset = auto)
  TXWTF_BASE_IMAGE     Local image alias to launch    (default: ubuntu-24.04-container)
"""

import asyncio
import hashlib
import os
import shutil
import subprocess
import tempfile
import time
import uuid

import pytest

from txwtf_tools.backup import chain_functions, get_fixed_base64_from_utf8_string
from txwtf_tools.relay import relay_stream

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------

SFTP_HOST = os.environ.get("TXWTF_SFTP_HOST", "star-01")
SFTP_USER = os.environ.get("TXWTF_SFTP_USER", "tfx")
SFTP_DIR = os.environ.get("TXWTF_SFTP_DIR", "/media/catx-easystore/txwtf-tools-tests")
INCUS_TARGET = os.environ.get("TXWTF_INCUS_TARGET", "")
BASE_IMAGE = os.environ.get("TXWTF_BASE_IMAGE", "ubuntu-24.04-container")

# Unique run ID to avoid collisions between concurrent CI runs
RUN_ID = uuid.uuid4().hex[:8]
CONTAINER_NAME = f"txwtf-ci-{RUN_ID}"
RESTORED_NAME = f"txwtf-ci-{RUN_ID}-restored"
IMAGE_ALIAS = f"txwtf-ci-{RUN_ID}-img"
RESTORED_ALIAS = f"txwtf-ci-{RUN_ID}-restored-img"
BACKUP_FILENAME = f"txwtf-ci-{RUN_ID}-backup.enc"
PASSPHRASE = f"test-passphrase-{RUN_ID}"
CANARY_CONTENT = f"txwtf-ci-canary-{RUN_ID}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(cmd: str, check: bool = True, timeout: int = 300) -> subprocess.CompletedProcess:
    """Run a shell command and return the result."""
    return subprocess.run(
        cmd, shell=True, capture_output=True, text=True,
        check=check, timeout=timeout,
    )


def _incus(subcmd: str, **kwargs) -> subprocess.CompletedProcess:
    """Run an ``incus`` CLI command."""
    return _run(f"incus {subcmd}", **kwargs)


def _container_exists(name: str) -> bool:
    r = _incus(f"list --format csv -c n", check=False)
    return name in r.stdout.splitlines()


def _image_exists(alias: str) -> bool:
    r = _incus(f"image alias list --format csv", check=False)
    return any(alias == line.split(",")[0] for line in r.stdout.splitlines())


def _cleanup_container(name: str):
    """Force-delete a container if it exists."""
    if _container_exists(name):
        _incus(f"delete {name} --force", check=False, timeout=120)


def _cleanup_image(alias: str):
    """Delete an image by alias if it exists."""
    if _image_exists(alias):
        _incus(f"image delete {alias}", check=False, timeout=60)


def _cleanup_sftp_file():
    """Remove the backup file from the SFTP host."""
    remote_path = f"{SFTP_DIR}/{BACKUP_FILENAME}"
    _run(f"ssh -o StrictHostKeyChecking=no {SFTP_USER}@{SFTP_HOST} 'rm -f {remote_path}'", check=False)


def _full_cleanup(tmpdir: str | None = None):
    """Best-effort cleanup of all resources created by this test run."""
    _cleanup_container(RESTORED_NAME)
    _cleanup_container(CONTAINER_NAME)
    _cleanup_image(RESTORED_ALIAS)
    _cleanup_image(IMAGE_ALIAS)
    _cleanup_sftp_file()
    if tmpdir and os.path.isdir(tmpdir):
        shutil.rmtree(tmpdir, ignore_errors=True)


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Encryption / decryption helpers (Fernet, matching backup.py)
# ---------------------------------------------------------------------------

def _make_encrypt_func(passphrase: str):
    from cryptography.fernet import Fernet
    key = get_fixed_base64_from_utf8_string(passphrase)
    enc = Fernet(key)
    return lambda chunk: enc.encrypt(chunk)


def _make_decrypt_func(passphrase: str):
    from cryptography.fernet import Fernet
    key = get_fixed_base64_from_utf8_string(passphrase)
    dec = Fernet(key)
    return lambda chunk: dec.decrypt(chunk)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def tmpdir():
    d = tempfile.mkdtemp(prefix="txwtf-ci-")
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.live
class TestBackupRoundTrip:
    """End-to-end encrypted backup and restore via SFTP relay."""

    def test_00_preflight(self):
        """Verify incus CLI is available and the base image exists."""
        r = _incus("version", check=False)
        assert r.returncode == 0, f"incus not available: {r.stderr}"

        assert _image_exists(BASE_IMAGE) or self._try_cache_image(), (
            f"Base image '{BASE_IMAGE}' not found. "
            "Run: incus image copy images:ubuntu/24.04 local: --alias ubuntu-24.04-container"
        )

    @staticmethod
    def _try_cache_image() -> bool:
        """Attempt to cache the image from the remote if not present."""
        r = _incus(
            f"image copy images:ubuntu/24.04 local: --alias {BASE_IMAGE}",
            check=False, timeout=600,
        )
        return r.returncode == 0

    def test_01_launch_container(self):
        """Launch a temp container and write a canary file."""
        _full_cleanup()

        target = f"--target {INCUS_TARGET}" if INCUS_TARGET else ""
        r = _incus(f"launch {BASE_IMAGE} {CONTAINER_NAME} {target}", timeout=120)
        assert r.returncode == 0, f"Failed to launch: {r.stderr}"

        # Wait for container networking
        for _ in range(30):
            r2 = _incus(f"exec {CONTAINER_NAME} -- echo ready", check=False, timeout=10)
            if r2.returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail("Container did not become ready in time")

        # Write canary file
        _incus(
            f'exec {CONTAINER_NAME} -- sh -c "echo {CANARY_CONTENT} > /canary.txt"',
            timeout=30,
        )

        # Verify canary
        r3 = _incus(f"exec {CONTAINER_NAME} -- cat /canary.txt", timeout=10)
        assert CANARY_CONTENT in r3.stdout

    def test_02_publish_image(self):
        """Stop container and publish as image."""
        _incus(f"stop {CONTAINER_NAME}", timeout=120)
        r = _incus(f"publish {CONTAINER_NAME} --alias {IMAGE_ALIAS}", timeout=300)
        assert r.returncode == 0, f"Publish failed: {r.stderr}"
        assert _image_exists(IMAGE_ALIAS)

    def test_03_export_image(self, tmpdir):
        """Export the image to a local tarball."""
        export_path = os.path.join(tmpdir, "export")
        r = _incus(f"image export {IMAGE_ALIAS} {export_path}", timeout=300)
        assert r.returncode == 0, f"Export failed: {r.stderr}"

        # incus image export appends .tar.gz
        tarball = export_path + ".tar.gz"
        assert os.path.isfile(tarball), f"Expected {tarball} to exist"
        size = os.path.getsize(tarball)
        assert size > 1_000_000, f"Export too small ({size} bytes), something is wrong"
        print(f"Exported image: {size / 1024 / 1024:.1f} MB")

    def test_04_relay_to_sftp(self, tmpdir):
        """Encrypt and relay the exported image to SFTP backup."""
        tarball = os.path.join(tmpdir, "export.tar.gz")
        assert os.path.isfile(tarball), "Export tarball missing — did test_03 run?"

        original_hash = _sha256_file(tarball)
        with open(os.path.join(tmpdir, "original.sha256"), "w") as f:
            f.write(original_hash)
        print(f"Original SHA-256: {original_hash}")

        sftp_url = f"sftp://{SFTP_USER}@{SFTP_HOST}{SFTP_DIR}/{BACKUP_FILENAME}"
        file_url = f"file://{tarball}"

        encrypt_func = _make_encrypt_func(PASSPHRASE)

        asyncio.run(relay_stream(
            get_url=file_url,
            post_urls=[sftp_url],
            process_func=encrypt_func,
            chunk_size=512 * 1024,
            queue_maxsize=20,
            post_configs=[{"known_hosts": None}],
        ))

        # Verify remote file exists
        remote_path = f"{SFTP_DIR}/{BACKUP_FILENAME}"
        r = _run(
            f"ssh -o StrictHostKeyChecking=no {SFTP_USER}@{SFTP_HOST} "
            f"'test -f {remote_path} && stat --format=%s {remote_path}'",
        )
        remote_size = int(r.stdout.strip())
        assert remote_size > 0, "Remote backup file is empty"
        print(f"Remote backup: {remote_size / 1024 / 1024:.1f} MB (encrypted)")

    def test_05_delete_originals(self, tmpdir):
        """Delete the local export, image, and container."""
        tarball = os.path.join(tmpdir, "export.tar.gz")
        if os.path.isfile(tarball):
            os.remove(tarball)

        _cleanup_image(IMAGE_ALIAS)
        _cleanup_container(CONTAINER_NAME)

        assert not _container_exists(CONTAINER_NAME)
        assert not _image_exists(IMAGE_ALIAS)
        assert not os.path.isfile(tarball)

    def test_06_relay_from_sftp(self, tmpdir):
        """Relay the encrypted backup from SFTP, decrypt, and save locally."""
        sftp_url = f"sftp://{SFTP_USER}@{SFTP_HOST}{SFTP_DIR}/{BACKUP_FILENAME}"
        restored_tarball = os.path.join(tmpdir, "restored.tar.gz")
        file_url = f"file://{restored_tarball}"

        decrypt_func = _make_decrypt_func(PASSPHRASE)

        asyncio.run(relay_stream(
            get_url=sftp_url,
            post_urls=[file_url],
            process_func=decrypt_func,
            chunk_size=512 * 1024,
            queue_maxsize=20,
            get_config={"known_hosts": None},
        ))

        assert os.path.isfile(restored_tarball), "Restored tarball not created"

        # Verify SHA-256 matches original
        restored_hash = _sha256_file(restored_tarball)
        with open(os.path.join(tmpdir, "original.sha256")) as f:
            original_hash = f.read().strip()

        assert restored_hash == original_hash, (
            f"Hash mismatch!\n  original: {original_hash}\n  restored: {restored_hash}"
        )
        print(f"SHA-256 verified: {restored_hash}")

    def test_07_import_image(self, tmpdir):
        """Import the restored tarball as an Incus image."""
        restored_tarball = os.path.join(tmpdir, "restored.tar.gz")
        assert os.path.isfile(restored_tarball)

        r = _incus(
            f"image import {restored_tarball} --alias {RESTORED_ALIAS}",
            timeout=300,
        )
        assert r.returncode == 0, f"Import failed: {r.stderr}"
        assert _image_exists(RESTORED_ALIAS)

    def test_08_launch_restored(self):
        """Launch a container from the restored image and verify the canary."""
        target = f"--target {INCUS_TARGET}" if INCUS_TARGET else ""
        r = _incus(
            f"launch {RESTORED_ALIAS} {RESTORED_NAME} {target}",
            timeout=120,
        )
        assert r.returncode == 0, f"Launch failed: {r.stderr}"

        # Wait for ready
        for _ in range(30):
            r2 = _incus(f"exec {RESTORED_NAME} -- echo ready", check=False, timeout=10)
            if r2.returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail("Restored container did not become ready in time")

        # Verify canary
        r3 = _incus(f"exec {RESTORED_NAME} -- cat /canary.txt", timeout=10)
        assert CANARY_CONTENT in r3.stdout, (
            f"Canary mismatch! Expected '{CANARY_CONTENT}', got '{r3.stdout.strip()}'"
        )
        print("Canary file verified in restored container!")

    def test_09_cleanup(self, tmpdir):
        """Clean up all resources."""
        _full_cleanup(tmpdir)

        assert not _container_exists(CONTAINER_NAME)
        assert not _container_exists(RESTORED_NAME)
        assert not _image_exists(IMAGE_ALIAS)
        assert not _image_exists(RESTORED_ALIAS)
