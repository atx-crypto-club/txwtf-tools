"""
Live backup round-trip integration test — streaming via Incus API.

Requires a running Incus cluster with:
  - ``incus`` CLI accessible to the runner user
  - TLS client cert at ``~/.config/incus/client.{crt,key}`` trusted by the cluster
  - A cached container image aliased ``ubuntu-24.04-container``
  - SFTP access from the runner to ``TXWTF_SFTP_HOST`` (default: star-01)
  - A writable storage directory on the SFTP host

The test exercises the full relay pipeline with NO intermediate temp files:
  1. Launch a temp container, write a canary file, stop & publish as image
  2. Stream image directly from Incus HTTPS export API → encrypt → SFTP backup
  3. Delete the image and container from the cluster
  4. Stream from SFTP backup → decrypt → Incus HTTPS import API
  5. Launch restored container, verify canary file
  6. Clean up everything

All resources use the ``txwtf-ci-`` prefix so they cannot collide with
production workloads.

Env vars (all optional, with sane defaults):
  TXWTF_SFTP_HOST      SFTP target hostname          (default: star-01)
  TXWTF_SFTP_USER      SFTP username                 (default: tfx)
  TXWTF_SFTP_DIR       Remote directory for backups   (default: /media/catx-easystore/txwtf-tools-tests)
  TXWTF_INCUS_TARGET   Incus cluster member to target (default: unset = auto)
  TXWTF_BASE_IMAGE     Local image alias to launch    (default: ubuntu-24.04-container)
  TXWTF_INCUS_API      Incus API base URL             (default: https://10.66.77.217:8443)
  TXWTF_INCUS_CERT     Path to client cert            (default: ~/.config/incus/client.crt)
  TXWTF_INCUS_KEY      Path to client key             (default: ~/.config/incus/client.key)
"""

import asyncio
import os
import subprocess
import time
import uuid

import pytest

from txwtf_tools.backup import get_fixed_base64_from_utf8_string
from txwtf_tools.relay import relay_stream

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------

SFTP_HOST = os.environ.get("TXWTF_SFTP_HOST", "star-01")
SFTP_USER = os.environ.get("TXWTF_SFTP_USER", "tfx")
SFTP_DIR = os.environ.get("TXWTF_SFTP_DIR", "/media/catx-easystore/txwtf-tools-tests")
INCUS_TARGET = os.environ.get("TXWTF_INCUS_TARGET", "")
BASE_IMAGE = os.environ.get("TXWTF_BASE_IMAGE", "ubuntu-24.04-container")

_home = os.path.expanduser("~")
INCUS_API = os.environ.get("TXWTF_INCUS_API", "https://10.66.77.217:8443")
INCUS_CERT = os.environ.get("TXWTF_INCUS_CERT", f"{_home}/.config/incus/client.crt")
INCUS_KEY = os.environ.get("TXWTF_INCUS_KEY", f"{_home}/.config/incus/client.key")

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
    r = _incus("list --format csv -c n", check=False)
    return name in r.stdout.splitlines()


def _image_exists(alias: str) -> bool:
    r = _incus("image alias list --format csv", check=False)
    return any(alias == line.split(",")[0] for line in r.stdout.splitlines())


def _get_image_fingerprint(alias: str) -> str | None:
    """Get the fingerprint for an image alias."""
    r = _incus(f"image info {alias}", check=False)
    for line in r.stdout.splitlines():
        if line.strip().startswith("Fingerprint:"):
            return line.split(":", 1)[1].strip()
    return None


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
    _run(
        f"ssh -o StrictHostKeyChecking=no {SFTP_USER}@{SFTP_HOST} "
        f"'rm -f {remote_path}'",
        check=False,
    )


def _full_cleanup():
    """Best-effort cleanup of all resources created by this test run."""
    _cleanup_container(RESTORED_NAME)
    _cleanup_container(CONTAINER_NAME)
    _cleanup_image(RESTORED_ALIAS)
    _cleanup_image(IMAGE_ALIAS)
    _cleanup_sftp_file()


def _incus_api_ssl_config() -> dict:
    """SSL config dict for relay_stream to talk to the Incus HTTPS API."""
    return {
        "client_cert": INCUS_CERT,
        "client_key": INCUS_KEY,
        "verify": False,  # server cert SAN doesn't match IP
    }


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
# Shared state across ordered tests (populated by earlier tests)
# ---------------------------------------------------------------------------

_state: dict = {}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.live
class TestBackupRoundTrip:
    """End-to-end encrypted backup and restore — streaming through relay_stream
    with no intermediate temp files on disk."""

    def test_00_preflight(self):
        """Verify incus CLI, base image, TLS certs, and SFTP access."""
        r = _incus("version", check=False)
        assert r.returncode == 0, f"incus not available: {r.stderr}"

        assert _image_exists(BASE_IMAGE) or self._try_cache_image(), (
            f"Base image '{BASE_IMAGE}' not found. "
            "Run: incus image copy images:ubuntu/24.04 local: --alias ubuntu-24.04-container"
        )

        for path, desc in [
            (INCUS_CERT, "client cert"),
            (INCUS_KEY, "client key"),
        ]:
            assert os.path.isfile(path), f"Missing {desc}: {path}"

        # Verify SFTP host is reachable
        r = _run(
            f"ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "
            f"{SFTP_USER}@{SFTP_HOST} 'echo OK'",
            check=False,
        )
        assert "OK" in r.stdout, f"Cannot reach SFTP host {SFTP_HOST}: {r.stderr}"

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

        # Wait for container to be ready
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

        fp = _get_image_fingerprint(IMAGE_ALIAS)
        assert fp, f"Could not get fingerprint for {IMAGE_ALIAS}"
        _state["fingerprint"] = fp
        print(f"Image fingerprint: {fp}")

    def test_03_stream_to_sftp(self):
        """Stream image from Incus API → encrypt → SFTP (no temp files)."""
        fp = _state.get("fingerprint")
        assert fp, "No fingerprint — did test_02 run?"

        export_url = f"{INCUS_API}/1.0/images/{fp}/export"
        sftp_url = f"sftp://{SFTP_USER}@{SFTP_HOST}{SFTP_DIR}/{BACKUP_FILENAME}"

        encrypt_func = _make_encrypt_func(PASSPHRASE)

        asyncio.run(relay_stream(
            get_url=export_url,
            post_urls=[sftp_url],
            process_func=encrypt_func,
            chunk_size=512 * 1024,
            queue_maxsize=20,
            get_config=_incus_api_ssl_config(),
            post_configs=[{"known_hosts": None}],
        ))

        # Verify remote file exists and has content
        remote_path = f"{SFTP_DIR}/{BACKUP_FILENAME}"
        r = _run(
            f"ssh -o StrictHostKeyChecking=no {SFTP_USER}@{SFTP_HOST} "
            f"'test -f {remote_path} && stat --format=%s {remote_path}'",
        )
        remote_size = int(r.stdout.strip())
        assert remote_size > 1_000_000, f"Remote backup too small ({remote_size} bytes)"
        _state["remote_size"] = remote_size
        print(f"Streamed encrypted backup to SFTP: {remote_size / 1024 / 1024:.1f} MB")

    def test_04_delete_originals(self):
        """Delete the image and container from the cluster."""
        _cleanup_image(IMAGE_ALIAS)
        _cleanup_container(CONTAINER_NAME)

        assert not _container_exists(CONTAINER_NAME), "Container still exists"
        assert not _image_exists(IMAGE_ALIAS), "Image still exists"
        print("Deleted original image and container from cluster")

    def test_05_stream_from_sftp(self):
        """Stream from SFTP → decrypt → Incus import API (no temp files)."""
        sftp_url = f"sftp://{SFTP_USER}@{SFTP_HOST}{SFTP_DIR}/{BACKUP_FILENAME}"
        import_url = f"{INCUS_API}/1.0/images"

        decrypt_func = _make_decrypt_func(PASSPHRASE)

        asyncio.run(relay_stream(
            get_url=sftp_url,
            post_urls=[import_url],
            process_func=decrypt_func,
            chunk_size=512 * 1024,
            queue_maxsize=20,
            get_config={"known_hosts": None},
            post_headers=[{
                "Content-Type": "application/octet-stream",
                "X-Incus-public": "false",
            }],
            post_configs=[_incus_api_ssl_config()],
        ))

        # Wait for image to appear (import is async)
        fp = _state.get("fingerprint")
        for _ in range(60):
            r = _incus(f"image info {fp}", check=False)
            if r.returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail(f"Image {fp} did not appear after import")

        # Add an alias for the restored image
        _incus(f"image alias create {RESTORED_ALIAS} {fp}")
        assert _image_exists(RESTORED_ALIAS)
        print(f"Restored image from SFTP backup (fingerprint: {fp})")

    def test_06_launch_restored(self):
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

    def test_07_cleanup(self):
        """Clean up all resources."""
        _full_cleanup()

        assert not _container_exists(CONTAINER_NAME)
        assert not _container_exists(RESTORED_NAME)
        assert not _image_exists(IMAGE_ALIAS)
        assert not _image_exists(RESTORED_ALIAS)
