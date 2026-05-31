"""
LXD / Incus backup helpers — snapshot a VM, publish it as an image,
and stream it to another cluster or to encrypted SFTP storage,
all without writing temporary files to disk.
"""

import base64
import hashlib
import time
import zlib

from cryptography.fernet import Fernet

from .relay import relay
from .streamer import create_ssl_connector, streamer  # noqa: F401 — re-export


def get_fixed_base64_from_utf8_string(input_string: str) -> str:
    """SHA-256 hash the string, then return URL-safe base64 of the 32-byte digest."""
    key_bytes = input_string.encode("utf-8")
    hashed_bytes = hashlib.sha256(key_bytes).digest()
    return base64.urlsafe_b64encode(hashed_bytes).decode("ascii")


def chain_functions(*funcs):
    """Return a function that applies *funcs* in sequence on bytes input."""

    def chained(input_chunk: bytes) -> bytes:
        current = input_chunk
        for func in funcs:
            current = func(current)
        return current

    return chained


# ---------------------------------------------------------------------------
# pylxd helpers — guarded behind try/except so the package works without pylxd
# ---------------------------------------------------------------------------

def _get_pylxd():
    try:
        import pylxd
        return pylxd
    except ImportError:
        raise ImportError(
            "pylxd is required for LXD backup operations. "
            "Install it with: pip install txwtf-tools[lxd]"
        )


def prepare_backup(
    project: str,
    vm_name: str,
    source_endpoint: str,
    cert_path: str,
    key_path: str,
    ca_path: str,
):
    """Snapshot the VM and publish it as an image. Returns (image, snapshot, alias)."""
    pylxd = _get_pylxd()

    source_client = pylxd.Client(
        project=project,
        endpoint=source_endpoint,
        cert=(cert_path, key_path),
        verify=ca_path,
    )

    try:
        instance = source_client.instances.get(vm_name)
    except pylxd.exceptions.NotFound:
        print(f"VM '{vm_name}' not found in project '{project}'.")
        return None, None, None

    snapshot_name = "temp_backup3"
    try:
        snapshot = instance.snapshots.get(snapshot_name)
        print(f"Snapshot '{snapshot_name}' already exists. Deleting it.")
        snapshot.delete()
    except pylxd.exceptions.NotFound:
        pass

    print(f"Creating snapshot '{snapshot_name}' of VM '{vm_name}'.")
    snapshot = instance.snapshots.create(snapshot_name, stateful=False, wait=True)

    image_alias = f"{project}-{vm_name}-backup"
    print(f"Publishing snapshot to image with alias '{image_alias}'.")
    image = snapshot.publish(wait=True)
    try:
        image.add_alias(image_alias, f"backup for {vm_name}")
    except pylxd.exceptions.Conflict:
        image.delete_alias(image_alias)
        image.add_alias(image_alias, f"backup for {vm_name}")

    print(f"Image fingerprint: {image.fingerprint}")
    return image, snapshot, image_alias


def cleanup_backup(image, snapshot):
    """Remove the temporary snapshot (image is kept)."""
    if snapshot:
        snapshot.delete()


def do_copy(
    project: str,
    vm_name: str,
    source_endpoint: str,
    target_endpoint: str,
    cert_path: str,
    key_path: str,
    ca_path: str,
    target_ca_path: str,
    target_project: str,
    chunk_size: int = 1024 * 1024,
    max_queue_size: int = 128,
    rate_limit: float | None = None,
):
    """Copy an LXD/Incus VM image between clusters using the relay stream."""
    pylxd = _get_pylxd()

    image, snapshot, image_alias = prepare_backup(
        project, vm_name, source_endpoint, cert_path, key_path, ca_path
    )
    if not image:
        return

    try:
        export_url = f"{source_endpoint}/1.0/images/{image.fingerprint}/export"
        import_url = f"{target_endpoint}/1.0/images"

        relay_args = {
            "queue_maxsize": max_queue_size,
            "chunk_size": chunk_size,
            "get_url": export_url,
            "get_headers": {"User-Agent": "txwtf-tools 0.1.0"},
            "get_config": {
                "client_cert": cert_path,
                "client_key": key_path,
                "ca_cert": ca_path,
                "verify": True,
            },
            "post_urls": [import_url],
            "post_headers": [
                {
                    "User-Agent": "txwtf-tools 0.1.0",
                    "Content-Type": "application/octet-stream",
                    "X-LXD-public": "false",
                }
            ],
            "monitor_queues_flag": True,
            "post_configs": [
                {
                    "client_cert": cert_path,
                    "client_key": key_path,
                    "ca_cert": target_ca_path,
                    "verify": True,
                }
            ],
        }

        if rate_limit:
            relay_args["rate_limit"] = rate_limit

        relay(**relay_args)

        target_client = pylxd.Client(
            project=target_project,
            endpoint=target_endpoint,
            cert=(cert_path, key_path),
            verify=target_ca_path,
        )
        while True:
            try:
                target_image = target_client.images.get(image.fingerprint)
                break
            except pylxd.exceptions.NotFound:
                time.sleep(1.0)
        target_image.add_alias(name=image_alias, description="backup")

    finally:
        cleanup_backup(image, snapshot)


def do_store(
    project: str,
    vm_name: str,
    source_endpoint: str,
    sftp_url: str,
    passphrase: str,
    cert_path: str,
    key_path: str,
    ca_path: str,
    compress: bool = True,
    encrypt: bool = True,
    chunk_size: int = 1024 * 1024,
    max_queue_size: int = 512,
    rate_limit: float | None = None,
):
    """Compress + encrypt an LXD/Incus image and stream it to SFTP."""
    image, snapshot, _image_alias = prepare_backup(
        project, vm_name, source_endpoint, cert_path, key_path, ca_path
    )
    if not image:
        return

    try:
        compressor = zlib.compressobj(9, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
        encryptor = Fernet(get_fixed_base64_from_utf8_string(passphrase))

        def pass_chunk(chunk: bytes) -> bytes:
            return chunk

        def compress_chunk(chunk: bytes) -> bytes:
            if chunk:
                return compressor.compress(chunk)

        def encrypt_chunk(chunk: bytes) -> bytes:
            if chunk:
                return encryptor.encrypt(chunk)

        chunk_chain = [pass_chunk]
        if compress:
            chunk_chain.append(compress_chunk)
        if encrypt:
            chunk_chain.append(encrypt_chunk)

        process_chunk = chain_functions(*chunk_chain)

        def flush_process():
            compressed_flush = compressor.flush(zlib.Z_FINISH)
            if compressed_flush:
                return encryptor.encrypt(compressed_flush)
            return compressed_flush

        export_url = f"{source_endpoint}/1.0/images/{image.fingerprint}/export"

        pairs = [
            {
                "input_uri": export_url,
                "output_uris": [sftp_url],
                "input_kwargs": {
                    "chunk_size": chunk_size,
                    "cert_file": cert_path,
                    "key_file": key_path,
                    "ca_file": ca_path,
                    "http_kwargs": {
                        "ssl": True,
                        "headers": {"User-Agent": "txwtf-tools 0.1.0"},
                        "allow_redirects": True,
                    },
                },
                "output_kwargs_list": [
                    {
                        "max_queue_size": max_queue_size,
                        "callback": process_chunk,
                        "finalize_callback": flush_process,
                    }
                ],
                "rate_limit": rate_limit,
            }
        ]

        streamer(pairs)

    finally:
        cleanup_backup(image, snapshot)
