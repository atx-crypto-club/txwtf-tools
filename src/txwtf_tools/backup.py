"""
LXD / Incus backup helpers — snapshot a VM, publish it as an image,
and stream it to another cluster or to encrypted SFTP storage,
all without writing temporary files to disk.
"""

import base64
import hashlib
import struct
import time
import zlib

from cryptography.fernet import Fernet
from tqdm import tqdm

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
# Length-prefixed Fernet encrypt / decrypt helpers
# ---------------------------------------------------------------------------
#
# Each Fernet token is variable-length.  When tokens are written to a flat
# byte stream (e.g. SFTP file) we prepend a 4-byte big-endian length so the
# reader can reliably reassemble tokens from fixed-size read chunks.

def make_encrypt_func(passphrase: str):
    """Return a function that Fernet-encrypts a chunk and prepends a 4-byte
    big-endian length header."""
    key = get_fixed_base64_from_utf8_string(passphrase)
    enc = Fernet(key)

    def encrypt(chunk: bytes) -> bytes:
        if not chunk:
            return b""
        token = enc.encrypt(chunk)
        return struct.pack(">I", len(token)) + token

    return encrypt


def make_decrypt_func(passphrase: str):
    """Return a *stateful* function that accumulates bytes and decrypts
    complete length-prefixed Fernet tokens.

    Safe for sequential use from ``relay_stream``'s ``process_and_yield``
    (each call is awaited before the next).
    """
    key = get_fixed_base64_from_utf8_string(passphrase)
    dec = Fernet(key)
    buf = bytearray()

    def decrypt(chunk: bytes) -> bytes:
        if not chunk:
            return b""
        buf.extend(chunk)
        out = bytearray()
        while len(buf) >= 4:
            token_len = struct.unpack(">I", buf[:4])[0]
            if len(buf) < 4 + token_len:
                break
            token = bytes(buf[4 : 4 + token_len])
            del buf[: 4 + token_len]
            out.extend(dec.decrypt(token))
        return bytes(out)

    return decrypt


# ---------------------------------------------------------------------------
# Compression / decompression helpers (gzip format)
# ---------------------------------------------------------------------------

def make_compress_func(level: int = 9):
    """Return ``(compress_func, finalize_func)`` for gzip compression.

    *compress_func* is a stateful per-chunk compressor.  It may return empty
    bytes when zlib buffers internally — the relay skips empty results
    automatically.

    *finalize_func* must be called once after the last chunk to flush the
    gzip trailer.  Pass it as ``finalize_func`` to :func:`relay_stream`.
    """
    compressor = zlib.compressobj(level, zlib.DEFLATED, 16 + zlib.MAX_WBITS)

    def compress(chunk: bytes) -> bytes:
        if not chunk:
            return b""
        return compressor.compress(chunk)

    def finalize() -> bytes:
        return compressor.flush(zlib.Z_FINISH)

    return compress, finalize


def make_decompress_func():
    """Return a decompression function for gzip data.

    Stateful — each call decompresses whatever is available from the
    internal zlib buffer.
    """
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)

    def decompress(chunk: bytes) -> bytes:
        if not chunk:
            return b""
        return decompressor.decompress(chunk)

    return decompress


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
    ca_path: str | None = None,
):
    """Snapshot the VM and publish it as an image. Returns (image, snapshot, alias)."""
    pylxd = _get_pylxd()

    source_client = pylxd.Client(
        project=project,
        endpoint=source_endpoint,
        cert=(cert_path, key_path),
        verify=ca_path if ca_path else False,
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


def list_instances(
    endpoint: str,
    cert_path: str,
    key_path: str,
    ca_path: str | None = None,
    project: str = "default",
    vm_type: str | None = None,
    name_prefix: str | None = None,
    name_contains: str | None = None,
    status: str | None = None,
    exclude: list[str] | None = None,
):
    """Return a list of instance names from the LXD/Incus API, with optional filters.

    *vm_type* can be ``"virtual-machine"`` or ``"container"`` to filter by type.
    *name_prefix* keeps only instances whose name starts with the given string.
    *name_contains* keeps only instances whose name contains the given substring.
    *status* keeps only instances with a matching status (e.g. ``"Running"``, ``"Stopped"``).
    *exclude* is a list of instance names to skip.
    """
    pylxd = _get_pylxd()

    client = pylxd.Client(
        project=project,
        endpoint=endpoint,
        cert=(cert_path, key_path),
        verify=ca_path if ca_path else False,
    )

    instances = client.instances.all()
    names = []
    for inst in instances:
        if vm_type and inst.type != vm_type:
            continue
        if name_prefix and not inst.name.startswith(name_prefix):
            continue
        if name_contains and name_contains not in inst.name:
            continue
        if status and inst.status != status:
            continue
        if exclude and inst.name in exclude:
            continue
        names.append(inst.name)

    return sorted(names)


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
    ca_path: str | None = None,
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
                token = encryptor.encrypt(chunk)
                return struct.pack(">I", len(token)) + token

        chunk_chain = [pass_chunk]
        if compress:
            chunk_chain.append(compress_chunk)
        if encrypt:
            chunk_chain.append(encrypt_chunk)

        process_chunk = chain_functions(*chunk_chain)

        def flush_process():
            compressed_flush = compressor.flush(zlib.Z_FINISH)
            if compressed_flush:
                if encrypt:
                    token = encryptor.encrypt(compressed_flush)
                    return struct.pack(">I", len(token)) + token
                return compressed_flush
            return compressed_flush

        export_url = f"{source_endpoint}/1.0/images/{image.fingerprint}/export"

        input_kwargs = {
            "chunk_size": chunk_size,
            "cert_file": cert_path,
            "key_file": key_path,
            "http_kwargs": {
                "headers": {"User-Agent": "txwtf-tools 0.1.0"},
                "allow_redirects": True,
            },
        }
        if ca_path:
            input_kwargs["ca_file"] = ca_path

        pairs = [
            {
                "input_uri": export_url,
                "output_uris": [sftp_url],
                "input_kwargs": input_kwargs,
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


def do_restore(
    sftp_url: str,
    target_endpoint: str,
    passphrase: str,
    cert_path: str,
    key_path: str,
    ca_path: str | None = None,
    verify_target: bool = False,
    compress: bool = True,
    encrypt: bool = True,
    chunk_size: int = 512 * 1024,
    max_queue_size: int = 20,
    rate_limit: float | None = None,
    image_alias: str | None = None,
):
    """Restore an LXD/Incus image from an encrypted SFTP backup — the
    inverse of :func:`do_store`.

    Reads length-prefixed Fernet tokens from *sftp_url*, decrypts and
    decompresses them, then streams the raw image to the Incus import API
    at *target_endpoint*.
    """
    import_url = f"{target_endpoint}/1.0/images"

    # Build input-side process function: decrypt then decompress (reverse of store).
    # Applied on the input (SFTP read) side via get_process_func so the raw
    # decrypted/decompressed bytes are streamed directly to the Incus API.
    funcs: list = []
    if encrypt:
        funcs.append(make_decrypt_func(passphrase))
    if compress:
        funcs.append(make_decompress_func())

    get_process_func = chain_functions(*funcs) if funcs else None

    ssl_config: dict = {
        "client_cert": cert_path,
        "client_key": key_path,
        "verify": verify_target,
    }
    if ca_path:
        ssl_config["ca_cert"] = ca_path

    relay_kwargs: dict = {
        "get_url": sftp_url,
        "post_urls": [import_url],
        "chunk_size": chunk_size,
        "queue_maxsize": max_queue_size,
        "get_config": {"known_hosts": None},
        "post_headers": [
            {
                "Content-Type": "application/octet-stream",
                "X-LXD-public": "false",
            }
        ],
        "post_configs": [ssl_config],
    }

    if get_process_func is not None:
        relay_kwargs["get_process_func"] = get_process_func
    if rate_limit:
        relay_kwargs["rate_limit"] = rate_limit

    relay(**relay_kwargs)


def do_store_all(
    endpoint: str,
    target_url: str,
    cert_path: str,
    key_path: str,
    ca_path: str | None,
    passphrase: str,
    project: str = "default",
    compress: bool = True,
    encrypt: bool = True,
    vm_type: str | None = None,
    name_prefix: str | None = None,
    name_contains: str | None = None,
    status: str | None = None,
    exclude: list[str] | None = None,
    chunk_size: int = 1024 * 1024,
    max_queue_size: int = 512,
    rate_limit: float | None = None,
):
    """Back up all matching VMs from an LXD/Incus endpoint to *target_url*.

    Each VM image is stored at ``<target_url>/<project>-<vm_name>-backup.img``
    (with ``.gz`` and/or ``.enc`` extensions based on *compress*/*encrypt*).

    Returns a dict mapping VM names to ``"ok"`` or an error message.
    """
    names = list_instances(
        endpoint=endpoint,
        cert_path=cert_path,
        key_path=key_path,
        ca_path=ca_path,
        project=project,
        vm_type=vm_type,
        name_prefix=name_prefix,
        name_contains=name_contains,
        status=status,
        exclude=exclude,
    )

    if not names:
        print("No matching instances found.")
        return {}

    # Build target filename suffix based on flags.
    suffix = ".img"
    if compress:
        suffix += ".gz"
    if encrypt:
        suffix += ".enc"

    # Normalise base URL — strip trailing slash.
    base = target_url.rstrip("/")

    results: dict[str, str] = {}

    # Outer progress bar tracks VM count; inner bars from streamer/relay
    # appear below it via tqdm's positioning.
    with tqdm(
        total=len(names),
        unit="vm",
        desc="Overall",
        position=0,
        leave=True,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} VMs [{elapsed}<{remaining}]",
    ) as outer:
        for name in names:
            alias = f"{project}-{name}-backup"
            dest = f"{base}/{alias}{suffix}"
            outer.set_postfix_str(name, refresh=True)

            try:
                do_store(
                    project=project,
                    vm_name=name,
                    source_endpoint=endpoint,
                    sftp_url=dest,
                    passphrase=passphrase,
                    cert_path=cert_path,
                    key_path=key_path,
                    ca_path=ca_path,
                    compress=compress,
                    encrypt=encrypt,
                    chunk_size=chunk_size,
                    max_queue_size=max_queue_size,
                    rate_limit=rate_limit,
                )
                results[name] = "ok"
            except Exception as exc:
                results[name] = str(exc)
                tqdm.write(f"  ✗ '{name}' failed: {exc}")

            outer.update(1)

    # Summary.
    ok = sum(1 for v in results.values() if v == "ok")
    failed = len(results) - ok
    tqdm.write(f"\nDone: {ok} succeeded, {failed} failed out of {len(results)} total.")
    return results


def do_restore_all(
    source_url: str,
    target_endpoint: str,
    names: list[str],
    passphrase: str,
    cert_path: str,
    key_path: str,
    ca_path: str | None = None,
    verify_target: bool = False,
    project: str = "default",
    compress: bool = True,
    encrypt: bool = True,
    chunk_size: int = 512 * 1024,
    max_queue_size: int = 20,
    rate_limit: float | None = None,
):
    """Restore all named VMs from SFTP backups — the inverse of :func:`do_store_all`.

    Each backup is expected at ``<source_url>/<project>-<name>-backup.img``
    (with ``.gz`` and/or ``.enc`` extensions matching *compress*/*encrypt*).

    Returns a dict mapping VM names to ``"ok"`` or an error message.
    """
    if not names:
        print("No VM names provided.")
        return {}

    # Build filename suffix (must match what do_store_all produced).
    suffix = ".img"
    if compress:
        suffix += ".gz"
    if encrypt:
        suffix += ".enc"

    base = source_url.rstrip("/")

    results: dict[str, str] = {}

    with tqdm(
        total=len(names),
        unit="vm",
        desc="Overall",
        position=0,
        leave=True,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} VMs [{elapsed}<{remaining}]",
    ) as outer:
        for name in names:
            alias = f"{project}-{name}-backup"
            src = f"{base}/{alias}{suffix}"
            outer.set_postfix_str(name, refresh=True)

            try:
                do_restore(
                    sftp_url=src,
                    target_endpoint=target_endpoint,
                    passphrase=passphrase,
                    cert_path=cert_path,
                    key_path=key_path,
                    ca_path=ca_path,
                    verify_target=verify_target,
                    compress=compress,
                    encrypt=encrypt,
                    chunk_size=chunk_size,
                    max_queue_size=max_queue_size,
                    rate_limit=rate_limit,
                )
                results[name] = "ok"
            except Exception as exc:
                results[name] = str(exc)
                tqdm.write(f"  ✗ '{name}' failed: {exc}")

            outer.update(1)

    ok = sum(1 for v in results.values() if v == "ok")
    failed = len(results) - ok
    tqdm.write(f"\nDone: {ok} succeeded, {failed} failed out of {len(results)} total.")
    return results
