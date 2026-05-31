"""CLI entry-point for txwtf-tools, built with Click."""

import click

from . import __version__


@click.group()
@click.version_option(version=__version__, prog_name="txwtf-tools")
def cli():
    """txwtf-tools — stream relay and LXD/Incus backup migration tools."""


# ---------------------------------------------------------------------------
# relay command
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("get_url")
@click.argument("post_urls", nargs=-1, required=True)
@click.option("--chunk-size", default=1024 * 1024, show_default=True, help="Chunk size in bytes.")
@click.option("--queue-maxsize", default=128, show_default=True, help="Max queue depth per output.")
@click.option("--monitor/--no-monitor", default=False, help="Show queue fill-level bars.")
@click.option(
    "--get-cert", type=click.Path(exists=True), default=None, help="Client cert for input (HTTPS)."
)
@click.option(
    "--get-key", type=click.Path(exists=True), default=None, help="Client key for input (HTTPS)."
)
@click.option(
    "--get-ca", type=click.Path(exists=True), default=None, help="CA cert for input (HTTPS)."
)
@click.option(
    "--post-cert", type=click.Path(exists=True), default=None, help="Client cert for output (HTTPS)."
)
@click.option(
    "--post-key", type=click.Path(exists=True), default=None, help="Client key for output (HTTPS)."
)
@click.option(
    "--post-ca", type=click.Path(exists=True), default=None, help="CA cert for output (HTTPS)."
)
@click.option(
    "--rate-limit", type=float, default=None,
    help="Max input read rate in bytes/sec (e.g. 1048576 for 1 MB/s). 0 = unlimited.",
)
@click.option(
    "--decrypt-passphrase", default=None, hide_input=True,
    help="Passphrase to decrypt the input stream (length-prefixed Fernet).",
)
@click.option(
    "--encrypt-passphrase", default=None, hide_input=True,
    help="Passphrase to encrypt the output stream (length-prefixed Fernet).",
)
@click.option("--compress", is_flag=True, default=False, help="Gzip-compress the output stream.")
@click.option("--decompress", is_flag=True, default=False, help="Gzip-decompress the input stream.")
def relay(
    get_url,
    post_urls,
    chunk_size,
    queue_maxsize,
    monitor,
    get_cert,
    get_key,
    get_ca,
    post_cert,
    post_key,
    post_ca,
    rate_limit,
    decrypt_passphrase,
    encrypt_passphrase,
    compress,
    decompress,
):
    """Relay a stream from GET_URL to one or more POST_URLS.

    Supports http(s), sftp, and file:// schemes on both sides.
    """
    from .relay import relay as do_relay

    get_process_func = None
    process_func = None
    finalize_func = None

    if decompress:
        from .backup import make_decompress_func
        get_process_func = make_decompress_func()

    if decrypt_passphrase:
        from .backup import make_decrypt_func
        if get_process_func:
            from .backup import chain_functions
            get_process_func = chain_functions(get_process_func, make_decrypt_func(decrypt_passphrase))
        else:
            get_process_func = make_decrypt_func(decrypt_passphrase)

    if compress:
        from .backup import make_compress_func
        compress_func, finalize_func = make_compress_func()
        process_func = compress_func

    if encrypt_passphrase:
        from .backup import make_encrypt_func
        enc = make_encrypt_func(encrypt_passphrase)
        if process_func:
            from .backup import chain_functions
            old_process = process_func
            old_finalize = finalize_func

            def chained_process(chunk: bytes) -> bytes:
                compressed = old_process(chunk)
                if compressed:
                    return enc(compressed)
                return b""

            def chained_finalize() -> bytes:
                parts = bytearray()
                if old_finalize:
                    flushed = old_finalize()
                    if flushed:
                        parts.extend(enc(flushed))
                return bytes(parts)

            process_func = chained_process
            finalize_func = chained_finalize
        else:
            process_func = enc

    get_config = None
    if get_cert and get_key:
        get_config = {
            "client_cert": get_cert,
            "client_key": get_key,
            "verify": True,
        }
        if get_ca:
            get_config["ca_cert"] = get_ca

    post_configs = None
    if post_cert and post_key:
        pc = {
            "client_cert": post_cert,
            "client_key": post_key,
            "verify": True,
        }
        if post_ca:
            pc["ca_cert"] = post_ca
        post_configs = [pc] * len(post_urls)

    do_relay(
        get_url=get_url,
        post_urls=list(post_urls),
        chunk_size=chunk_size,
        queue_maxsize=queue_maxsize,
        monitor_queues_flag=monitor,
        get_config=get_config,
        post_configs=post_configs,
        rate_limit=rate_limit,
        get_process_func=get_process_func,
        process_func=process_func,
        finalize_func=finalize_func,
    )


# ---------------------------------------------------------------------------
# lxd-copy command
# ---------------------------------------------------------------------------

@cli.command("lxd-copy")
@click.argument("project")
@click.argument("vm_name")
@click.argument("source_endpoint")
@click.argument("target_endpoint")
@click.option("--cert", required=True, type=click.Path(exists=True), help="Client certificate path.")
@click.option("--key", required=True, type=click.Path(exists=True), help="Client key path.")
@click.option("--ca", required=True, type=click.Path(exists=True), help="Source cluster CA cert.")
@click.option("--target-ca", required=True, type=click.Path(exists=True), help="Target cluster CA cert.")
@click.option("--target-project", required=True, help="Project name on the target cluster.")
@click.option("--chunk-size", default=1024 * 1024, show_default=True, help="Chunk size in bytes.")
@click.option("--queue-maxsize", default=128, show_default=True, help="Max queue depth.")
@click.option(
    "--rate-limit", type=float, default=None,
    help="Max input read rate in bytes/sec (e.g. 1048576 for 1 MB/s). 0 = unlimited.",
)
def lxd_copy(
    project,
    vm_name,
    source_endpoint,
    target_endpoint,
    cert,
    key,
    ca,
    target_ca,
    target_project,
    chunk_size,
    queue_maxsize,
    rate_limit,
):
    """Copy an LXD/Incus VM image from SOURCE_ENDPOINT to TARGET_ENDPOINT.

    Snapshots the VM, publishes it as an image, and streams it to the
    destination cluster without writing a temporary file to disk.
    """
    from .backup import do_copy

    do_copy(
        project=project,
        vm_name=vm_name,
        source_endpoint=source_endpoint,
        target_endpoint=target_endpoint,
        cert_path=cert,
        key_path=key,
        ca_path=ca,
        target_ca_path=target_ca,
        target_project=target_project,
        chunk_size=chunk_size,
        max_queue_size=queue_maxsize,
        rate_limit=rate_limit,
    )


# ---------------------------------------------------------------------------
# lxd-store command
# ---------------------------------------------------------------------------

@cli.command("lxd-store")
@click.argument("project")
@click.argument("vm_name")
@click.argument("source_endpoint")
@click.argument("sftp_url")
@click.option(
    "--passphrase",
    prompt=True,
    hide_input=True,
    confirmation_prompt=True,
    help="Passphrase for symmetric encryption.",
)
@click.option("--cert", required=True, type=click.Path(exists=True), help="Client certificate path.")
@click.option("--key", required=True, type=click.Path(exists=True), help="Client key path.")
@click.option("--ca", required=True, type=click.Path(exists=True), help="Cluster CA cert.")
@click.option("--no-compress", is_flag=True, default=False, help="Disable compression.")
@click.option("--no-encrypt", is_flag=True, default=False, help="Disable encryption.")
@click.option("--chunk-size", default=1024 * 1024, show_default=True, help="Chunk size in bytes.")
@click.option("--queue-maxsize", default=512, show_default=True, help="Max queue depth.")
@click.option(
    "--rate-limit", type=float, default=None,
    help="Max input read rate in bytes/sec (e.g. 1048576 for 1 MB/s). 0 = unlimited.",
)
def lxd_store(
    project,
    vm_name,
    source_endpoint,
    sftp_url,
    passphrase,
    cert,
    key,
    ca,
    no_compress,
    no_encrypt,
    chunk_size,
    queue_maxsize,
    rate_limit,
):
    """Compress, encrypt, and stream an LXD/Incus VM image to SFTP_URL.

    The passphrase is prompted interactively (not passed on the command line).
    """
    from .backup import do_store

    do_store(
        project=project,
        vm_name=vm_name,
        source_endpoint=source_endpoint,
        sftp_url=sftp_url,
        passphrase=passphrase,
        cert_path=cert,
        key_path=key,
        ca_path=ca,
        compress=not no_compress,
        encrypt=not no_encrypt,
        chunk_size=chunk_size,
        max_queue_size=queue_maxsize,
        rate_limit=rate_limit,
    )


# ---------------------------------------------------------------------------
# lxd-restore command
# ---------------------------------------------------------------------------

@cli.command("lxd-restore")
@click.argument("sftp_url")
@click.argument("target_endpoint")
@click.option(
    "--passphrase",
    prompt=True,
    hide_input=True,
    help="Passphrase for symmetric decryption.",
)
@click.option("--cert", required=True, type=click.Path(exists=True), help="Client certificate path.")
@click.option("--key", required=True, type=click.Path(exists=True), help="Client key path.")
@click.option("--ca", type=click.Path(exists=True), default=None, help="Target cluster CA cert.")
@click.option("--no-verify", is_flag=True, default=False, help="Disable TLS verification for target.")
@click.option("--no-decompress", is_flag=True, default=False, help="Disable decompression.")
@click.option("--no-decrypt", is_flag=True, default=False, help="Disable decryption.")
@click.option("--chunk-size", default=512 * 1024, show_default=True, help="Chunk size in bytes.")
@click.option("--queue-maxsize", default=20, show_default=True, help="Max queue depth.")
@click.option(
    "--rate-limit", type=float, default=None,
    help="Max input read rate in bytes/sec (e.g. 1048576 for 1 MB/s). 0 = unlimited.",
)
def lxd_restore(
    sftp_url,
    target_endpoint,
    passphrase,
    cert,
    key,
    ca,
    no_verify,
    no_decompress,
    no_decrypt,
    chunk_size,
    queue_maxsize,
    rate_limit,
):
    """Restore an LXD/Incus image from an encrypted SFTP backup at SFTP_URL
    to TARGET_ENDPOINT.

    The inverse of lxd-store: reads from SFTP, decrypts, decompresses,
    and streams to the Incus/LXD image import API.
    """
    from .backup import do_restore

    do_restore(
        sftp_url=sftp_url,
        target_endpoint=target_endpoint,
        passphrase=passphrase,
        cert_path=cert,
        key_path=key,
        ca_path=ca,
        verify_target=not no_verify,
        compress=not no_decompress,
        encrypt=not no_decrypt,
        chunk_size=chunk_size,
        max_queue_size=queue_maxsize,
        rate_limit=rate_limit,
    )


# ---------------------------------------------------------------------------
# lxd-store-all command
# ---------------------------------------------------------------------------

@cli.command("lxd-store-all")
@click.argument("source_endpoint")
@click.argument("target_url")
@click.option(
    "--passphrase",
    prompt=True,
    hide_input=True,
    confirmation_prompt=True,
    help="Passphrase for symmetric encryption.",
)
@click.option("--cert", required=True, type=click.Path(exists=True), help="Client certificate path.")
@click.option("--key", required=True, type=click.Path(exists=True), help="Client key path.")
@click.option("--ca", required=True, type=click.Path(exists=True), help="Cluster CA cert.")
@click.option("--project", default="default", show_default=True, help="LXD/Incus project name.")
@click.option("--no-compress", is_flag=True, default=False, help="Disable compression.")
@click.option("--no-encrypt", is_flag=True, default=False, help="Disable encryption.")
@click.option(
    "--type",
    "vm_type",
    type=click.Choice(["virtual-machine", "container"], case_sensitive=False),
    default=None,
    help="Filter by instance type.",
)
@click.option("--prefix", default=None, help="Only back up instances whose name starts with this string.")
@click.option("--contains", default=None, help="Only back up instances whose name contains this substring.")
@click.option(
    "--status",
    default=None,
    help="Only back up instances with this status (e.g. Running, Stopped).",
)
@click.option(
    "--exclude",
    multiple=True,
    help="Instance name(s) to skip (can be repeated).",
)
@click.option("--chunk-size", default=1024 * 1024, show_default=True, help="Chunk size in bytes.")
@click.option("--queue-maxsize", default=512, show_default=True, help="Max queue depth.")
@click.option(
    "--rate-limit", type=float, default=None,
    help="Max input read rate in bytes/sec (e.g. 1048576 for 1 MB/s). 0 = unlimited.",
)
def lxd_store_all(
    source_endpoint,
    target_url,
    passphrase,
    cert,
    key,
    ca,
    project,
    no_compress,
    no_encrypt,
    vm_type,
    prefix,
    contains,
    status,
    exclude,
    chunk_size,
    queue_maxsize,
    rate_limit,
):
    """Back up all matching VMs from SOURCE_ENDPOINT to TARGET_URL.

    Each VM is snapshotted, published as an image, compressed, encrypted,
    and streamed to TARGET_URL/<project>-<vm_name>-backup.img[.gz][.enc].

    Use the filter options to select which instances to back up.

    \b
    Examples:
      # Back up all VMs
      txwtf-tools lxd-store-all https://cluster:8443 sftp://user@host/backups \\
        --cert client.crt --key client.key --ca ca.pem

      # Only running containers with name starting with "web-"
      txwtf-tools lxd-store-all https://cluster:8443 sftp://user@host/backups \\
        --cert client.crt --key client.key --ca ca.pem \\
        --type container --prefix web- --status Running

      # Exclude specific VMs
      txwtf-tools lxd-store-all https://cluster:8443 sftp://user@host/backups \\
        --cert client.crt --key client.key --ca ca.pem \\
        --exclude temp-vm --exclude test-vm
    """
    from .backup import do_store_all

    results = do_store_all(
        endpoint=source_endpoint,
        target_url=target_url,
        cert_path=cert,
        key_path=key,
        ca_path=ca,
        passphrase=passphrase,
        project=project,
        compress=not no_compress,
        encrypt=not no_encrypt,
        vm_type=vm_type,
        name_prefix=prefix,
        name_contains=contains,
        status=status,
        exclude=list(exclude) if exclude else None,
        chunk_size=chunk_size,
        max_queue_size=queue_maxsize,
        rate_limit=rate_limit,
    )

    failed = sum(1 for v in results.values() if v != "ok")
    raise SystemExit(1 if failed else 0)
