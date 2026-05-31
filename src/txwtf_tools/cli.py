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
):
    """Relay a stream from GET_URL to one or more POST_URLS.

    Supports http(s), sftp, and file:// schemes on both sides.
    """
    from .relay import relay as do_relay

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
    )
