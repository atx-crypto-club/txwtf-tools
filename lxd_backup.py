import pylxd
from pylxd import exceptions
import argparse
import os
import tempfile
import requests
from tqdm import tqdm
from urllib.parse import urlparse, unquote
import zlib
import base64
import hashlib
import time

from cryptography.fernet import Fernet

from lxd_backup_async import streamer, create_ssl_connector
from relay_stream import relay


def get_fixed_base64_from_utf8_string(input_string: str) -> str:
    """
    Given a UTF-8 string, encodes it to bytes, hashes with SHA-256 to get exactly 32 bytes.
    Then, returns the 32 bytes encoded as a URL-safe base64 string (with padding = if applicable).

    This ensures the output is always a fixed 32-byte value's base64 encoding, reliable for use as an encoded key representation,
    without losing information from long strings via truncation—hashing incorporates the entire input.
    """
    # Encode the full string to bytes (no truncation needed)
    key_bytes = input_string.encode('utf-8')
    
    # Hash to get exactly 32 bytes
    hashed_bytes = hashlib.sha256(key_bytes).digest()
    
    # URL-safe base64 encode (includes == padding for 32 bytes; if you want no padding, add .rstrip(b'=='))
    base64_encoded = base64.urlsafe_b64encode(hashed_bytes).decode('ascii')
    
    return base64_encoded


def prepare_backup(args):
    source_client = pylxd.Client(
        project=args.project,
        endpoint=args.source_endpoint,
        cert=(args.cert_path, args.key_path),
        verify=args.ca_path
    )

    try:
        instance = source_client.instances.get(args.vm_name)
    except pylxd.exceptions.NotFound:
        print(f"VM '{args.vm_name}' not found in project '{args.project}' on cluster A.")
        return None, None, None

    snapshot_name = "temp_backup3"
    try:
        snapshot = instance.snapshots.get(snapshot_name)
        print(f"Snapshot '{snapshot_name}' exists for instance '{args.vm_name}'.")
        # You can access snapshot details like creation date:
        print(f"Created at: {snapshot.created_at}") 
        print(f"Snapshot '{snapshot_name}' already exists. Deleting it.")
        snapshot.delete()
    except exceptions.NotFound:
        print(f"Snapshot '{snapshot_name}' does not exist for instance '{args.vm_name}'.")

    print(f"Creating snapshot '{snapshot_name}' of VM '{args.vm_name}'.")
    snapshot = instance.snapshots.create(snapshot_name, stateful=False, wait=True)

    image_alias = f"{args.project}-{args.vm_name}-backup"
    print(f"Publishing snapshot to image with alias '{image_alias}'.")
    image = snapshot.publish(wait=True)
    try:
        image.add_alias(image_alias, f"backup for {args.vm_name}")
    except pylxd.exceptions.Conflict:
        image.delete_alias(image_alias)
        image.add_alias(image_alias, f"backup for {args.vm_name}")

    print(f"Image fingerprint: {image.fingerprint}")

    return image, snapshot, image_alias


def cleanup_backup(image, snapshot):
    #print("Cleaning up temporary image and snapshot")
    #if image:
    #    image.delete()
    if snapshot:
        snapshot.delete()


def do_copy(args):
    image, snapshot, image_alias = prepare_backup(args)
    if not image:
        return

    try:
         # Export and stream with compression and encryption
        print(f"Streaming image export from {args.source_endpoint}")
        export_url = f"{args.source_endpoint}/1.0/images/{image.fingerprint}/export"
        import_url = f"{args.target_endpoint}/1.0/images"

        pairs = [
            {
                'input_uri': export_url,
                'output_uris': [import_url],
                'input_kwargs': {
                    'chunk_size': args.chunk_size,
                    'cert_file': args.cert_path,
                    'key_file': args.key_path,
                    'ca_file': args.ca_path,
                    'http_kwargs': {
                        'ssl': True,  # TODO: make a flag for this
                        'headers': {'User-Agent': 'datacourier 0.1.0'},
                        'allow_redirects': True,  # TODO: make a flag for this
                    },
                },
                'output_kwargs_list': [
                    {
                        'max_queue_size': args.max_queue_size,
                        'cert_file': args.cert_path,
                        'key_file': args.key_path,
                        'ca_file': args.target_ca_path,
                        'http_kwargs': {
                            'ssl': True,  # TODO: make a flag for this
                            'headers': {
                                'User-Agent': 'datacourier 0.1.0',
                                'Content-Type': 'application/octet-stream',
                                'X-LXD-public': 'false',
                            },
                            'allow_redirects': True,  # TODO: make a flag for this
                        },
                    }
                ]
            }
        ]

        streamer(pairs)

        target_client = pylxd.Client(
            project=args.target_project,
            endpoint=args.target_endpoint,
            cert=(args.cert_path, args.key_path),
            verify=args.target_ca_path)
        # Create a new alias for the image
        while True:
            # poll until it is available
            try:
                target_image = target_client.images.get(image.fingerprint)
                break
            except pylxd.exceptions.NotFound:
                time.sleep(1.0)
        target_image.add_alias(
            name=image_alias,
            description="backup")

    finally:
        cleanup_backup(image, snapshot)


def do_copy2(args):
    image, snapshot, image_alias = prepare_backup(args)
    if not image:
        return

    try:
         # Export and stream with compression and encryption
        print(f"Streaming image export from {args.source_endpoint}")
        export_url = f"{args.source_endpoint}/1.0/images/{image.fingerprint}/export"
        import_url = f"{args.target_endpoint}/1.0/images"

        relay_args = {
            "queue_maxsize": 128,
            "chunk_size": args.chunk_size,
            "get_url": export_url,
            "get_headers": {'User-Agent': 'datacourier 0.1.0'},
            "get_config": {
                'client_cert': args.cert_path,
                'client_key': args.key_path,
                'ca_cert': args.ca_path,
                'verify': True,
                # TODO: pass this and headers as part of kwargs for get
                #'allow_redirects': True,  # TODO: make a flag for this
            },
            "post_urls": [import_url],
            "post_headers": [
                {
                    'User-Agent': 'datacourier 0.1.0',
                    'Content-Type': 'application/octet-stream',
                    'X-LXD-public': 'false',
                }
            ],
            "monitor_queues_flag": True,
            "post_configs": [
                {
                    'client_cert': args.cert_path,
                    'client_key': args.key_path,
                    'ca_cert': args.target_ca_path,
                    'verify': True,
                }
            ]
        }

        relay(**relay_args)

        target_client = pylxd.Client(
            project=args.target_project,
            endpoint=args.target_endpoint,
            cert=(args.cert_path, args.key_path),
            verify=args.target_ca_path)
        # Create a new alias for the image
        while True:
            # poll until it is available
            try:
                target_image = target_client.images.get(image.fingerprint)
                break
            except pylxd.exceptions.NotFound:
                time.sleep(1.0)
        target_image.add_alias(
            name=image_alias,
            description="backup")

    finally:
        cleanup_backup(image, snapshot)


# Function to chain multiple byte-processing functions
def chain_functions(*funcs):
    """
    Chains multiple functions that each take and return bytes.
    
    Returns a new function that applies each function in sequence.
    """
    def chained(input_chunk: bytes) -> bytes:
        current = input_chunk
        for func in funcs:
            current = func(current)
        return current
    return chained


def do_store(args):
    image, snapshot, image_alias = prepare_backup(args)
    if not image:
        return

    try:
        compressor = zlib.compressobj(9, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
        encryptor = Fernet(get_fixed_base64_from_utf8_string(args.passphrase))

        def pass_chunk(chunk: bytes) -> bytes:
            return chunk

        def compress_chunk(chunk: bytes) -> bytes:
            if chunk:
                return compressor.compress(chunk)
        
        def encrypt_chunk(chunk: bytes) -> bytes:
            if chunk:
                return encryptor.encrypt(chunk)
            
        chunk_chain = [pass_chunk]
        if args.compress:
            chunk_chain.append(compress_chunk)
        if args.encrypt:
            chunk_chain.append(encrypt_chunk)

        process_chunk = chain_functions(*chunk_chain)
        
        def flush_process():
            # Flush compressor and encryptor
            compressed_flush = compressor.flush(zlib.Z_FINISH)
            if compressed_flush:
                return encryptor.encrypt(compressed_flush)
            return compressed_flush
                
        # Export and stream with compression and encryption
        print(f"Streaming image export from {args.source_endpoint}, compressing, encrypting, and uploading to SFTP with progress.")
        export_url = f"{args.source_endpoint}/1.0/images/{image.fingerprint}/export"

        pairs = [
            {  # HTTP input to multiple outputs: HTTP (unmodified) and SFTP (processed via callback)
                'input_uri': export_url,
                'output_uris': [
                    args.sftp_url # SFTP: with callback and finalize
                ],
                'input_kwargs': {
                    'chunk_size': args.chunk_size,
                    'cert_file': args.cert_path,  # Optional client cert for input GET
                    'key_file': args.key_path,
                    'ca_file': args.ca_path,
                    'http_kwargs': {
                        'ssl': True,  # TODO: make a flag for this
                        'headers': {'User-Agent': 'datacourier 0.1.0'},
                        'allow_redirects': True,  # TODO: make a flag for this
                    },
                },
                'output_kwargs_list': [
                    {
                        'max_queue_size': args.max_queue_size,
                        'callback': process_chunk,  # Example callback for this output
                        'finalize_callback': flush_process  # Example finalize callback, e.g., flush buffer
                    }
                ]
            }
        ]

        streamer(pairs)

    finally:
        cleanup_backup(image, snapshot)


def main():
    parser = argparse.ArgumentParser(description="Backup LXD VM with different operations")
    subparsers = parser.add_subparsers(dest="operation", required=True, help="Operation to perform: copy or store")

    # Copy subparser
    copy_parser = subparsers.add_parser("copy", help="Copy backup to cluster B")
    copy_parser.add_argument("project", help="Project name on cluster A")
    copy_parser.add_argument("vm_name", help="VM name to backup")
    copy_parser.add_argument("source_endpoint", help="Endpoint for cluster A, e.g., https://catx-00.aus.tx:8443")
    copy_parser.add_argument("target_endpoint", help="Endpoint for cluster B, e.g., https://catx-03.aus.tx:8443")
    copy_parser.add_argument("cert_path", help="Path to client certificate file")
    copy_parser.add_argument("key_path", help="Path to client key file")
    copy_parser.add_argument("ca_path", help="Path to source server key file")
    copy_parser.add_argument("target_ca_path", help="Path to target server key file")
    copy_parser.add_argument("target_project", help="Target project for the image")

    # Store subparser
    store_parser = subparsers.add_parser("store", help="Store encrypted backup to SFTP")
    store_parser.add_argument("project", help="Project name on cluster A")
    store_parser.add_argument("vm_name", help="VM name to backup")
    store_parser.add_argument("source_endpoint", help="Endpoint for cluster A, e.g., https://catx-00.aus.tx:8443")
    store_parser.add_argument("sftp_url", help="SFTP URL, e.g., sftp://user:pass@host:port/path/to/backup.img.gz.enc")
    store_parser.add_argument("passphrase", help="Passphrase for symmetric encryption")
    store_parser.add_argument("cert_path", help="Path to client certificate file")
    store_parser.add_argument("key_path", help="Path to client key file")
    store_parser.add_argument("ca_path", help="Path to server key file")

    args = parser.parse_args()

    args.compress = True
    args.encrypt = True
    args.chunk_size = 1024 * 1024
    args.max_queue_size = 512

    if args.operation == "copy":
        do_copy2(args)
    elif args.operation == "store":
        do_store(args)


if __name__ == "__main__":
    main()