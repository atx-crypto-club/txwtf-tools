# txwtf-tools

Stream relay and LXD/Incus backup migration tools — pipe data between HTTP(S), SFTP, and local file streams without writing temporary files to disk.

## Features

- **Stream Relay** — relay data from any source (HTTP/HTTPS/SFTP/file) to one or more destinations with backpressure, progress bars, and optional transforms
- **Compression** — gzip compress on output, decompress on input, at the relay level
- **Encryption** — Fernet symmetric encrypt on output, decrypt on input, with independent passphrases for each direction (supports re-encryption in a single pass)
- **Fan-out** — stream a single input to multiple outputs simultaneously; a slow consumer cannot block faster ones
- **Rate limiting** — throttle input read rate to a target bytes/sec
- **LXD/Incus Copy** — snapshot a VM, publish it as an image, and stream it directly to another cluster
- **LXD/Incus Store** — compress + encrypt a VM image and stream it to SFTP storage
- **LXD/Incus Store All** — batch backup all (or filtered) VMs from a cluster with nested progress bars
- **LXD/Incus Restore** — read an encrypted backup from SFTP, decrypt, decompress, and import it back into a cluster
- **No temp files** — everything streams end-to-end; no intermediate files touch disk

## Installation

```bash
pip install txwtf-tools

# With LXD/Incus support (requires pylxd):
pip install txwtf-tools[lxd]
```

### Development

```bash
git clone https://github.com/atx-crypto-club/txwtf-tools.git
cd txwtf-tools
poetry install --all-extras
```

## Quick start

### Relay a file

```bash
# Copy a local file
txwtf-tools relay file:///data/input.bin file:///data/output.bin

# HTTP source to local file
txwtf-tools relay http://server:8080/export file:///local/backup.bin
```

### Compress a file

```bash
# Compress on output
txwtf-tools relay file:///data/large.bin file:///data/large.bin.gz --compress

# Decompress on input
txwtf-tools relay file:///data/large.bin.gz file:///data/large.bin --decompress
```

### Encrypt a file

```bash
# Encrypt on output (passphrase is hidden on input)
txwtf-tools relay file:///data/secret.bin file:///data/secret.bin.enc \
  --encrypt-passphrase "my-passphrase"

# Decrypt on input
txwtf-tools relay file:///data/secret.bin.enc file:///data/secret.bin \
  --decrypt-passphrase "my-passphrase"
```

### Compress + encrypt

Transforms chain automatically: compress then encrypt on output, decrypt then decompress on input.

```bash
# Store: compress + encrypt → SFTP
txwtf-tools relay file:///data/database.dump \
  sftp://user@backup-host/backups/database.dump.gz.enc \
  --compress --encrypt-passphrase "my-passphrase"

# Restore: SFTP → decrypt + decompress
txwtf-tools relay sftp://user@backup-host/backups/database.dump.gz.enc \
  file:///data/database.dump \
  --decompress --decrypt-passphrase "my-passphrase"
```

### Fan-out to multiple destinations

Stream a single source to multiple outputs simultaneously. Each destination gets its own bounded queue — a slow consumer cannot starve faster ones.

```bash
# Back up to two locations at once
txwtf-tools relay file:///data/important.bin \
  file:///mnt/usb/important.bin \
  sftp://user@offsite/backups/important.bin

# Fan-out with compression + encryption
txwtf-tools relay file:///data/important.bin \
  file:///mnt/usb/important.bin.gz.enc \
  sftp://user@offsite/backups/important.bin.gz.enc \
  --compress --encrypt-passphrase "my-passphrase"
```

### Re-encrypt with a different key

Decrypt the input with one key and re-encrypt the output with another — in a single streaming pass, no temp files.

```bash
txwtf-tools relay file:///data/old-encrypted.bin file:///data/new-encrypted.bin \
  --decrypt-passphrase "old-key" \
  --encrypt-passphrase "new-key"
```

### Rate limiting

Throttle input read rate to avoid saturating a network link.

```bash
# Limit to 10 MB/s
txwtf-tools relay http://source/data sftp://user@dest/data \
  --rate-limit 10485760

# Limit to 1 MB/s with compression
txwtf-tools relay file:///data/large.bin file:///data/large.bin.gz \
  --compress --rate-limit 1048576
```

### HTTPS with client certificates

```bash
txwtf-tools relay https://source:8443/export https://dest:8443/import \
  --get-cert client.crt --get-key client.key --get-ca source-ca.pem \
  --post-cert client.crt --post-key client.key --post-ca dest-ca.pem
```

---

## LXD / Incus VM backup commands

These commands automate the full VM backup lifecycle: snapshot → publish → stream. They require TLS client certificates trusted by the LXD/Incus cluster.

### Setting up credentials

#### Incus

Incus stores client credentials in `~/.config/incus/`:

```bash
# Generate a client certificate (if you don't have one)
incus remote add my-cluster https://cluster-host:8443

# This creates:
#   ~/.config/incus/client.crt   — client certificate
#   ~/.config/incus/client.key   — client private key

# Trust the client cert on the cluster (run on a cluster member)
incus config trust add client.crt

# Verify connectivity
curl --cert ~/.config/incus/client.crt \
     --key ~/.config/incus/client.key \
     -k https://cluster-host:8443/1.0
```

#### LXD

LXD stores client credentials in `~/snap/lxd/common/config/` (snap) or `~/.config/lxc/`:

```bash
# Generate and trust a client certificate
lxc remote add my-cluster https://cluster-host:8443

# This creates:
#   ~/snap/lxd/common/config/client.crt
#   ~/snap/lxd/common/config/client.key

# Verify connectivity
curl --cert ~/snap/lxd/common/config/client.crt \
     --key ~/snap/lxd/common/config/client.key \
     -k https://cluster-host:8443/1.0
```

#### Generating a standalone certificate

For CI or automated use, generate a dedicated certificate:

```bash
# Generate an EC key + self-signed cert
openssl ecparam -genkey -name secp384r1 -out client.key
openssl req -new -x509 -key client.key -out client.crt -days 3650 \
  -subj "/CN=txwtf-tools-ci"

# Trust it on the cluster
incus config trust add client.crt   # Incus
lxc config trust add client.crt     # LXD
```

### Copy a VM image between clusters

Snapshots the VM, publishes it as an image, and streams it directly to the target cluster — no temp file on disk.

```bash
txwtf-tools lxd-copy myproject my-vm \
  https://cluster-a:8443 https://cluster-b:8443 \
  --cert ~/.config/incus/client.crt \
  --key ~/.config/incus/client.key \
  --ca cluster-a-ca.pem \
  --target-ca cluster-b-ca.pem \
  --target-project myproject
```

### Store an encrypted backup to SFTP

Snapshots the VM, publishes it as an image, compresses (gzip) and encrypts (Fernet), and streams it to SFTP. The passphrase is prompted interactively.

```bash
txwtf-tools lxd-store myproject my-vm \
  https://cluster:8443 \
  sftp://user@backup-host/backups/my-vm.img.gz.enc \
  --cert ~/.config/incus/client.crt \
  --key ~/.config/incus/client.key \
  --ca ca.pem
```

Options:
- `--no-compress` — skip gzip compression
- `--no-encrypt` — skip encryption (store plaintext)
- `--rate-limit 5242880` — throttle to 5 MB/s

### Restore from an encrypted SFTP backup

Reads the encrypted backup from SFTP, decrypts, decompresses, and streams it to the cluster's image import API.

```bash
txwtf-tools lxd-restore \
  sftp://user@backup-host/backups/my-vm.img.gz.enc \
  https://cluster:8443 \
  --cert ~/.config/incus/client.crt \
  --key ~/.config/incus/client.key \
  --no-verify
```

Options:
- `--no-decrypt` — input is not encrypted
- `--no-decompress` — input is not compressed
- `--ca ca.pem` — verify the target cluster's TLS cert
- `--no-verify` — skip TLS verification (e.g. when the cluster cert SAN doesn't match the IP)

### Store and restore round-trip

```bash
# Store
txwtf-tools lxd-store default my-vm https://10.0.0.1:8443 \
  sftp://tfx@backup-host/backups/my-vm.enc \
  --cert client.crt --key client.key --ca ca.pem

# Restore (on same or different cluster)
txwtf-tools lxd-restore \
  sftp://tfx@backup-host/backups/my-vm.enc \
  https://10.0.0.1:8443 \
  --cert client.crt --key client.key --no-verify
```

### Batch backup all VMs

Back up every VM (or a filtered subset) from a cluster in a single command. An outer progress bar tracks VM count while inner bars show per-VM streaming throughput.

```bash
# Back up all instances
txwtf-tools lxd-store-all https://cluster:8443 \
  sftp://user@backup-host/backups \
  --cert client.crt --key client.key --ca ca.pem

# Only running containers whose name starts with "web-"
txwtf-tools lxd-store-all https://cluster:8443 \
  sftp://user@backup-host/backups \
  --cert client.crt --key client.key --ca ca.pem \
  --type container --prefix web- --status Running

# Exclude specific VMs, use a different project
txwtf-tools lxd-store-all https://cluster:8443 \
  sftp://user@backup-host/backups \
  --cert client.crt --key client.key --ca ca.pem \
  --project production --exclude temp-vm --exclude test-vm

# Uncompressed, unencrypted
txwtf-tools lxd-store-all https://cluster:8443 \
  sftp://user@backup-host/backups \
  --cert client.crt --key client.key --ca ca.pem \
  --no-compress --no-encrypt
```

Filter options:
- `--type virtual-machine` or `--type container` — filter by instance type
- `--prefix web-` — only instances whose name starts with the given string
- `--contains prod` — only instances whose name contains the substring
- `--status Running` — only instances with this status (Running, Stopped, etc.)
- `--exclude NAME` — skip specific instances (can be repeated)
- `--project NAME` — LXD/Incus project (default: `default`)

Each backup is stored at `<target_url>/<project>-<name>-backup.img[.gz][.enc]`.

---

## Python API

The relay and transform functions are available as a library:

```python
from txwtf_tools.relay import relay, relay_stream
from txwtf_tools.backup import (
    make_encrypt_func,
    make_decrypt_func,
    make_compress_func,
    make_decompress_func,
    chain_functions,
)

# Simple file relay
relay(get_url="file:///src.bin", post_urls=["file:///dst.bin"])

# Compress + encrypt → file, with finalize to flush zlib
compress, finalize = make_compress_func()
encrypt = make_encrypt_func("my-passphrase")

def store_process(chunk):
    compressed = compress(chunk)
    return encrypt(compressed) if compressed else b""

def store_finalize():
    flushed = finalize()
    return encrypt(flushed) if flushed else b""

relay(
    get_url="file:///data.bin",
    post_urls=["file:///data.bin.gz.enc"],
    process_func=store_process,
    finalize_func=store_finalize,
)

# Decrypt + decompress from file
restore_func = chain_functions(
    make_decrypt_func("my-passphrase"),
    make_decompress_func(),
)
relay(
    get_url="file:///data.bin.gz.enc",
    post_urls=["file:///data.bin"],
    get_process_func=restore_func,
)
```

### Transform hooks

| Parameter | Side | Purpose |
|-----------|------|---------|
| `get_process_func` | Input | Applied to each chunk after reading, before queuing. Use for decrypt / decompress. |
| `process_func` | Output | Applied to each chunk after dequeuing, before writing. Use for encrypt / compress. |
| `finalize_func` | Output | Called once after the last chunk to flush buffered state (e.g. zlib trailer). |

## Environment variables

All commonly reused options can be set via `TXWTF_*` environment variables, useful for CI/automation and cron jobs. CLI flags always override env vars.

| Variable | Commands | Purpose |
|----------|----------|---------|
| `TXWTF_CERT` | `lxd-copy`, `lxd-store`, `lxd-restore`, `lxd-store-all` | Client certificate path |
| `TXWTF_KEY` | `lxd-copy`, `lxd-store`, `lxd-restore`, `lxd-store-all` | Client key path |
| `TXWTF_CA` | `lxd-copy`, `lxd-store`, `lxd-restore`, `lxd-store-all` | Cluster CA certificate |
| `TXWTF_PASSPHRASE` | `lxd-store`, `lxd-restore`, `lxd-store-all` | Encryption/decryption passphrase (skips interactive prompt) |
| `TXWTF_PROJECT` | `lxd-store-all` | LXD/Incus project name |
| `TXWTF_RATE_LIMIT` | all commands | Max input read rate (bytes/sec) |
| `TXWTF_TARGET_CA` | `lxd-copy` | Target cluster CA certificate |
| `TXWTF_TARGET_PROJECT` | `lxd-copy` | Target cluster project name |
| `TXWTF_GET_CERT`, `TXWTF_GET_KEY`, `TXWTF_GET_CA` | `relay` | Input-side TLS client certs |
| `TXWTF_POST_CERT`, `TXWTF_POST_KEY`, `TXWTF_POST_CA` | `relay` | Output-side TLS client certs |
| `TXWTF_ENCRYPT_PASSPHRASE` | `relay` | Output encryption passphrase |
| `TXWTF_DECRYPT_PASSPHRASE` | `relay` | Input decryption passphrase |

Example — automated nightly backup via cron:

```bash
export TXWTF_CERT=~/.config/incus/client.crt
export TXWTF_KEY=~/.config/incus/client.key
export TXWTF_CA=/etc/incus/ca.pem
export TXWTF_PASSPHRASE="my-backup-secret"

# No interactive prompts needed
txwtf-tools lxd-store-all https://cluster:8443 \
  sftp://backup-user@nas/backups/nightly
```

## Testing

```bash
# Unit tests only
poetry run pytest tests/ -m "not integration and not live" -v

# Integration tests (HTTP servers in subprocesses)
poetry run pytest tests/ -m integration -v

# All tests (excluding live cluster tests)
poetry run pytest tests/ -m "not live" -v

# Live backup round-trip (requires Incus cluster + SFTP)
poetry run pytest tests/ -m live -v
```

## License

MIT
