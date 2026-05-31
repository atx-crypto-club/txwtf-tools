# txwtf-tools

Stream relay and LXD/Incus backup migration tools — pipe data between HTTP(S), SFTP, and file streams without writing temporary files to disk.

## Features

- **Stream Relay** — relay data from any source (HTTP/HTTPS/SFTP/file) to one or more destinations with backpressure, progress bars, and optional per-chunk transforms (compression, encryption, etc.)
- **LXD/Incus Copy** — snapshot a VM, publish it as an image, and stream it directly to another cluster
- **LXD/Incus Store** — compress + encrypt a VM image and stream it to SFTP storage
- **Fan-out** — stream a single input to multiple outputs simultaneously
- **CLI** — `txwtf-tools` command with `relay`, `lxd-copy`, and `lxd-store` subcommands

## Installation

```bash
pip install txwtf-tools

# With LXD/Incus support (requires pylxd):
pip install txwtf-tools[lxd]
```

### Development

```bash
poetry install --all-extras
```

## Usage

### Relay a stream

```bash
# file to file
txwtf-tools relay file:///path/to/input.bin file:///path/to/output.bin

# HTTP to HTTP (fan-out to two destinations)
txwtf-tools relay https://source/data https://dest1/upload https://dest2/upload \
  --get-cert client.crt --get-key client.key --get-ca ca.pem \
  --post-cert client.crt --post-key client.key --post-ca ca.pem

# HTTP to file
txwtf-tools relay http://source:8080/data file:///local/copy.bin --chunk-size 2097152
```

### Copy LXD/Incus VM between clusters

```bash
txwtf-tools lxd-copy myproject my-vm \
  https://cluster-a:8443 https://cluster-b:8443 \
  --cert client.crt --key client.key \
  --ca cluster-a-ca.pem --target-ca cluster-b-ca.pem \
  --target-project myproject
```

### Store encrypted backup to SFTP

```bash
txwtf-tools lxd-store myproject my-vm \
  https://cluster-a:8443 \
  sftp://user@backup-host:22/backups/my-vm.img.gz.enc \
  --cert client.crt --key client.key --ca ca.pem
```

## Testing

```bash
# Unit tests only
poetry run pytest tests/ -m "not integration" -v

# Integration tests (spins up HTTP servers in subprocesses)
poetry run pytest tests/ -m integration -v

# All tests
poetry run pytest tests/ -v
```

## License

MIT
