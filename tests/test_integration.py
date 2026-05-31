"""
Integration tests — multi-process HTTP(S) relay end-to-end.

These spin up lightweight aiohttp servers in subprocesses and use the relay
to stream data between them, proving the full pipeline works over real TCP.
"""

import asyncio
import multiprocessing
import os
import ssl
import tempfile
import time
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Helpers to generate self-signed certs for HTTPS tests
# ---------------------------------------------------------------------------

def _generate_self_signed_cert(cert_path: str, key_path: str):
    """Generate a self-signed certificate and key using the cryptography library."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    import datetime

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
    ])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([
                x509.DNSName("localhost"),
                x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
            ]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )

    with open(key_path, "wb") as f:
        f.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ))

    with open(cert_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))


import ipaddress  # noqa: E402 — needed by _generate_self_signed_cert


# ---------------------------------------------------------------------------
# Simple aiohttp servers used as fixtures
# ---------------------------------------------------------------------------

def _run_http_source_server(host, port, data_path, started_event, ssl_cert=None, ssl_key=None):
    """Serve a file via GET on /data. Runs in a subprocess."""
    import aiohttp.web

    async def handle_get(request):
        data = Path(data_path).read_bytes()
        return aiohttp.web.Response(
            body=data,
            content_type="application/octet-stream",
            headers={"Content-Length": str(len(data))},
        )

    app = aiohttp.web.Application()
    app.router.add_get("/data", handle_get)

    ssl_ctx = None
    if ssl_cert and ssl_key:
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_ctx.load_cert_chain(ssl_cert, ssl_key)

    runner = aiohttp.web.AppRunner(app)

    async def start():
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, host, port, ssl_context=ssl_ctx)
        await site.start()
        started_event.set()
        # run forever
        while True:
            await asyncio.sleep(3600)

    asyncio.run(start())


def _run_http_sink_server(host, port, output_path, started_event, ssl_cert=None, ssl_key=None):
    """Accept POST on /upload, write body to output_path. Runs in a subprocess."""
    import aiohttp.web

    async def handle_post(request):
        data = await request.read()
        Path(output_path).write_bytes(data)
        return aiohttp.web.json_response({"status": "ok"})

    app = aiohttp.web.Application()
    app.router.add_post("/upload", handle_post)

    ssl_ctx = None
    if ssl_cert and ssl_key:
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_ctx.load_cert_chain(ssl_cert, ssl_key)

    runner = aiohttp.web.AppRunner(app)

    async def start():
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, host, port, ssl_context=ssl_ctx)
        await site.start()
        started_event.set()
        while True:
            await asyncio.sleep(3600)

    asyncio.run(start())


def _wait_for_event(event, timeout=10):
    """Wait for a multiprocessing Event with timeout."""
    deadline = time.monotonic() + timeout
    while not event.is_set():
        if time.monotonic() > deadline:
            raise TimeoutError("Server did not start in time")
        time.sleep(0.1)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def integration_dir(tmp_path):
    return tmp_path


@pytest.fixture
def certs(integration_dir):
    cert = str(integration_dir / "server.crt")
    key = str(integration_dir / "server.key")
    _generate_self_signed_cert(cert, key)
    return cert, key


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestHTTPRelay:
    """Plain HTTP relay: source server -> relay -> sink server."""

    def test_http_to_http(self, integration_dir):
        src_file = integration_dir / "source.bin"
        dst_file = integration_dir / "dest.bin"
        data = os.urandom(32 * 1024)  # 32 KB
        src_file.write_bytes(data)

        src_started = multiprocessing.Event()
        sink_started = multiprocessing.Event()

        src_proc = multiprocessing.Process(
            target=_run_http_source_server,
            args=("127.0.0.1", 18091, str(src_file), src_started),
            daemon=True,
        )
        sink_proc = multiprocessing.Process(
            target=_run_http_sink_server,
            args=("127.0.0.1", 18092, str(dst_file), sink_started),
            daemon=True,
        )
        src_proc.start()
        sink_proc.start()

        try:
            _wait_for_event(src_started)
            _wait_for_event(sink_started)

            from txwtf_tools.relay import relay

            relay(
                get_url="http://127.0.0.1:18091/data",
                post_urls=["http://127.0.0.1:18092/upload"],
                chunk_size=4096,
                queue_maxsize=8,
                get_config={"verify": False},
                post_configs=[{"verify": False}],
            )

            assert dst_file.read_bytes() == data
        finally:
            src_proc.terminate()
            sink_proc.terminate()
            src_proc.join(timeout=5)
            sink_proc.join(timeout=5)

    def test_http_fanout(self, integration_dir):
        src_file = integration_dir / "source.bin"
        dst1 = integration_dir / "dest1.bin"
        dst2 = integration_dir / "dest2.bin"
        data = os.urandom(16 * 1024)
        src_file.write_bytes(data)

        src_started = multiprocessing.Event()
        sink1_started = multiprocessing.Event()
        sink2_started = multiprocessing.Event()

        src_proc = multiprocessing.Process(
            target=_run_http_source_server,
            args=("127.0.0.1", 18093, str(src_file), src_started),
            daemon=True,
        )
        sink1_proc = multiprocessing.Process(
            target=_run_http_sink_server,
            args=("127.0.0.1", 18094, str(dst1), sink1_started),
            daemon=True,
        )
        sink2_proc = multiprocessing.Process(
            target=_run_http_sink_server,
            args=("127.0.0.1", 18095, str(dst2), sink2_started),
            daemon=True,
        )
        src_proc.start()
        sink1_proc.start()
        sink2_proc.start()

        try:
            _wait_for_event(src_started)
            _wait_for_event(sink1_started)
            _wait_for_event(sink2_started)

            from txwtf_tools.relay import relay

            relay(
                get_url="http://127.0.0.1:18093/data",
                post_urls=[
                    "http://127.0.0.1:18094/upload",
                    "http://127.0.0.1:18095/upload",
                ],
                chunk_size=4096,
                queue_maxsize=8,
                get_config={"verify": False},
                post_configs=[{"verify": False}, {"verify": False}],
            )

            assert dst1.read_bytes() == data
            assert dst2.read_bytes() == data
        finally:
            src_proc.terminate()
            sink1_proc.terminate()
            sink2_proc.terminate()
            src_proc.join(timeout=5)
            sink1_proc.join(timeout=5)
            sink2_proc.join(timeout=5)


@pytest.mark.integration
class TestHTTPSRelay:
    """HTTPS relay with self-signed certificates."""

    def test_https_to_https(self, integration_dir, certs):
        cert, key = certs
        src_file = integration_dir / "source.bin"
        dst_file = integration_dir / "dest.bin"
        data = os.urandom(24 * 1024)
        src_file.write_bytes(data)

        src_started = multiprocessing.Event()
        sink_started = multiprocessing.Event()

        src_proc = multiprocessing.Process(
            target=_run_http_source_server,
            args=("127.0.0.1", 18096, str(src_file), src_started, cert, key),
            daemon=True,
        )
        sink_proc = multiprocessing.Process(
            target=_run_http_sink_server,
            args=("127.0.0.1", 18097, str(dst_file), sink_started, cert, key),
            daemon=True,
        )
        src_proc.start()
        sink_proc.start()

        try:
            _wait_for_event(src_started)
            _wait_for_event(sink_started)

            from txwtf_tools.relay import relay

            relay(
                get_url="https://127.0.0.1:18096/data",
                post_urls=["https://127.0.0.1:18097/upload"],
                chunk_size=4096,
                queue_maxsize=8,
                get_config={"verify": False},
                post_configs=[{"verify": False}],
            )

            assert dst_file.read_bytes() == data
        finally:
            src_proc.terminate()
            sink_proc.terminate()
            src_proc.join(timeout=5)
            sink_proc.join(timeout=5)

    def test_https_with_ca_verification(self, integration_dir, certs):
        cert, key = certs
        src_file = integration_dir / "source.bin"
        dst_file = integration_dir / "dest.bin"
        data = os.urandom(12 * 1024)
        src_file.write_bytes(data)

        src_started = multiprocessing.Event()
        sink_started = multiprocessing.Event()

        src_proc = multiprocessing.Process(
            target=_run_http_source_server,
            args=("127.0.0.1", 18098, str(src_file), src_started, cert, key),
            daemon=True,
        )
        sink_proc = multiprocessing.Process(
            target=_run_http_sink_server,
            args=("127.0.0.1", 18099, str(dst_file), sink_started, cert, key),
            daemon=True,
        )
        src_proc.start()
        sink_proc.start()

        try:
            _wait_for_event(src_started)
            _wait_for_event(sink_started)

            from txwtf_tools.relay import relay

            # Use the self-signed cert as the CA to verify against
            relay(
                get_url="https://127.0.0.1:18098/data",
                post_urls=["https://127.0.0.1:18099/upload"],
                chunk_size=4096,
                queue_maxsize=8,
                get_config={"ca_cert": cert, "verify": True},
                post_configs=[{"ca_cert": cert, "verify": True}],
            )

            assert dst_file.read_bytes() == data
        finally:
            src_proc.terminate()
            sink_proc.terminate()
            src_proc.join(timeout=5)
            sink_proc.join(timeout=5)


@pytest.mark.integration
class TestMixedRelay:
    """Mixed protocol relays: file -> HTTP, HTTP -> file, etc."""

    def test_file_to_http(self, integration_dir):
        src_file = integration_dir / "source.bin"
        dst_file = integration_dir / "dest.bin"
        data = os.urandom(20 * 1024)
        src_file.write_bytes(data)

        sink_started = multiprocessing.Event()
        sink_proc = multiprocessing.Process(
            target=_run_http_sink_server,
            args=("127.0.0.1", 18100, str(dst_file), sink_started),
            daemon=True,
        )
        sink_proc.start()

        try:
            _wait_for_event(sink_started)

            from txwtf_tools.relay import relay

            relay(
                get_url=f"file://{src_file}",
                post_urls=["http://127.0.0.1:18100/upload"],
                chunk_size=4096,
                queue_maxsize=8,
                post_configs=[{"verify": False}],
            )

            assert dst_file.read_bytes() == data
        finally:
            sink_proc.terminate()
            sink_proc.join(timeout=5)

    def test_http_to_file(self, integration_dir):
        src_file = integration_dir / "source.bin"
        dst_file = integration_dir / "dest.bin"
        data = os.urandom(15 * 1024)
        src_file.write_bytes(data)

        src_started = multiprocessing.Event()
        src_proc = multiprocessing.Process(
            target=_run_http_source_server,
            args=("127.0.0.1", 18101, str(src_file), src_started),
            daemon=True,
        )
        src_proc.start()

        try:
            _wait_for_event(src_started)

            from txwtf_tools.relay import relay

            relay(
                get_url="http://127.0.0.1:18101/data",
                post_urls=[f"file://{dst_file}"],
                chunk_size=4096,
                queue_maxsize=8,
                get_config={"verify": False},
            )

            assert dst_file.read_bytes() == data
        finally:
            src_proc.terminate()
            src_proc.join(timeout=5)

    def test_http_to_file_and_http(self, integration_dir):
        """Fan-out: HTTP source -> file + HTTP sink simultaneously."""
        src_file = integration_dir / "source.bin"
        dst_file = integration_dir / "dest_file.bin"
        dst_http = integration_dir / "dest_http.bin"
        data = os.urandom(10 * 1024)
        src_file.write_bytes(data)

        src_started = multiprocessing.Event()
        sink_started = multiprocessing.Event()

        src_proc = multiprocessing.Process(
            target=_run_http_source_server,
            args=("127.0.0.1", 18102, str(src_file), src_started),
            daemon=True,
        )
        sink_proc = multiprocessing.Process(
            target=_run_http_sink_server,
            args=("127.0.0.1", 18103, str(dst_http), sink_started),
            daemon=True,
        )
        src_proc.start()
        sink_proc.start()

        try:
            _wait_for_event(src_started)
            _wait_for_event(sink_started)

            from txwtf_tools.relay import relay

            relay(
                get_url="http://127.0.0.1:18102/data",
                post_urls=[
                    f"file://{dst_file}",
                    "http://127.0.0.1:18103/upload",
                ],
                chunk_size=4096,
                queue_maxsize=8,
                get_config={"verify": False},
                post_configs=[None, {"verify": False}],
            )

            assert dst_file.read_bytes() == data
            assert dst_http.read_bytes() == data
        finally:
            src_proc.terminate()
            sink_proc.terminate()
            src_proc.join(timeout=5)
            sink_proc.join(timeout=5)


@pytest.mark.integration
class TestRelayWithTransform:
    """Relay with a process_func transform over HTTP."""

    def test_transform_over_http(self, integration_dir):
        src_file = integration_dir / "source.txt"
        dst_file = integration_dir / "dest.txt"
        data = b"hello world test data " * 500
        src_file.write_bytes(data)

        src_started = multiprocessing.Event()
        sink_started = multiprocessing.Event()

        src_proc = multiprocessing.Process(
            target=_run_http_source_server,
            args=("127.0.0.1", 18104, str(src_file), src_started),
            daemon=True,
        )
        sink_proc = multiprocessing.Process(
            target=_run_http_sink_server,
            args=("127.0.0.1", 18105, str(dst_file), sink_started),
            daemon=True,
        )
        src_proc.start()
        sink_proc.start()

        try:
            _wait_for_event(src_started)
            _wait_for_event(sink_started)

            from txwtf_tools.relay import relay

            relay(
                get_url="http://127.0.0.1:18104/data",
                post_urls=["http://127.0.0.1:18105/upload"],
                process_func=lambda chunk: chunk.upper(),
                chunk_size=2048,
                queue_maxsize=8,
                get_config={"verify": False},
                post_configs=[{"verify": False}],
            )

            assert dst_file.read_bytes() == data.upper()
        finally:
            src_proc.terminate()
            sink_proc.terminate()
            src_proc.join(timeout=5)
            sink_proc.join(timeout=5)


@pytest.mark.integration
class TestStreamerIntegration:
    """Integration tests for the streamer (process_stream) over HTTP."""

    def test_streamer_http_to_http(self, integration_dir):
        src_file = integration_dir / "source.bin"
        dst_file = integration_dir / "dest.bin"
        data = os.urandom(16 * 1024)
        src_file.write_bytes(data)

        src_started = multiprocessing.Event()
        sink_started = multiprocessing.Event()

        src_proc = multiprocessing.Process(
            target=_run_http_source_server,
            args=("127.0.0.1", 18106, str(src_file), src_started),
            daemon=True,
        )
        sink_proc = multiprocessing.Process(
            target=_run_http_sink_server,
            args=("127.0.0.1", 18107, str(dst_file), sink_started),
            daemon=True,
        )
        src_proc.start()
        sink_proc.start()

        try:
            _wait_for_event(src_started)
            _wait_for_event(sink_started)

            from txwtf_tools.streamer import streamer

            streamer(
                [
                    {
                        "input_uri": "http://127.0.0.1:18106/data",
                        "output_uris": ["http://127.0.0.1:18107/upload"],
                        "input_kwargs": {
                            "chunk_size": 4096,
                            "http_kwargs": {"ssl": False},
                        },
                        "output_kwargs_list": [
                            {
                                "max_queue_size": 8,
                                "http_kwargs": {
                                    "ssl": False,
                                    "headers": {"Content-Type": "application/octet-stream"},
                                },
                            }
                        ],
                    }
                ]
            )

            assert dst_file.read_bytes() == data
        finally:
            src_proc.terminate()
            sink_proc.terminate()
            src_proc.join(timeout=5)
            sink_proc.join(timeout=5)
