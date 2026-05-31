"""
Integration tests — multi-process HTTP(S) relay end-to-end.

These spin up lightweight aiohttp servers in subprocesses and use the relay
to stream data between them, proving the full pipeline works over real TCP.
"""

import asyncio
import hashlib
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


# ---------------------------------------------------------------------------
# Large-data / slow-consumer helpers
# ---------------------------------------------------------------------------

def _run_streaming_source_server(host, port, total_bytes, chunk_size, started_event, digest_path):
    """
    HTTP source that generates *total_bytes* of deterministic pseudo-random data
    on the fly (no temp file), streaming it in *chunk_size* chunks.
    Writes the SHA-256 hex digest of the full payload to *digest_path*.
    """
    import aiohttp.web

    async def handle_get(request):
        h = hashlib.sha256()
        remaining = total_bytes

        async def body_gen():
            nonlocal remaining
            seed = 0
            while remaining > 0:
                n = min(chunk_size, remaining)
                # deterministic chunk seeded by offset
                chunk = seed.to_bytes(8, "little") * (n // 8 + 1)
                chunk = chunk[:n]
                h.update(chunk)
                remaining -= n
                seed += 1
                yield chunk
            # write digest so the test can compare
            Path(digest_path).write_text(h.hexdigest())

        resp = aiohttp.web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(total_bytes),
            },
        )
        await resp.prepare(request)
        async for chunk in body_gen():
            await resp.write(chunk)
        await resp.write_eof()
        return resp

    app = aiohttp.web.Application()
    app.router.add_get("/data", handle_get)
    runner = aiohttp.web.AppRunner(app)

    async def start():
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, host, port)
        await site.start()
        started_event.set()
        while True:
            await asyncio.sleep(3600)

    asyncio.run(start())


def _run_throttled_sink_server(
    host, port, output_path, digest_path, started_event, delay_per_mb=0.0, timing_path=None
):
    """
    HTTP sink that accepts POST on /upload, writing body to *output_path*.
    Optionally sleeps *delay_per_mb* seconds per MB received to simulate a slow
    consumer.  Writes SHA-256 digest to *digest_path* and elapsed time to *timing_path*.
    """
    import aiohttp.web

    async def handle_post(request):
        t0 = time.monotonic()
        h = hashlib.sha256()
        received = 0
        with open(str(output_path), "wb") as f:
            async for chunk in request.content.iter_any():
                f.write(chunk)
                h.update(chunk)
                received += len(chunk)
                if delay_per_mb > 0:
                    delay = delay_per_mb * (len(chunk) / (1024 * 1024))
                    if delay > 0:
                        await asyncio.sleep(delay)
        elapsed = time.monotonic() - t0
        Path(digest_path).write_text(h.hexdigest())
        if timing_path:
            Path(timing_path).write_text(f"{elapsed:.6f}")
        return aiohttp.web.json_response({"status": "ok", "bytes": received})

    app = aiohttp.web.Application(client_max_size=0)  # no body size limit
    app.router.add_post("/upload", handle_post)
    runner = aiohttp.web.AppRunner(app)

    async def start():
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, host, port)
        await site.start()
        started_event.set()
        while True:
            await asyncio.sleep(3600)

    asyncio.run(start())


# ---------------------------------------------------------------------------
# Large-data & slow-consumer tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestLargeDataRelay:
    """Stream large payloads (multi-MB) through the relay and verify integrity."""

    def test_large_1to1_relay(self, integration_dir):
        """1:1 relay of 8 MB — verify SHA-256 digest matches."""
        total_bytes = 8 * 1024 * 1024  # 8 MB
        chunk_size = 64 * 1024

        src_digest = str(integration_dir / "src_digest.txt")
        dst_file = str(integration_dir / "dest.bin")
        dst_digest = str(integration_dir / "dst_digest.txt")

        src_started = multiprocessing.Event()
        sink_started = multiprocessing.Event()

        src_proc = multiprocessing.Process(
            target=_run_streaming_source_server,
            args=("127.0.0.1", 18110, total_bytes, chunk_size, src_started, src_digest),
            daemon=True,
        )
        sink_proc = multiprocessing.Process(
            target=_run_throttled_sink_server,
            args=("127.0.0.1", 18111, dst_file, dst_digest, sink_started),
            daemon=True,
        )
        src_proc.start()
        sink_proc.start()

        try:
            _wait_for_event(src_started)
            _wait_for_event(sink_started)

            from txwtf_tools.relay import relay

            relay(
                get_url="http://127.0.0.1:18110/data",
                post_urls=["http://127.0.0.1:18111/upload"],
                chunk_size=chunk_size,
                queue_maxsize=32,
                get_config={"verify": False, "timeout": {"total": 120}},
                post_configs=[{"verify": False, "timeout": {"total": 120}}],
            )

            sd = Path(src_digest).read_text()
            dd = Path(dst_digest).read_text()
            assert sd == dd, f"SHA-256 mismatch: src={sd} dst={dd}"
            assert os.path.getsize(dst_file) == total_bytes
        finally:
            src_proc.terminate()
            sink_proc.terminate()
            src_proc.join(timeout=5)
            sink_proc.join(timeout=5)


@pytest.mark.integration
class TestSlowConsumerFanout:
    """Fan-out where sinks consume at different rates.

    Proves that:
    1. All destinations receive correct, complete data.
    2. The fast consumer finishes well before the slow one (no head-of-line blocking).
    """

    def test_fanout_fast_and_slow(self, integration_dir):
        """Fan-out to a fast sink and a slow sink (0.05s delay/MB).
        Verify both get identical data and the fast one isn't starved."""
        total_bytes = 4 * 1024 * 1024  # 4 MB
        chunk_size = 64 * 1024

        src_digest = str(integration_dir / "src_digest.txt")

        fast_file = str(integration_dir / "fast.bin")
        fast_digest = str(integration_dir / "fast_digest.txt")
        fast_timing = str(integration_dir / "fast_timing.txt")

        slow_file = str(integration_dir / "slow.bin")
        slow_digest = str(integration_dir / "slow_digest.txt")
        slow_timing = str(integration_dir / "slow_timing.txt")

        src_started = multiprocessing.Event()
        fast_started = multiprocessing.Event()
        slow_started = multiprocessing.Event()

        src_proc = multiprocessing.Process(
            target=_run_streaming_source_server,
            args=("127.0.0.1", 18112, total_bytes, chunk_size, src_started, src_digest),
            daemon=True,
        )
        fast_proc = multiprocessing.Process(
            target=_run_throttled_sink_server,
            args=("127.0.0.1", 18113, fast_file, fast_digest, fast_started, 0.0, fast_timing),
            daemon=True,
        )
        slow_proc = multiprocessing.Process(
            target=_run_throttled_sink_server,
            args=("127.0.0.1", 18114, slow_file, slow_digest, slow_started, 0.05, slow_timing),
            daemon=True,
        )
        src_proc.start()
        fast_proc.start()
        slow_proc.start()

        try:
            _wait_for_event(src_started)
            _wait_for_event(fast_started)
            _wait_for_event(slow_started)

            from txwtf_tools.relay import relay

            relay(
                get_url="http://127.0.0.1:18112/data",
                post_urls=[
                    "http://127.0.0.1:18113/upload",
                    "http://127.0.0.1:18114/upload",
                ],
                chunk_size=chunk_size,
                queue_maxsize=16,
                get_config={"verify": False, "timeout": {"total": 120}},
                post_configs=[
                    {"verify": False, "timeout": {"total": 120}},
                    {"verify": False, "timeout": {"total": 120}},
                ],
            )

            # Both must have correct data
            sd = Path(src_digest).read_text()
            fd = Path(fast_digest).read_text()
            sld = Path(slow_digest).read_text()
            assert sd == fd, f"Fast digest mismatch: src={sd} fast={fd}"
            assert sd == sld, f"Slow digest mismatch: src={sd} slow={sld}"
            assert os.path.getsize(fast_file) == total_bytes
            assert os.path.getsize(slow_file) == total_bytes

            # Check timing — fast should be noticeably quicker than slow
            fast_elapsed = float(Path(fast_timing).read_text())
            slow_elapsed = float(Path(slow_timing).read_text())
            print(f"\n  Fast consumer: {fast_elapsed:.3f}s")
            print(f"  Slow consumer: {slow_elapsed:.3f}s")
            print(f"  Ratio (slow/fast): {slow_elapsed / max(fast_elapsed, 0.001):.1f}x")

            # The slow consumer should take measurably longer
            assert slow_elapsed > fast_elapsed, (
                f"Slow consumer ({slow_elapsed:.3f}s) should be slower than fast ({fast_elapsed:.3f}s)"
            )
        finally:
            src_proc.terminate()
            fast_proc.terminate()
            slow_proc.terminate()
            src_proc.join(timeout=5)
            fast_proc.join(timeout=5)
            slow_proc.join(timeout=5)

    def test_fanout_with_per_queue_maxsize(self, integration_dir):
        """Give the slow consumer a larger buffer to absorb bursts."""
        total_bytes = 4 * 1024 * 1024  # 4 MB
        chunk_size = 64 * 1024

        src_digest = str(integration_dir / "src_digest.txt")

        fast_file = str(integration_dir / "fast.bin")
        fast_digest = str(integration_dir / "fast_digest.txt")
        fast_timing = str(integration_dir / "fast_timing.txt")

        slow_file = str(integration_dir / "slow.bin")
        slow_digest = str(integration_dir / "slow_digest.txt")
        slow_timing = str(integration_dir / "slow_timing.txt")

        src_started = multiprocessing.Event()
        fast_started = multiprocessing.Event()
        slow_started = multiprocessing.Event()

        src_proc = multiprocessing.Process(
            target=_run_streaming_source_server,
            args=("127.0.0.1", 18115, total_bytes, chunk_size, src_started, src_digest),
            daemon=True,
        )
        fast_proc = multiprocessing.Process(
            target=_run_throttled_sink_server,
            args=("127.0.0.1", 18116, fast_file, fast_digest, fast_started, 0.0, fast_timing),
            daemon=True,
        )
        slow_proc = multiprocessing.Process(
            target=_run_throttled_sink_server,
            args=("127.0.0.1", 18117, slow_file, slow_digest, slow_started, 0.05, slow_timing),
            daemon=True,
        )
        src_proc.start()
        fast_proc.start()
        slow_proc.start()

        try:
            _wait_for_event(src_started)
            _wait_for_event(fast_started)
            _wait_for_event(slow_started)

            from txwtf_tools.relay import relay

            relay(
                get_url="http://127.0.0.1:18115/data",
                post_urls=[
                    "http://127.0.0.1:18116/upload",
                    "http://127.0.0.1:18117/upload",
                ],
                chunk_size=chunk_size,
                queue_maxsize=16,
                # Give the slow consumer (index 1) a 4x larger buffer
                per_queue_maxsize=[16, 64],
                get_config={"verify": False, "timeout": {"total": 120}},
                post_configs=[
                    {"verify": False, "timeout": {"total": 120}},
                    {"verify": False, "timeout": {"total": 120}},
                ],
            )

            # Both must have correct data
            sd = Path(src_digest).read_text()
            fd = Path(fast_digest).read_text()
            sld = Path(slow_digest).read_text()
            assert sd == fd, f"Fast digest mismatch"
            assert sd == sld, f"Slow digest mismatch"
            assert os.path.getsize(fast_file) == total_bytes
            assert os.path.getsize(slow_file) == total_bytes

            fast_elapsed = float(Path(fast_timing).read_text())
            slow_elapsed = float(Path(slow_timing).read_text())
            print(f"\n  Fast consumer (buf=16): {fast_elapsed:.3f}s")
            print(f"  Slow consumer (buf=64): {slow_elapsed:.3f}s")
            print(f"  Ratio (slow/fast): {slow_elapsed / max(fast_elapsed, 0.001):.1f}x")

            assert slow_elapsed > fast_elapsed
        finally:
            src_proc.terminate()
            fast_proc.terminate()
            slow_proc.terminate()
            src_proc.join(timeout=5)
            fast_proc.join(timeout=5)
            slow_proc.join(timeout=5)

    def test_fanout_three_speeds(self, integration_dir):
        """Fan-out to three sinks: fast, medium (0.02s/MB), slow (0.08s/MB)."""
        total_bytes = 2 * 1024 * 1024  # 2 MB
        chunk_size = 64 * 1024

        src_digest = str(integration_dir / "src_digest.txt")

        files = {}
        digests = {}
        timings = {}
        for label in ("fast", "medium", "slow"):
            files[label] = str(integration_dir / f"{label}.bin")
            digests[label] = str(integration_dir / f"{label}_digest.txt")
            timings[label] = str(integration_dir / f"{label}_timing.txt")

        src_started = multiprocessing.Event()
        fast_started = multiprocessing.Event()
        med_started = multiprocessing.Event()
        slow_started = multiprocessing.Event()

        src_proc = multiprocessing.Process(
            target=_run_streaming_source_server,
            args=("127.0.0.1", 18118, total_bytes, chunk_size, src_started, src_digest),
            daemon=True,
        )
        fast_proc = multiprocessing.Process(
            target=_run_throttled_sink_server,
            args=(
                "127.0.0.1", 18119,
                files["fast"], digests["fast"], fast_started,
                0.0, timings["fast"],
            ),
            daemon=True,
        )
        med_proc = multiprocessing.Process(
            target=_run_throttled_sink_server,
            args=(
                "127.0.0.1", 18120,
                files["medium"], digests["medium"], med_started,
                0.02, timings["medium"],
            ),
            daemon=True,
        )
        slow_proc = multiprocessing.Process(
            target=_run_throttled_sink_server,
            args=(
                "127.0.0.1", 18121,
                files["slow"], digests["slow"], slow_started,
                0.08, timings["slow"],
            ),
            daemon=True,
        )
        src_proc.start()
        fast_proc.start()
        med_proc.start()
        slow_proc.start()

        try:
            _wait_for_event(src_started)
            _wait_for_event(fast_started)
            _wait_for_event(med_started)
            _wait_for_event(slow_started)

            from txwtf_tools.relay import relay

            relay(
                get_url="http://127.0.0.1:18118/data",
                post_urls=[
                    "http://127.0.0.1:18119/upload",
                    "http://127.0.0.1:18120/upload",
                    "http://127.0.0.1:18121/upload",
                ],
                chunk_size=chunk_size,
                queue_maxsize=16,
                get_config={"verify": False, "timeout": {"total": 120}},
                post_configs=[
                    {"verify": False, "timeout": {"total": 120}},
                    {"verify": False, "timeout": {"total": 120}},
                    {"verify": False, "timeout": {"total": 120}},
                ],
            )

            sd = Path(src_digest).read_text()
            for label in ("fast", "medium", "slow"):
                dd = Path(digests[label]).read_text()
                assert sd == dd, f"{label} digest mismatch: src={sd} {label}={dd}"
                assert os.path.getsize(files[label]) == total_bytes

            elapsed = {
                label: float(Path(timings[label]).read_text())
                for label in ("fast", "medium", "slow")
            }
            print(f"\n  Fast:   {elapsed['fast']:.3f}s")
            print(f"  Medium: {elapsed['medium']:.3f}s")
            print(f"  Slow:   {elapsed['slow']:.3f}s")

            # Slow should be slowest, medium in between
            assert elapsed["slow"] > elapsed["fast"]
            assert elapsed["slow"] >= elapsed["medium"]
        finally:
            src_proc.terminate()
            fast_proc.terminate()
            med_proc.terminate()
            slow_proc.terminate()
            src_proc.join(timeout=5)
            fast_proc.join(timeout=5)
            med_proc.join(timeout=5)
            slow_proc.join(timeout=5)


@pytest.mark.integration
class TestRateLimitedRelay:
    """Verify that the rate_limit parameter measurably throttles HTTP relays."""

    def test_http_rate_limited(self, integration_dir):
        """HTTP relay with rate_limit should be slower than without."""
        src_file = integration_dir / "source.bin"
        dst_unlimited = integration_dir / "dest_unlimited.bin"
        dst_limited = integration_dir / "dest_limited.bin"
        data = os.urandom(100_000)  # 100 KB
        src_file.write_bytes(data)

        src_started = multiprocessing.Event()
        sink_started = multiprocessing.Event()

        src_proc = multiprocessing.Process(
            target=_run_http_source_server,
            args=("127.0.0.1", 18130, str(src_file), src_started),
            daemon=True,
        )
        sink_proc = multiprocessing.Process(
            target=_run_http_sink_server,
            args=("127.0.0.1", 18131, str(dst_unlimited), sink_started),
            daemon=True,
        )
        src_proc.start()
        sink_proc.start()

        try:
            _wait_for_event(src_started)
            _wait_for_event(sink_started)

            from txwtf_tools.relay import relay

            # Unlimited run
            t0 = time.monotonic()
            relay(
                get_url="http://127.0.0.1:18130/data",
                post_urls=["http://127.0.0.1:18131/upload"],
                chunk_size=10_000,
                queue_maxsize=8,
                get_config={"verify": False},
                post_configs=[{"verify": False}],
            )
            fast_elapsed = time.monotonic() - t0
            assert dst_unlimited.read_bytes() == data
        finally:
            src_proc.terminate()
            sink_proc.terminate()
            src_proc.join(timeout=5)
            sink_proc.join(timeout=5)

        # Now do a rate-limited run with fresh servers
        src_started2 = multiprocessing.Event()
        sink_started2 = multiprocessing.Event()

        src_proc2 = multiprocessing.Process(
            target=_run_http_source_server,
            args=("127.0.0.1", 18132, str(src_file), src_started2),
            daemon=True,
        )
        sink_proc2 = multiprocessing.Process(
            target=_run_http_sink_server,
            args=("127.0.0.1", 18133, str(dst_limited), sink_started2),
            daemon=True,
        )
        src_proc2.start()
        sink_proc2.start()

        try:
            _wait_for_event(src_started2)
            _wait_for_event(sink_started2)

            # Rate-limited to 50 KB/s → 100 KB should take ~2s
            t0 = time.monotonic()
            relay(
                get_url="http://127.0.0.1:18132/data",
                post_urls=["http://127.0.0.1:18133/upload"],
                chunk_size=10_000,
                queue_maxsize=8,
                get_config={"verify": False},
                post_configs=[{"verify": False}],
                rate_limit=50_000,
            )
            slow_elapsed = time.monotonic() - t0
            assert dst_limited.read_bytes() == data

            print(f"\n  Unlimited: {fast_elapsed:.3f}s")
            print(f"  Rate-limited (50 KB/s): {slow_elapsed:.3f}s")
            assert slow_elapsed > fast_elapsed + 0.5, (
                f"rate-limited ({slow_elapsed:.2f}s) should be much slower "
                f"than unlimited ({fast_elapsed:.2f}s)"
            )
        finally:
            src_proc2.terminate()
            sink_proc2.terminate()
            src_proc2.join(timeout=5)
            sink_proc2.join(timeout=5)

    def test_file_relay_rate_limited(self, integration_dir):
        """File-to-file relay with rate_limit should take at least expected time."""
        src = integration_dir / "src.bin"
        dst = integration_dir / "dst.bin"
        data = os.urandom(50_000)  # 50 KB
        src.write_bytes(data)

        from txwtf_tools.relay import relay

        t0 = time.monotonic()
        relay(
            get_url=f"file://{src}",
            post_urls=[f"file://{dst}"],
            chunk_size=10_000,
            queue_maxsize=4,
            rate_limit=25_000,  # 25 KB/s → ~2s
        )
        elapsed = time.monotonic() - t0

        assert dst.read_bytes() == data
        assert elapsed >= 1.0, f"Expected >= 1.0s, got {elapsed:.2f}s"


class TestRelayTransforms:
    """End-to-end tests for relay's encrypt, decrypt, compress, decompress,
    and finalize_func — exercising get_process_func (input-side) and
    process_func + finalize_func (output-side) independently and combined."""

    def test_encrypt_file_decrypt(self, integration_dir):
        """Encrypt on output, then decrypt on input."""
        from txwtf_tools.backup import make_decrypt_func, make_encrypt_func
        from txwtf_tools.relay import relay

        src = integration_dir / "plain.bin"
        enc = integration_dir / "encrypted.bin"
        dst = integration_dir / "decrypted.bin"
        data = os.urandom(50_000)
        src.write_bytes(data)

        passphrase = "integration-test-key"

        relay(
            get_url=f"file://{src}",
            post_urls=[f"file://{enc}"],
            process_func=make_encrypt_func(passphrase),
            chunk_size=8192,
            queue_maxsize=8,
        )

        enc_data = enc.read_bytes()
        assert len(enc_data) > len(data), "Encrypted data should be larger"
        assert enc_data != data

        relay(
            get_url=f"file://{enc}",
            post_urls=[f"file://{dst}"],
            get_process_func=make_decrypt_func(passphrase),
            chunk_size=8192,
            queue_maxsize=8,
        )

        assert dst.read_bytes() == data

    def test_compress_file_decompress(self, integration_dir):
        """Compress on output (with finalize), then decompress on input."""
        from txwtf_tools.backup import make_compress_func, make_decompress_func
        from txwtf_tools.relay import relay

        src = integration_dir / "plain.bin"
        gz = integration_dir / "compressed.gz"
        dst = integration_dir / "decompressed.bin"
        data = b"ABCDEFGH" * 10_000
        src.write_bytes(data)

        compress, finalize = make_compress_func()
        relay(
            get_url=f"file://{src}",
            post_urls=[f"file://{gz}"],
            process_func=compress,
            finalize_func=finalize,
            chunk_size=8192,
            queue_maxsize=8,
        )

        gz_data = gz.read_bytes()
        assert len(gz_data) < len(data), "Compressed should be smaller for repetitive data"

        relay(
            get_url=f"file://{gz}",
            post_urls=[f"file://{dst}"],
            get_process_func=make_decompress_func(),
            chunk_size=8192,
            queue_maxsize=8,
        )

        assert dst.read_bytes() == data

    def test_compress_encrypt_file_decrypt_decompress(self, integration_dir):
        """Full compress+encrypt → file → decrypt+decompress round-trip.

        Output side: compress then encrypt (with chained finalize).
        Input side: decrypt then decompress.
        Mirrors do_store → do_restore pipeline at the relay level."""
        from txwtf_tools.backup import (
            chain_functions,
            make_compress_func,
            make_decompress_func,
            make_decrypt_func,
            make_encrypt_func,
        )
        from txwtf_tools.relay import relay

        src = integration_dir / "plain.bin"
        stored = integration_dir / "stored.bin"
        dst = integration_dir / "restored.bin"
        data = b"ABCDEFGH" * 10_000
        src.write_bytes(data)

        passphrase = "compress-encrypt-test"

        # Store: compress + encrypt on output side
        compress, compress_finalize = make_compress_func()
        encrypt = make_encrypt_func(passphrase)

        def store_process(chunk: bytes) -> bytes:
            compressed = compress(chunk)
            if compressed:
                return encrypt(compressed)
            return b""

        def store_finalize() -> bytes:
            flushed = compress_finalize()
            if flushed:
                return encrypt(flushed)
            return b""

        relay(
            get_url=f"file://{src}",
            post_urls=[f"file://{stored}"],
            process_func=store_process,
            finalize_func=store_finalize,
            chunk_size=8192,
            queue_maxsize=8,
        )

        stored_data = stored.read_bytes()
        assert len(stored_data) < len(data)
        assert stored_data != data

        # Restore: decrypt + decompress on input side
        restore_func = chain_functions(
            make_decrypt_func(passphrase),
            make_decompress_func(),
        )

        relay(
            get_url=f"file://{stored}",
            post_urls=[f"file://{dst}"],
            get_process_func=restore_func,
            chunk_size=8192,
            queue_maxsize=8,
        )

        assert dst.read_bytes() == data

    def test_reencrypt_with_different_passphrases(self, integration_dir):
        """Encrypt → re-encrypt with different key → decrypt — proves relay
        can decrypt input and encrypt output simultaneously with independent
        passphrases."""
        from txwtf_tools.backup import make_decrypt_func, make_encrypt_func
        from txwtf_tools.relay import relay

        src = integration_dir / "plain.bin"
        enc_a = integration_dir / "encrypted_a.bin"
        enc_b = integration_dir / "encrypted_b.bin"
        dst = integration_dir / "decrypted.bin"
        data = os.urandom(80_000)
        src.write_bytes(data)

        key_a = "first-passphrase"
        key_b = "second-passphrase"

        relay(
            get_url=f"file://{src}",
            post_urls=[f"file://{enc_a}"],
            process_func=make_encrypt_func(key_a),
            chunk_size=8192,
            queue_maxsize=8,
        )

        relay(
            get_url=f"file://{enc_a}",
            post_urls=[f"file://{enc_b}"],
            get_process_func=make_decrypt_func(key_a),
            process_func=make_encrypt_func(key_b),
            chunk_size=8192,
            queue_maxsize=8,
        )

        assert enc_a.read_bytes() != enc_b.read_bytes()

        relay(
            get_url=f"file://{enc_b}",
            post_urls=[f"file://{dst}"],
            get_process_func=make_decrypt_func(key_b),
            chunk_size=8192,
            queue_maxsize=8,
        )

        assert dst.read_bytes() == data
