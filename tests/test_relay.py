"""Unit tests for txwtf_tools.relay"""

import asyncio
import os
import tempfile
import time

import pytest

from txwtf_tools.relay import (
    TokenBucketRateLimiter,
    create_ssl_value,
    file_consumer,
    file_producer,
    relay,
    relay_stream,
)


@pytest.fixture
def tmp_dir(tmp_path):
    return tmp_path


class TestCreateSslValue:
    def test_http_returns_false(self):
        assert create_ssl_value(None, is_https=False) is False

    def test_https_no_config_returns_true(self):
        assert create_ssl_value(None, is_https=True) is True

    def test_https_verify_false(self):
        ctx = create_ssl_value({"verify": False}, is_https=True)
        import ssl

        assert isinstance(ctx, ssl.SSLContext)
        assert ctx.verify_mode == ssl.CERT_NONE

    def test_https_with_ca(self, tmp_dir):
        # Just verify it doesn't crash with a non-existent CA (will raise on real use)
        with pytest.raises(Exception):
            create_ssl_value({"ca_cert": "/nonexistent/ca.pem"}, is_https=True)


class TestFileConsumer:
    @pytest.mark.asyncio
    async def test_reads_file_chunks(self, tmp_dir):
        src = tmp_dir / "input.bin"
        data = os.urandom(4096)
        src.write_bytes(data)

        q = asyncio.Queue(maxsize=10)
        pbar = _FakePbar()

        await file_consumer(f"file://{src}", [q], pbar, chunk_size=1024)

        chunks = []
        while not q.empty():
            item = q.get_nowait()
            if item is None:
                break
            chunks.append(item)

        result = b"".join(chunks)
        assert result == data

    @pytest.mark.asyncio
    async def test_fanout_to_multiple_queues(self, tmp_dir):
        src = tmp_dir / "input.bin"
        data = os.urandom(2048)
        src.write_bytes(data)

        q1 = asyncio.Queue(maxsize=10)
        q2 = asyncio.Queue(maxsize=10)
        pbar = _FakePbar()

        await file_consumer(f"file://{src}", [q1, q2], pbar, chunk_size=512)

        def drain(q):
            parts = []
            while not q.empty():
                item = q.get_nowait()
                if item is None:
                    break
                parts.append(item)
            return b"".join(parts)

        assert drain(q1) == data
        assert drain(q2) == data


class TestFileProducer:
    @pytest.mark.asyncio
    async def test_writes_file(self, tmp_dir):
        dst = tmp_dir / "output.bin"
        data = os.urandom(3000)
        q = asyncio.Queue()
        # feed chunks
        for i in range(0, len(data), 1024):
            await q.put(data[i : i + 1024])
        await q.put(None)

        pbar = _FakePbar()
        await file_producer(f"file://{dst}", q, lambda x: x, pbar)

        assert dst.read_bytes() == data


class TestRelayFileToFile:
    @pytest.mark.asyncio
    async def test_single_destination(self, tmp_dir):
        src = tmp_dir / "src.bin"
        dst = tmp_dir / "dst.bin"
        data = os.urandom(8192)
        src.write_bytes(data)

        await relay_stream(
            get_url=f"file://{src}",
            post_urls=[f"file://{dst}"],
            chunk_size=2048,
            queue_maxsize=4,
        )

        assert dst.read_bytes() == data

    @pytest.mark.asyncio
    async def test_multiple_destinations(self, tmp_dir):
        src = tmp_dir / "src.bin"
        dst1 = tmp_dir / "dst1.bin"
        dst2 = tmp_dir / "dst2.bin"
        data = os.urandom(10000)
        src.write_bytes(data)

        await relay_stream(
            get_url=f"file://{src}",
            post_urls=[f"file://{dst1}", f"file://{dst2}"],
            chunk_size=2048,
            queue_maxsize=4,
        )

        assert dst1.read_bytes() == data
        assert dst2.read_bytes() == data

    @pytest.mark.asyncio
    async def test_with_transform(self, tmp_dir):
        src = tmp_dir / "src.bin"
        dst = tmp_dir / "dst.bin"
        data = b"hello world " * 500
        src.write_bytes(data)

        await relay_stream(
            get_url=f"file://{src}",
            post_urls=[f"file://{dst}"],
            process_func=lambda chunk: chunk.upper(),
            chunk_size=1024,
            queue_maxsize=4,
        )

        assert dst.read_bytes() == data.upper()

    @pytest.mark.asyncio
    async def test_with_get_process_func(self, tmp_dir):
        """Input-side transform via get_process_func."""
        src = tmp_dir / "src.bin"
        dst = tmp_dir / "dst.bin"
        data = b"hello world " * 500
        src.write_bytes(data)

        await relay_stream(
            get_url=f"file://{src}",
            post_urls=[f"file://{dst}"],
            get_process_func=lambda chunk: chunk.upper(),
            chunk_size=1024,
            queue_maxsize=4,
        )

        assert dst.read_bytes() == data.upper()

    @pytest.mark.asyncio
    async def test_get_and_post_process_funcs(self, tmp_dir):
        """Both input-side and output-side transforms applied in sequence."""
        src = tmp_dir / "src.bin"
        dst = tmp_dir / "dst.bin"
        data = b"hello world " * 500
        src.write_bytes(data)

        await relay_stream(
            get_url=f"file://{src}",
            post_urls=[f"file://{dst}"],
            get_process_func=lambda chunk: chunk.upper(),
            process_func=lambda chunk: chunk.replace(b" ", b"-"),
            chunk_size=1024,
            queue_maxsize=4,
        )

        assert dst.read_bytes() == data.upper().replace(b" ", b"-")

    @pytest.mark.asyncio
    async def test_with_finalize_func(self, tmp_dir):
        """finalize_func flushes trailing data after last chunk."""
        src = tmp_dir / "src.bin"
        dst = tmp_dir / "dst.bin"
        data = b"ABCDEFGH" * 1000
        src.write_bytes(data)

        from txwtf_tools.backup import make_compress_func, make_decompress_func

        compress, finalize = make_compress_func()

        await relay_stream(
            get_url=f"file://{src}",
            post_urls=[f"file://{dst}"],
            process_func=compress,
            finalize_func=finalize,
            chunk_size=1024,
            queue_maxsize=4,
        )

        compressed = dst.read_bytes()
        assert len(compressed) < len(data)

        # Verify it's valid gzip by decompressing
        decompress = make_decompress_func()
        assert decompress(compressed) == data

    def test_sync_wrapper(self, tmp_dir):
        src = tmp_dir / "src.bin"
        dst = tmp_dir / "dst.bin"
        data = os.urandom(4096)
        src.write_bytes(data)

        relay(
            get_url=f"file://{src}",
            post_urls=[f"file://{dst}"],
            chunk_size=1024,
        )

        assert dst.read_bytes() == data


class TestRelayValidation:
    @pytest.mark.asyncio
    async def test_mismatched_headers_raises(self, tmp_dir):
        src = tmp_dir / "src.bin"
        src.write_bytes(b"data")

        with pytest.raises(ValueError, match="post_headers"):
            await relay_stream(
                get_url=f"file://{src}",
                post_urls=[f"file://{tmp_dir}/out.bin"],
                post_headers=[{}, {}],  # 2 headers for 1 url
            )

    @pytest.mark.asyncio
    async def test_mismatched_configs_raises(self, tmp_dir):
        src = tmp_dir / "src.bin"
        src.write_bytes(b"data")

        with pytest.raises(ValueError, match="post_configs"):
            await relay_stream(
                get_url=f"file://{src}",
                post_urls=[f"file://{tmp_dir}/out.bin"],
                post_configs=[None, None],  # 2 configs for 1 url
            )

    @pytest.mark.asyncio
    async def test_unsupported_scheme_raises(self, tmp_dir):
        src = tmp_dir / "src.bin"
        src.write_bytes(b"data")

        with pytest.raises(ValueError, match="Unsupported scheme"):
            await relay_stream(
                get_url=f"ftp://{src}",
                post_urls=[f"file://{tmp_dir}/out.bin"],
            )


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

class _FakePbar:
    """Minimal stand-in for tqdm so we can test without real progress bars."""

    total = 0

    def update(self, n):
        pass

    def set_description(self, desc):
        pass

    def refresh(self):
        pass

    def close(self):
        pass


class TestTokenBucketRateLimiter:
    @pytest.mark.asyncio
    async def test_disabled_when_zero(self):
        limiter = TokenBucketRateLimiter(0)
        t0 = time.monotonic()
        for _ in range(100):
            await limiter.acquire(1024)
        elapsed = time.monotonic() - t0
        assert elapsed < 0.1, "disabled limiter should not throttle"

    @pytest.mark.asyncio
    async def test_disabled_when_none(self):
        limiter = TokenBucketRateLimiter(None)
        t0 = time.monotonic()
        for _ in range(100):
            await limiter.acquire(1024)
        elapsed = time.monotonic() - t0
        assert elapsed < 0.1

    @pytest.mark.asyncio
    async def test_throttles_to_target_rate(self):
        rate = 10_000  # 10 KB/s
        limiter = TokenBucketRateLimiter(rate)
        total_bytes = 30_000  # 30 KB — bucket starts with 10 KB burst,
        chunk = 1_000          # remaining 20 KB at 10 KB/s → ~2s
        t0 = time.monotonic()
        for _ in range(total_bytes // chunk):
            await limiter.acquire(chunk)
        elapsed = time.monotonic() - t0
        # Initial 10 KB burst is free, remaining 20 KB at 10 KB/s → ~2s
        assert elapsed >= 1.5, f"expected >= 1.5s, got {elapsed:.2f}s"


class TestRateLimitedFileRelay:
    @pytest.mark.asyncio
    async def test_rate_limited_file_relay(self, tmp_dir):
        """Relay with rate_limit should be measurably slower than without."""
        src = tmp_dir / "src.bin"
        dst_fast = tmp_dir / "dst_fast.bin"
        dst_slow = tmp_dir / "dst_slow.bin"
        data = os.urandom(50_000)
        src.write_bytes(data)

        # Unlimited
        t0 = time.monotonic()
        await relay_stream(
            get_url=f"file://{src}",
            post_urls=[f"file://{dst_fast}"],
            chunk_size=10_000,
            queue_maxsize=4,
        )
        fast_elapsed = time.monotonic() - t0

        # Rate-limited to 25 KB/s — 50 KB should take ~2s
        t0 = time.monotonic()
        await relay_stream(
            get_url=f"file://{src}",
            post_urls=[f"file://{dst_slow}"],
            chunk_size=10_000,
            queue_maxsize=4,
            rate_limit=25_000,
        )
        slow_elapsed = time.monotonic() - t0

        assert dst_fast.read_bytes() == data
        assert dst_slow.read_bytes() == data
        # Rate-limited should take noticeably longer
        assert slow_elapsed > fast_elapsed + 0.5, (
            f"rate-limited ({slow_elapsed:.2f}s) should be much slower than "
            f"unlimited ({fast_elapsed:.2f}s)"
        )

    @pytest.mark.asyncio
    async def test_rate_limit_zero_is_unlimited(self, tmp_dir):
        """rate_limit=0 should behave like no limit."""
        src = tmp_dir / "src.bin"
        dst = tmp_dir / "dst.bin"
        data = os.urandom(8192)
        src.write_bytes(data)

        t0 = time.monotonic()
        await relay_stream(
            get_url=f"file://{src}",
            post_urls=[f"file://{dst}"],
            chunk_size=2048,
            queue_maxsize=4,
            rate_limit=0,
        )
        elapsed = time.monotonic() - t0
        assert dst.read_bytes() == data
        assert elapsed < 1.0

    @pytest.mark.asyncio
    async def test_rate_limited_with_transform(self, tmp_dir):
        """Rate limit should work together with process_func."""
        src = tmp_dir / "src.bin"
        dst = tmp_dir / "dst.bin"
        data = b"hello world " * 2000  # 24 KB
        src.write_bytes(data)

        t0 = time.monotonic()
        await relay_stream(
            get_url=f"file://{src}",
            post_urls=[f"file://{dst}"],
            process_func=lambda chunk: chunk.upper(),
            chunk_size=4096,
            queue_maxsize=4,
            rate_limit=12_000,  # 12 KB/s → should take ~2s
        )
        elapsed = time.monotonic() - t0

        assert dst.read_bytes() == data.upper()
        assert elapsed >= 1.0

    @pytest.mark.asyncio
    async def test_rate_limited_fanout(self, tmp_dir):
        """Rate limit should apply to the input side, throttling all outputs."""
        src = tmp_dir / "src.bin"
        dst1 = tmp_dir / "dst1.bin"
        dst2 = tmp_dir / "dst2.bin"
        data = os.urandom(30_000)
        src.write_bytes(data)

        t0 = time.monotonic()
        await relay_stream(
            get_url=f"file://{src}",
            post_urls=[f"file://{dst1}", f"file://{dst2}"],
            chunk_size=10_000,
            queue_maxsize=4,
            rate_limit=15_000,  # 15 KB/s → ~2s
        )
        elapsed = time.monotonic() - t0

        assert dst1.read_bytes() == data
        assert dst2.read_bytes() == data
        assert elapsed >= 1.0

    @property
    def n(self):
        return 0

    @n.setter
    def n(self, v):
        pass
