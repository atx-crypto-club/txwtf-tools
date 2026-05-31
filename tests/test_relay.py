"""Unit tests for txwtf_tools.relay"""

import asyncio
import os
import tempfile

import pytest

from txwtf_tools.relay import (
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

    @property
    def n(self):
        return 0

    @n.setter
    def n(self, v):
        pass
