"""Unit tests for txwtf_tools.streamer"""

import asyncio
import os

import pytest

from txwtf_tools.streamer import (
    consumer_gen,
    get_input_stream,
    get_input_stream_size,
    process_stream,
    sanitize_uri,
    streamer,
)


class TestSanitizeUri:
    def test_no_password(self):
        assert sanitize_uri("https://example.com/path") == "https://example.com/path"

    def test_password_obscured(self):
        result = sanitize_uri("sftp://user:secret@host:22/path")
        assert "secret" not in result
        assert "***" in result
        assert "user" in result

    def test_password_with_port(self):
        result = sanitize_uri("sftp://user:secret@host:2222/path")
        assert ":2222" in result
        assert "secret" not in result


class TestGetInputStreamSize:
    @pytest.mark.asyncio
    async def test_file_size(self, tmp_path):
        f = tmp_path / "test.bin"
        data = os.urandom(5000)
        f.write_bytes(data)
        size = await get_input_stream_size(f.as_uri())
        assert size == 5000

    @pytest.mark.asyncio
    async def test_unknown_scheme_raises(self):
        with pytest.raises(ValueError, match="Unknown scheme"):
            await get_input_stream_size("ftp://host/path")


class TestGetInputStream:
    @pytest.mark.asyncio
    async def test_file_stream(self, tmp_path):
        f = tmp_path / "test.bin"
        data = os.urandom(4096)
        f.write_bytes(data)

        gen = await get_input_stream(f.as_uri(), chunk_size=1024)

        chunks = []
        async for chunk in gen:
            chunks.append(chunk)
        assert b"".join(chunks) == data


class TestConsumerGen:
    @pytest.mark.asyncio
    async def test_passthrough(self):
        q = asyncio.Queue()
        await q.put(b"hello")
        await q.put(b"world")
        await q.put(None)

        chunks = []
        async for chunk in consumer_gen(10, "file:///test", q):
            chunks.append(chunk)

        assert chunks == [b"hello", b"world"]

    @pytest.mark.asyncio
    async def test_with_callback(self):
        q = asyncio.Queue()
        await q.put(b"hello")
        await q.put(None)

        chunks = []
        async for chunk in consumer_gen(5, "file:///test", q, callback=lambda c: c.upper()):
            chunks.append(chunk)

        assert chunks == [b"HELLO"]

    @pytest.mark.asyncio
    async def test_callback_returning_none_skips(self):
        q = asyncio.Queue()
        await q.put(b"hello")
        await q.put(b"world")
        await q.put(None)

        chunks = []
        async for chunk in consumer_gen(
            10, "file:///test", q, callback=lambda c: None if c == b"hello" else c
        ):
            chunks.append(chunk)

        assert chunks == [b"world"]

    @pytest.mark.asyncio
    async def test_finalize_callback(self):
        q = asyncio.Queue()
        await q.put(b"data")
        await q.put(None)

        chunks = []
        async for chunk in consumer_gen(
            4, "file:///test", q, finalize_callback=lambda: b"FINAL"
        ):
            chunks.append(chunk)

        assert chunks == [b"data", b"FINAL"]


class TestProcessStream:
    @pytest.mark.asyncio
    async def test_file_to_file(self, tmp_path):
        src = tmp_path / "src.bin"
        dst = tmp_path / "dst.bin"
        data = os.urandom(8192)
        src.write_bytes(data)

        await process_stream(
            input_uri=src.as_uri(),
            output_uris=[dst.as_uri()],
            input_kwargs={"chunk_size": 2048},
        )

        assert dst.read_bytes() == data

    @pytest.mark.asyncio
    async def test_file_fanout(self, tmp_path):
        src = tmp_path / "src.bin"
        dst1 = tmp_path / "dst1.bin"
        dst2 = tmp_path / "dst2.bin"
        data = os.urandom(6000)
        src.write_bytes(data)

        await process_stream(
            input_uri=src.as_uri(),
            output_uris=[dst1.as_uri(), dst2.as_uri()],
            input_kwargs={"chunk_size": 1024},
        )

        assert dst1.read_bytes() == data
        assert dst2.read_bytes() == data

    @pytest.mark.asyncio
    async def test_mismatched_output_lists_raises(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"data")

        with pytest.raises(ValueError, match="must match"):
            await process_stream(
                input_uri=src.as_uri(),
                output_uris=[(tmp_path / "a.bin").as_uri()],
                output_kwargs_list=[{}, {}],
            )

    def test_sync_wrapper(self, tmp_path):
        src = tmp_path / "src.bin"
        dst = tmp_path / "dst.bin"
        data = os.urandom(4096)
        src.write_bytes(data)

        streamer(
            [
                {
                    "input_uri": src.as_uri(),
                    "output_uris": [dst.as_uri()],
                    "input_kwargs": {"chunk_size": 1024},
                }
            ]
        )

        assert dst.read_bytes() == data
