"""
Async streaming engine — fan-out from a single input source to multiple output
destinations using asyncio queues, with optional per-output callbacks and
progress tracking.

Supports HTTP(S), SFTP, and local file schemes for both input and output.
"""

import asyncio
import os
import os.path
import ssl
from typing import Any, AsyncGenerator, Callable, Optional
from urllib.parse import ParseResult, urlparse, urlunparse
from urllib.request import url2pathname

import aiofiles
import aiohttp
import asyncssh
from tqdm.asyncio import tqdm as async_tqdm

from .relay import TokenBucketRateLimiter, _path_from_file_uri

CHUNK_SIZE = 1024 * 1024  # 1 MB
MAX_QUEUE_SIZE = 128


def sanitize_uri(uri: str) -> str:
    """Return *uri* with password obscured as ``***``."""
    parsed = urlparse(uri)
    if parsed.password:
        netloc = f"{parsed.username}:***@{parsed.hostname}"
        if parsed.port:
            netloc += f":{parsed.port}"
        sanitized = ParseResult(
            scheme=parsed.scheme,
            netloc=netloc,
            path=parsed.path,
            params=parsed.params,
            query=parsed.query,
            fragment=parsed.fragment,
        )
        return urlunparse(sanitized)
    return uri


def create_ssl_connector(client_cert_path, client_key_path, ca_cert_path=None, verify=True):
    """Return an :class:`aiohttp.TCPConnector` configured for mTLS.

    When *verify* is False, server certificate verification is disabled
    (useful for self-signed clusters).
    """
    if not verify:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
    elif ca_cert_path:
        ssl_ctx = ssl.create_default_context(cafile=ca_cert_path)
    else:
        ssl_ctx = ssl.create_default_context()

    ssl_ctx.load_cert_chain(client_cert_path, client_key_path)
    return aiohttp.TCPConnector(ssl=ssl_ctx)


async def get_input_stream_size(uri: str, **input_kwargs: Any) -> int:
    """Return the size (in bytes) of the resource at *uri*, or 0 if unknown."""
    parse = urlparse(uri)
    scheme = parse.scheme.lower()

    if scheme in ("http", "https"):
        session_args = {}
        if all(k in input_kwargs for k in ("cert_file", "key_file")):
            session_args["connector"] = create_ssl_connector(
                input_kwargs["cert_file"],
                input_kwargs["key_file"],
                input_kwargs.get("ca_file"),
                verify=bool(input_kwargs.get("ca_file")),
            )

        async with aiohttp.ClientSession(**session_args) as session:
            # Use HEAD to avoid downloading the full body just for the size.
            try:
                async with session.head(uri, **input_kwargs.get("http_kwargs", {})) as resp:
                    if resp.status < 400:
                        return int(resp.headers.get("Content-Length", 0))
            except Exception:
                pass
            return 0

    elif scheme == "sftp":
        host = parse.hostname
        if not host:
            raise ValueError(f"Missing host in SFTP URI: {uri}")
        port = parse.port or input_kwargs.get("port", 22)
        username = parse.username or input_kwargs.get("username")
        password = parse.password or input_kwargs.get("password")
        path = parse.path
        if not path:
            raise ValueError(f"Missing path in SFTP URI: {uri}")

        conn = await asyncssh.connect(
            host,
            username=username,
            password=password,
            port=port,
            known_hosts=input_kwargs.get("known_hosts", None),
            **input_kwargs.get("sftp_kwargs", {}),
        )
        sftp = await conn.start_sftp_client()
        try:
            return (await sftp.stat(path)).st_size
        finally:
            await sftp.exit()
            await conn.close()

    elif scheme == "file":
        path = _path_from_file_uri(uri)
        if not path:
            raise ValueError(f"Missing path in FILE URI: {uri}")
        return os.path.getsize(path)
    else:
        raise ValueError(f"Unknown scheme in URI: {scheme}")


async def get_input_stream(uri: str, **input_kwargs: Any) -> AsyncGenerator[bytes, None]:
    """Return an async generator yielding chunks from *uri*."""
    parse = urlparse(uri)
    scheme = parse.scheme.lower()

    if scheme in ("http", "https"):
        session_args = {}
        if all(k in input_kwargs for k in ("cert_file", "key_file")):
            session_args["connector"] = create_ssl_connector(
                input_kwargs["cert_file"],
                input_kwargs["key_file"],
                input_kwargs.get("ca_file"),
                verify=bool(input_kwargs.get("ca_file")),
            )

        async def gen():
            async with aiohttp.ClientSession(raise_for_status=True, **session_args) as session:
                async with session.get(uri, **input_kwargs.get("http_kwargs", {})) as resp:
                    total_size = int(resp.headers.get("Content-Length", 0))
                    with async_tqdm(
                        total=total_size,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=parse.hostname + " ->",
                        leave=True,
                    ) as pbar:
                        async for chunk in resp.content.iter_chunked(
                            input_kwargs.get("chunk_size", CHUNK_SIZE)
                        ):
                            if chunk:
                                pbar.update(len(chunk))
                                yield chunk

        return gen()

    elif scheme == "sftp":
        host = parse.hostname
        if not host:
            raise ValueError(f"Missing host in SFTP URI: {uri}")
        port = parse.port or input_kwargs.get("port", 22)
        username = parse.username or input_kwargs.get("username")
        password = parse.password or input_kwargs.get("password")
        path = parse.path
        if not path:
            raise ValueError(f"Missing path in SFTP URI: {uri}")

        conn = await asyncssh.connect(
            host,
            username=username,
            password=password,
            port=port,
            known_hosts=input_kwargs.get("known_hosts", None),
            **input_kwargs.get("sftp_kwargs", {}),
        )
        sftp = await conn.start_sftp_client()
        try:
            total_size = (await sftp.stat(path)).st_size
        except Exception:
            await sftp.exit()
            await conn.close()
            raise

        async def gen():
            file = await sftp.open(path, "rb")
            with async_tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=host + " ->",
                leave=True,
            ) as pbar:
                try:
                    while True:
                        chunk = await file.read(input_kwargs.get("chunk_size", CHUNK_SIZE))
                        if not chunk:
                            break
                        pbar.update(len(chunk))
                        yield chunk
                finally:
                    await file.close()
                    await sftp.exit()
                    await conn.close()

        return gen()

    elif scheme == "file":
        path = _path_from_file_uri(uri)
        if not path:
            raise ValueError(f"Missing path in FILE URI: {uri}")
        total_size = os.path.getsize(path)

        async def gen():
            async with aiofiles.open(path, "rb") as f:
                with async_tqdm(
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=os.path.basename(path) + " ->",
                    leave=True,
                ) as pbar:
                    while True:
                        chunk = await f.read(input_kwargs.get("chunk_size", CHUNK_SIZE))
                        if not chunk:
                            break
                        pbar.update(len(chunk))
                        yield chunk

        return gen()
    else:
        raise ValueError(f"Unknown scheme in URI: {scheme}")


async def consumer_gen(
    total_size: int,
    uri: str,
    q: asyncio.Queue,
    callback: Optional[Callable[[bytes], Optional[bytes]]] = None,
    finalize_callback: Optional[Callable[[], Optional[bytes]]] = None,
) -> AsyncGenerator[bytes, None]:
    """Consume chunks from *q*, optionally transform them, and yield results."""
    with async_tqdm(
        total=total_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=sanitize_uri(uri) + " <-",
        leave=True,
    ) as pbar:
        while True:
            item = await q.get()
            if item is None:
                if finalize_callback:
                    final = finalize_callback()
                    if final is not None:
                        yield final
                break
            pbar.update(len(item))
            if callback:
                result = callback(item)
                if result is not None:
                    yield result
            else:
                yield item


async def process_stream(
    input_uri: str,
    output_uris: list[str],
    input_kwargs: dict[str, Any] | None = None,
    output_kwargs_list: list[dict[str, Any]] | None = None,
    rate_limit: float | None = None,
) -> None:
    """Pipeline *input_uri* to every URI in *output_uris* via fan-out queues.

    *rate_limit* caps the input read rate to the given number of bytes per
    second.  ``0`` or ``None`` means unlimited.
    """
    if input_kwargs is None:
        input_kwargs = {}
    if output_kwargs_list is None:
        output_kwargs_list = [{} for _ in output_uris]
    if len(output_uris) != len(output_kwargs_list):
        raise ValueError("Number of output URIs must match output kwargs list.")

    input_size = await get_input_stream_size(input_uri, **input_kwargs)
    input_gen = await get_input_stream(input_uri, **input_kwargs)

    all_queues = [
        asyncio.Queue(maxsize=kwargs.get("max_queue_size", MAX_QUEUE_SIZE))
        for kwargs in output_kwargs_list
    ]

    tasks = []
    for uri, kw, q in zip(output_uris, output_kwargs_list, all_queues):
        gen = consumer_gen(
            input_size,
            uri,
            q,
            callback=kw.get("callback"),
            finalize_callback=kw.get("finalize_callback"),
        )
        scheme = urlparse(uri).scheme.lower()

        if scheme in ("http", "https"):
            session_args = {}
            if all(k in kw for k in ("cert_file", "key_file")):
                session_args["connector"] = create_ssl_connector(
                    kw["cert_file"],
                    kw["key_file"],
                    kw.get("ca_file"),
                    verify=bool(kw.get("ca_file")),
                )

            async def post_task(uri=uri, kw=kw, gen=gen, sa=session_args):
                async with aiohttp.ClientSession(raise_for_status=True, **sa) as session:
                    async with session.post(uri, data=gen, **kw.get("http_kwargs", {})) as resp:
                        resp.raise_for_status()
                        if resp.status in (200, 202):
                            return await resp.json()
                        else:
                            error_text = await resp.text()
                            raise Exception(f"Failed to add image: {resp.status} - {error_text}")

            tasks.append(asyncio.create_task(post_task()))

        elif scheme == "sftp":

            async def sftp_task(uri=uri, kw=kw, gen=gen):
                parse = urlparse(uri)
                host = parse.hostname
                if not host:
                    raise ValueError(f"Missing host in SFTP URI: {uri}")
                port = parse.port or kw.get("port", 22)
                username = parse.username or kw.get("username")
                password = parse.password or kw.get("password")
                path = parse.path
                if not path:
                    raise ValueError(f"Missing path in SFTP URI: {uri}")

                async with asyncssh.connect(
                    host,
                    username=username,
                    password=password,
                    port=port,
                    known_hosts=kw.get("known_hosts"),
                    **kw.get("sftp_kwargs", {}),
                ) as conn:
                    async with conn.start_sftp_client() as sftp:
                        parent = os.path.dirname(path)
                        if parent:
                            await sftp.makedirs(parent, exist_ok=True)
                        async with sftp.open(path, "wb") as file:
                            async for chunk in gen:
                                await file.write(chunk)

            tasks.append(asyncio.create_task(sftp_task()))

        elif scheme == "file":

            async def file_task(uri=uri, kw=kw, gen=gen):
                path = _path_from_file_uri(uri)
                if not path:
                    raise ValueError(f"Missing path in FILE URI: {uri}")
                async with aiofiles.open(path, "wb") as f:
                    async for chunk in gen:
                        await f.write(chunk)

            tasks.append(asyncio.create_task(file_task()))
        else:
            raise ValueError(f"Unknown scheme for output: {scheme}")

    rate_limiter = TokenBucketRateLimiter(rate_limit) if rate_limit else None

    async for chunk in input_gen:
        if rate_limiter:
            await rate_limiter.acquire(len(chunk))
        for q in all_queues:
            await q.put(chunk)

    for q in all_queues:
        await q.put(None)

    await asyncio.gather(*tasks)


async def stream_main(stream_pairs: list[dict[str, Any]]) -> None:
    """Process multiple stream pairs concurrently."""
    coros = [
        process_stream(
            pair["input_uri"],
            pair["output_uris"],
            input_kwargs=pair.get("input_kwargs", {}),
            output_kwargs_list=pair.get("output_kwargs_list"),
            rate_limit=pair.get("rate_limit"),
        )
        for pair in stream_pairs
    ]
    await asyncio.gather(*coros)


def streamer(stream_pairs: list[dict[str, Any]]):
    """Synchronous wrapper around :func:`stream_main`."""
    asyncio.run(stream_main(stream_pairs))
