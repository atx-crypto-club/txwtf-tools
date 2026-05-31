"""
Async stream relay — consumer/producer pattern for piping data between
HTTP(S), SFTP, and local file endpoints without temporary files on disk.

Supports fan-out to multiple destinations with backpressure via asyncio queues,
optional per-chunk processing (compression, encryption, etc.), and tqdm progress bars.
"""

import asyncio
import os
import ssl
import time
from typing import Callable
from urllib.parse import urlparse

import aiofiles
import aiohttp
import asyncssh
from tqdm.asyncio import tqdm


class TokenBucketRateLimiter:
    """Async token-bucket rate limiter.

    Tokens represent bytes.  Call :meth:`acquire` before emitting each
    chunk to throttle throughput to *rate* bytes per second.  A value of
    ``0`` or ``None`` disables the limiter (no waiting).
    """

    def __init__(self, rate: float | None):
        self._rate = rate or 0
        # Start with one chunk_size worth of burst (capped at 1 second of
        # tokens) so the very first read isn't delayed, while still
        # preventing a large initial burst.
        self._tokens = float(self._rate) if self._rate else 0
        self._last = time.monotonic()

    async def acquire(self, amount: int) -> None:
        if not self._rate:
            return
        while True:
            now = time.monotonic()
            elapsed = now - self._last
            self._last = now
            self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
            if self._tokens >= amount:
                self._tokens -= amount
                return
            deficit = amount - self._tokens
            await asyncio.sleep(deficit / self._rate)


def create_ssl_value(ssl_config: dict | None, is_https: bool):
    """
    Create the ssl value for aiohttp connector based on config.
    - For HTTP: False
    - For HTTPS: SSLContext or True (default verify)
    """
    if not is_https:
        return False

    if ssl_config is None:
        return True  # Default: verify with system CAs

    context = ssl.create_default_context()

    verify = ssl_config.get("verify", True)
    if not verify:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    else:
        ca_cert = ssl_config.get("ca_cert")
        if ca_cert:
            context.load_verify_locations(cafile=ca_cert)

        server_cert = ssl_config.get("server_cert")
        if server_cert:
            context.load_verify_locations(cafile=server_cert)

    client_cert = ssl_config.get("client_cert")
    client_key = ssl_config.get("client_key")
    client_passwd = ssl_config.get("client_passwd")
    if client_cert and client_key:
        context.load_cert_chain(certfile=client_cert, keyfile=client_key, password=client_passwd)

    return context


async def http_consumer(
    get_url: str,
    queues: list[asyncio.Queue],
    pbar: tqdm,
    headers: dict,
    chunk_size: int,
    config: dict | None,
    rate_limiter: TokenBucketRateLimiter | None = None,
    get_process_func: Callable[[bytes], bytes] | None = None,
):
    """Streams data from HTTP/HTTPS GET request and fans out chunks into queues."""
    is_https = get_url.startswith("https://")
    ssl_val = create_ssl_value(config, is_https)
    connector = aiohttp.TCPConnector(ssl=ssl_val)

    timeout_dict = config.get("timeout", {"total": 300}) if config else {"total": 300}
    timeout = aiohttp.ClientTimeout(**timeout_dict)

    loop = asyncio.get_running_loop()
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async with session.get(get_url, headers=headers) as resp:
            if resp.status not in (200, 202):
                raise ValueError(f"GET request failed with status {resp.status}")

            content_length = int(resp.headers.get("Content-Length", 0))
            pbar.total = content_length

            async for chunk in resp.content.iter_chunked(chunk_size):
                if rate_limiter:
                    await rate_limiter.acquire(len(chunk))
                pbar.update(len(chunk))
                if get_process_func:
                    chunk = await loop.run_in_executor(None, get_process_func, chunk)
                    if not chunk:
                        continue
                await asyncio.gather(*(q.put(chunk) for q in queues))

            await asyncio.gather(*(q.put(None) for q in queues))


async def sftp_consumer(
    get_url: str,
    queues: list[asyncio.Queue],
    pbar: tqdm,
    chunk_size: int,
    config: dict | None,
    rate_limiter: TokenBucketRateLimiter | None = None,
    get_process_func: Callable[[bytes], bytes] | None = None,
):
    """Streams data from SFTP file and fans out chunks into queues."""
    parsed = urlparse(get_url)
    host = parsed.hostname
    port = parsed.port or 22
    path = parsed.path

    if config is None:
        config = {}

    connect_kwargs = {
        "host": host,
        "port": port,
        "known_hosts": None,
    }
    if parsed.username:
        connect_kwargs["username"] = parsed.username
    if parsed.password:
        connect_kwargs["password"] = parsed.password
    connect_kwargs.update(config)

    loop = asyncio.get_running_loop()
    async with asyncssh.connect(**connect_kwargs) as conn:
        async with conn.start_sftp_client() as sftp:
            stat = await sftp.stat(path)
            pbar.total = stat.size

            async with await sftp.open(path, "rb") as f:
                while True:
                    chunk = await f.read(chunk_size)
                    if not chunk:
                        break
                    if rate_limiter:
                        await rate_limiter.acquire(len(chunk))
                    pbar.update(len(chunk))
                    if get_process_func:
                        chunk = await loop.run_in_executor(None, get_process_func, chunk)
                        if not chunk:
                            continue
                    await asyncio.gather(*(q.put(chunk) for q in queues))

            await asyncio.gather(*(q.put(None) for q in queues))


async def file_consumer(
    get_url: str,
    queues: list[asyncio.Queue],
    pbar: tqdm,
    chunk_size: int,
    rate_limiter: TokenBucketRateLimiter | None = None,
    get_process_func: Callable[[bytes], bytes] | None = None,
):
    """Streams data from local file and fans out chunks into queues."""
    parsed = urlparse(get_url)
    path = parsed.path

    content_length = os.path.getsize(path)
    pbar.total = content_length

    loop = asyncio.get_running_loop()
    async with aiofiles.open(path, "rb") as f:
        while True:
            chunk = await f.read(chunk_size)
            if not chunk:
                break
            if rate_limiter:
                await rate_limiter.acquire(len(chunk))
            pbar.update(len(chunk))
            if get_process_func:
                chunk = await loop.run_in_executor(None, get_process_func, chunk)
                if not chunk:
                    continue
            await asyncio.gather(*(q.put(chunk) for q in queues))

    await asyncio.gather(*(q.put(None) for q in queues))


async def consume(
    get_url: str,
    queues: list[asyncio.Queue],
    pbar: tqdm,
    headers: dict,
    chunk_size: int,
    config: dict | None,
    rate_limiter: TokenBucketRateLimiter | None = None,
    get_process_func: Callable[[bytes], bytes] | None = None,
):
    """General consumer dispatcher based on URL scheme."""
    scheme = urlparse(get_url).scheme.lower()
    if scheme in ("http", "https"):
        await http_consumer(get_url, queues, pbar, headers, chunk_size, config, rate_limiter, get_process_func)
    elif scheme == "sftp":
        await sftp_consumer(get_url, queues, pbar, chunk_size, config, rate_limiter, get_process_func)
    elif scheme == "file":
        await file_consumer(get_url, queues, pbar, chunk_size, rate_limiter, get_process_func)
    else:
        raise ValueError(f"Unsupported scheme: {scheme}")


async def process_and_yield(queue: asyncio.Queue, process_func, out_pbar: tqdm):
    """
    Async generator that pulls from queue, processes chunks (in executor if CPU-bound),
    and yields processed chunks for streaming output.

    Empty results from *process_func* (e.g. a stateful decryptor that hasn't
    accumulated a full token yet) are silently skipped — this avoids sending a
    zero-length chunk in HTTP chunked transfer encoding, which would terminate
    the stream prematurely.
    """
    loop = asyncio.get_running_loop()
    while True:
        chunk = await queue.get()
        if chunk is None:
            break
        processed = await loop.run_in_executor(None, process_func, chunk)
        if processed:
            out_pbar.update(len(processed))
            yield processed


async def http_producer(
    post_url: str,
    queue: asyncio.Queue,
    process_func,
    headers: dict,
    out_pbar: tqdm,
    config: dict | None,
):
    """Streams processed data to HTTP/HTTPS POST request."""
    is_https = post_url.startswith("https://")
    ssl_val = create_ssl_value(config, is_https)
    connector = aiohttp.TCPConnector(ssl=ssl_val)

    timeout_dict = config.get("timeout", {"total": 300}) if config else {"total": 300}
    timeout = aiohttp.ClientTimeout(**timeout_dict)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async with session.post(
            post_url, headers=headers, data=process_and_yield(queue, process_func, out_pbar)
        ) as resp:
            if resp.status not in (200, 202):
                raise ValueError(f"POST request failed with status {resp.status}")
            await resp.read()


async def sftp_producer(
    post_url: str,
    queue: asyncio.Queue,
    process_func,
    out_pbar: tqdm,
    config: dict | None,
):
    """Streams processed data to SFTP file."""
    parsed = urlparse(post_url)
    host = parsed.hostname
    port = parsed.port or 22
    path = parsed.path

    if config is None:
        config = {}

    connect_kwargs = {
        "host": host,
        "port": port,
        "known_hosts": None,
    }
    if parsed.username:
        connect_kwargs["username"] = parsed.username
    if parsed.password:
        connect_kwargs["password"] = parsed.password
    connect_kwargs.update(config)

    async with asyncssh.connect(**connect_kwargs) as conn:
        async with conn.start_sftp_client() as sftp:
            async with await sftp.open(path, "wb") as f:
                async for processed in process_and_yield(queue, process_func, out_pbar):
                    await f.write(processed)


async def file_producer(
    post_url: str,
    queue: asyncio.Queue,
    process_func,
    out_pbar: tqdm,
):
    """Streams processed data to local file."""
    parsed = urlparse(post_url)
    path = parsed.path

    async with aiofiles.open(path, "wb") as f:
        async for processed in process_and_yield(queue, process_func, out_pbar):
            await f.write(processed)


async def produce(
    post_url: str,
    queue: asyncio.Queue,
    process_func,
    headers: dict,
    out_pbar: tqdm,
    config: dict | None,
):
    """General producer dispatcher based on URL scheme."""
    scheme = urlparse(post_url).scheme.lower()
    if scheme in ("http", "https"):
        await http_producer(post_url, queue, process_func, headers, out_pbar, config)
    elif scheme == "sftp":
        await sftp_producer(post_url, queue, process_func, out_pbar, config)
    elif scheme == "file":
        await file_producer(post_url, queue, process_func, out_pbar)
    else:
        raise ValueError(f"Unsupported scheme: {scheme}")


async def monitor_queues(queues: list[asyncio.Queue], queue_pbars: list[tqdm], stop_event: asyncio.Event):
    """Periodically updates tqdm bars showing queue fill levels."""
    prev_sizes = [0] * len(queues)
    while not stop_event.is_set():
        for i, (q, pbar) in enumerate(zip(queues, queue_pbars)):
            size = q.qsize()
            rate = size - prev_sizes[i]
            pbar.set_description(f"Queue {i} (delta={rate})")
            pbar.n = size
            pbar.refresh()
            prev_sizes[i] = size
        await asyncio.sleep(1)


async def relay_stream(
    get_url: str,
    post_urls: list[str],
    process_func: Callable[[bytes], bytes] | None = None,
    queue_maxsize: int = 10,
    get_headers: dict | None = None,
    post_headers: list[dict] | None = None,
    chunk_size: int = 1024 * 1024,
    monitor_queues_flag: bool = False,
    get_config: dict | None = None,
    post_configs: list[dict | None] | None = None,
    per_queue_maxsize: list[int] | None = None,
    rate_limit: float | None = None,
    get_process_func: Callable[[bytes], bytes] | None = None,
):
    """
    Main relay function.

    Reads from *get_url* (http/https/sftp/file), optionally transforms each chunk
    with *process_func*, and streams to every URL in *post_urls* concurrently.

    *get_process_func* is applied on the **input** side — each chunk read from
    *get_url* is transformed before being placed into the per-destination queues.
    Use this for decryption of an encrypted source.

    *process_func* is applied on the **output** side — each chunk pulled from a
    queue is transformed before being written to the destination.  Use this for
    encryption of the outgoing stream.

    Both may be combined (e.g. decrypt with one key on input, re-encrypt with a
    different key on output).

    Backpressure is handled via bounded asyncio queues (one per destination).
    Queue puts happen concurrently so a slow consumer cannot block faster ones.

    Use *per_queue_maxsize* to give individual destinations different buffer depths
    (e.g. a larger buffer for a known-slow sink).  Falls back to *queue_maxsize*.

    *rate_limit* caps the input read rate to the given number of bytes per second.
    ``0`` or ``None`` means unlimited.
    """
    if process_func is None:
        process_func = lambda x: x  # noqa: E731

    if get_headers is None:
        get_headers = {}

    if post_headers is None:
        post_headers = [{} for _ in post_urls]
    elif len(post_headers) != len(post_urls):
        raise ValueError("post_headers must match the number of post_urls")

    if post_configs is None:
        post_configs = [None for _ in post_urls]
    elif len(post_configs) != len(post_urls):
        raise ValueError("post_configs must match the number of post_urls")

    if per_queue_maxsize is None:
        per_queue_maxsize = [queue_maxsize] * len(post_urls)
    elif len(per_queue_maxsize) != len(post_urls):
        raise ValueError("per_queue_maxsize must match the number of post_urls")

    queues = [asyncio.Queue(maxsize=sz) for sz in per_queue_maxsize]

    rate_limiter = TokenBucketRateLimiter(rate_limit) if rate_limit else None

    in_pbar = tqdm(total=0, unit="B", unit_scale=True, desc="Input stream")
    out_pbars = [
        tqdm(total=in_pbar.total, unit="B", unit_scale=True, desc=f"Output stream {i}")
        for i in range(len(post_urls))
    ]

    queue_pbars = None
    if monitor_queues_flag:
        queue_pbars = [
            tqdm(total=queue_maxsize, unit="chunks", desc=f"Queue {i} (delta=0)", leave=True)
            for i in range(len(queues))
        ]

    consumer_task = asyncio.create_task(consume(get_url, queues, in_pbar, get_headers, chunk_size, get_config, rate_limiter, get_process_func))

    producer_tasks = [
        asyncio.create_task(produce(post_url, queue, process_func, post_header, out_pbar, post_config))
        for post_url, queue, post_header, out_pbar, post_config in zip(
            post_urls, queues, post_headers, out_pbars, post_configs
        )
    ]

    stop_event = None
    monitor_task = None
    if monitor_queues_flag:
        stop_event = asyncio.Event()
        monitor_task = asyncio.create_task(monitor_queues(queues, queue_pbars, stop_event))

    await asyncio.gather(consumer_task, *producer_tasks)

    if monitor_queues_flag:
        stop_event.set()
        await monitor_task
        for pbar in queue_pbars:
            pbar.close()

    in_pbar.close()
    for out_pbar in out_pbars:
        out_pbar.close()


def relay(**kwargs):
    """Synchronous wrapper around :func:`relay_stream`."""
    asyncio.run(relay_stream(**kwargs))
