import asyncio
import aiohttp
from tqdm.asyncio import tqdm
import functools
import ssl
from urllib.parse import urlparse
import asyncssh
import aiofiles
import os

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
    
    verify = ssl_config.get('verify', True)
    if not verify:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    else:
        ca_cert = ssl_config.get('ca_cert')
        if ca_cert:
            context.load_verify_locations(cafile=ca_cert)
        
        server_cert = ssl_config.get('server_cert')
        if server_cert:
            context.load_verify_locations(cafile=server_cert)
    
    client_cert = ssl_config.get('client_cert')
    client_key = ssl_config.get('client_key')
    client_passwd = ssl_config.get('client_passwd')
    if client_cert and client_key:
        context.load_cert_chain(certfile=client_cert, keyfile=client_key, password=client_passwd)
    
    return context

async def http_consumer(get_url: str, queues: list[asyncio.Queue], pbar: tqdm, headers: dict, chunk_size: int, config: dict | None):
    """
    HTTP/HTTPS consumer: Streams data from GET request and puts chunks into each queue.
    """
    is_https = get_url.startswith('https://')
    ssl_val = create_ssl_value(config, is_https)
    connector = aiohttp.TCPConnector(ssl=ssl_val)
    
    timeout_dict = config.get('timeout', {'total': 300}) if config else {'total': 300}
    timeout = aiohttp.ClientTimeout(**timeout_dict)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async with session.get(get_url, headers=headers) as resp:
            if resp.status != 200 and resp.status != 202:
                raise ValueError(f"GET request failed with status {resp.status}")
            
            content_length = int(resp.headers.get('Content-Length', 0))
            pbar.total = content_length  # Set total if available
            
            async for chunk in resp.content.iter_chunked(chunk_size):
                for q in queues:
                    await q.put(chunk)
                pbar.update(len(chunk))
            
            # Send sentinel to each queue to signal end
            for q in queues:
                await q.put(None)

async def sftp_consumer(get_url: str, queues: list[asyncio.Queue], pbar: tqdm, chunk_size: int, config: dict | None):
    """
    SFTP consumer: Streams data from SFTP file and puts chunks into each queue.
    """
    parsed = urlparse(get_url)
    host = parsed.hostname
    port = parsed.port or 22
    path = parsed.path
    
    if config is None:
        config = {}
    
    connect_kwargs = {
        'host': host,
        'port': port,
        'known_hosts': None,  # Disable host key verification by default; set in config if needed
    }
    if parsed.username:
        connect_kwargs['username'] = parsed.username
    if parsed.password:
        connect_kwargs['password'] = parsed.password
    connect_kwargs.update(config)
    
    async with asyncssh.connect(**connect_kwargs) as conn:
        async with conn.start_sftp_client() as sftp:
            stat = await sftp.stat(path)
            pbar.total = stat.size
            
            async with await sftp.open(path, 'rb') as f:
                while True:
                    chunk = await f.read(chunk_size)
                    if not chunk:
                        break
                    for q in queues:
                        await q.put(chunk)
                    pbar.update(len(chunk))
            
            # Send sentinel to each queue to signal end
            for q in queues:
                await q.put(None)

async def file_consumer(get_url: str, queues: list[asyncio.Queue], pbar: tqdm, chunk_size: int):
    """
    File consumer: Streams data from local file and puts chunks into each queue.
    """
    parsed = urlparse(get_url)
    path = parsed.path
    
    content_length = os.path.getsize(path)
    pbar.total = content_length
    
    async with aiofiles.open(path, 'rb') as f:
        while True:
            chunk = await f.read(chunk_size)
            if not chunk:
                break
            for q in queues:
                await q.put(chunk)
            pbar.update(len(chunk))
    
    # Send sentinel to each queue to signal end
    for q in queues:
        await q.put(None)

async def consumer(get_url: str, queues: list[asyncio.Queue], pbar: tqdm, headers: dict, chunk_size: int, config: dict | None):
    """
    General consumer dispatcher based on URL scheme.
    """
    scheme = urlparse(get_url).scheme.lower()
    if scheme in ('http', 'https'):
        await http_consumer(get_url, queues, pbar, headers, chunk_size, config)
    elif scheme == 'sftp':
        await sftp_consumer(get_url, queues, pbar, chunk_size, config)
    elif scheme == 'file':
        await file_consumer(get_url, queues, pbar, chunk_size)
    else:
        raise ValueError(f"Unsupported scheme: {scheme}")

async def process_and_yield(queue: asyncio.Queue, process_func, out_pbar: tqdm):
    """
    Async generator that pulls from queue, processes chunks (in executor if CPU-bound),
    and yields processed chunks for streaming output. Updates output progress bar.
    """
    loop = asyncio.get_running_loop()
    while True:
        chunk = await queue.get()
        if chunk is None:
            break
        # If process_func is CPU-bound, run it in a thread pool executor to avoid blocking the event loop
        processed = await loop.run_in_executor(None, process_func, chunk)
        out_pbar.update(len(processed))  # Update with processed size
        yield processed

async def http_producer(post_url: str, queue: asyncio.Queue, process_func, headers: dict, out_pbar: tqdm, config: dict | None):
    """
    HTTP/HTTPS producer: Streams processed data to POST request.
    """
    is_https = post_url.startswith('https://')
    ssl_val = create_ssl_value(config, is_https)
    connector = aiohttp.TCPConnector(ssl=ssl_val)
    
    timeout_dict = config.get('timeout', {'total': 300}) if config else {'total': 300}
    timeout = aiohttp.ClientTimeout(**timeout_dict)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async with session.post(post_url, headers=headers, data=process_and_yield(queue, process_func, out_pbar)) as resp:
            if resp.status != 200 and resp.status != 202:  # Adjust based on API (LXD might use 202 or other)
                raise ValueError(f"POST request failed with status {resp.status}")
            # Optionally read response if needed
            await resp.read()

async def sftp_producer(post_url: str, queue: asyncio.Queue, process_func, out_pbar: tqdm, config: dict | None):
    """
    SFTP producer: Streams processed data to SFTP file.
    """
    parsed = urlparse(post_url)
    host = parsed.hostname
    port = parsed.port or 22
    path = parsed.path
    
    if config is None:
        config = {}
    
    connect_kwargs = {
        'host': host,
        'port': port,
        'known_hosts': None,  # Disable host key verification by default; set in config if needed
    }
    if parsed.username:
        connect_kwargs['username'] = parsed.username
    if parsed.password:
        connect_kwargs['password'] = parsed.password
    connect_kwargs.update(config)
    
    async with asyncssh.connect(**connect_kwargs) as conn:
        async with conn.start_sftp_client() as sftp:
            async with await sftp.open(path, 'wb') as f:
                async for processed in process_and_yield(queue, process_func, out_pbar):
                    await f.write(processed)

async def file_producer(post_url: str, queue: asyncio.Queue, process_func, out_pbar: tqdm):
    """
    File producer: Streams processed data to local file.
    """
    parsed = urlparse(post_url)
    path = parsed.path
    
    async with aiofiles.open(path, 'wb') as f:
        async for processed in process_and_yield(queue, process_func, out_pbar):
            await f.write(processed)

async def producer(post_url: str, queue: asyncio.Queue, process_func, headers: dict, out_pbar: tqdm, config: dict | None):
    """
    General producer dispatcher based on URL scheme.
    """
    scheme = urlparse(post_url).scheme.lower()
    if scheme in ('http', 'https'):
        await http_producer(post_url, queue, process_func, headers, out_pbar, config)
    elif scheme == 'sftp':
        await sftp_producer(post_url, queue, process_func, out_pbar, config)
    elif scheme == 'file':
        await file_producer(post_url, queue, process_func, out_pbar)
    else:
        raise ValueError(f"Unsupported scheme: {scheme}")

async def monitor_queues(queues: list[asyncio.Queue], queue_pbars: list[tqdm], stop_event: asyncio.Event):
    """
    Monitor task: Periodically updates tqdm progress bars for queue sizes and fill rates.
    Each bar represents the current fill level of the queue (0 to maxsize).
    """
    prev_sizes = [0] * len(queues)
    while not stop_event.is_set():
        for i, (q, pbar) in enumerate(zip(queues, queue_pbars)):
            size = q.qsize()
            rate = size - prev_sizes[i]
            pbar.set_description(f"Queue {i} (delta={rate})")
            pbar.n = size
            pbar.refresh()
            prev_sizes[i] = size
        await asyncio.sleep(1)  # Monitor every second

async def relay_stream(get_url: str, post_urls: list[str], process_func=None, queue_maxsize=10,
                       get_headers: dict = None, post_headers: list[dict] = None,
                       chunk_size: int = 1024 * 1024, monitor_queues_flag: bool = False,
                       get_config: dict | None = None, post_configs: list[dict | None] = None):
    """
    Main relay function.
    - get_url: Source URL for input stream (http/https/sftp/file).
    - post_urls: List of destination URLs for output streams (http/https/sftp/file).
    - process_func: Optional function to process each chunk (e.g., compress, encrypt). Defaults to identity.
    - queue_maxsize: Max size for each queue to handle backpressure.
    - get_headers: Custom headers for GET request (if http/https).
    - post_headers: List of custom headers for each POST request (if http/https; must match len(post_urls)).
    - chunk_size: Size of chunks to read from input stream.
    - monitor_queues_flag: If True, starts a monitor task with tqdm bars for queue sizes.
    - get_config: Dict for input config (SSL keys for https, SSH keys for sftp, timeout for http/https).
    - post_configs: List of dicts for each output config (SSL or SSH keys or timeout based on scheme).
    """
    if process_func is None:
        process_func = lambda x: x  # Identity function
    
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
    
    # Create a queue for each output stream
    queues = [asyncio.Queue(maxsize=queue_maxsize) for _ in post_urls]
    
    # Shared input progress bar (tracks input bytes read)
    in_pbar = tqdm(total=0, unit='B', unit_scale=True, desc="Input stream")
    
    # Output progress bars (one per producer, tracks output bytes written)
    out_pbars = [tqdm(total=in_pbar.total, unit='B', unit_scale=True, desc=f"Output stream {i}") for i in range(len(post_urls))]
    # Note: out_pbar.total is initially 0; for SFTP/HTTP without length, it remains indeterminate.
    
    # Queue monitor pbars if flag is set
    queue_pbars = None
    if monitor_queues_flag:
        queue_pbars = [tqdm(total=queue_maxsize, unit='chunks', desc=f"Queue {i} (delta=0)", leave=True) for i in range(len(queues))]
    
    # Create consumer task
    consumer_task = asyncio.create_task(consumer(get_url, queues, in_pbar, get_headers, chunk_size, get_config))
    
    # Create producer tasks
    producer_tasks = [
        asyncio.create_task(producer(post_url, queue, process_func, post_header, out_pbar, post_config))
        for post_url, queue, post_header, out_pbar, post_config in zip(post_urls, queues, post_headers, out_pbars, post_configs)
    ]
    
    tasks = [consumer_task] + producer_tasks
    
    if monitor_queues_flag:
        stop_event = asyncio.Event()
        monitor_task = asyncio.create_task(monitor_queues(queues, queue_pbars, stop_event))
        tasks.append(monitor_task)
    
    # Wait for core tasks to complete
    await asyncio.gather(consumer_task, *producer_tasks)
    
    if monitor_queues_flag:
        stop_event.set()
        await monitor_task
        for pbar in queue_pbars:
            pbar.close()
    
    in_pbar.close()
    for out_pbar in out_pbars:
        out_pbar.close()

def relay(**args):
    asyncio.run(relay_stream(**args))

# Example usage:
# async def main():
#     get_url = "file:///path/to/source/file"
#     post_urls = ["file:///path/to/dest/file", "https://dest-server/upload"]
#     post_configs = [None, {'client_cert': '/path/to/client.crt', 'client_key': '/path/to/client.key', 'timeout': {'total': 600, 'sock_read': 600}}]
#     post_headers = [{}, {"Content-Type": "application/octet-stream"}]
#     # Example process_func for compression (using snappy if available)
#     # import snappy
#     # process_func = snappy.compress
#     await relay_stream(get_url, post_urls, post_headers=post_headers, post_configs=post_configs, monitor_queues_flag=True)
#
# asyncio.run(main())

# Notes:
# - Requires `pip install asyncssh aiofiles` for SFTP and local file support.
# - For SSH configs, common keys: 'username', 'password', 'client_keys' (str or list of str paths), 'known_hosts' (None to disable verification, or path).
# - If using key auth, generate keys with ssh-keygen and add public key to server ~/.ssh/authorized_keys.
# - For HTTP/HTTPS, headers and config (SSL keys) are used; for SFTP/file, headers and config ignored.
# - Username and password from URL are added to connect_kwargs for SFTP, overridden by config if provided.
# - Mixed protocols work via queues providing buffering and backpressure to prevent starvation/deadlocks.
# - If process_func changes sizes (e.g., compression), output pbars may not match input total; adjust manually if needed.
# - Chunk size defaults to 1MB; adjust based on needs.
# - For security, avoid passwords in code or URLs; use key auth or environment variables.
# - Changed to use tqdm.asyncio for better compatibility with async code, which should fix progress bar display issues.
# - For file:// URLs, use absolute paths like file:///path/to/file; relative paths may not work reliably.
# - For HTTP/HTTPS, timeouts can be set via config['timeout'] = {'total': 300, 'connect': None, 'sock_connect': None, 'sock_read': None, ...}; defaults to {'total': 300}.