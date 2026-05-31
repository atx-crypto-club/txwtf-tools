import asyncio
import aiohttp
import aiofiles
import asyncssh  # Requires 'pip install asyncssh' for SFTP support; use if needed.
import ssl
import os.path
from tqdm.asyncio import tqdm as async_tqdm  # For async progress bars
from typing import Callable, List, Optional, AsyncGenerator, Dict, Any, Tuple
from urllib.parse import urlparse, urlunparse, ParseResult


CHUNK_SIZE = 1024 * 1024  # 1MB
MAX_QUEUE_SIZE = 128  # Maximum number of chunks per queue


def sanitize_uri(uri: str) -> str:
    """
    Sanitizes the URI by obscuring the password if present.
    Replaces password with '***' in the netloc part.
    """
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
            fragment=parsed.fragment
        )
        return urlunparse(sanitized)
    return uri


def create_ssl_connector(client_cert_path, client_key_path, ca_cert_path=None):
    """
    Returns a TCPConnector with client certificate authentication.

    Args:
        url (str): The URL to make the request to.
        client_cert_path (str): Path to the client's public key certificate file (e.g., 'client.crt').
        client_key_path (str): Path to the client's private key file (e.g., 'client.key').
        ca_cert_path (str, optional): Path to the Certificate Authority (CA) bundle file 
                                     to verify the server's certificate (e.g., 'ca-bundle.crt').
                                     If None, default system CAs will be used for server certificate validation.
    """
    # Create an SSLContext object
    # For client authentication (mTLS), use ssl.Purpose.CLIENT_AUTH
    if ca_cert_path:
        ssl_ctx = ssl.create_default_context(cafile=ca_cert_path)
    else:
        ssl_ctx = ssl.create_default_context()

    # Load the client's certificate and private key
    ssl_ctx.load_cert_chain(client_cert_path, client_key_path)

    # Create a TCPConnector with the custom SSLContext
    # Prior to aiohttp 3.0, you might have used `ssl_context` directly,
    # but the current recommendation is to use the `ssl` argument in ClientSession
    # or pass a connector with the `ssl_context` configured.
    return aiohttp.TCPConnector(ssl=ssl_ctx)


async def get_input_stream_size(
        uri: str,
        **input_kwargs: Any
) -> int:
    """
    Returns the size of the input stream. In the case of http it returns the Content-Length header.
    For SFTP it stats the file.
    """
    parse = urlparse(uri)
    scheme = parse.scheme.lower()
    
    if scheme in ('http', 'https'):
        session_args = {}
        if all([k in input_kwargs.keys() for k in ["cert_file", "key_file"]]):
            session_args["connector"] =  create_ssl_connector(
                input_kwargs['cert_file'],
                input_kwargs['key_file'],
                input_kwargs.get('ca_file', None))
        
        async with aiohttp.ClientSession(raise_for_status=True, **session_args) as session:
            async with session.get(
                uri, 
                **input_kwargs.get(
                    'http_kwargs', {})) as resp:
                return int(resp.headers.get('Content-Length', 0))
    elif scheme == 'sftp':
        host = parse.hostname
        if not host:
            raise ValueError(f"Missing host in SFTP URI: {uri}")
        port = parse.port or input_kwargs.get('port', 22)
        username = parse.username or input_kwargs.get('username')
        password = parse.password or input_kwargs.get('password')
        path = parse.path
        if not path:
            raise ValueError(f"Missing path in SFTP URI: {uri}")
        
        conn = await asyncssh.connect(
            host,
            username=username,
            password=password,
            port=port,
            known_hosts=input_kwargs.get('known_hosts', None),  # Set to None for testing; insecure!
            **input_kwargs.get('sftp_kwargs', {})
        )
        sftp = await conn.start_sftp_client()
        try:
            return (await sftp.stat(path)).st_size
        except Exception as e:
            raise e
        finally:
            await sftp.exit()
            await conn.close()
    elif scheme == 'file':
        path = parse.path
        if not path:
            raise ValueError(f"Missing path in FILE URI: {uri}")
        return os.path.getsize(path)
    else:
        raise ValueError(f"Unknown scheme in URI: {scheme}")


async def get_input_stream(
    uri: str,
    **input_kwargs: Any
) -> AsyncGenerator[bytes, None]:
    """
    Asynchronous function that returns the total content length (if available) and
    an async generator yielding binary chunks from the input source.
    
    Parses the URI scheme to determine type: 'http'/'https' for HTTP, 'sftp' for SFTP.
    Extracts host, port, username, password, path from URI; overrides with input_kwargs if provided.
    
    For HTTP: Optional input_kwargs['http_kwargs'] for headers, etc.
    Optional input_kwargs['cert_file'] and input_kwargs['key_file'] for client certificate authentication (both required for cert auth).
    For SFTP: Optional input_kwargs['known_hosts'], input_kwargs['sftp_kwargs']; overrides for username/password/port if not in URI.
    
    Returns (total_size, generator). total_size is 0 if unknown.
    Raises ValueError for unknown scheme.
    """
    parse = urlparse(uri)
    scheme = parse.scheme.lower()
    
    if scheme in ('http', 'https'):
        session_args = {}
        if all([k in input_kwargs.keys() for k in ["cert_file", "key_file"]]):
            session_args["connector"] =  create_ssl_connector(
                input_kwargs['cert_file'],
                input_kwargs['key_file'],
                input_kwargs.get('ca_file', None))
        
        async def gen():
            async with aiohttp.ClientSession(raise_for_status=True, **session_args) as session:
                async with session.get(
                    uri, 
                    **input_kwargs.get(
                        'http_kwargs', {})) as resp:
                    total_size = int(resp.headers.get('Content-Length', 0))
                    
                    # Set up tqdm progress bar (unit_scale for human-readable sizes like KB/MB)
                    with async_tqdm(
                        total=total_size,
                        unit='B',
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=parse.hostname + " ->",
                        leave=True) as pbar:
                            async for chunk in resp.content.iter_chunked(input_kwargs.get("chunk_size", CHUNK_SIZE)):
                                if chunk:
                                    pbar.update(len(chunk))
                                    yield chunk
        
        return gen()
    elif scheme == 'sftp':
        host = parse.hostname
        if not host:
            raise ValueError(f"Missing host in SFTP URI: {uri}")
        port = parse.port or input_kwargs.get('port', 22)
        username = parse.username or input_kwargs.get('username')
        password = parse.password or input_kwargs.get('password')
        path = parse.path
        if not path:
            raise ValueError(f"Missing path in SFTP URI: {uri}")
        
        conn = await asyncssh.connect(
            host,
            username=username,
            password=password,
            port=port,
            known_hosts=input_kwargs.get('known_hosts', None),  # Set to None for testing; insecure!
            **input_kwargs.get('sftp_kwargs', {})
        )
        sftp = await conn.start_sftp_client()
        try:
            total_size = (await sftp.stat(path)).st_size
        except Exception as e:
            await sftp.exit()
            await conn.close()
            raise e
        
        async def gen():
            file = await sftp.open(path, 'rb')

            # Set up tqdm progress bar (unit_scale for human-readable sizes like KB/MB)
            with async_tqdm(
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                desc=host + " ->",
                leave=True) as pbar:
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
                        await conn.close()  # Ensure resources close after gen
        
        return gen()
    elif scheme == 'file':
        path = parse.path
        if not path:
            raise ValueError(f"Missing path in FILE URI: {uri}")
        total_size = os.path.getsize(path)
        
        async def gen():
            async with aiofiles.open(path, 'rb') as f:
                with async_tqdm(
                    total=total_size,
                    unit='B',
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=os.path.basename(path) + " ->",
                    leave=True) as pbar:
                        while True:
                            chunk = await f.read(input_kwargs.get("chunk_size", CHUNK_SIZE))
                            if not chunk:
                                break
                            pbar.update(len(chunk))
                            yield chunk
        
        return total_size, gen()
    else:
        raise ValueError(f"Unknown scheme in URI: {scheme}")


async def consumer_gen(
    total_size: int,
    uri: str,
    q: asyncio.Queue,
    callback: Optional[Callable[[bytes], Optional[bytes]]] = None,
    finalize_callback: Optional[Callable[[], Optional[bytes]]] = None
) -> AsyncGenerator[bytes, None]:
    """
    Asynchronous generator that consumes from a queue until None is received.
    If a callback is provided, applies it to each chunk and yields the result if not None;
    otherwise, yields the unmodified chunk.
    If a finalize_callback is provided, calls it when None is received (end of stream) and yields its result if not None, before breaking.
    """
    with async_tqdm(
        total=total_size,
        unit='B',
        unit_scale=True,
        unit_divisor=1024,
        desc=sanitize_uri(uri) + " <-",
        leave=True) as pbar:
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
    output_uris: List[str],
    input_kwargs: Dict[str, Any] = {},
    output_kwargs_list: List[Dict[str, Any]] = None
) -> None:
    """
    Pipelines the input stream to multiple output streams, with tqdm progress on input.
    The input is read once and copied to all outputs efficiently using queues for fanning out.
    Each output can optionally have a 'callback' in its output_kwargs, which is applied in its consumer_gen.
    If no callback, the unmodified bytes are yielded.
    Each output can also optionally have a 'finalize_callback' in its output_kwargs, called at the end of the stream to flush buffers, etc.
    
    Pass input_kwargs for input options, and output_kwargs_list (one dict per output_uri) for output options.
    If output_kwargs_list is None, defaults to empty dicts for each output.
    
    For HTTP outputs: Optional 'cert_file' and 'key_file' in output_kwargs for client certificate authentication (both required).
    """
    if output_kwargs_list is None:
        output_kwargs_list = [{} for _ in output_uris]
    if len(output_uris) != len(output_kwargs_list):
        raise ValueError("Number of output URIs must match output kwargs list.")
    
    input_size = await get_input_stream_size(input_uri, **input_kwargs)
    input_gen = await get_input_stream(input_uri, **input_kwargs)
    
    # Create queues for each output
    all_queues = [
        asyncio.Queue(maxsize=kwargs.get(
            "max_queue_size", MAX_QUEUE_SIZE)
        ) for _, kwargs in zip(output_uris, output_kwargs_list)]
    
    # Create consumer tasks for each output
    tasks = []
    for uri, kw, q in zip(output_uris, output_kwargs_list, all_queues):
        gen = consumer_gen(input_size, uri, q, callback=kw.get('callback'), finalize_callback=kw.get('finalize_callback'))
        scheme = urlparse(uri).scheme.lower()
        
        if scheme in ('http', 'https'):
            session_args = {}
            if all([k in kw.keys() for k in ["cert_file", "key_file"]]):
                session_args["connector"] =  create_ssl_connector(
                    kw['cert_file'],
                    kw['key_file'],
                    kw.get('ca_file'))

            async def post_task(uri=uri, kw=kw, gen=gen):                
                async with aiohttp.ClientSession(raise_for_status=True, **session_args) as session:
                    async with session.post(uri, data=gen, **kw.get('http_kwargs', {})) as resp:
                        resp.raise_for_status()
                        # TODO: make accepted status part of the kw args
                        if resp.status == 200 or resp.status == 202:
                            result = await resp.json()
                            return result
                        else:
                            error_text = await resp.text()
                            raise Exception(f"Failed to add image: {resp.status} - {error_text}")
            tasks.append(asyncio.create_task(post_task()))
        elif scheme == 'sftp':
            async def sftp_task(uri=uri, kw=kw, gen=gen):
                parse = urlparse(uri)
                host = parse.hostname
                if not host:
                    raise ValueError(f"Missing host in SFTP URI: {uri}")
                port = parse.port or kw.get('port', 22)
                username = parse.username or kw.get('username')
                password = parse.password or kw.get('password')
                path = parse.path[1:]
                if not path:
                    raise ValueError(f"Missing path in SFTP URI: {uri}")
                
                asyncssh.logging.set_debug_level(3)
                async with asyncssh.connect(
                    host,
                    username=username,
                    password=password,
                    port=port,
                    known_hosts=kw.get('known_hosts'),
                    **kw.get('sftp_kwargs', {})
                ) as conn:
                    async with conn.start_sftp_client() as sftp:
                        async with sftp.open(path, 'wb') as file:
                            async for chunk in gen:
                                await file.write(chunk)
            tasks.append(asyncio.create_task(sftp_task()))
        elif scheme == 'file':
            async def file_task(uri=uri, kw=kw, gen=gen):
                parse = urlparse(uri)
                path = parse.path
                if not path:
                    raise ValueError(f"Missing path in FILE URI: {uri}")
                
                async with aiofiles.open(path, 'wb') as f:
                    async for chunk in gen:
                        await f.write(chunk)
            tasks.append(asyncio.create_task(file_task()))
        else:
            raise ValueError(f"Unknown scheme for output: {scheme}")
    
    # Read input and distribute original chunks to all queues
    async for chunk in input_gen:
        for q in all_queues:
            await q.put(chunk)
    
    # Signal end of data to all queues
    for q in all_queues:
        await q.put(None)
    
    # Wait for all output tasks to complete
    await asyncio.gather(*tasks)

async def main(
    stream_pairs: List[Dict[str, Any]]
) -> None:
    """
    Asynchronous main function to handle multiple stream pairs concurrently.
    Each pair is a dict with 'input_uri', 'output_uris' (list), and optionally 'input_kwargs', 'output_kwargs_list' (list of dicts).
    Callbacks are provided per-output in 'callback' key of each output_kwargs dict.
    Finalize callbacks are provided per-output in 'finalize_callback' key of each output_kwargs dict.
    
    Uses asyncio.gather for concurrent processing. Each stream gets its own progress bar.
    """
    coros = [
        process_stream(
            pair['input_uri'],
            pair['output_uris'],
            input_kwargs=pair.get('input_kwargs', {}),
            output_kwargs_list=pair.get('output_kwargs_list')
        ) for pair in stream_pairs
    ]
    await asyncio.gather(*coros)


def streamer(
    stream_pairs: List[Dict[str, Any]]
):
    asyncio.run(main(stream_pairs))


# Example usage
if __name__ == "__main__":
    # Sample stream pairs (adapt to your actual URIs/paths; mix types as needed)
    pairs = [
        {  # HTTP input to multiple outputs: HTTP (unmodified) and SFTP (processed via callback)
            'input_uri': 'https://example.com/input1.bin',
            'output_uris': [
                'https://example.com/output1',  # HTTP: no callback, unmodified
                'sftp://user:pass@sftp.example.com:22/remote/path/output2.bin'  # SFTP: with callback and finalize
            ],
            'input_kwargs': {
                'http_kwargs': {
                    'allow_redirects': True,
                    'headers': {'Authorization': 'Bearer input_token'}},
                'cert_file': '/path/to/input_cert.pem',  # Optional client cert for input GET
                'key_file': '/path/to/input_key.pem'
            },
            'output_kwargs_list': [
                {
                    'http_kwargs': {'headers': {'Authorization': 'Bearer output_token1'}},
                    'cert_file': '/path/to/output_cert.pem',  # Optional client cert for output POST
                    'key_file': '/path/to/output_key.pem'
                },
                {
                    'known_hosts': None,
                    'callback': lambda chunk: chunk[::-1] if b'keep' in chunk else None,  # Example callback for this output
                    'finalize_callback': lambda: b'END_OF_STREAM'  # Example finalize callback, e.g., flush buffer
                }
            ]
        }
    ]
    
    asyncio.run(main(pairs))