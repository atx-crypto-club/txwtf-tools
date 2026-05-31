"""Unit tests for txwtf_tools.backup (non-pylxd parts)"""

import os
import struct

from txwtf_tools.backup import (
    chain_functions,
    get_fixed_base64_from_utf8_string,
    make_compress_func,
    make_decompress_func,
    make_decrypt_func,
    make_encrypt_func,
)


class TestGetFixedBase64:
    def test_deterministic(self):
        a = get_fixed_base64_from_utf8_string("my-secret")
        b = get_fixed_base64_from_utf8_string("my-secret")
        assert a == b

    def test_different_inputs_differ(self):
        a = get_fixed_base64_from_utf8_string("password1")
        b = get_fixed_base64_from_utf8_string("password2")
        assert a != b

    def test_length(self):
        result = get_fixed_base64_from_utf8_string("test")
        # base64 of 32 bytes = 44 characters (with padding)
        assert len(result) == 44


class TestChainFunctions:
    def test_single(self):
        fn = chain_functions(lambda x: x.upper())
        assert fn(b"hello") == b"HELLO"

    def test_multiple(self):
        fn = chain_functions(lambda x: x + b"!", lambda x: x * 2)
        assert fn(b"hi") == b"hi!hi!"

    def test_identity(self):
        fn = chain_functions(lambda x: x)
        assert fn(b"data") == b"data"


class TestMakeEncryptFunc:
    def test_produces_length_prefixed_token(self):
        encrypt = make_encrypt_func("passphrase")
        result = encrypt(b"hello world")
        # First 4 bytes are big-endian token length
        token_len = struct.unpack(">I", result[:4])[0]
        assert len(result) == 4 + token_len

    def test_empty_chunk_returns_empty(self):
        encrypt = make_encrypt_func("passphrase")
        assert encrypt(b"") == b""

    def test_deterministic_key(self):
        enc1 = make_encrypt_func("same-key")
        enc2 = make_encrypt_func("same-key")
        # Both should produce valid tokens (different ciphertext due to IV/timestamp)
        r1 = enc1(b"data")
        r2 = enc2(b"data")
        assert len(r1) > 4
        assert len(r2) > 4
        # Token lengths should match (same plaintext size)
        assert struct.unpack(">I", r1[:4])[0] == struct.unpack(">I", r2[:4])[0]


class TestMakeDecryptFunc:
    def test_single_token_round_trip(self):
        plaintext = b"hello world"
        encrypt = make_encrypt_func("secret")
        decrypt = make_decrypt_func("secret")
        ciphertext = encrypt(plaintext)
        result = decrypt(ciphertext)
        assert result == plaintext

    def test_multi_chunk_round_trip(self):
        """Encrypt multiple chunks, concatenate, then decrypt in different splits."""
        encrypt = make_encrypt_func("secret")
        chunks = [os.urandom(1024) for _ in range(5)]
        encrypted = b"".join(encrypt(c) for c in chunks)

        decrypt = make_decrypt_func("secret")
        # Feed in different-sized splits to test the accumulator
        result = bytearray()
        pos = 0
        for split_size in [100, 500, 2000, 300, len(encrypted)]:
            piece = encrypted[pos : pos + split_size]
            if not piece:
                break
            result.extend(decrypt(piece))
            pos += split_size
        assert bytes(result) == b"".join(chunks)

    def test_empty_chunk_returns_empty(self):
        decrypt = make_decrypt_func("secret")
        assert decrypt(b"") == b""

    def test_wrong_passphrase_fails(self):
        encrypt = make_encrypt_func("right-key")
        decrypt = make_decrypt_func("wrong-key")
        ciphertext = encrypt(b"data")
        import pytest
        with pytest.raises(Exception):
            decrypt(ciphertext)


class TestMakeCompressFunc:
    def test_compress_and_finalize(self):
        compress, finalize = make_compress_func()
        data = b"ABCDEFGH" * 5000
        compressed = bytearray()
        for i in range(0, len(data), 4096):
            chunk = data[i : i + 4096]
            out = compress(chunk)
            if out:
                compressed.extend(out)
        flushed = finalize()
        if flushed:
            compressed.extend(flushed)
        # Compressed should be smaller for repetitive data
        assert len(compressed) < len(data)
        assert len(compressed) > 0

    def test_empty_chunk_returns_empty(self):
        compress, _ = make_compress_func()
        assert compress(b"") == b""

    def test_round_trip(self):
        data = os.urandom(10_000)
        compress, finalize = make_compress_func()
        decompress = make_decompress_func()
        compressed = bytearray()
        for i in range(0, len(data), 2048):
            out = compress(data[i : i + 2048])
            if out:
                compressed.extend(out)
        flushed = finalize()
        if flushed:
            compressed.extend(flushed)
        result = decompress(bytes(compressed))
        assert result == data


class TestMakeDecompressFunc:
    def test_empty_chunk_returns_empty(self):
        decompress = make_decompress_func()
        assert decompress(b"") == b""
