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


class TestListInstances:
    """Tests for list_instances using mock pylxd client."""

    def _make_instance(self, name, inst_type="virtual-machine", status="Running"):
        class FakeInstance:
            pass
        inst = FakeInstance()
        inst.name = name
        inst.type = inst_type
        inst.status = status
        return inst

    def _patch_list_instances(self, instances, monkeypatch):
        """Patch _get_pylxd to return a fake client with the given instances."""
        from unittest.mock import MagicMock
        from txwtf_tools import backup

        fake_pylxd = MagicMock()
        fake_client = MagicMock()
        fake_client.instances.all.return_value = instances
        fake_pylxd.Client.return_value = fake_client
        monkeypatch.setattr(backup, "_get_pylxd", lambda: fake_pylxd)

    def test_returns_all_names(self, monkeypatch):
        from txwtf_tools.backup import list_instances
        instances = [self._make_instance("vm-a"), self._make_instance("vm-b")]
        self._patch_list_instances(instances, monkeypatch)

        result = list_instances("https://host:8443", "c.crt", "c.key", "ca.pem")
        assert result == ["vm-a", "vm-b"]

    def test_filter_by_type(self, monkeypatch):
        from txwtf_tools.backup import list_instances
        instances = [
            self._make_instance("vm-1", inst_type="virtual-machine"),
            self._make_instance("ct-1", inst_type="container"),
        ]
        self._patch_list_instances(instances, monkeypatch)

        result = list_instances("https://h:8443", "c", "k", "ca", vm_type="container")
        assert result == ["ct-1"]

    def test_filter_by_prefix(self, monkeypatch):
        from txwtf_tools.backup import list_instances
        instances = [
            self._make_instance("web-1"),
            self._make_instance("db-1"),
            self._make_instance("web-2"),
        ]
        self._patch_list_instances(instances, monkeypatch)

        result = list_instances("https://h:8443", "c", "k", "ca", name_prefix="web-")
        assert result == ["web-1", "web-2"]

    def test_filter_by_contains(self, monkeypatch):
        from txwtf_tools.backup import list_instances
        instances = [
            self._make_instance("prod-web-1"),
            self._make_instance("staging-db-1"),
            self._make_instance("prod-db-2"),
        ]
        self._patch_list_instances(instances, monkeypatch)

        result = list_instances("https://h:8443", "c", "k", "ca", name_contains="db")
        assert result == ["prod-db-2", "staging-db-1"]

    def test_filter_by_status(self, monkeypatch):
        from txwtf_tools.backup import list_instances
        instances = [
            self._make_instance("vm-1", status="Running"),
            self._make_instance("vm-2", status="Stopped"),
        ]
        self._patch_list_instances(instances, monkeypatch)

        result = list_instances("https://h:8443", "c", "k", "ca", status="Stopped")
        assert result == ["vm-2"]

    def test_exclude(self, monkeypatch):
        from txwtf_tools.backup import list_instances
        instances = [
            self._make_instance("vm-a"),
            self._make_instance("vm-b"),
            self._make_instance("vm-c"),
        ]
        self._patch_list_instances(instances, monkeypatch)

        result = list_instances("https://h:8443", "c", "k", "ca", exclude=["vm-b"])
        assert result == ["vm-a", "vm-c"]

    def test_combined_filters(self, monkeypatch):
        from txwtf_tools.backup import list_instances
        instances = [
            self._make_instance("web-prod", inst_type="container", status="Running"),
            self._make_instance("web-staging", inst_type="container", status="Stopped"),
            self._make_instance("db-prod", inst_type="virtual-machine", status="Running"),
            self._make_instance("web-test", inst_type="container", status="Running"),
        ]
        self._patch_list_instances(instances, monkeypatch)

        result = list_instances(
            "https://h:8443", "c", "k", "ca",
            vm_type="container",
            name_prefix="web-",
            status="Running",
            exclude=["web-test"],
        )
        assert result == ["web-prod"]

    def test_empty_result(self, monkeypatch):
        from txwtf_tools.backup import list_instances
        self._patch_list_instances([], monkeypatch)

        result = list_instances("https://h:8443", "c", "k", "ca")
        assert result == []


class TestDoStoreAll:
    """Tests for do_store_all with mocked list_instances and do_store."""

    def _patch(self, monkeypatch, instance_names, do_store_side_effect=None):
        from unittest.mock import MagicMock, patch
        from txwtf_tools import backup

        # Mock list_instances to return given names
        monkeypatch.setattr(
            backup, "list_instances",
            lambda **kwargs: sorted(instance_names),
        )

        # Mock do_store
        mock_store = MagicMock()
        if do_store_side_effect:
            mock_store.side_effect = do_store_side_effect
        monkeypatch.setattr(backup, "do_store", mock_store)
        return mock_store

    def test_backs_up_all_vms(self, monkeypatch):
        from txwtf_tools.backup import do_store_all

        mock_store = self._patch(monkeypatch, ["vm-a", "vm-b"])

        results = do_store_all(
            endpoint="https://h:8443",
            target_url="sftp://host/backups",
            cert_path="c", key_path="k", ca_path="ca",
            passphrase="secret",
        )

        assert results == {"vm-a": "ok", "vm-b": "ok"}
        assert mock_store.call_count == 2

        # Verify correct SFTP URLs were used
        calls = mock_store.call_args_list
        assert calls[0].kwargs["sftp_url"] == "sftp://host/backups/default-vm-a-backup.img.gz.enc"
        assert calls[1].kwargs["sftp_url"] == "sftp://host/backups/default-vm-b-backup.img.gz.enc"

    def test_no_matching_instances(self, monkeypatch):
        from txwtf_tools.backup import do_store_all

        self._patch(monkeypatch, [])

        results = do_store_all(
            endpoint="https://h:8443",
            target_url="sftp://host/backups",
            cert_path="c", key_path="k", ca_path="ca",
            passphrase="secret",
        )

        assert results == {}

    def test_suffix_no_compress(self, monkeypatch):
        from txwtf_tools.backup import do_store_all

        mock_store = self._patch(monkeypatch, ["vm-x"])

        do_store_all(
            endpoint="https://h:8443",
            target_url="sftp://host/backups",
            cert_path="c", key_path="k", ca_path="ca",
            passphrase="secret",
            compress=False,
        )

        assert mock_store.call_args.kwargs["sftp_url"].endswith(".img.enc")

    def test_suffix_no_encrypt(self, monkeypatch):
        from txwtf_tools.backup import do_store_all

        mock_store = self._patch(monkeypatch, ["vm-x"])

        do_store_all(
            endpoint="https://h:8443",
            target_url="sftp://host/backups",
            cert_path="c", key_path="k", ca_path="ca",
            passphrase="secret",
            encrypt=False,
        )

        assert mock_store.call_args.kwargs["sftp_url"].endswith(".img.gz")

    def test_continues_on_failure(self, monkeypatch):
        from txwtf_tools.backup import do_store_all

        def side_effect(**kwargs):
            if kwargs["vm_name"] == "vm-a":
                raise RuntimeError("disk full")

        mock_store = self._patch(monkeypatch, ["vm-a", "vm-b"],
                                  do_store_side_effect=side_effect)

        results = do_store_all(
            endpoint="https://h:8443",
            target_url="sftp://host/backups",
            cert_path="c", key_path="k", ca_path="ca",
            passphrase="secret",
        )

        assert results["vm-a"] == "disk full"
        assert results["vm-b"] == "ok"
        assert mock_store.call_count == 2

    def test_custom_project(self, monkeypatch):
        from txwtf_tools.backup import do_store_all

        mock_store = self._patch(monkeypatch, ["vm-1"])

        do_store_all(
            endpoint="https://h:8443",
            target_url="sftp://host/backups",
            cert_path="c", key_path="k", ca_path="ca",
            passphrase="secret",
            project="production",
        )

        assert mock_store.call_args.kwargs["sftp_url"] == \
            "sftp://host/backups/production-vm-1-backup.img.gz.enc"
        assert mock_store.call_args.kwargs["project"] == "production"


class TestDoRestoreAll:
    """Tests for do_restore_all with mocked do_restore."""

    def _patch(self, monkeypatch, do_restore_side_effect=None):
        from unittest.mock import MagicMock
        from txwtf_tools import backup

        mock_restore = MagicMock()
        if do_restore_side_effect:
            mock_restore.side_effect = do_restore_side_effect
        monkeypatch.setattr(backup, "do_restore", mock_restore)
        return mock_restore

    def test_restores_all_vms(self, monkeypatch):
        from txwtf_tools.backup import do_restore_all

        mock_restore = self._patch(monkeypatch)

        results = do_restore_all(
            source_url="sftp://host/backups",
            target_endpoint="https://h:8443",
            names=["vm-a", "vm-b"],
            passphrase="secret",
            cert_path="c", key_path="k",
        )

        assert results == {"vm-a": "ok", "vm-b": "ok"}
        assert mock_restore.call_count == 2

        calls = mock_restore.call_args_list
        assert calls[0].kwargs["sftp_url"] == "sftp://host/backups/default-vm-a-backup.img.gz.enc"
        assert calls[1].kwargs["sftp_url"] == "sftp://host/backups/default-vm-b-backup.img.gz.enc"

    def test_empty_names(self, monkeypatch):
        from txwtf_tools.backup import do_restore_all

        self._patch(monkeypatch)

        results = do_restore_all(
            source_url="sftp://host/backups",
            target_endpoint="https://h:8443",
            names=[],
            passphrase="secret",
            cert_path="c", key_path="k",
        )

        assert results == {}

    def test_suffix_no_compress(self, monkeypatch):
        from txwtf_tools.backup import do_restore_all

        mock_restore = self._patch(monkeypatch)

        do_restore_all(
            source_url="sftp://host/backups",
            target_endpoint="https://h:8443",
            names=["vm-x"],
            passphrase="secret",
            cert_path="c", key_path="k",
            compress=False,
        )

        assert mock_restore.call_args.kwargs["sftp_url"].endswith(".img.enc")

    def test_suffix_no_encrypt(self, monkeypatch):
        from txwtf_tools.backup import do_restore_all

        mock_restore = self._patch(monkeypatch)

        do_restore_all(
            source_url="sftp://host/backups",
            target_endpoint="https://h:8443",
            names=["vm-x"],
            passphrase="secret",
            cert_path="c", key_path="k",
            encrypt=False,
        )

        assert mock_restore.call_args.kwargs["sftp_url"].endswith(".img.gz")

    def test_continues_on_failure(self, monkeypatch):
        from txwtf_tools.backup import do_restore_all

        def side_effect(**kwargs):
            if "vm-a" in kwargs["sftp_url"]:
                raise RuntimeError("connection refused")

        mock_restore = self._patch(monkeypatch, do_restore_side_effect=side_effect)

        results = do_restore_all(
            source_url="sftp://host/backups",
            target_endpoint="https://h:8443",
            names=["vm-a", "vm-b"],
            passphrase="secret",
            cert_path="c", key_path="k",
        )

        assert results["vm-a"] == "connection refused"
        assert results["vm-b"] == "ok"
        assert mock_restore.call_count == 2

    def test_custom_project(self, monkeypatch):
        from txwtf_tools.backup import do_restore_all

        mock_restore = self._patch(monkeypatch)

        do_restore_all(
            source_url="sftp://host/backups",
            target_endpoint="https://h:8443",
            names=["vm-1"],
            passphrase="secret",
            cert_path="c", key_path="k",
            project="production",
        )

        assert mock_restore.call_args.kwargs["sftp_url"] == \
            "sftp://host/backups/production-vm-1-backup.img.gz.enc"

    def test_passes_restore_options(self, monkeypatch):
        from txwtf_tools.backup import do_restore_all

        mock_restore = self._patch(monkeypatch)

        do_restore_all(
            source_url="sftp://host/backups",
            target_endpoint="https://h:8443",
            names=["vm-1"],
            passphrase="secret",
            cert_path="c", key_path="k",
            ca_path="ca.pem",
            verify_target=True,
            compress=False,
            encrypt=True,
            chunk_size=256 * 1024,
            max_queue_size=10,
            rate_limit=1048576.0,
        )

        kw = mock_restore.call_args.kwargs
        assert kw["target_endpoint"] == "https://h:8443"
        assert kw["passphrase"] == "secret"
        assert kw["cert_path"] == "c"
        assert kw["key_path"] == "k"
        assert kw["ca_path"] == "ca.pem"
        assert kw["verify_target"] is True
        assert kw["compress"] is False
        assert kw["encrypt"] is True
        assert kw["chunk_size"] == 256 * 1024
        assert kw["max_queue_size"] == 10
        assert kw["rate_limit"] == 1048576.0
