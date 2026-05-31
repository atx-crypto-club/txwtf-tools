"""Unit tests for txwtf_tools.backup (non-pylxd parts)"""

from txwtf_tools.backup import chain_functions, get_fixed_base64_from_utf8_string


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
