"""Unit tests for txwtf_tools.cli"""

import os

from click.testing import CliRunner

from txwtf_tools.cli import cli


class TestCliRelay:
    def test_relay_file_to_file(self, tmp_path):
        src = tmp_path / "src.bin"
        dst = tmp_path / "dst.bin"
        data = os.urandom(4096)
        src.write_bytes(data)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["relay", src.as_uri(), dst.as_uri(), "--chunk-size", "1024"],
        )
        assert result.exit_code == 0, result.output
        assert dst.read_bytes() == data

    def test_relay_file_to_multiple(self, tmp_path):
        src = tmp_path / "src.bin"
        dst1 = tmp_path / "dst1.bin"
        dst2 = tmp_path / "dst2.bin"
        data = os.urandom(2048)
        src.write_bytes(data)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "relay",
                src.as_uri(),
                dst1.as_uri(),
                dst2.as_uri(),
                "--chunk-size",
                "512",
            ],
        )
        assert result.exit_code == 0, result.output
        assert dst1.read_bytes() == data
        assert dst2.read_bytes() == data


class TestCliVersion:
    def test_version(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "txwtf-tools" in result.output


class TestCliHelp:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "relay" in result.output
        assert "lxd-copy" in result.output
        assert "lxd-store" in result.output
        assert "lxd-restore" in result.output
        assert "lxd-store-all" in result.output

    def test_relay_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["relay", "--help"])
        assert result.exit_code == 0
        assert "GET_URL" in result.output

    def test_lxd_copy_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["lxd-copy", "--help"])
        assert result.exit_code == 0
        assert "SOURCE_ENDPOINT" in result.output

    def test_lxd_store_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["lxd-store", "--help"])
        assert result.exit_code == 0
        assert "SFTP_URL" in result.output

    def test_lxd_restore_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["lxd-restore", "--help"])
        assert result.exit_code == 0
        assert "SFTP_URL" in result.output
        assert "TARGET_ENDPOINT" in result.output

    def test_lxd_store_all_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["lxd-store-all", "--help"])
        assert result.exit_code == 0
        assert "SOURCE_ENDPOINT" in result.output
        assert "TARGET_URL" in result.output
        assert "--prefix" in result.output
        assert "--contains" in result.output
        assert "--exclude" in result.output
        assert "--type" in result.output
        assert "--status" in result.output
