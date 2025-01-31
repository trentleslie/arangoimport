"""Test command line interface."""

import json
import os
import tempfile
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from arangoimport.cli import cli

# Constants for test values
DEFAULT_PROCESSES = 4
CLICK_ERROR_CODE = 2
DEFAULT_DB_NAME = "spoke_human"
DEFAULT_USERNAME = "root"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8529
EXPECTED_ARGS = 2
EXPECTED_NODES = 2
EXPECTED_EDGES = 1


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return {
        "nodes": [
            {"_id": "test/1", "data": "node1"},
            {"_id": "test/2", "data": "node2"},
        ],
        "edges": [
            {
                "_id": "test/e1",
                "_from": "test/1",
                "_to": "test/2",
                "type": "test_edge",
            }
        ],
    }


@pytest.fixture
def temp_json_file(sample_data):
    """Create a temporary JSON file with sample data."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(sample_data, f)
        f.flush()
        yield f.name
    os.unlink(f.name)


@pytest.fixture
def runner():
    """Create a CLI runner."""
    return CliRunner()


def test_cli_version(runner):
    """Test CLI version command."""
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert "version" in result.output.lower()


def test_cli_help(runner):
    """Test CLI help command."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.output


@patch("arangoimport.cli.parallel_load_data")
def test_import_data_command(mock_parallel_load, runner, temp_json_file):
    """Test import data command."""
    # Mock successful import
    mock_parallel_load.return_value = (EXPECTED_NODES, EXPECTED_EDGES)

    # Test with minimum required arguments
    result = runner.invoke(
        cli,
        [
            "import-data",
            temp_json_file,
            "--password",
            "test_password",
            "--host",
            DEFAULT_HOST,
            "--port",
            str(DEFAULT_PORT),
        ],
    )
    assert result.exit_code == 0, f"Command failed with output: {result.output}"
    assert "Import successfully completed!" in result.output

    # Verify the command was called with correct arguments
    args = mock_parallel_load.call_args
    assert args[0][0] == temp_json_file  # file_path
    assert args[0][1] == {  # db_config
        "username": DEFAULT_USERNAME,  # default value
        "password": "test_password",
        "db_name": DEFAULT_DB_NAME,  # default value
        "host": DEFAULT_HOST,
        "port": DEFAULT_PORT,
    }
    assert args[1]["num_processes"] is None  # default value

    # Test with all arguments
    result = runner.invoke(
        cli,
        [
            "import-data",
            temp_json_file,
            "--db-name",
            "test_db",
            "--username",
            "test_user",
            "--password",
            "test_password",
            "--processes",
            str(DEFAULT_PROCESSES),
            "--host",
            "test_host",
            "--port",
            "8530",
        ],
    )
    assert result.exit_code == 0
    assert "Import successfully completed!" in result.output

    # Verify all arguments were passed correctly
    args = mock_parallel_load.call_args
    assert args[0][0] == temp_json_file
    assert args[0][1] == {
        "username": "test_user",
        "password": "test_password",
        "db_name": "test_db",
        "host": "test_host",
        "port": 8530,
    }
    assert args[1]["num_processes"] == DEFAULT_PROCESSES


def test_import_data_missing_password(runner, temp_json_file):
    """Test import data command without password."""
    result = runner.invoke(cli, ["import-data", temp_json_file])
    assert result.exit_code == CLICK_ERROR_CODE  # Click's error exit code
    assert "Missing option '--password'" in result.output


def test_import_data_nonexistent_file(runner):
    """Test import data command with nonexistent file."""
    result = runner.invoke(
        cli, ["import-data", "nonexistent.json", "--password", "test"]
    )
    assert result.exit_code == CLICK_ERROR_CODE
    assert "does not exist" in result.output.lower()


@patch("arangoimport.cli.parallel_load_data")
def test_import_data_error_handling(mock_parallel_load, runner, temp_json_file):
    """Test error handling in import data command."""
    # Mock an error during import
    mock_parallel_load.side_effect = Exception("Test error")

    result = runner.invoke(cli, ["import-data", temp_json_file, "--password", "test"])
    assert result.exit_code == 1
    assert "Unexpected error: Test error" in result.output
