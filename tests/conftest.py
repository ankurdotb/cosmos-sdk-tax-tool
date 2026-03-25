import sys
import os
import pytest

# Add project root to path so tx_to_koinly and fetch_transactions can be imported
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tx_to_koinly import KoinlyConverter
from tests.helpers import WALLET


@pytest.fixture
def converter(tmp_path):
    """Create a KoinlyConverter with temporary input/output files."""
    input_file = tmp_path / "txs.json"
    input_file.write_text("[]")
    output_file = tmp_path / "koinly.csv"
    return KoinlyConverter(
        input_file=str(input_file),
        output_file=str(output_file),
        address=WALLET,
    )


@pytest.fixture
def converter_with_archive(tmp_path):
    """Create a KoinlyConverter with archive REST API configured."""
    input_file = tmp_path / "txs.json"
    input_file.write_text("[]")
    output_file = tmp_path / "koinly.csv"
    return KoinlyConverter(
        input_file=str(input_file),
        output_file=str(output_file),
        address=WALLET,
        archive_rest_api_url="https://archive.example.com/",
    )
