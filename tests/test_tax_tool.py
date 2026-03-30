import pytest
from argparse import Namespace
from unittest.mock import patch


class TestBuildParser:
    def test_address_is_required(self):
        from tax_tool import build_parser

        with pytest.raises(SystemExit):
            build_parser().parse_args([])

    def test_address_only_uses_defaults(self):
        from tax_tool import build_parser

        args = build_parser().parse_args(["--address", "cheqd1abc"])
        assert args.address == "cheqd1abc"
        assert args.endpoint == "https://explorer-gql.cheqd.io/v1/graphql"
        assert args.batch_size == 100
        assert args.max_transactions == 5000
        assert args.alias is None
        assert args.fetch_only is False
        assert args.convert_only is False
        assert args.input is None
        assert args.output_json is None
        assert args.output_csv is None
        assert args.debug is False
        assert args.hash is None
        assert args.archive_rest_api_url is None

    def test_fetch_only_and_convert_only_mutually_exclusive(self):
        from tax_tool import build_parser

        with pytest.raises(SystemExit):
            build_parser().parse_args(["--address", "x", "--fetch-only", "--convert-only"])

    def test_alias_stored(self):
        from tax_tool import build_parser

        args = build_parser().parse_args(["--address", "cheqd1abc", "--alias", "myval"])
        assert args.alias == "myval"

    def test_all_fetch_options(self):
        from tax_tool import build_parser

        args = build_parser().parse_args(
            [
                "--address",
                "cheqd1abc",
                "--endpoint",
                "https://custom.example.com/v1/graphql",
                "--batch-size",
                "50",
                "--max-transactions",
                "1000",
            ]
        )
        assert args.endpoint == "https://custom.example.com/v1/graphql"
        assert args.batch_size == 50
        assert args.max_transactions == 1000

    def test_all_convert_options(self):
        from tax_tool import build_parser

        args = build_parser().parse_args(
            [
                "--address",
                "cheqd1abc",
                "--input",
                "txs.json",
                "--archive-rest-api-url",
                "https://archive.example.com",
                "--debug",
                "--hash",
                "ABC123",
            ]
        )
        assert args.input == "txs.json"
        assert args.archive_rest_api_url == "https://archive.example.com"
        assert args.debug is True
        assert args.hash == "ABC123"

    def test_output_json_and_csv_overrides(self):
        from tax_tool import build_parser

        args = build_parser().parse_args(
            [
                "--address",
                "cheqd1abc",
                "--output-json",
                "custom.json",
                "--output-csv",
                "custom.csv",
            ]
        )
        assert args.output_json == "custom.json"
        assert args.output_csv == "custom.csv"


class TestResolveFilenames:
    def test_default_names_from_address(self):
        from tax_tool import resolve_filenames

        args = Namespace(address="cheqd1abc", alias=None, output_json=None, output_csv=None)
        assert resolve_filenames(args) == ("cheqd1abc.json", "cheqd1abc.csv")

    def test_alias_overrides_address(self):
        from tax_tool import resolve_filenames

        args = Namespace(address="cheqd1abc", alias="myval", output_json=None, output_csv=None)
        assert resolve_filenames(args) == ("myval.json", "myval.csv")

    def test_explicit_output_json_overrides_alias(self):
        from tax_tool import resolve_filenames

        args = Namespace(address="cheqd1abc", alias="myval", output_json="custom.json", output_csv=None)
        assert resolve_filenames(args) == ("custom.json", "myval.csv")

    def test_explicit_output_csv_overrides_alias(self):
        from tax_tool import resolve_filenames

        args = Namespace(address="cheqd1abc", alias="myval", output_json=None, output_csv="custom.csv")
        assert resolve_filenames(args) == ("myval.json", "custom.csv")

    def test_both_explicit_overrides(self):
        from tax_tool import resolve_filenames

        args = Namespace(address="cheqd1abc", alias="myval", output_json="a.json", output_csv="b.csv")
        assert resolve_filenames(args) == ("a.json", "b.csv")


DEFAULT_ENDPOINT = "https://explorer-gql.cheqd.io/v1/graphql"


def make_args(**overrides):
    defaults = {
        "address": "cheqd1abc",
        "fetch_only": False,
        "convert_only": False,
        "endpoint": DEFAULT_ENDPOINT,
        "batch_size": 100,
        "max_transactions": 5000,
        "input": None,
        "archive_rest_api_url": None,
        "debug": False,
        "hash": None,
        "alias": None,
        "output_json": None,
        "output_csv": None,
    }
    defaults.update(overrides)
    return Namespace(**defaults)


class TestRun:
    @patch("tax_tool.KoinlyConverter")
    @patch("tax_tool.TransactionFetcher")
    def test_default_mode_calls_fetch_then_convert(self, mock_fetcher_cls, mock_converter_cls):
        from tax_tool import run

        mock_fetcher_cls.return_value.fetch_all.return_value = True
        run(make_args())

        mock_fetcher_cls.assert_called_once_with(
            endpoint=DEFAULT_ENDPOINT,
            address="cheqd1abc",
            batch_size=100,
            max_transactions=5000,
            output_file="cheqd1abc.json",
        )
        mock_fetcher_cls.return_value.fetch_all.assert_called_once()

        mock_converter_cls.assert_called_once_with("cheqd1abc.json", "cheqd1abc.csv", "cheqd1abc", False, None, None)
        mock_converter_cls.return_value.convert.assert_called_once()

    @patch("tax_tool.KoinlyConverter")
    @patch("tax_tool.TransactionFetcher")
    def test_fetch_only_skips_conversion(self, mock_fetcher_cls, mock_converter_cls):
        from tax_tool import run

        mock_fetcher_cls.return_value.fetch_all.return_value = True
        run(make_args(fetch_only=True))

        mock_fetcher_cls.return_value.fetch_all.assert_called_once()
        mock_converter_cls.assert_not_called()

    @patch("tax_tool.KoinlyConverter")
    @patch("tax_tool.TransactionFetcher")
    def test_convert_only_skips_fetch(self, mock_fetcher_cls, mock_converter_cls):
        from tax_tool import run

        run(make_args(convert_only=True, input="existing.json"))

        mock_fetcher_cls.assert_not_called()
        mock_converter_cls.assert_called_once_with("existing.json", "cheqd1abc.csv", "cheqd1abc", False, None, None)
        mock_converter_cls.return_value.convert.assert_called_once()

    def test_convert_only_without_input_exits_with_error(self):
        from tax_tool import run

        with pytest.raises(SystemExit):
            run(make_args(convert_only=True))

    @patch("tax_tool.KoinlyConverter")
    @patch("tax_tool.TransactionFetcher")
    def test_alias_flows_to_output_filenames(self, mock_fetcher_cls, mock_converter_cls):
        from tax_tool import run

        mock_fetcher_cls.return_value.fetch_all.return_value = True
        run(make_args(alias="myval"))

        mock_fetcher_cls.assert_called_once_with(
            endpoint=DEFAULT_ENDPOINT,
            address="cheqd1abc",
            batch_size=100,
            max_transactions=5000,
            output_file="myval.json",
        )
        mock_converter_cls.assert_called_once_with("myval.json", "myval.csv", "cheqd1abc", False, None, None)

    @patch("tax_tool.KoinlyConverter")
    @patch("tax_tool.TransactionFetcher")
    def test_custom_endpoint_passed_to_fetcher(self, mock_fetcher_cls, mock_converter_cls):
        from tax_tool import run

        mock_fetcher_cls.return_value.fetch_all.return_value = True
        run(make_args(endpoint="https://custom.example.com"))

        mock_fetcher_cls.assert_called_once_with(
            endpoint="https://custom.example.com",
            address="cheqd1abc",
            batch_size=100,
            max_transactions=5000,
            output_file="cheqd1abc.json",
        )

    @patch("tax_tool.KoinlyConverter")
    @patch("tax_tool.TransactionFetcher")
    def test_convert_options_passed_through(self, mock_fetcher_cls, mock_converter_cls):
        from tax_tool import run

        mock_fetcher_cls.return_value.fetch_all.return_value = True
        run(make_args(archive_rest_api_url="https://archive.example.com", debug=True, hash="ABC123"))

        mock_converter_cls.assert_called_once_with(
            "cheqd1abc.json",
            "cheqd1abc.csv",
            "cheqd1abc",
            True,
            "ABC123",
            "https://archive.example.com",
        )

    @patch("tax_tool.KoinlyConverter")
    @patch("tax_tool.TransactionFetcher")
    def test_fetch_failure_stops_pipeline(self, mock_fetcher_cls, mock_converter_cls):
        from tax_tool import run

        mock_fetcher_cls.return_value.fetch_all.return_value = False
        with pytest.raises(SystemExit):
            run(make_args())

        mock_converter_cls.assert_not_called()

    @patch("tax_tool.KoinlyConverter")
    @patch("tax_tool.TransactionFetcher")
    def test_convert_only_uses_address_for_csv_name(self, mock_fetcher_cls, mock_converter_cls):
        from tax_tool import run

        run(make_args(convert_only=True, input="foo.json"))

        mock_converter_cls.assert_called_once_with("foo.json", "cheqd1abc.csv", "cheqd1abc", False, None, None)
