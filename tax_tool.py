#!/usr/bin/env python3

import argparse
import sys

from fetch_transactions import TransactionFetcher
from tx_to_koinly import KoinlyConverter

DEFAULT_ENDPOINT = "https://explorer-gql.cheqd.io/v1/graphql"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch Cosmos SDK blockchain transactions and convert to Koinly CSV for tax reporting",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--address", required=True, help="Wallet address to fetch and process transactions for")

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--fetch-only", action="store_true", help="Only fetch transactions (skip conversion)")
    mode.add_argument("--convert-only", action="store_true", help="Only convert (skip fetching, requires --input)")

    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="BigDipper GraphQL endpoint URL")
    parser.add_argument("--batch-size", type=int, default=100, help="Number of transactions per request")
    parser.add_argument("--max-transactions", type=int, default=5000, help="Maximum number of transactions to fetch")
    parser.add_argument("--input", default=None, help="Input JSON file (required for --convert-only)")
    parser.add_argument(
        "--archive-rest-api-url", default=None, help="Base archive REST API URL for fallback tx lookups"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging to file")
    parser.add_argument("--hash", default=None, help="Transaction hash to debug")
    parser.add_argument("--alias", default=None, help="Friendly name for output files (default: uses address)")
    parser.add_argument("--output-json", default=None, help="Override JSON output filename")
    parser.add_argument("--output-csv", default=None, help="Override CSV output filename")

    return parser


def resolve_filenames(args) -> tuple[str, str]:
    base = args.alias or args.address
    json_path = args.output_json or f"{base}.json"
    csv_path = args.output_csv or f"{base}.csv"
    return json_path, csv_path


def run(args) -> None:
    json_path, csv_path = resolve_filenames(args)

    if args.convert_only:
        if not args.input:
            print("Error: --input is required when using --convert-only", file=sys.stderr)
            sys.exit(1)
        json_path = args.input

    if not args.convert_only:
        fetcher = TransactionFetcher(
            endpoint=args.endpoint,
            address=args.address,
            batch_size=args.batch_size,
            max_transactions=args.max_transactions,
            output_file=json_path,
        )
        success = fetcher.fetch_all()
        if not success:
            print("Error: fetching transactions failed", file=sys.stderr)
            sys.exit(1)

    if not args.fetch_only:
        converter = KoinlyConverter(
            input_file=json_path,
            output_file=csv_path,
            address=args.address,
            debug=args.debug,
            debug_hash=args.hash,
            archive_rest_api_url=args.archive_rest_api_url,
        )
        converter.convert()


def main():
    run(build_parser().parse_args())


if __name__ == "__main__":
    main()
