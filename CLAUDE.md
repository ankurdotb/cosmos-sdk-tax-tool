# Cosmos SDK Tax Tool

A Python CLI tool that converts Cosmos SDK blockchain transactions into Koinly-compatible CSV for tax reporting.

## Pipeline

`tax_tool.py` is the CLI entry point. It fetches transactions and converts to Koinly CSV in one step.

Library modules (imported by `tax_tool.py`, not invoked directly):
1. **fetch_transactions.py** — `TransactionFetcher` class: fetches from BigDipper GraphQL API → JSON
2. **tx_to_koinly.py** — `KoinlyConverter` class: converts JSON → Koinly CSV

## Key Conventions

- Amounts are stored on-chain as **ncheq** (nano-CHEQ). Convert to CHEQ by dividing by `1_000_000_000`.
- Transaction deduplication is by tx hash (GraphQL may return duplicates from multi-message txs).
- Authz reward claims are consolidated to max 1 record per day (validators claim hundreds of times daily).
- Failed transactions are labeled as `cost` (only the fee matters for tax purposes).

## Transaction Types

Bank sends, staking rewards, delegations, undelegations, redelegations, IBC transfers, governance votes, authz exec/grant/revoke.

## Running

```bash
# Unified: fetch + convert in one step (defaults to cheqd GraphQL endpoint)
python3 tax_tool.py --address <wallet>

# With alias for friendlier filenames
python3 tax_tool.py --address <wallet> --alias myvalidator

# Custom endpoint for other Cosmos SDK chains
python3 tax_tool.py --address <wallet> --endpoint <graphql-url>

# Fetch only or convert only
python3 tax_tool.py --address <wallet> --fetch-only
python3 tax_tool.py --address <wallet> --convert-only --input <file>.json
```

## Dependencies

- Python 3.13+
- `requests` (only external dependency)
- Install: `pip install -r requirements.txt`
