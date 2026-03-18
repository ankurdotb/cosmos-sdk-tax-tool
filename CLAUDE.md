# Cosmos SDK Tax Tool

A Python CLI tool that converts Cosmos SDK blockchain transactions into Koinly-compatible CSV for tax reporting.

## Pipeline

1. **fetch_transactions.py** — Fetches transactions from BigDipper GraphQL API → saves as JSON
2. **tx_to_koinly.py** — Converts JSON transactions → Koinly CSV format

## Key Conventions

- Amounts are stored on-chain as **ncheq** (nano-CHEQ). Convert to CHEQ by dividing by `1_000_000_000`.
- Transaction deduplication is by tx hash (GraphQL may return duplicates from multi-message txs).
- Authz reward claims are consolidated to max 1 record per day (validators claim hundreds of times daily).
- Failed transactions are labeled as `cost` (only the fee matters for tax purposes).

## Transaction Types

Bank sends, staking rewards, delegations, undelegations, redelegations, IBC transfers, governance votes, authz exec/grant/revoke.

## Running

```bash
python3 fetch_transactions.py --endpoint <graphql-url> --address <wallet> --output txs.json
python3 tx_to_koinly.py --input txs.json --output koinly.csv --address <wallet>
```

## Dependencies

- Python 3.13+
- `requests` (only external dependency)
- Install: `pip install -r requirements.txt`
