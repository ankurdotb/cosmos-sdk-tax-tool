# Cosmos SDK to Koinly Tax Tool

This repository contains tools for fetching and processing blockchain transactions from Cosmos SDK chains into a format compatible with cryptocurrency tax reporting platforms like [Koinly](https://koinly.io/?via=AF4EDE54&utm_source=friend).

## Motivation

The go-to tool for exporting transactions from Cosmos SDK chains is [Stake.tax](https://stake.tax/). I would **highly-recommend** using that where possible, because it covers a far broader range of export formats.

However, I had a personal need to build this because:

- For any chains not supported by default ("Cosmos+"), you need to have an archive REST API endpoint that *definitely* covers the period being imported for. As a user, it's very hard to know if this is true.
- Often, a Stake.tax export will fail because the REST API being used either crashes or gets rate limited and it's hard to recover.
- If you've ever enabled automatic restaking, your transaction history will be full of reward withdrawal and restake events every hour/few hours/day. When imported into Koinly, this is a massive pain and I wanted to build something that would *summarise* such transactions to a maximum of one per day.

A **pre-requisite** for using this tool is that the target chain must have a **BigDipper blockchain explorer**. This is because BigDipper indexes all transactions and exposes it via a GraphQL API that is far more performant when fetching transactions, as opposed to a Cosmos SDK REST API which might ditch historical details due to default pruning that removes historical states beyond a certain number of days/weeks.

## Overview

The toolkit consists of two main components:

1. `fetch_transactions.py`: Fetches transaction history from a BigDipper explorer GraphQL endpoint
2. `tx_to_koinly.py`: Converts the fetched transactions into Koinly's CSV import format

## Requirements

- Python 3.7+
- `requests` library (for GraphQL API calls)

### Install dependencies

```sh
pip install -r requirements.txt
```

## Usage

### 1. Fetch Transactions

First, fetch your transaction history:

```sh
python fetch_transactions.py \
    --endpoint "YOUR_GRAPHQL_ENDPOINT" \
    --address "YOUR_WALLET_ADDRESS" \
    --max-transactions 5000 \
    --batch-size 100
```

#### Options

- `--endpoint`: GraphQL API endpoint URL (required), e.g., `https://explorer-gql.cheqd.io/v1/graphql` for [cheqd's blockchain explorer](https://explorer.cheqd.io/)
- `--address`: Your wallet address (required)
- `--batch-size`: Number of transactions per request (default: 100)
- `--max-transactions`: Maximum transactions to fetch (default: 5000)
- `--output`: Custom output filename (default: transactions_YYYYMMDD_HHMMSS.json)

The script includes automatic retry logic and progress saving in case of interruptions.

### 2. Convert to Koinly Format

After fetching transactions, convert them to Koinly format:

```sh
python tx_to_koinly.py \
    --input "transactions_20240122_123456.json" \
    --output "koinly_export.csv" \
    --address "YOUR_WALLET_ADDRESS"
```

#### Options

- `--input`: Input JSON file from previous step (required)
- `--output`: Output CSV filename (default: koinly_export.csv)
- `--address`: Your wallet address (required)
- `--debug`: Enable debug logging
- `--hash`: Transaction hash to debug specific transactions

## Understanding the Code

### fetch_transactions.py

- Uses pagination to handle large transaction histories
- Saves progress to allow resuming interrupted fetches
- Implements retry logic for failed requests
- Configurable batch sizes and limits

#### Key classes

- `TransactionFetcher`: Handles GraphQL API interaction and transaction fetching

### tx_to_koinly.py

The converter handles various transaction types:

- Transfers (bank send)
- Staking rewards
- Delegations/Undelegations
- Redelegations
- IBC transfers
- Governance votes
- Authz operations

#### Key features

- Deduplicates transactions
- Consolidates multiple Authz reward claims per day
- Converts amounts from blockchain denomination (ncheq) to standard units (CHEQ)
- Detailed logging for debugging

#### Key classes

- `KoinlyConverter`: Core conversion logic and transaction processing

## Modifying the Code

### Adding New Transaction Types

To add support for new transaction types:

1. Identify the message type in the GraphQL response
2. Add a new condition in the `process_transaction` method
3. Extract relevant information (amounts, addresses, etc.)
4. Map to appropriate Koinly fields

Example structure:
```python
elif msg_type == '/your.new.message.type':
    record['Label'].add('appropriate_label')
    record['Sent Amount'] = amount
    record['Sent Currency'] = 'CURRENCY'
    # ... set other fields as needed
```

### Customizing Output Format

The Koinly CSV format is defined by `KOINLY_HEADERS` in `KoinlyConverter`. Modify these headers and the corresponding record creation to match different platform requirements.

## Debugging

Enable debug mode for detailed logging:
```sh
python tx_to_koinly.py --debug --hash YOUR_TX_HASH ...
```

Debug logs include:

- Raw transaction data
- Processing steps
- Reward calculations
- Error details

## Contributing

Feel free to submit issues and enhancement requests!
