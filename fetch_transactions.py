#!/usr/bin/env python3

import requests
import json
import time
from pathlib import Path
from datetime import datetime
import argparse

class TransactionFetcher:
    def __init__(self, endpoint, address, batch_size=100):
        self.endpoint = endpoint
        self.address = address
        self.batch_size = batch_size
        self.progress_file = Path("fetch_progress.json")
        self.output_file = Path(f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")

    def load_progress(self):
        if self.progress_file.exists():
            with open(self.progress_file) as f:
                progress = json.load(f)
                print(f"Found saved progress - Offset: {progress['offset']}, Transactions: {len(progress['transactions'])}")
                return progress
        return {"offset": 0, "transactions": []}

    def save_progress(self, offset, transactions):
        with open(self.progress_file, "w") as f:
            json.dump({"offset": offset, "transactions": transactions}, f)
        print(f"Progress saved - Offset: {offset}, Transactions: {len(transactions)}")

    def fetch_batch(self, offset, retries=3):
        query = """
        query GetMessagesByAddress($limit: bigint = 100, $offset: bigint = 0, $types: _text = "{}") {
            messagesByAddress: messages_by_address(
                args: {
                    addresses: "{%s}",
                    types: $types,
                    limit: $limit,
                    offset: $offset
                }
            ) {
                transaction {
                    height
                    hash
                    success
                    messages
                    logs
                    fee
                    block {
                        height
                        timestamp
                    }
                }
            }
        }
        """ % self.address

        variables = {
            "limit": self.batch_size,
            "offset": offset,
            "types": "{}"
        }

        for attempt in range(retries):
            try:
                response = requests.post(
                    self.endpoint,
                    json={"query": query, "variables": variables},
                    headers={"Content-Type": "application/json"}
                )
                response.raise_for_status()
                data = response.json()
                
                if "errors" in data:
                    raise Exception(f"GraphQL errors: {data['errors']}")
                
                return data["data"]["messagesByAddress"]
                
            except Exception as e:
                if attempt == retries - 1:
                    raise
                print(f"Attempt {attempt + 1} failed: {str(e)}. Retrying...")
                time.sleep(2)

    def fetch_all(self):
        progress = self.load_progress()
        offset = progress["offset"]
        all_transactions = progress["transactions"]
        last_save = len(all_transactions)

        try:
            while True:
                print(f"\nFetching batch starting at offset {offset}...")
                batch = self.fetch_batch(offset)
                
                if not batch:
                    break

                current_height = batch[-1]["transaction"]["block"]["height"]
                all_transactions.extend(batch)
                
                print(f"Fetched {len(batch)} transactions.")
                print(f"Current height: {current_height}")
                print(f"Total transactions: {len(all_transactions)}")

                # Save progress every 1000 transactions
                if len(all_transactions) - last_save >= 1000:
                    self.save_progress(offset + self.batch_size, all_transactions)
                    last_save = len(all_transactions)

                offset += self.batch_size
                time.sleep(0.5)  # Rate limiting

            # Save final results
            with open(self.output_file, "w") as f:
                json.dump(all_transactions, f, indent=2)
            print(f"\nSuccess! Saved {len(all_transactions)} transactions to {self.output_file}")
            
            # Clean up progress file
            if self.progress_file.exists():
                self.progress_file.unlink()

        except Exception as e:
            print(f"\nError occurred: {str(e)}")
            self.save_progress(offset, all_transactions)
            print("Progress saved. You can resume by running the script again.")
            return False

        return True

def main():
    parser = argparse.ArgumentParser(description="Fetch blockchain transactions from BigDipper")
    parser.add_argument("--endpoint", required=True, help="GraphQL endpoint URL")
    parser.add_argument("--address", required=True, help="Address to fetch transactions for")
    parser.add_argument("--batch-size", type=int, default=100, help="Number of transactions per request")
    
    args = parser.parse_args()

    fetcher = TransactionFetcher(
        endpoint=args.endpoint,
        address=args.address,
        batch_size=args.batch_size
    )

    fetcher.fetch_all()

if __name__ == "__main__":
    main()
