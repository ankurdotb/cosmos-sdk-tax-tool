#!/usr/bin/env python3

import requests
import json
import time
from pathlib import Path
from datetime import datetime
import argparse

class TransactionFetcher:
    """
    Fetches blockchain transactions from a GraphQL endpoint and saves them to a file.
    
    This class handles pagination, progress tracking, and resumable downloads of
    blockchain transactions for a specific address.
    """

    def __init__(self, endpoint: str, address: str, batch_size: int = 100, 
                 max_transactions: int = 5000, output_file: str = None):
        """
        Initialize the TransactionFetcher with the given parameters.
        
        Args:
            endpoint (str): The GraphQL API endpoint URL
            address (str): The blockchain address to fetch transactions for
            batch_size (int): Number of transactions to fetch per request
            max_transactions (int): Maximum number of transactions to fetch in total
            output_file (str): Path where to save the transaction data
        """
        self.endpoint = endpoint
        self.address = address
        self.batch_size = batch_size
        self.max_transactions = max_transactions
        
        # Set default output file name if none provided
        if not output_file:
            output_file = f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        self.output_file = Path(output_file)
        
        # File to track progress in case of interruption
        self.progress_file = Path("fetch_progress.json")

    def load_progress(self):
        """
        Load previously saved progress if it exists.
        
        Returns:
            dict: Progress data containing offset and previously fetched transactions
        """
        if self.progress_file.exists():
            with open(self.progress_file) as f:
                progress = json.load(f)
                print(f"Found saved progress - Offset: {progress['offset']}, Transactions: {len(progress['transactions'])}")
                return progress
        return {"offset": 0, "transactions": []}

    def save_progress(self, offset: int, transactions: list):
        """
        Save current progress to allow resuming interrupted fetches.
        
        Args:
            offset (int): Current pagination offset
            transactions (list): List of transactions fetched so far
        """
        with open(self.progress_file, "w") as f:
            json.dump({"offset": offset, "transactions": transactions}, f)
        print(f"Progress saved - Offset: {offset}, Transactions: {len(transactions)}")

    def fetch_batch(self, offset: int, retries: int = 3):
        """
        Fetch a single batch of transactions from the GraphQL endpoint.
        
        Args:
            offset (int): Pagination offset for the query
            retries (int): Number of retry attempts for failed requests
            
        Returns:
            list: Batch of transaction data
            
        Raises:
            Exception: If all retry attempts fail
        """
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
        """
        Fetch all transactions up to max_transactions limit.
        
        Returns:
            bool: True if fetch completed successfully, False otherwise
        """
        progress = self.load_progress()
        offset = progress["offset"]
        all_transactions = progress["transactions"]
        last_save = len(all_transactions)

        try:
            while len(all_transactions) < self.max_transactions:
                print(f"\nFetching batch starting at offset {offset}...")
                batch = self.fetch_batch(offset)
                
                if not batch:
                    break

                # Calculate how many transactions we can add without exceeding the limit
                space_remaining = self.max_transactions - len(all_transactions)
                batch = batch[:space_remaining]  # Truncate batch if needed
                
                current_height = batch[-1]["transaction"]["block"]["height"]
                all_transactions.extend(batch)
                
                print(f"Fetched {len(batch)} transactions.")
                print(f"Current height: {current_height}")
                print(f"Total transactions: {len(all_transactions)}/{self.max_transactions}")

                # Save progress every 1000 transactions
                if len(all_transactions) - last_save >= 1000:
                    self.save_progress(offset + len(batch), all_transactions)
                    last_save = len(all_transactions)

                if len(batch) < self.batch_size or len(all_transactions) >= self.max_transactions:
                    break

                offset += len(batch)
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
    parser = argparse.ArgumentParser(
        description="Fetch blockchain transactions from BigDipper GraphQL API",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--endpoint", required=True, 
                      help="GraphQL endpoint URL")
    parser.add_argument("--address", required=True, 
                      help="Address to fetch transactions for")
    parser.add_argument("--batch-size", type=int, default=100, 
                      help="Number of transactions per request")
    parser.add_argument("--max-transactions", type=int, default=5000,
                      help="Maximum number of transactions to fetch")
    parser.add_argument("--output", type=str, default=None,
                      help="Output file path (default: transactions_YYYYMMDD_HHMMSS.json)")
    
    args = parser.parse_args()

    fetcher = TransactionFetcher(
        endpoint=args.endpoint,
        address=args.address,
        batch_size=args.batch_size,
        max_transactions=args.max_transactions,
        output_file=args.output
    )

    fetcher.fetch_all()

if __name__ == "__main__":
    main()
