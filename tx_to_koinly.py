#!/usr/bin/env python3

import json
import csv
from datetime import datetime
import argparse
from typing import List, Dict, Any
from pathlib import Path
import logging

class KoinlyConverter:
    def __init__(self, input_file: str, output_file: str, address: str, debug: bool = False, debug_hash: str = None):
        self.input_file = input_file
        self.output_file = output_file
        self.address = address
        self.debug_hash = debug_hash
        self.NCHEQ_TO_CHEQ = 1_000_000_000  # 1 CHEQ = 10^9 ncheq
        self.KOINLY_HEADERS = [
            'Date', 'Sent Amount', 'Sent Currency', 'Received Amount', 'Received Currency',
            'Fee Amount', 'Fee Currency', 'Recipient', 'Sender', 'Label', 'TxHash', 'Description'
        ]

        # Set up logging
        self.logger = logging.getLogger('koinly_converter')
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)
        
        # Console handler with minimal output
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(console_handler)

        # File handler with detailed output if debug is enabled
        if debug:
            file_handler = logging.FileHandler('koinly_debug.log', mode='w')  # 'w' mode overwrites the file
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(file_handler)

    def load_transactions(self) -> List[Dict[str, Any]]:
        """Load transactions and deduplicate by hash"""
        with open(self.input_file, 'r') as f:
            transactions = json.load(f)
        
        # Use a dictionary to deduplicate by hash
        unique_txs = {}
        for tx in transactions:
            tx_hash = tx['transaction']['hash']
            if tx_hash not in unique_txs:
                unique_txs[tx_hash] = tx
        
        self.logger.info(f"Loaded {len(transactions)} transactions, {len(unique_txs)} unique")
        return list(unique_txs.values())

    def parse_timestamp(self, timestamp: str) -> str:
        """Convert blockchain timestamp to Koinly format"""
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M')

    def get_reward_amount(self, tx_data: Dict) -> float:
        """Extract reward amount from transaction logs"""
        if self.debug_hash and tx_data['hash'] == self.debug_hash:
            self.logger.debug(f"\nProcessing transaction: {tx_data['hash']}")
            self.logger.debug(f"Full transaction data: {json.dumps(tx_data, indent=2)}")
            
        logs = tx_data.get('logs', [])
        self.logger.debug(f"Found {len(logs)} log entries")
        
        if not logs:
            self.logger.debug("No logs found!")
            self.logger.debug(f"Transaction data: {json.dumps(tx_data, indent=2)}")
        
        total_amount = 0
        for log in logs:
            self.logger.debug(f"\nChecking log entry: {json.dumps(log, indent=2)}")
            for event in log.get('events', []):
                self.logger.debug(f"Event type: {event.get('type')}")
                if event.get('type') == 'withdraw_rewards':
                    attrs = event.get('attributes', [])
                    self.logger.debug(f"Found withdraw_rewards event with {len(attrs)} attributes")
                    for attr in attrs:
                        if attr.get('key') == 'amount' and attr.get('value', '').endswith('ncheq'):
                            amount = float(attr['value'].rstrip('ncheq'))
                            total_amount += amount
                            self.logger.debug(f"Added amount: {amount} ncheq")
        
        final_amount = total_amount / self.NCHEQ_TO_CHEQ if total_amount > 0 else 0
        self.logger.debug(f"Final total amount: {final_amount} CHEQ")
        return final_amount

    def get_fee(self, tx_data: Dict) -> float:
        """Extract fee amount from transaction"""
        if tx_data.get('fee', {}).get('amount'):
            return float(tx_data['fee']['amount'][0]['amount']) / self.NCHEQ_TO_CHEQ
        return 0.0

    def process_transaction(self, tx: Dict) -> Dict:
        """Convert a single transaction to Koinly format"""
        tx_data = tx['transaction']
        timestamp = self.parse_timestamp(tx_data['block']['timestamp'])
        fee_amount = self.get_fee(tx_data)

        # Initialize record with empty values
        record = {
            'Date': timestamp,
            'Sent Amount': '',
            'Sent Currency': '',
            'Received Amount': '',
            'Received Currency': '',
            'Fee Amount': fee_amount if fee_amount > 0 else '',
            'Fee Currency': 'CHEQ' if fee_amount > 0 else '',
            'Label': set(),
            'TxHash': tx_data['hash'],
            'Description': '',
            'Recipient': set(),
            'Sender': set()
        }

        self.logger.debug(f"\nProcessing transaction {tx_data['hash']}")
        for msg in tx_data.get('messages', []):
            msg_type = msg.get('@type')
            
            if msg_type == '/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward':
                self.logger.debug(f"Found reward message: {json.dumps(msg, indent=2)}")

            elif msg_type == '/cosmos.staking.v1beta1.MsgDelegate':
                amount = float(msg['amount']['amount']) / self.NCHEQ_TO_CHEQ
                record['Sent Amount'] = amount
                record['Sent Currency'] = 'CHEQ'
                record['Label'].add('staking')
                record['Sender'].add(msg['delegator_address'])
                record['Recipient'].add(msg['validator_address'])

            elif msg_type == '/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward':
                amount = self.get_reward_amount(tx_data)
                if amount > 0:
                    record['Received Amount'] = amount
                    record['Received Currency'] = 'CHEQ'
                    record['Label'].add('reward')
                    record['Sender'].add(msg['validator_address'])
                    record['Recipient'].add(self.address)

            elif msg_type == '/cosmos.bank.v1beta1.MsgSend':
                amount = float(msg['amount'][0]['amount']) / self.NCHEQ_TO_CHEQ
                is_sender = msg['from_address'] == self.address
                
                if is_sender:
                    record['Sent Amount'] = amount
                    record['Sent Currency'] = 'CHEQ'
                else:
                    record['Received Amount'] = amount
                    record['Received Currency'] = 'CHEQ'
                
                record['Sender'].add(msg['from_address'])
                record['Recipient'].add(msg['to_address'])

            elif msg_type == '/ibc.applications.transfer.v1.MsgTransfer':
                amount = float(msg['token']['amount']) / self.NCHEQ_TO_CHEQ
                is_sender = msg['sender'] == self.address
                
                if is_sender:
                    record['Sent Amount'] = amount
                    record['Sent Currency'] = 'CHEQ'
                else:
                    record['Received Amount'] = amount
                    record['Received Currency'] = 'CHEQ'
                
                record['Label'].add('transfer')
                record['Sender'].add(msg['sender'])
                record['Recipient'].add(msg['receiver'])

            elif msg_type == '/cosmos.gov.v1beta1.MsgVote':
                if fee_amount > 0:
                    record['Label'].add('governance')

        # Convert sets to comma-separated strings
        record['Label'] = ','.join(sorted(record['Label'])) if record['Label'] else ''
        record['Sender'] = ','.join(sorted(record['Sender'])) if record['Sender'] else ''
        record['Recipient'] = ','.join(sorted(record['Recipient'])) if record['Recipient'] else ''

        return record

    def convert(self):
        transactions = self.load_transactions()
        koinly_records = []
        
        self.logger.info(f"Processing {len(transactions)} transactions...")
        
        for tx in transactions:
            try:
                record = self.process_transaction(tx)
                if any([record['Sent Amount'], record['Received Amount'], record['Fee Amount']]):
                    koinly_records.append(record)
            except Exception as e:
                self.logger.error(f"Error processing transaction {tx.get('transaction', {}).get('hash')}: {str(e)}")
                continue

        # Sort by timestamp
        koinly_records.sort(key=lambda x: x['Date'])
        
        # Write to CSV (with 'w' mode to overwrite)
        with open(self.output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.KOINLY_HEADERS)
            writer.writeheader()
            writer.writerows(koinly_records)
        
        self.logger.info(f"Processed {len(koinly_records)} records")
        self.logger.info(f"Output saved to {self.output_file}")

def main():
    parser = argparse.ArgumentParser(description="Convert blockchain transactions to Koinly CSV format")
    parser.add_argument("--input", required=True, help="Input JSON file from GraphQL fetch")
    parser.add_argument("--output", help="Output CSV file name", default="koinly_export.csv")
    parser.add_argument("--address", required=True, help="Your wallet address")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging to file")
    parser.add_argument("--hash", help="Transaction hash to debug")
    
    args = parser.parse_args()
    
    converter = KoinlyConverter(args.input, args.output, args.address, args.debug, args.hash)
    converter.convert()

if __name__ == "__main__":
    main()
