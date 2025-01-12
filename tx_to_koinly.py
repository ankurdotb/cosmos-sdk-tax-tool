#!/usr/bin/env python3

import logging
import csv
import argparse
import json
from datetime import datetime
from typing import List, Dict, Any

class KoinlyConverter:
    # In the KoinlyConverter class initialization
    def __init__(self, input_file: str, output_file: str, address: str, debug: bool = False):
        self.input_file = input_file
        self.output_file = output_file
        self.address = address
        self.NCHEQ_TO_CHEQ = 1_000_000_000  # 1 CHEQ = 10^9 ncheq
        self.KOINLY_HEADERS = [
            'Date', 'Sent Amount', 'Sent Currency', 'Received Amount', 'Received Currency',
            'Fee Amount', 'Fee Currency', 'Recipient', 'Sender', 'Label', 'TxHash', 'Description'
        ]
        self.authz_summary = {}

        # Set up logging to terminal
        self.logger = logging.getLogger('koinly_converter')
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG if debug else logging.INFO)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(console_handler)

    def load_transactions(self) -> List[Dict[str, Any]]:
        with open(self.input_file, 'r') as f:
            transactions = json.load(f)
        
        unique_txs = {}
        skipped_txs = 0
        for tx in transactions:
            tx_hash = tx.get('transaction', {}).get('hash')
            if tx_hash:
                if tx_hash not in unique_txs:
                    unique_txs[tx_hash] = tx
            else:
                skipped_txs += 1
        
        if skipped_txs > 0:
            self.logger.debug(f"Skipped {skipped_txs} transactions missing hash")
        
        self.logger.info(f"Loaded {len(transactions)} transactions, {len(unique_txs)} unique")
        return list(unique_txs.values())

    def parse_timestamp(self, timestamp: str) -> str:
        """Convert blockchain timestamp to Koinly format"""
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M')

    def get_reward_amount(self, tx_data: Dict) -> float:
        """Extract reward amount from transaction logs, summing all rewards in the transaction"""
        total_amount = 0
        logs = tx_data.get('logs', [])
        
        if not logs:
            return 0.0
        
        for log in logs:
            for event in log.get('events', []):
                if event.get('type') == 'coin_received':
                    attributes = event.get('attributes', [])
                    amount = None
                    is_receiver = False
                    
                    for attr in attributes:
                        if attr.get('key') == 'receiver' and attr.get('value') == self.address:
                            is_receiver = True
                        if attr.get('key') == 'amount' and attr.get('value', '').endswith('ncheq'):
                            amount = float(attr.get('value').rstrip('ncheq'))
                    
                    if is_receiver and amount:
                        total_amount += amount / self.NCHEQ_TO_CHEQ
        
        return total_amount

    def get_fee(self, tx_data: Dict) -> float:
        """Extract fee amount from transaction"""
        fee_amount = 0
        fees = tx_data.get('tx', {}).get('auth_info', {}).get('fee', {}).get('amount', [])
        for fee in fees:
            if fee.get('denom') == 'ncheq':
                fee_amount += float(fee.get('amount')) / self.NCHEQ_TO_CHEQ
        return fee_amount

    def process_transaction(self, tx: Dict) -> Dict:
        tx_hash = tx.get('transaction', {}).get('hash')
        if not tx_hash:
            self.logger.warning(f"Transaction missing 'hash': {tx}")
            return None

        tx_data = tx.get('transaction', {})
        if not tx_data:
            self.logger.warning(f"Transaction missing 'transaction' data: {tx}")
            return None

        messages = tx_data.get('tx', {}).get('body', {}).get('messages', [])
        has_non_client_updates = any(
            msg.get('@type') != '/ibc.core.client.v1.MsgUpdateClient'
            for msg in messages
        )
        
        # Skip entirely if only IBC client updates
        if not has_non_client_updates:
            return None

        timestamp = self.parse_timestamp(tx_data['block']['timestamp'])
        fee_amount = self.get_fee(tx_data)
        
        record = {
            'Date': timestamp,
            'Sent Amount': '',
            'Sent Currency': '',
            'Received Amount': '',
            'Received Currency': '',
            'Fee Amount': fee_amount if fee_amount > 0 and has_non_client_updates else '',
            'Fee Currency': 'CHEQ' if fee_amount > 0 and has_non_client_updates else '',
            'Label': set(),
            'TxHash': tx_hash,
            'Description': '',
            'Recipient': set(),
            'Sender': set()
        }

        # Process messages
        for msg in messages:
            msg_type = msg.get('@type')
            
            # Skip IBC client update messages
            if msg_type == '/ibc.core.client.v1.MsgUpdateClient':
                record['Label'].add('discard')
                record['Description'] = 'IBC client update'
                continue
            
            # Bank Send
            if msg_type == '/cosmos.bank.v1beta1.MsgSend':
                amount = float(msg['amount'][0]['amount']) / self.NCHEQ_TO_CHEQ
                is_sender = msg['from_address'] == self.address
                
                # Always record both sender and recipient
                record['Sender'].add(msg['from_address'])
                record['Recipient'].add(msg['to_address'])
                
                if is_sender:
                    record['Sent Amount'] = amount
                    record['Sent Currency'] = 'CHEQ'
                elif msg['to_address'] == self.address:
                    record['Received Amount'] = amount
                    record['Received Currency'] = 'CHEQ'
                record['Label'].add('transfer')

            # Staking Delegate - only record fee and description
            elif msg_type == '/cosmos.staking.v1beta1.MsgDelegate':
                amount = msg.get('amount')
                if amount:
                    amount_value = float(amount['amount']) / self.NCHEQ_TO_CHEQ
                    record['Label'].add('cost')
                    record['Sent Amount'] = ''
                    record['Sent Currency'] = ''
                    record['Received Amount'] = ''
                    record['Received Currency'] = ''
                    record['Sender'].add(self.address)
                    record['Description'] = f'Delegated {amount_value} CHEQ to {msg["validator_address"]}'
                else:
                    print(f"Warning: 'amount' is None for transaction {tx_hash}")

            # Cancel unbonding - only record fee and description
            elif msg_type == '/cosmos.staking.v1beta1.MsgCancelUnbondingDelegation':
                amount = msg.get('amount')
                if amount:
                    amount_value = float(amount['amount']) / self.NCHEQ_TO_CHEQ
                    record['Label'].add('cost')
                    record['Sent Amount'] = ''
                    record['Sent Currency'] = ''
                    record['Received Amount'] = ''
                    record['Received Currency'] = ''
                    record['Sender'].add(self.address)
                    record['Description'] = f'Cancelled unbonding delegation {amount_value} CHEQ from {msg["validator_address"]}'
                else:
                    print(f"Warning: 'amount' is None for transaction {tx_hash}")

            # Staking Redelegate - only record fee and description
            elif msg_type == '/cosmos.staking.v1beta1.MsgBeginRedelegate':
                amount = msg.get('amount')
                if amount:
                    amount_value = float(amount['amount']) / self.NCHEQ_TO_CHEQ
                    record['Label'].add('cost')
                    record['Sent Amount'] = ''
                    record['Sent Currency'] = ''
                    record['Received Amount'] = ''
                    record['Received Currency'] = ''
                    record['Sender'].add(self.address)
                    record['Description'] = f'Redelegated {amount_value} CHEQ from {msg["validator_src_address"]} to {msg["validator_dst_address"]}'
                else:
                    print(f"Warning: 'amount' is None for transaction {tx_hash}")

            # IBC Transfer
            elif msg_type == '/ibc.applications.transfer.v1.MsgTransfer':
                amount = float(msg['token']['amount']) / self.NCHEQ_TO_CHEQ
                is_sender = msg['sender'] == self.address
                
                if is_sender:
                    record['Sent Amount'] = amount
                    record['Sent Currency'] = 'CHEQ'
                    record['Recipient'].add(msg['receiver'])
                elif msg['receiver'] == self.address:
                    record['Received Amount'] = amount
                    record['Received Currency'] = 'CHEQ'
                    record['Sender'].add(msg['sender'])
                record['Label'].add('transfer')

            # Authz Exec (need to process wrapped messages)
            elif msg_type == '/cosmos.authz.v1beta1.MsgExec':
                date = timestamp.split(' ')[0]
                if date not in self.authz_summary:
                    self.authz_summary[date] = {
                        'Received Amount': 0,
                        'Fee Amount': 0,
                        'TxHashes': [],
                        'Label': set(),
                        'Description': ''
                    }

                # Process each message within the authz exec
                for inner_msg in msg.get('msgs', []):
                    inner_msg_type = inner_msg.get('@type')
                    
                    # Handle Withdraw Delegator Reward
                    if inner_msg_type == '/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward':
                        reward_amount = self.get_reward_amount(tx_data)
                        if reward_amount > 0:
                            self.authz_summary[date]['Received Amount'] += reward_amount
                            self.authz_summary[date]['Label'].add('reward')

                    # Handle Delegate
                    elif inner_msg_type == '/cosmos.staking.v1beta1.MsgDelegate':
                        amount = inner_msg.get('amount')
                        if amount:
                            amount_value = float(amount['amount']) / self.NCHEQ_TO_CHEQ
                            self.authz_summary[date]['Label'].add('cost')

                # Extract coins_received amount from logs
                logs = tx_data.get('logs', [])
                for log in logs:
                    for event in log.get('events', []):
                        if event.get('type') == 'coin_received':
                            attributes = event.get('attributes', [])
                            amount = None
                            is_receiver = False
                            
                            # Check both receiver and amount in the same event
                            for attr in attributes:
                                if attr.get('key') == 'receiver' and attr.get('value') == self.address:
                                    is_receiver = True
                                if attr.get('key') == 'amount' and attr.get('value', '').endswith('ncheq'):
                                    amount = float(attr.get('value').rstrip('ncheq'))
                            
                            # Only set received amount if this event was for our address
                            if is_receiver and amount:
                                self.authz_summary[date]['Received Amount'] += amount / self.NCHEQ_TO_CHEQ
                                self.authz_summary[date]['Label'].add('authz')

                # Sum up fees
                fee_amount = self.get_fee(tx_data)
                self.authz_summary[date]['Fee Amount'] += fee_amount

                # Add transaction hash to description
                self.authz_summary[date]['TxHashes'].append(tx_hash)

        return record

    def convert(self):
        transactions = self.load_transactions()
        koinly_records = []
        
        self.logger.info(f"Processing {len(transactions)} transactions...")
        
        for tx in transactions:
            try:
                record = self.process_transaction(tx)
                if record is None:
                    continue
                    
                # Convert sets to strings for CSV output
                if isinstance(record['Label'], set):
                    record['Label'] = ','.join(sorted(record['Label']))
                if isinstance(record['Recipient'], set):
                    record['Recipient'] = ','.join(sorted(record['Recipient']))
                if isinstance(record['Sender'], set):
                    record['Sender'] = ','.join(sorted(record['Sender']))

                # Only add records that have some meaningful data
                if any([
                    record['Sent Amount'], 
                    record['Received Amount'], 
                    record['Fee Amount'],
                    record.get('Description')  # Include if there's a description
                ]):
                    koinly_records.append(record)
            except Exception as e:
                self.logger.error(f"Error processing transaction {tx.get('transaction', {}).get('hash')}: {str(e)}")
                continue

        # Sort by timestamp
        koinly_records.sort(key=lambda x: x['Date'])
        
        # Write summarized Authz Exec transactions
        for date, summary in self.authz_summary.items():
            record = {
                'Date': f'{date} 23:59',
                'Sent Amount': '',
                'Sent Currency': '',
                'Received Amount': summary['Received Amount'],
                'Received Currency': 'CHEQ',
                'Fee Amount': summary['Fee Amount'],
                'Fee Currency': 'CHEQ',
                'Recipient': '',
                'Sender': self.address,
                'Label': ','.join(summary['Label']),
                'TxHash': '',
                'Description': 'TxHashes: ' + ', '.join(summary['TxHashes'])
            }
            koinly_records.append(record)

        # Ensure we're using the correct file output
        try:
            with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.KOINLY_HEADERS)
                writer.writeheader()
                
                if koinly_records:
                    writer.writerows(koinly_records)
                    self.logger.info(f"Successfully wrote {len(koinly_records)} records to {self.output_file}")
                else:
                    self.logger.warning("No records to write to CSV. Check your input file and transaction processing logic.")
        except IOError as e:
            self.logger.error(f"Error writing to file {self.output_file}: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description="Convert blockchain transactions to Koinly CSV format")
    parser.add_argument("--input", required=True, help="Input JSON file from GraphQL fetch")
    parser.add_argument("--output", help="Output CSV file name", default="koinly_export.csv")
    parser.add_argument("--address", required=True, help="Your wallet address")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    converter = KoinlyConverter(args.input, args.output, args.address, args.debug)
    converter.convert()

if __name__ == "__main__":
    main()
