#!/usr/bin/env python3

"""
This script converts blockchain transaction data from JSON format to Koinly-compatible CSV.
It processes various transaction types including rewards, delegations, transfers, and IBC transactions.
The output follows Koinly's import format for easier tax reporting and portfolio tracking.

Usage:
    python tx_to_koinly.py --input transactions.json --output koinly_export.csv --address your_wallet_address
"""

import json
import csv
from datetime import datetime
import argparse
from typing import List, Dict, Any
from pathlib import Path
import logging

class KoinlyConverter:
    def __init__(self, input_file: str, output_file: str, address: str, debug: bool = False, debug_hash: str = None):
        """
        Converts blockchain transaction data to Koinly-compatible format.

        This class handles:
        - Loading and deduplicating transactions
        - Converting blockchain timestamps to Koinly format
        - Extracting and processing different transaction types (rewards, delegations, transfers)
        - Consolidating multiple reward claims from Authz transactions
        - Converting amounts from ncheq (nano CHEQ) to CHEQ
        """
        self.input_file = input_file
        self.output_file = output_file
        self.address = address
        self.debug_hash = debug_hash

        """
        NCHEQ_TO_CHEQ: Conversion factor from nano-CHEQ to CHEQ (10^9)
        KOINLY_HEADERS: Required CSV column headers for Koinly import format:
            - Date: Transaction timestamp
            - Sent/Received Amount/Currency: Transaction values
            - Fee Amount/Currency: Transaction fees
            - Label: Transaction type classification
            - Description: Human-readable transaction details
            - TxHash: Unique transaction identifier
        """
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
        """
        Loads and deduplicates transactions from the input JSON file.
        Deduplication is necessary because the same transaction may appear
        multiple times in the GraphQL response due to multiple messages.

        Returns:
            List[Dict[str, Any]]: List of unique transactions, indexed by hash

        Note: Preserves the first occurrence of each transaction when duplicates exist
        """
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
        """
        Converts blockchain UTC timestamps (Z-suffixed ISO format)
        to Koinly's expected format: YYYY-MM-DD HH:MM
        """
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M')

    def get_reward_amount(self, tx_data: Dict) -> float:
        """
        Extracts reward amounts from transaction logs.
        Rewards are found in withdraw_rewards events within transaction logs.
        Amounts are in ncheq and need to be converted to CHEQ.

        Args:
            tx_data (Dict): Transaction data containing logs and events

        Returns:
            float: Total reward amount in CHEQ (not ncheq)

        Debug Note:
            If debug_hash matches transaction hash, full transaction data
            is logged to help troubleshoot reward extraction issues
        """
        if not tx_data or not tx_data.get('success', False):
            return 0.0
            
        if self.debug_hash and tx_data.get('hash') == self.debug_hash:
            self.logger.debug(f"\nProcessing transaction: {tx_data.get('hash')}")
            self.logger.debug(f"Full transaction data: {json.dumps(tx_data, indent=2)}")
        
        logs = tx_data.get('logs', [])
        if not logs:
            self.logger.debug(f"No logs found in tx {tx_data.get('hash')}")
            return 0.0
            
        total_amount = 0
        for log in logs:
            if not isinstance(log, dict):
                continue
                
            events = log.get('events', [])
            if not events:
                continue
                
            for event in events:
                if not isinstance(event, dict):
                    continue
                    
                if event.get('type') == 'withdraw_rewards':
                    attrs = event.get('attributes', [])
                    for attr in attrs:
                        if not isinstance(attr, dict):
                            continue
                        if attr.get('key') == 'amount' and attr.get('value', '').endswith('ncheq'):
                            try:
                                amount = float(attr['value'].rstrip('ncheq'))
                                total_amount += amount
                            except (ValueError, TypeError):
                                self.logger.debug(f"Invalid amount value in tx {tx_data.get('hash')}")
                                continue
        
        return total_amount / self.NCHEQ_TO_CHEQ if total_amount > 0 else 0.0

    # Handle redelegation rewards safely
    def get_redelegate_reward_amount(self, tx_data: Dict, msg: Dict) -> float:
        """
        Redelegation transactions are complex because:
        1. They automatically trigger reward withdrawal
        2. The reward amount appears in coin_received events
        3. Multiple coin_received events may exist
        4. Need to match receiver address with reward amount
        """
        if not tx_data or not msg or not tx_data.get('success', False):
            return 0.0

        try:
            logs = tx_data.get('logs', [])
            if not logs:
                self.logger.debug(f"No logs found in redelegation tx {tx_data.get('hash')}")
                return 0.0

            total_reward = 0.0
            for log in logs:
                if not isinstance(log, dict):
                    continue

                events = log.get('events', [])
                if not events:
                    continue

                for event in events:
                    if not isinstance(event, dict):
                        continue

                    if event.get('type') == 'coin_received':
                        attributes = event.get('attributes', [])
                        if not attributes:
                            continue

                        amount = None
                        is_receiver = False
                        
                        # Process all attributes in the event
                        for attr in attributes:
                            if not isinstance(attr, dict):
                                continue
                                
                            if attr.get('key') == 'receiver' and attr.get('value') == self.address:
                                is_receiver = True
                            if attr.get('key') == 'amount' and attr.get('value', '').endswith('ncheq'):
                                try:
                                    amount = float(attr.get('value', '0').rstrip('ncheq'))
                                except (ValueError, TypeError):
                                    self.logger.debug(f"Invalid amount in redelegation reward: {attr.get('value')}")
                                    continue
                        
                        # Only add reward amount if this event was for our address
                        if is_receiver and amount:
                            total_reward += amount

            return total_reward / self.NCHEQ_TO_CHEQ if total_reward > 0 else 0.0

        except Exception as e:
            self.logger.debug(f"Error processing redelegation rewards for tx {tx_data.get('hash')}: {str(e)}")
            return 0.0

    # Rewards collected through Authz Exec transactions need to be consolidated since there are often hundreds per day
    def consolidate_authz_records(self, records):
        """
        Special handling for Authz transactions is required because:
        1. Validators often use Authz to claim rewards multiple times per day
        2. Having hundreds of small reward claims makes tax reporting difficult

        This consolidation:
        - Groups all Authz reward claims by day
        - Sums up the rewards and fees
        - Creates a single daily record with the total amounts
        - Preserves transaction counts in the description
        """
        daily_authz = {}
        consolidated_records = []
        
        for record in records:
            # If it's not an authz reward transaction, keep as is
            if not (('authz,reward' in record['Label']) or ('reward,authz' in record['Label'])):
                consolidated_records.append(record)
                continue
            
            # Extract date without time
            date = record['Date'].split()[0]
            
            if date not in daily_authz:
                daily_authz[date] = {
                    'Date': f"{date} 23:59",
                    'Sent Amount': '',
                    'Sent Currency': '',
                    'Received Amount': 0,
                    'Received Currency': 'CHEQ',
                    'Fee Amount': 0,
                    'Fee Currency': 'CHEQ',
                    'Recipient': record['Recipient'],
                    'Sender': record['Sender'],
                    'Label': 'reward',  # Simplified from 'authz,reward' for consolidated records
                    'TxHash': '',  # Skip collecting hashes for consolidated entries
                    'Description': '',
                    'tx_count': 0
                }
            
            # Add up amounts
            daily_authz[date]['Received Amount'] += float(record['Received Amount'] or 0)
            daily_authz[date]['Fee Amount'] += float(record['Fee Amount'] or 0)
            daily_authz[date]['tx_count'] += 1
        
        # Convert daily summaries to records
        for date, summary in daily_authz.items():
            tx_count = summary['tx_count']
            summary['Description'] = f"Summarised rewards withdrawn in {tx_count} separate Authz Exec transactions"
            del summary['tx_count']  # Remove helper field
            consolidated_records.append(summary)
        
        return consolidated_records

    def get_fee(self, tx_data: Dict) -> float:
        """
        Extracts transaction fee and converts from ncheq to CHEQ.
        Fees are in the first amount object in the fee array.
        Returns 0.0 if no fee is found.
        """
        if tx_data.get('fee', {}).get('amount'):
            return float(tx_data['fee']['amount'][0]['amount']) / self.NCHEQ_TO_CHEQ
        return 0.0

    def process_transaction(self, tx: Dict) -> Dict:
        """
        Core transaction processing logic that:
        1. Extracts timestamps, fees, and transaction details
        2. Identifies transaction type from messages
        3. Processes amounts and addresses based on transaction type
        4. Handles special cases like:
        - IBC transfers
        - Delegation rewards
        - Redelegations with automatic reward claims
        - Authz executions
        5. Returns a Koinly-compatible record with all required fields

        Transaction types handled:
        - Bank sends (transfers)
        - Reward withdrawals
        - Delegations/Undelegations
        - Redelegations
        - IBC transfers 
        - Governance votes
        - Authz operations
        """
        tx_data = tx.get('transaction', {})
        if not tx_data:
            self.logger.debug(f"Empty transaction data found")
            return None
        
        # Check if transaction contains any non-IBC client update messages
        messages = tx_data.get('messages', [])
        if not messages:
            self.logger.debug(f"No messages found in transaction {tx_data.get('hash')}")
            return None
            
        has_non_client_updates = any(
            msg.get('@type') != '/ibc.core.client.v1.MsgUpdateClient'
            for msg in messages
        )
        
        # Skip entirely if only IBC client updates
        if not has_non_client_updates:
            return None

        timestamp = self.parse_timestamp(tx_data.get('block', {}).get('timestamp', ''))
        fee_amount = self.get_fee(tx_data)
        
        record = {
            'Date': timestamp,
            'Sent Amount': '',
            'Sent Currency': '',
            'Received Amount': '',
            'Received Currency': '',
            'Fee Amount': fee_amount if fee_amount > 0 and has_non_client_updates else '',
            'Fee Currency': 'CHEQ' if fee_amount > 0 and has_non_client_updates else '',
            'Label': {'cost'} if not tx_data.get('success', False) else set(),
            'TxHash': tx_data.get('hash', ''),
            'Description': '',
            'Recipient': set(),
            'Sender': set()
        }

        # Process messages with null checks
        for msg in messages:
            if not isinstance(msg, dict):
                self.logger.debug(f"Invalid message format in tx {tx_data.get('hash')}: {msg}")
                continue

            msg_type = msg.get('@type')
            if not msg_type:
                self.logger.debug(f"Message missing @type in tx {tx_data.get('hash')}")
                continue
            
            # Skip IBC client update messages
            if msg_type == '/ibc.core.client.v1.MsgUpdateClient':
                record['Label'].add('cost')
                record['Description'] = 'IBC client update'
                continue
            
            # Bank send messages represent direct transfers:
            # - Need to identify if we're sender or receiver
            # - Always record both addresses for completeness
            # - Record amount as sent or received based on our role
            # - Watch for our address being neither sender nor receiver
            if msg_type == '/cosmos.bank.v1beta1.MsgSend':
                amount = float(msg['amount'][0]['amount']) / self.NCHEQ_TO_CHEQ
                is_sender = msg['from_address'] == self.address
                is_receiver = msg['to_address'] == self.address
                
                # Always record both sender and recipient
                record['Sender'].add(msg['from_address'])
                record['Recipient'].add(msg['to_address'])
                
                if is_sender:
                    record['Sent Amount'] = amount
                    record['Sent Currency'] = 'CHEQ'
                elif is_receiver:
                    record['Received Amount'] = amount
                    record['Received Currency'] = 'CHEQ'
                else:
                    # If we're neither sender nor receiver, we should still record this as a transfer we're tracking
                    record['Description'] = f'Transfer of {amount} CHEQ from {msg["from_address"]} to {msg["to_address"]}'

            # Rewards
            elif msg_type == '/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward':
                record['Label'].add('reward')
                if not record['Received Amount']:  # Only get reward amount once per transaction
                    reward_amount = self.get_reward_amount(tx_data)
                    if reward_amount > 0:
                        record['Received Amount'] = reward_amount
                        record['Received Currency'] = 'CHEQ'
                record['Recipient'].add(self.address)

            # Governance Vote - only record fee with "cost" label
            elif msg_type == '/cosmos.gov.v1beta1.MsgVote':
                record['Label'].add('cost')
                # Clear any amounts since we only want to record the fee
                record['Sent Amount'] = ''
                record['Sent Currency'] = ''
                record['Received Amount'] = ''
                record['Received Currency'] = ''
                record['Sender'].add(self.address)
                record['Description'] = f'Voted on proposal {msg["proposal_id"]}'
                
            # Staking operations have several complexities:
            # - Delegations only incur fees (no actual token movement)
            # - Undelegations trigger automatic reward withdrawal
            # - Redelegations may include rewards from source validator
            # - All amounts need conversion from ncheq to CHEQ
            # - Validator addresses should be preserved for tracking
            elif msg_type == '/cosmos.staking.v1beta1.MsgDelegate':
                amount = float(msg['amount']['amount']) / self.NCHEQ_TO_CHEQ
                record['Label'].add('cost')
                record['Sent Amount'] = ''
                record['Sent Currency'] = ''
                record['Received Amount'] = ''
                record['Received Currency'] = ''
                record['Sender'].add(self.address)
                record['Recipient'] = {msg['validator_address']}
                record['Description'] = f'Delegated {amount} CHEQ to {msg["validator_address"]}'
            
            # Cancel unbonding
            elif msg_type == '/cosmos.staking.v1beta1.MsgCancelUnbondingDelegation':
                amount = float(msg['amount']['amount']) / self.NCHEQ_TO_CHEQ
                record['Label'].add('stake')
                record['Sent Amount'] = ''
                record['Sent Currency'] = ''
                record['Received Amount'] = ''
                record['Received Currency'] = ''
                record['Sender'].add(self.address)
                record['Recipient'] = {msg['validator_address']}
                record['Description'] = f'Cancelled unbonding of {amount} CHEQ from {msg["validator_address"]}'

            # Staking Undelegate - record both fee and automatic reward withdrawal
            elif msg_type == '/cosmos.staking.v1beta1.MsgUndelegate':
                stake_amount = float(msg['amount']['amount']) / self.NCHEQ_TO_CHEQ
                record['Label'].add('reward' if tx_data.get('success', False) else 'cost')
                
                # Only process rewards if transaction was successful and has logs
                if tx_data.get('success', False) and tx_data.get('logs'):
                    # Find reward amount from logs
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
                                
                                # Only set reward amount if this event was for our address
                                if is_receiver and amount:
                                    record['Received Amount'] = amount / self.NCHEQ_TO_CHEQ
                                    record['Received Currency'] = 'CHEQ'

                record['Recipient'].add(self.address)
                status = "failed" if not tx_data.get('success', False) else "succeeded"
                record['Description'] = f'Undelegated {stake_amount} CHEQ from {msg["validator_address"]} ({status})'
                if record.get('Received Amount'):
                    record['Description'] += f' and withdrew {record["Received Amount"]} CHEQ in rewards'

            # Staking Redelegate - record both fee and automatic reward withdrawal
            elif msg_type == '/cosmos.staking.v1beta1.MsgBeginRedelegate':
                try:
                    stake_amount = float(msg.get('amount', {}).get('amount', 0)) / self.NCHEQ_TO_CHEQ
                    record['Label'].add('reward' if tx_data.get('success', False) else 'cost')
        
                    # Get reward amount only if transaction succeeded
                    if tx_data.get('success', False):
                        reward_amount = self.get_redelegate_reward_amount(tx_data, msg)
                        if reward_amount > 0:
                            record['Received Amount'] = reward_amount
                            record['Received Currency'] = 'CHEQ'
                    
                    record['Recipient'].add(self.address)
                    
                    # Safe access to validator addresses
                    val_src = msg.get('validator_src_address', 'unknown_validator')
                    val_dst = msg.get('validator_dst_address', 'unknown_validator')
                    status = "failed" if not tx_data.get('success', False) else "succeeded"
                    record['Description'] = f'Redelegated {stake_amount} CHEQ from {val_src} to {val_dst} ({status})'
                    if reward_amount > 0:
                        record['Description'] += f' and withdrew {reward_amount} CHEQ in rewards'
                
                except Exception as e:
                    self.logger.debug(f"Error processing redelegation tx {tx_data.get('hash')}: {str(e)}")
                    # Still create a basic record even if we can't process everything
                    record['Label'].add('cost')
                    record['Description'] = 'Redelegation transaction (details unavailable)'

            # IBC (Inter-Blockchain Communication) transfers require special handling:
            # - Need to identify direction (outgoing/incoming) 
            # - Address formats differ between chains
            # - Token denomination may change during transfer
            elif msg_type == '/ibc.applications.transfer.v1.MsgTransfer':
                amount = float(msg['token']['amount']) / self.NCHEQ_TO_CHEQ
                is_sender = msg['sender'] == self.address
                
                if is_sender:
                    record['Sent Amount'] = amount
                    record['Sent Currency'] = 'CHEQ'
                    record['Recipient'].add(msg['receiver'])
                    record['Sender'].add(self.address)
                elif msg['receiver'] == self.address:
                    record['Received Amount'] = amount
                    record['Received Currency'] = 'CHEQ'
                    record['Sender'].add(msg['sender'])
                    record['Recipient'].add(self.address)
                record['Label'].add('transfer')

            # Authz (Authorization) transactions are complex:
            # - They wrap other message types
            # - Need to extract actual operation from wrapped messages
            # - Rewards are found in coin_received events
            # - Multiple rewards may be claimed in single transactions
            elif msg_type == '/cosmos.authz.v1beta1.MsgExec':
                record['Label'].add('authz')
                
                # Check for any rewards in the coin_received events only if transaction succeeded
                if tx_data.get('success', False):
                    logs = tx_data.get('logs', [])
                    if logs:  # Only process if logs exist
                        for log in logs:
                            for event in log.get('events', []):
                                if event.get('type') == 'coin_received':
                                    attributes = event.get('attributes', [])
                                    amount = None
                                    is_receiver = False
                                    
                                    # Check both receiver and amount in the same event group
                                    for attr in attributes:
                                        if attr.get('key') == 'receiver' and attr.get('value') == self.address:
                                            is_receiver = True
                                        if attr.get('key') == 'amount' and attr.get('value', '').endswith('ncheq'):
                                            try:
                                                amount = float(attr.get('value').rstrip('ncheq'))
                                            except (ValueError, TypeError):
                                                continue
                                                
                                    # Only set reward amount if this event was for our address
                                    if is_receiver and amount and not record['Received Amount']:
                                        record['Received Amount'] = amount / self.NCHEQ_TO_CHEQ
                                        record['Received Currency'] = 'CHEQ'
                                        record['Label'].add('reward')

            # Authz Grant
            elif msg_type == '/cosmos.authz.v1beta1.MsgGrant':
                record['Label'].add('cost')
                # Only record the fee for grants
                record['Sent Amount'] = ''
                record['Sent Currency'] = ''
                record['Received Amount'] = ''
                record['Received Currency'] = ''
                record['Sender'].add(msg['granter'])
                # Add description with authorization details
                auth_type = msg['grant']['authorization'].get('@type', '').split('.')[-1]  # Get the last part of the type
                expiry = msg['grant'].get('expiration', '')
                if expiry:
                    expiry = datetime.fromisoformat(expiry.replace('Z', '+00:00')).strftime('%Y-%m-%d')
                    record['Description'] = f'Granted {auth_type} authorization to {msg["grantee"]} until {expiry}'
                else:
                    record['Description'] = f'Granted {auth_type} authorization to {msg["grantee"]}'

            # Authz Revoke
            elif msg_type == '/cosmos.authz.v1beta1.MsgRevoke':
                record['Label'].add('cost')
                # Only record the fee for revokes
                record['Sent Amount'] = ''
                record['Sent Currency'] = ''
                record['Received Amount'] = ''
                record['Received Currency'] = ''
                record['Sender'].add(msg['granter'])
                # Add description with authorization details
                msg_type_url = msg.get('msg_type_url', '').split('.')[-1]  # Get the last part of the type
                record['Description'] = f'Revoked {msg_type_url} authorization from {msg["grantee"]}'

        # Convert sets to comma-separated strings
        record['Label'] = ','.join(sorted(record['Label'])) if record['Label'] else ''
        record['Sender'] = ','.join(sorted(record['Sender'])) if record['Sender'] else ''
        record['Recipient'] = ','.join(sorted(record['Recipient'])) if record['Recipient'] else ''
        
        return record

    # Convert transactions to Koinly format
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

        # Consolidate authz records
        koinly_records = self.consolidate_authz_records(koinly_records)
        
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
    """
    Main conversion pipeline that:
    1. Loads transactions from input JSON
    2. Processes each transaction into Koinly format
    3. Consolidates Authz reward claims
    4. Sorts by timestamp
    5. Writes to CSV in Koinly format

    Skips transactions that:
    - Contain only IBC client updates
    - Have no monetary impact (zero amounts/fees)
    - Failed to process due to unexpected formats

    Error handling:
    - Continues processing on individual transaction failures
    - Logs errors with transaction hashes for debugging
    - Preserves successfully processed transactions
    """
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
