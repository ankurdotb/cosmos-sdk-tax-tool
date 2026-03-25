import json
import csv
import pytest
from tx_to_koinly import KoinlyConverter
from tests.helpers import (
    WALLET,
    VALIDATOR,
    OTHER_WALLET,
    make_tx,
    make_bank_send,
    make_reward_withdrawal,
    make_reward_logs,
    make_delegate,
    make_undelegate,
    make_undelegate_logs,
    make_redelegate,
    make_redelegate_logs,
    make_ibc_transfer,
    make_vote,
    make_authz_exec,
    make_authz_exec_logs,
    make_authz_grant,
    make_authz_revoke,
    make_ibc_client_update,
    make_ibc_acknowledgement,
    make_ibc_recv_packet,
    make_ibc_recv_packet_logs,
)


# =============================================================================
# Amount conversion
# =============================================================================


class TestAmountConversion:
    def test_ncheq_to_cheq(self, converter):
        assert converter.NCHEQ_TO_CHEQ == 1_000_000_000

    def test_fee_conversion(self, converter):
        tx_data = {"fee": {"amount": [{"amount": "5000000000", "denom": "ncheq"}]}, "messages": []}
        assert converter.get_fee(tx_data) == 5.0

    def test_fee_conversion_small_amount(self, converter):
        tx_data = {"fee": {"amount": [{"amount": "1", "denom": "ncheq"}]}, "messages": []}
        assert converter.get_fee(tx_data) == 1e-9

    def test_fee_zero(self, converter):
        tx_data = {"fee": {}, "messages": []}
        assert converter.get_fee(tx_data) == 0.0

    def test_fee_missing_amount_array(self, converter):
        tx_data = {"fee": {"amount": []}, "messages": []}
        assert converter.get_fee(tx_data) == 0.0

    def test_reward_amount_conversion(self, converter):
        tx_data = {
            "success": True,
            "hash": "HASH1",
            "logs": make_reward_logs("7500000000"),
        }
        assert converter.get_reward_amount(tx_data) == 7.5


# =============================================================================
# Transaction deduplication
# =============================================================================


class TestDeduplication:
    def test_duplicate_hashes_deduplicated(self, converter, tmp_path):
        txs = [
            make_tx([make_bank_send(WALLET, OTHER_WALLET)], tx_hash="DUP1"),
            make_tx([make_bank_send(WALLET, OTHER_WALLET)], tx_hash="DUP1"),
            make_tx([make_bank_send(WALLET, OTHER_WALLET)], tx_hash="UNIQUE"),
        ]
        input_file = tmp_path / "txs.json"
        input_file.write_text(json.dumps(txs))
        converter.input_file = str(input_file)
        result = converter.load_transactions()
        assert len(result) == 2

    def test_first_occurrence_preserved(self, converter, tmp_path):
        txs = [
            make_tx([make_bank_send(WALLET, OTHER_WALLET, "1000000000")], tx_hash="DUP"),
            make_tx([make_bank_send(WALLET, OTHER_WALLET, "9999999999")], tx_hash="DUP"),
        ]
        input_file = tmp_path / "txs.json"
        input_file.write_text(json.dumps(txs))
        converter.input_file = str(input_file)
        result = converter.load_transactions()
        assert len(result) == 1
        assert result[0]["transaction"]["messages"][0]["amount"][0]["amount"] == "1000000000"


# =============================================================================
# Timestamp parsing
# =============================================================================


class TestTimestampParsing:
    def test_z_suffix(self, converter):
        assert converter.parse_timestamp("2024-01-15T10:30:00Z") == "2024-01-15 10:30"

    def test_fractional_seconds(self, converter):
        assert converter.parse_timestamp("2024-01-15T10:30:00.123456Z") == "2024-01-15 10:30"

    def test_high_precision_fractional(self, converter):
        assert converter.parse_timestamp("2024-01-15T10:30:00.123456789Z") == "2024-01-15 10:30"

    def test_trailing_whitespace(self, converter):
        assert converter.parse_timestamp("2024-01-15T10:30:00Z  ") == "2024-01-15 10:30"

    def test_timezone_offset(self, converter):
        result = converter.parse_timestamp("2024-01-15T10:30:00+00:00")
        assert result == "2024-01-15 10:30"

    def test_empty_timestamp_raises(self, converter):
        with pytest.raises(ValueError, match="Empty timestamp"):
            converter.parse_timestamp("")

    def test_none_timestamp_raises(self, converter):
        with pytest.raises(ValueError, match="Empty timestamp"):
            converter.parse_timestamp(None)

    def test_invalid_timestamp_raises(self, converter):
        with pytest.raises(ValueError, match="Invalid isoformat"):
            converter.parse_timestamp("not-a-timestamp")

    def test_space_separated_datetime(self, converter):
        result = converter.parse_iso_datetime("2024-01-15 10:30:00")
        assert result.year == 2024
        assert result.month == 1
        assert result.hour == 10


# =============================================================================
# Bank send transactions
# =============================================================================


class TestBankSend:
    def test_outgoing_send(self, converter):
        tx = make_tx([make_bank_send(WALLET, OTHER_WALLET, "10000000000")])
        record = converter.process_transaction(tx)
        assert record["Sent Amount"] == 10.0
        assert record["Sent Currency"] == "CHEQ"
        assert record["Received Amount"] == ""
        assert WALLET in record["Sender"]
        assert OTHER_WALLET in record["Recipient"]

    def test_incoming_send(self, converter):
        tx = make_tx([make_bank_send(OTHER_WALLET, WALLET, "10000000000")])
        record = converter.process_transaction(tx)
        assert record["Received Amount"] == 10.0
        assert record["Received Currency"] == "CHEQ"
        assert record["Sent Amount"] == ""
        assert OTHER_WALLET in record["Sender"]
        assert WALLET in record["Recipient"]

    def test_third_party_transfer(self, converter):
        tx = make_tx([make_bank_send("cheqd1sender", "cheqd1receiver", "10000000000")])
        record = converter.process_transaction(tx)
        assert "Transfer of 10.0 CHEQ" in record["Description"]


# =============================================================================
# Reward withdrawals
# =============================================================================


class TestRewards:
    def test_reward_with_logs(self, converter):
        logs = make_reward_logs("2000000000")
        tx = make_tx(
            [make_reward_withdrawal(WALLET, VALIDATOR)],
            logs=logs,
        )
        record = converter.process_transaction(tx)
        assert record["Received Amount"] == 2.0
        assert record["Received Currency"] == "CHEQ"
        assert "reward" in record["Label"]

    def test_reward_failed_tx(self, converter):
        logs = make_reward_logs("2000000000")
        tx = make_tx(
            [make_reward_withdrawal(WALLET, VALIDATOR)],
            success=False,
            logs=logs,
        )
        record = converter.process_transaction(tx)
        assert "cost" in record["Label"]
        assert record["Received Amount"] == ""

    def test_reward_no_logs(self, converter):
        tx = make_tx([make_reward_withdrawal(WALLET, VALIDATOR)])
        record = converter.process_transaction(tx)
        assert record["Received Amount"] == "" or record["Received Amount"] == 0.0

    def test_reward_zero_amount_in_logs(self, converter):
        tx_data = {
            "success": True,
            "hash": "HASH1",
            "logs": [
                {
                    "events": [
                        {
                            "type": "withdraw_rewards",
                            "attributes": [{"key": "amount", "value": "0ncheq"}],
                        }
                    ]
                }
            ],
        }
        assert converter.get_reward_amount(tx_data) == 0.0

    def test_reward_failed_tx_returns_zero(self, converter):
        tx_data = {"success": False, "hash": "HASH1", "logs": make_reward_logs()}
        assert converter.get_reward_amount(tx_data) == 0.0


# =============================================================================
# Delegations (cost-only)
# =============================================================================


class TestDelegation:
    def test_delegation_cost_only(self, converter):
        tx = make_tx([make_delegate(WALLET, VALIDATOR, "50000000000")])
        record = converter.process_transaction(tx)
        assert "cost" in record["Label"]
        assert record["Sent Amount"] == ""
        assert record["Received Amount"] == ""
        assert "Delegated 50.0 CHEQ" in record["Description"]
        assert VALIDATOR in record["Recipient"]

    def test_cancel_unbonding(self, converter):
        msg = {
            "@type": "/cosmos.staking.v1beta1.MsgCancelUnbondingDelegation",
            "delegator_address": WALLET,
            "validator_address": VALIDATOR,
            "amount": {"amount": "25000000000", "denom": "ncheq"},
            "creation_height": "100000",
        }
        tx = make_tx([msg])
        record = converter.process_transaction(tx)
        assert "stake" in record["Label"]
        assert "Cancelled unbonding" in record["Description"]


# =============================================================================
# Undelegations (with automatic reward)
# =============================================================================


class TestUndelegation:
    def test_undelegation_with_reward(self, converter):
        logs = make_undelegate_logs(WALLET, "3000000000")
        tx = make_tx(
            [make_undelegate(WALLET, VALIDATOR, "50000000000")],
            logs=logs,
        )
        record = converter.process_transaction(tx)
        assert "reward" in record["Label"]
        assert record["Received Amount"] == 3.0
        assert record["Received Currency"] == "CHEQ"
        assert "Undelegated 50.0 CHEQ" in record["Description"]
        assert "withdrew 3.0 CHEQ in rewards" in record["Description"]

    def test_undelegation_failed(self, converter):
        tx = make_tx(
            [make_undelegate(WALLET, VALIDATOR)],
            success=False,
        )
        record = converter.process_transaction(tx)
        assert "cost" in record["Label"]
        assert "failed" in record["Description"]


# =============================================================================
# Redelegations (with reward from source validator)
# =============================================================================


class TestRedelegation:
    def test_redelegation_with_reward(self, converter):
        val_dst = "cheqdvaloper1dst"
        logs = make_redelegate_logs(WALLET, "1500000000")
        tx = make_tx(
            [make_redelegate(WALLET, VALIDATOR, val_dst, "50000000000")],
            logs=logs,
        )
        record = converter.process_transaction(tx)
        assert "reward" in record["Label"]
        assert record["Received Amount"] == 1.5
        assert "Redelegated 50.0 CHEQ" in record["Description"]
        assert "withdrew 1.5 CHEQ in rewards" in record["Description"]

    def test_redelegation_failed(self, converter):
        tx = make_tx(
            [make_redelegate(WALLET, VALIDATOR, "cheqdvaloper1dst")],
            success=False,
        )
        record = converter.process_transaction(tx)
        assert "cost" in record["Label"]


# =============================================================================
# IBC transfers
# =============================================================================


class TestIBCTransfer:
    def test_outgoing_ibc(self, converter):
        tx = make_tx([make_ibc_transfer(WALLET, "cosmos1receiver", "20000000000")])
        record = converter.process_transaction(tx)
        assert record["Sent Amount"] == 20.0
        assert record["Sent Currency"] == "CHEQ"
        assert "transfer" in record["Label"]
        assert WALLET in record["Sender"]

    def test_incoming_ibc_via_msg_transfer(self, converter):
        tx = make_tx([make_ibc_transfer("cosmos1sender", WALLET, "20000000000")])
        record = converter.process_transaction(tx)
        assert record["Received Amount"] == 20.0
        assert record["Received Currency"] == "CHEQ"
        assert "transfer" in record["Label"]
        assert WALLET in record["Recipient"]

    def test_incoming_ibc_via_recv_packet(self, converter):
        """MsgRecvPacket is the receive side of IBC — amount comes from fungible_token_packet event."""
        logs = make_ibc_recv_packet_logs("osmo1sender", WALLET, "100000000000")
        tx = make_tx(
            [make_ibc_client_update(), make_ibc_recv_packet(WALLET)],
            logs=logs,
        )
        record = converter.process_transaction(tx)
        assert record["Received Amount"] == 100.0
        assert record["Received Currency"] == "CHEQ"
        assert "transfer" in record["Label"]
        assert WALLET in record["Recipient"]
        assert "osmo1sender" in record["Sender"]

    def test_incoming_ibc_recv_packet_failed(self, converter):
        """Failed MsgRecvPacket should not record received amount."""
        logs = make_ibc_recv_packet_logs("osmo1sender", WALLET, "100000000000")
        tx = make_tx(
            [make_ibc_client_update(), make_ibc_recv_packet(WALLET)],
            logs=logs,
            success=False,
        )
        record = converter.process_transaction(tx)
        assert "cost" in record["Label"]
        assert record["Received Amount"] == ""

    def test_incoming_ibc_recv_packet_unsuccessful_event(self, converter):
        """fungible_token_packet with success=false should not record amount."""
        logs = make_ibc_recv_packet_logs("osmo1sender", WALLET, "100000000000", success="false")
        tx = make_tx(
            [make_ibc_client_update(), make_ibc_recv_packet(WALLET)],
            logs=logs,
        )
        record = converter.process_transaction(tx)
        assert record["Received Amount"] == ""

    def test_incoming_ibc_recv_packet_different_receiver(self, converter):
        """MsgRecvPacket for a different receiver should not record amount for us."""
        logs = make_ibc_recv_packet_logs("osmo1sender", OTHER_WALLET, "100000000000")
        tx = make_tx(
            [make_ibc_client_update(), make_ibc_recv_packet(WALLET)],
            logs=logs,
        )
        record = converter.process_transaction(tx)
        assert record["Received Amount"] == ""


# =============================================================================
# Governance votes (cost-only)
# =============================================================================


class TestGovernanceVote:
    def test_vote_cost_only(self, converter):
        tx = make_tx([make_vote(WALLET, "42")])
        record = converter.process_transaction(tx)
        assert "cost" in record["Label"]
        assert record["Sent Amount"] == ""
        assert record["Received Amount"] == ""
        assert "Voted on proposal 42" in record["Description"]


# =============================================================================
# Authz exec/grant/revoke
# =============================================================================


class TestAuthzExec:
    def test_authz_exec_with_reward(self, converter):
        logs = make_authz_exec_logs(WALLET, "500000000")
        tx = make_tx(
            [make_authz_exec(WALLET)],
            logs=logs,
        )
        record = converter.process_transaction(tx)
        assert "authz" in record["Label"]
        assert "reward" in record["Label"]
        assert record["Received Amount"] == 0.5
        assert record["Received Currency"] == "CHEQ"

    def test_authz_exec_failed(self, converter):
        tx = make_tx([make_authz_exec(WALLET)], success=False)
        record = converter.process_transaction(tx)
        assert "cost" in record["Label"]

    def test_authz_grant(self, converter):
        tx = make_tx([make_authz_grant(WALLET, OTHER_WALLET, "2025-12-31T23:59:59Z")])
        record = converter.process_transaction(tx)
        assert "cost" in record["Label"]
        assert "Granted" in record["Description"]
        assert "StakeAuthorization" in record["Description"]
        assert "2025-12-31" in record["Description"]

    def test_authz_grant_no_expiration(self, converter):
        tx = make_tx([make_authz_grant(WALLET, OTHER_WALLET, expiration="")])
        record = converter.process_transaction(tx)
        assert "Granted" in record["Description"]
        assert "until" not in record["Description"]

    def test_authz_revoke(self, converter):
        tx = make_tx([make_authz_revoke(WALLET, OTHER_WALLET)])
        record = converter.process_transaction(tx)
        assert "cost" in record["Label"]
        assert "Revoked" in record["Description"]
        assert "MsgDelegate" in record["Description"]


# =============================================================================
# Authz consolidation
# =============================================================================


class TestAuthzConsolidation:
    def _make_authz_reward_record(self, date, received_amount, fee_amount):
        return {
            "Date": date,
            "Sent Amount": "",
            "Sent Currency": "",
            "Received Amount": received_amount,
            "Received Currency": "CHEQ",
            "Fee Amount": fee_amount,
            "Fee Currency": "CHEQ",
            "Recipient": WALLET,
            "Sender": "",
            "Label": "authz,reward",
            "TxHash": "HASH",
            "Description": "",
        }

    def test_consolidates_same_day(self, converter):
        records = [
            self._make_authz_reward_record("2024-01-15 10:00", 1.0, 0.1),
            self._make_authz_reward_record("2024-01-15 14:00", 2.0, 0.1),
            self._make_authz_reward_record("2024-01-15 18:00", 3.0, 0.1),
        ]
        result = converter.consolidate_authz_records(records)
        assert len(result) == 1
        assert result[0]["Received Amount"] == 6.0
        assert result[0]["Fee Amount"] == pytest.approx(0.3)
        assert result[0]["Label"] == "reward"
        assert "3 separate Authz Exec" in result[0]["Description"]
        assert result[0]["Date"] == "2024-01-15 23:59"

    def test_different_days_not_consolidated(self, converter):
        records = [
            self._make_authz_reward_record("2024-01-15 10:00", 1.0, 0.1),
            self._make_authz_reward_record("2024-01-16 10:00", 2.0, 0.1),
        ]
        result = converter.consolidate_authz_records(records)
        assert len(result) == 2

    def test_non_authz_records_preserved(self, converter):
        records = [
            {
                "Date": "2024-01-15 10:00",
                "Label": "reward",
                "Received Amount": 5.0,
                "Description": "regular reward",
            },
            self._make_authz_reward_record("2024-01-15 10:00", 1.0, 0.1),
        ]
        result = converter.consolidate_authz_records(records)
        assert len(result) == 2
        regular = [r for r in result if r.get("Description") == "regular reward"]
        assert len(regular) == 1


# =============================================================================
# Fee extraction
# =============================================================================


class TestFeeExtraction:
    def test_standard_fee(self, converter):
        tx_data = {"fee": {"amount": [{"amount": "5000000000", "denom": "ncheq"}]}, "messages": []}
        assert converter.get_fee(tx_data) == 5.0

    def test_no_fee(self, converter):
        tx_data = {"fee": {}, "messages": []}
        assert converter.get_fee(tx_data) == 0.0

    def test_did_doc_fee_from_logs(self, converter):
        """For DID/resource messages, fee comes from coin_received events."""
        tx_data = {
            "messages": [{"@type": "/cheqd.did.v2.MsgCreateDidDoc"}],
            "logs": [
                {
                    "events": [
                        {"type": "coin_received", "attributes": []},
                        {"type": "coin_spent", "attributes": []},
                        {"type": "transfer", "attributes": []},
                        {
                            "type": "coin_received",
                            "attributes": [
                                {"key": "receiver", "value": converter.EVENT_LOG_FEE_RECEIVER},
                                {"key": "amount", "value": "10000000000ncheq"},
                            ],
                        },
                    ]
                }
            ],
        }
        assert converter.get_fee(tx_data) == 10.0

    def test_flatten_log_events(self, converter):
        logs = [
            {"events": [{"type": "a"}, {"type": "b"}]},
            {"events": [{"type": "c"}]},
        ]
        result = converter.flatten_log_events(logs)
        assert len(result) == 3
        assert [e["type"] for e in result] == ["a", "b", "c"]

    def test_flatten_log_events_empty(self, converter):
        assert converter.flatten_log_events([]) == []
        assert converter.flatten_log_events(None) == []

    def test_flatten_log_events_non_dict(self, converter):
        assert converter.flatten_log_events(["not_a_dict"]) == []


# =============================================================================
# Failed transactions → "cost" label
# =============================================================================


class TestFailedTransactions:
    def test_failed_bank_send(self, converter):
        tx = make_tx(
            [make_bank_send(WALLET, OTHER_WALLET, "10000000000")],
            success=False,
        )
        record = converter.process_transaction(tx)
        assert "cost" in record["Label"]

    def test_failed_tx_fee_still_recorded(self, converter):
        tx = make_tx(
            [make_bank_send(WALLET, OTHER_WALLET)],
            success=False,
            fee_ncheq="5000000000",
        )
        record = converter.process_transaction(tx)
        assert record["Fee Amount"] == 5.0
        assert record["Fee Currency"] == "CHEQ"


# =============================================================================
# Edge cases
# =============================================================================


class TestEdgeCases:
    def test_empty_transaction_data(self, converter):
        tx = {"transaction": {}}
        result = converter.process_transaction(tx)
        assert result is None

    def test_no_messages(self, converter):
        tx = make_tx([])
        result = converter.process_transaction(tx)
        assert result is None

    def test_only_ibc_client_updates_skipped(self, converter):
        tx = make_tx([make_ibc_client_update()])
        result = converter.process_transaction(tx)
        assert result is None

    def test_ibc_client_update_mixed_with_send(self, converter):
        tx = make_tx(
            [
                make_ibc_client_update(),
                make_bank_send(WALLET, OTHER_WALLET, "10000000000"),
            ]
        )
        record = converter.process_transaction(tx)
        assert record is not None
        assert record["Sent Amount"] == 10.0

    def test_invalid_message_format(self, converter):
        """Non-dict messages cause an AttributeError in the IBC client check."""
        tx = make_tx(["not_a_dict", make_bank_send(WALLET, OTHER_WALLET)])
        with pytest.raises(AttributeError):
            converter.process_transaction(tx)

    def test_message_missing_type(self, converter):
        tx = make_tx([{"some_key": "some_value"}, make_bank_send(WALLET, OTHER_WALLET)])
        record = converter.process_transaction(tx)
        assert record is not None

    def test_reward_invalid_amount_in_logs(self, converter):
        tx_data = {
            "success": True,
            "hash": "H1",
            "logs": [
                {
                    "events": [
                        {
                            "type": "withdraw_rewards",
                            "attributes": [{"key": "amount", "value": "invalidncheq"}],
                        }
                    ]
                }
            ],
        }
        assert converter.get_reward_amount(tx_data) == 0.0

    def test_reward_non_dict_log(self, converter):
        tx_data = {"success": True, "hash": "H1", "logs": ["not_a_dict"]}
        assert converter.get_reward_amount(tx_data) == 0.0

    def test_reward_non_dict_event(self, converter):
        tx_data = {"success": True, "hash": "H1", "logs": [{"events": ["not_a_dict"]}]}
        assert converter.get_reward_amount(tx_data) == 0.0

    def test_reward_non_dict_attribute(self, converter):
        tx_data = {
            "success": True,
            "hash": "H1",
            "logs": [
                {
                    "events": [
                        {
                            "type": "withdraw_rewards",
                            "attributes": ["not_a_dict"],
                        }
                    ]
                }
            ],
        }
        assert converter.get_reward_amount(tx_data) == 0.0

    def test_none_logs_in_transaction(self, converter):
        """Real data has 18 txs where logs is null, not an empty list."""
        tx = make_tx([make_reward_withdrawal(WALLET, VALIDATOR)])
        tx["transaction"]["logs"] = None
        record = converter.process_transaction(tx)
        assert record is not None

    def test_none_logs_reward_amount(self, converter):
        """get_reward_amount should handle None logs gracefully."""
        tx_data = {"success": True, "hash": "H1", "logs": None}
        assert converter.get_reward_amount(tx_data) == 0.0

    def test_multiple_withdraw_rewards_in_single_log(self, converter):
        """Real data has 11 txs with multiple withdraw_rewards events in one log."""
        tx_data = {
            "success": True,
            "hash": "H1",
            "logs": [
                {
                    "events": [
                        {
                            "type": "withdraw_rewards",
                            "attributes": [
                                {"key": "amount", "value": "1000000000ncheq"},
                                {"key": "validator", "value": "val1"},
                            ],
                        },
                        {
                            "type": "withdraw_rewards",
                            "attributes": [
                                {"key": "amount", "value": "2000000000ncheq"},
                                {"key": "validator", "value": "val2"},
                            ],
                        },
                    ]
                }
            ],
        }
        assert converter.get_reward_amount(tx_data) == 3.0

    def test_unhandled_ibc_acknowledgement(self, converter):
        """MsgAcknowledgement (27 txs in real data) falls through all handlers."""
        tx = make_tx(
            [
                make_ibc_client_update(),
                make_ibc_acknowledgement(WALLET),
            ]
        )
        record = converter.process_transaction(tx)
        assert record is not None
        assert record["Sent Amount"] == ""
        assert record["Received Amount"] == ""

    def test_ibc_recv_packet_no_logs(self, converter):
        """MsgRecvPacket without logs produces no received amount."""
        tx = make_tx(
            [
                make_ibc_client_update(),
                make_ibc_recv_packet(WALLET),
            ]
        )
        record = converter.process_transaction(tx)
        assert record is not None
        assert record["Sent Amount"] == ""
        assert record["Received Amount"] == ""

    def test_fee_with_payer_granter_fields(self, converter):
        """Real data has fees with payer/granter fields alongside amount."""
        tx_data = {
            "fee": {
                "amount": [{"amount": "5000000000", "denom": "ncheq"}],
                "payer": WALLET,
                "granter": "",
                "gas_limit": "100000",
            },
            "messages": [],
        }
        assert converter.get_fee(tx_data) == 5.0


# =============================================================================
# CSV output format
# =============================================================================


class TestCSVOutput:
    def test_csv_headers_match_koinly(self, converter, tmp_path):
        txs = [make_tx([make_bank_send(WALLET, OTHER_WALLET, "10000000000")])]
        input_file = tmp_path / "txs.json"
        input_file.write_text(json.dumps(txs))
        converter.input_file = str(input_file)
        converter.convert()

        with open(converter.output_file) as f:
            reader = csv.DictReader(f)
            assert set(reader.fieldnames) == set(converter.KOINLY_HEADERS)

    def test_csv_row_values(self, converter, tmp_path):
        txs = [make_tx([make_bank_send(WALLET, OTHER_WALLET, "10000000000")])]
        input_file = tmp_path / "txs.json"
        input_file.write_text(json.dumps(txs))
        converter.input_file = str(input_file)
        converter.convert()

        with open(converter.output_file) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["Date"] == "2024-01-15 10:30"
        assert rows[0]["Sent Amount"] == "10.0"
        assert rows[0]["Sent Currency"] == "CHEQ"
        assert rows[0]["TxHash"] == "HASH123"

    def test_records_sorted_by_date(self, converter, tmp_path):
        txs = [
            make_tx([make_bank_send(WALLET, OTHER_WALLET)], tx_hash="H2", timestamp="2024-02-01T10:00:00Z"),
            make_tx([make_bank_send(WALLET, OTHER_WALLET)], tx_hash="H1", timestamp="2024-01-01T10:00:00Z"),
        ]
        input_file = tmp_path / "txs.json"
        input_file.write_text(json.dumps(txs))
        converter.input_file = str(input_file)
        converter.convert()

        with open(converter.output_file) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert rows[0]["Date"] < rows[1]["Date"]

    def test_zero_amount_tx_skipped(self, converter, tmp_path):
        """Transactions with no amounts or fees are not written to CSV."""
        txs = [make_tx([make_vote(WALLET)], fee_ncheq=None)]
        input_file = tmp_path / "txs.json"
        input_file.write_text(json.dumps(txs))
        converter.input_file = str(input_file)
        converter.convert()

        with open(converter.output_file) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 0


# =============================================================================
# Archive REST API fallback
# =============================================================================


class TestArchiveFallback:
    def test_no_archive_url_returns_empty(self, converter):
        result = converter.get_archive_fee_events("SOMEHASH")
        assert result == []

    def test_empty_hash_returns_empty(self, converter_with_archive):
        result = converter_with_archive.get_archive_fee_events("")
        assert result == []

    def test_cache_hit(self, converter_with_archive):
        converter_with_archive.archive_tx_cache["CACHED"] = [{"type": "coin_received"}]
        result = converter_with_archive.get_archive_fee_events("CACHED")
        assert len(result) == 1

    def test_archive_url_trailing_slash_stripped(self, tmp_path):
        input_file = tmp_path / "txs.json"
        input_file.write_text("[]")
        c = KoinlyConverter(
            str(input_file), str(tmp_path / "out.csv"), WALLET, archive_rest_api_url="https://archive.example.com/"
        )
        assert c.archive_rest_api_url == "https://archive.example.com"

    def test_fee_events_uses_local_when_complete(self, converter):
        """When local logs have all required event types, archive is not called."""
        tx_data = {
            "logs": [
                {
                    "events": [
                        {"type": "coin_received"},
                        {"type": "coin_spent"},
                        {"type": "transfer"},
                    ]
                }
            ],
            "hash": "H1",
        }
        result = converter.get_fee_events(tx_data)
        assert len(result) == 3
