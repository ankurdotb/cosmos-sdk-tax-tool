WALLET = "cheqd1testwalletaddress"
VALIDATOR = "cheqdvaloper1testvalidator"
OTHER_WALLET = "cheqd1otherwallet"


def make_tx(messages, success=True, fee_ncheq="5000000000", logs=None, tx_hash="HASH123", timestamp="2024-01-15T10:30:00Z"):
    """Build a transaction wrapper matching the expected JSON shape."""
    return {
        "transaction": {
            "height": 123456,
            "hash": tx_hash,
            "success": success,
            "messages": messages,
            "logs": logs or [],
            "fee": {"amount": [{"amount": fee_ncheq, "denom": "ncheq"}]} if fee_ncheq else {},
            "block": {"height": 123456, "timestamp": timestamp},
        }
    }


def make_bank_send(from_addr, to_addr, amount_ncheq="10000000000"):
    return {
        "@type": "/cosmos.bank.v1beta1.MsgSend",
        "from_address": from_addr,
        "to_address": to_addr,
        "amount": [{"amount": amount_ncheq, "denom": "ncheq"}],
    }


def make_reward_withdrawal(delegator, validator):
    return {
        "@type": "/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward",
        "delegator_address": delegator,
        "validator_address": validator,
    }


def make_reward_logs(amount_ncheq="2000000000"):
    return [
        {
            "events": [
                {
                    "type": "withdraw_rewards",
                    "attributes": [
                        {"key": "amount", "value": f"{amount_ncheq}ncheq"},
                        {"key": "validator", "value": VALIDATOR},
                    ],
                }
            ]
        }
    ]


def make_delegate(delegator, validator, amount_ncheq="50000000000"):
    return {
        "@type": "/cosmos.staking.v1beta1.MsgDelegate",
        "delegator_address": delegator,
        "validator_address": validator,
        "amount": {"amount": amount_ncheq, "denom": "ncheq"},
    }


def make_undelegate(delegator, validator, amount_ncheq="50000000000"):
    return {
        "@type": "/cosmos.staking.v1beta1.MsgUndelegate",
        "delegator_address": delegator,
        "validator_address": validator,
        "amount": {"amount": amount_ncheq, "denom": "ncheq"},
    }


def make_undelegate_logs(receiver, reward_ncheq="3000000000"):
    return [
        {
            "events": [
                {
                    "type": "coin_received",
                    "attributes": [
                        {"key": "receiver", "value": receiver},
                        {"key": "amount", "value": f"{reward_ncheq}ncheq"},
                    ],
                }
            ]
        }
    ]


def make_redelegate(delegator, val_src, val_dst, amount_ncheq="50000000000"):
    return {
        "@type": "/cosmos.staking.v1beta1.MsgBeginRedelegate",
        "delegator_address": delegator,
        "validator_src_address": val_src,
        "validator_dst_address": val_dst,
        "amount": {"amount": amount_ncheq, "denom": "ncheq"},
    }


def make_redelegate_logs(receiver, reward_ncheq="1500000000"):
    return [
        {
            "events": [
                {
                    "type": "coin_received",
                    "attributes": [
                        {"key": "receiver", "value": receiver},
                        {"key": "amount", "value": f"{reward_ncheq}ncheq"},
                    ],
                }
            ]
        }
    ]


def make_ibc_transfer(sender, receiver, amount_ncheq="20000000000"):
    return {
        "@type": "/ibc.applications.transfer.v1.MsgTransfer",
        "sender": sender,
        "receiver": receiver,
        "token": {"amount": amount_ncheq, "denom": "ncheq"},
        "source_channel": "channel-0",
        "source_port": "transfer",
    }


def make_vote(voter, proposal_id="42"):
    return {
        "@type": "/cosmos.gov.v1beta1.MsgVote",
        "voter": voter,
        "proposal_id": proposal_id,
        "option": "VOTE_OPTION_YES",
    }


def make_authz_exec(grantee, inner_msgs=None):
    return {
        "@type": "/cosmos.authz.v1beta1.MsgExec",
        "grantee": grantee,
        "msgs": inner_msgs or [],
    }


def make_authz_exec_logs(receiver, amount_ncheq="500000000"):
    return [
        {
            "events": [
                {
                    "type": "coin_received",
                    "attributes": [
                        {"key": "receiver", "value": receiver},
                        {"key": "amount", "value": f"{amount_ncheq}ncheq"},
                    ],
                }
            ]
        }
    ]


def make_authz_grant(granter, grantee, expiration="2025-12-31T23:59:59Z"):
    grant = {
        "authorization": {
            "@type": "/cosmos.staking.v1beta1.StakeAuthorization",
        },
    }
    if expiration:
        grant["expiration"] = expiration
    return {
        "@type": "/cosmos.authz.v1beta1.MsgGrant",
        "granter": granter,
        "grantee": grantee,
        "grant": grant,
    }


def make_authz_revoke(granter, grantee, msg_type_url="/cosmos.staking.v1beta1.MsgDelegate"):
    return {
        "@type": "/cosmos.authz.v1beta1.MsgRevoke",
        "granter": granter,
        "grantee": grantee,
        "msg_type_url": msg_type_url,
    }


def make_ibc_client_update():
    return {
        "@type": "/ibc.core.client.v1.MsgUpdateClient",
        "client_id": "07-tendermint-0",
        "signer": WALLET,
    }


def make_ibc_acknowledgement(signer):
    return {
        "@type": "/ibc.core.channel.v1.MsgAcknowledgement",
        "packet": {},
        "signer": signer,
        "proof_height": {"revision_number": "0", "revision_height": "100"},
        "proof_acked": "proof",
        "acknowledgement": "ack",
    }


def make_ibc_recv_packet(signer):
    return {
        "@type": "/ibc.core.channel.v1.MsgRecvPacket",
        "packet": {},
        "signer": signer,
        "proof_height": {"revision_number": "0", "revision_height": "100"},
        "proof_commitment": "proof",
    }


def make_ibc_recv_packet_logs(sender, receiver, amount_ncheq="100000000000", success="true"):
    """Build logs with fungible_token_packet event for incoming IBC transfer."""
    return [
        {
            "events": [
                {
                    "type": "recv_packet",
                    "attributes": [
                        {"key": "packet_data", "value": "{}"},
                    ],
                },
                {
                    "type": "coin_received",
                    "attributes": [
                        {"key": "receiver", "value": receiver},
                        {"key": "amount", "value": f"{amount_ncheq}ncheq"},
                    ],
                },
                {
                    "type": "transfer",
                    "attributes": [
                        {"key": "recipient", "value": receiver},
                        {"key": "amount", "value": f"{amount_ncheq}ncheq"},
                    ],
                },
                {
                    "type": "fungible_token_packet",
                    "attributes": [
                        {"key": "module", "value": "transfer"},
                        {"key": "sender", "value": sender},
                        {"key": "receiver", "value": receiver},
                        {"key": "denom", "value": "transfer/channel-108/ncheq"},
                        {"key": "amount", "value": amount_ncheq},
                        {"key": "memo", "value": None},
                        {"key": "success", "value": success},
                    ],
                },
            ]
        }
    ]
