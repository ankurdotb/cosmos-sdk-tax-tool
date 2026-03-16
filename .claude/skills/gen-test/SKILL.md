---
name: gen-test
description: Generate pytest tests for transaction processing and CSV conversion functions
disable-model-invocation: true
---

# Generate Tests

Generate pytest test cases for this project. Focus on:

## What to Test

1. **Amount conversion** — ncheq to CHEQ (divide by 1,000,000,000)
2. **Transaction deduplication** — same hash should be deduplicated
3. **Timestamp parsing** — ISO format with Z suffix → "YYYY-MM-DD HH:MM"
4. **Transaction type processing** — each message type produces correct Koinly record:
   - Bank sends (sender vs receiver perspective)
   - Reward withdrawals
   - Delegations (cost-only, no amounts)
   - Undelegations (with automatic reward withdrawal)
   - Redelegations (with reward from source validator)
   - IBC transfers (direction detection)
   - Governance votes (cost-only)
   - Authz exec/grant/revoke
5. **Authz consolidation** — multiple daily authz rewards → single daily record
6. **Fee extraction** — from transaction fee structure
7. **Failed transactions** — labeled as "cost"
8. **Edge cases** — empty logs, missing fields, zero amounts

## Structure

- Create tests in `tests/` directory
- Use pytest fixtures for sample transaction data
- Mock `requests.post` for fetch_transactions.py tests
- Test CSV output format matches Koinly headers

## Sample Transaction Shape

```python
{
    "transaction": {
        "height": 123456,
        "hash": "ABC123...",
        "success": True,
        "messages": [{"@type": "/cosmos.bank.v1beta1.MsgSend", ...}],
        "logs": [{"events": [...]}],
        "fee": {"amount": [{"amount": "5000000000", "denom": "ncheq"}]},
        "block": {"height": 123456, "timestamp": "2024-01-15T10:30:00Z"}
    }
}
```
