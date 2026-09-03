#!/usr/bin/env python3
"""
Unit tests for the Data Masking Module.
Verifies that sensitive fields (client_id, montant, siret) are obfuscated,
while preserving their presence/absence, so the LLM can still run its diagnosis.
"""

import sys
from pathlib import Path

# Add 'agents' folder to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "agents"))

from common.masking import mask_json_value, mask_tool_outputs


def test_mask_json_value():
    # 1. Valid message with siret
    msg_valid = '{"client_id": 12345, "siret": "14141414141414", "montant": 99.9}'
    masked = mask_json_value(msg_valid)
    assert "12345" not in masked
    assert "14141414141414" not in masked
    assert "99.9" not in masked
    assert "siret" in masked  # The key remains present!
    assert '"siret": "***"' in masked

    # 2. Poison message with missing siret
    msg_poison = '{"client_id": 12345, "montant": 150}'
    masked_poison = mask_json_value(msg_poison)
    assert "12345" not in masked_poison
    assert "siret" not in masked_poison  # The key remains absent!
    assert '"client_id": "***"' in masked_poison

    # 3. Non-JSON values
    non_json = "plain text"
    assert mask_json_value(non_json) == "plain text"


def test_mask_tool_outputs():
    # 1. List of messages
    outputs = {
        "result": [
            {
                "offset": 1452,
                "value": '{"client_id": 123, "siret": "11111"}'
            },
            {
                "offset": 1453,
                "value": '{"client_id": 456, "montant": 222}'
            }
        ]
    }
    masked_outputs = mask_tool_outputs(outputs)
    result = masked_outputs["result"]
    assert "123" not in result[0]["value"]
    assert "11111" not in result[0]["value"]
    assert "456" not in result[1]["value"]
    assert "222" not in result[1]["value"]


if __name__ == "__main__":
    print("Running masking tests...")
    test_mask_json_value()
    test_mask_tool_outputs()
    print("PASS: all masking tests passed!")
