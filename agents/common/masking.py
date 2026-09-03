"""
Data Masking Module — provides functions to sanitize sensitive fields in Kafka
messages before they are exposed to the LLM context.
"""

import json
import logging

logger = logging.getLogger(__name__)

# List of fields to be obfuscated/masked
SENSITIVE_FIELDS = {"client_id", "montant", "siret"}


def mask_json_value(value_str: str) -> str:
    """
    Parses value_str as JSON and masks any sensitive fields found within.
    Preserves the key's presence (so LLM can still check for existence)
    but obfuscates the actual value.
    """
    if not value_str:
        return value_str

    try:
        data = json.loads(value_str)
    except (json.JSONDecodeError, TypeError):
        # Fallback to returning original value if not valid JSON
        return value_str

    if not isinstance(data, dict):
        return value_str

    modified = False
    for field in SENSITIVE_FIELDS:
        if field in data:
            data[field] = "***"
            modified = True

    if modified:
        return json.dumps(data)

    return value_str


def mask_tool_outputs(outputs: dict) -> dict:
    """
    Traverses the tool outputs dictionary to find Kafka message patterns and
    obfuscates any sensitive fields inside their values.
    """
    if not outputs or not isinstance(outputs, dict):
        return outputs

    # If the output contains a 'result' key (ADK's standard wrapping)
    result = outputs.get("result")
    if result is None:
        return outputs

    # The result could be a list of messages (from read_from_offset)
    if isinstance(result, list):
        for msg in result:
            if isinstance(msg, dict) and "value" in msg:
                msg["value"] = mask_json_value(msg["value"])
    # Or a single message dict
    elif isinstance(result, dict) and "value" in result:
        result["value"] = mask_json_value(result["value"])

    return outputs
