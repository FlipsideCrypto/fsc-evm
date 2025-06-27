{% macro create_udf_mapping_slot() %}
  {% if var("UPDATE_UDFS_AND_SPS", false) and execute %}
    {% set create_udf_sql %}
        CREATE OR REPLACE FUNCTION {{target.database}}.UTILS.UDF_MAPPING_SLOT(
            address_hex VARCHAR, 
            slot_index INT
        )
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.9'
    PACKAGES = ('pycryptodome==3.15.0')
    HANDLER = 'compute_slot'
    AS '
from Crypto.Hash import keccak

def compute_slot(address_hex, slot_index):
    # Remove "0x" if present
    if address_hex.startswith("0x"):
        address_hex = address_hex[2:]
    
    # Pad address to 32 bytes (64 hex chars)
    padded_address = address_hex.rjust(64, "0")
    
    # Convert slot index to hex and pad to 32 bytes
    slot_hex = hex(slot_index)[2:].rjust(64, "0")
    
    # Concatenate as raw bytes
    combined = bytes.fromhex(padded_address + slot_hex)

    # Hash with keccak256
    keccak_hash = keccak.new(digest_bits=256)
    keccak_hash.update(combined)
    
    return "0x" + keccak_hash.hexdigest()
    ';
    {% endset %}

    {% do run_query(create_udf_sql) %}
    {{ log("Created UDF_MAPPING_SLOT function ", info=true) }}
  {% endif %}
{% endmacro %}