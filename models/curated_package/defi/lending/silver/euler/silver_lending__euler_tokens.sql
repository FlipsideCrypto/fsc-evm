{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    tags = ['silver','defi','lending','curated','euler','tokens']
) }}
WITH euler_addresses AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_LENDING_CONTRACT_MAPPING
    ) }}
    WHERE
        type = 'euler_origin_to_address'
),
base_events as (
    select 
        block_number,
        block_timestamp,
        tx_hash,
        origin_from_address, 
        origin_to_address,
        origin_function_signature, 
        event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS creator,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS underlying_address,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS dToken,
        modified_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    from 
        {{ ref('core__fact_event_logs') }} l
    where 
        topic_0 = '0x0cd345140b9008a43f99a999a328ece572a0193e8c8bf5f5755585e6f293b85e'
    AND origin_to_address in (
        select
            distinct(contract_address)
        from
            euler_addresses
    )
{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
      MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
)
select
    b.block_number,
    b.block_timestamp,
    b.tx_hash,
    b.origin_from_address,
    b.origin_to_address,
    b.origin_function_signature,
    b.event_index,
    b.contract_address,
    c.token_name,
    c.token_symbol,
    c.token_decimals,
    b.segmented_data,
    b.creator,
    b.underlying_address,
    u.token_name as underlying_name,
    u.token_symbol as underlying_symbol,
    u.token_decimals as underlying_decimals,
    b.dToken,
    e.protocol,
    e.version,
    b._log_id,
    b.modified_timestamp
from base_events b
left join {{ ref('silver__contracts') }} c
    on b.contract_address = c.contract_address
left join {{ ref('silver__contracts') }} u
    on b.underlying_address = u.contract_address
left join euler_addresses e
    on b.origin_to_address = e.contract_address