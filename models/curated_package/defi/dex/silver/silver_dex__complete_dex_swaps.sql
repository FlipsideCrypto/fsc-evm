{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform','version'],
  cluster_by = ['block_timestamp::DATE','platform'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_function_signature, origin_from_address, origin_to_address, contract_address, pool_name, event_name, sender, tx_to, token_in, token_out, symbol_in, symbol_out), SUBSTRING(origin_function_signature, pool_name, event_name, sender, tx_to, token_in, token_out, symbol_in, symbol_out)",
  tags = ['silver_dex','defi','dex','curated','heal','complete','swap']
) }}

WITH contracts AS (

  SELECT
    address AS contract_address,
    symbol AS token_symbol,
    decimals AS token_decimals,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('core__dim_contracts') }}
  UNION ALL
  SELECT
    '0x0000000000000000000000000000000000000000' AS contract_address,
    '{{ vars.GLOBAL_NATIVE_ASSET_SYMBOL }}' AS token_symbol,
    decimals AS token_decimals,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('core__dim_contracts') }}
  WHERE
    address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
),
prices AS (
  SELECT
    token_address,
    price,
    HOUR,
    is_verified,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('price__ez_prices_hourly') }}
  UNION ALL
  SELECT
    '0x0000000000000000000000000000000000000000' AS token_address,
    price,
    HOUR,
    is_verified,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('price__ez_prices_hourly') }}
  WHERE
    token_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
),
swap_evt_v3 AS (

  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address AS contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    recipient AS tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__swap_evt_v3_swaps') }}

{% if is_incremental() and 'swap_evt_v3' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
swap_evt_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__swap_evt_v2_swaps') }}

{% if is_incremental() and 'swap_evt_v2' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
quickswap_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address AS contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    recipient AS tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__quickswap_v2_swaps') }}

{% if is_incremental() and 'quickswap_v2' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
woofi AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__woofi_swaps') }}

{% if is_incremental() and 'woofi' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
kyberswap_v1_dynamic AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__kyberswap_v1_dynamic_swaps') }}

{% if is_incremental() and 'kyberswap_v1_dynamic' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
kyberswap_v1_static AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__kyberswap_v1_static_swaps') }}

{% if is_incremental() and 'kyberswap_v1_static' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
kyberswap_v2_elastic AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__kyberswap_v2_elastic_swaps') }}

{% if is_incremental() and 'kyberswap_v2_elastic' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
hashflow AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__hashflow_swaps') }}

{% if is_incremental() and 'hashflow' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
hashflow_v3 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__hashflow_v3_swaps') }}

{% if is_incremental() and 'hashflow_v3' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
curve AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    tokens_sold AS amount_in_unadj,
    tokens_bought AS amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__curve_swaps') }}

{% if is_incremental() and 'curve' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
balancer AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__balancer_swaps') }}

{% if is_incremental() and 'balancer' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
dodo_v1 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__dodo_v1_swaps') }}

{% if is_incremental() and 'dodo_v1' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
dodo_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__dodo_v2_swaps') }}

{% if is_incremental() and 'dodo_v2' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
dexalot AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__dexalot_swaps') }}

{% if is_incremental() and 'dexalot' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
gmx AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__gmx_swaps') }}

{% if is_incremental() and 'gmx' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
gmx_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__gmx_v2_swaps') }}

{% if is_incremental() and 'gmx_v2' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
pharaoh_v1 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__pharaoh_v1_swaps') }}

{% if is_incremental() and 'pharaoh_v1' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
sushiswap AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__sushiswap_swaps') }}

{% if is_incremental() and 'sushiswap' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
platypus AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__platypus_swaps') }}

{% if is_incremental() and 'platypus' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
trader_joe_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__trader_joe_v2_swaps') }}

{% if is_incremental() and 'trader_joe_v2' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
trader_joe_v2_1 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__trader_joe_v2_1_swaps') }}

{% if is_incremental() and 'trader_joe_v2_1' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
velodrome_v1 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__velodrome_v1_swaps') }}

{% if is_incremental() and 'velodrome_v1' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
velodrome_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__velodrome_v2_swaps') }}

{% if is_incremental() and 'velodrome_v2' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
synthetix AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__synthetix_swaps') }}

{% if is_incremental() and 'synthetix' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
maverick AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__maverick_swaps') }}

{% if is_incremental() and 'maverick' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
maverick_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__maverick_v2_swaps') }}

{% if is_incremental() and 'maverick_v2' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
pancakeswap_v2_ss AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__pancakeswap_v2_ss_swaps') }}

{% if is_incremental() and 'pancakeswap_v2_ss' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
pancakeswap_v2_mm AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__pancakeswap_v2_mm_swaps') }}

{% if is_incremental() and 'pancakeswap_v2_mm' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
pancakeswap_v3 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address AS contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender_address AS sender,
    recipient_address AS tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__pancakeswap_v3_swaps') }}

{% if is_incremental() and 'pancakeswap_v3' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
dackie AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address AS contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender_address AS sender,
    recipient_address AS tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__dackie_swaps') }}

{% if is_incremental() and 'dackie' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
uniswap_v4 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address AS contract_address,
    pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    recipient AS tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__uniswap_v4_swaps') }}

{% if is_incremental() and 'uniswap_v4' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
bitflux AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address AS contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__bitflux_swaps') }}

{% if is_incremental() and 'bitflux' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
glyph_v4 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address AS contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__glyph_v4_swaps') }}

{% if is_incremental() and 'glyph_v4' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
levelfi AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__levelfi_swaps') }}

{% if is_incremental() and 'levelfi' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
camelot_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__camelot_v2_swaps') }}

{% if is_incremental() and 'camelot_v2' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
zyberswap_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__zyberswap_v2_swaps') }}

{% if is_incremental() and 'zyberswap_v2' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
voodoo AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__voodoo_swaps') }}

{% if is_incremental() and 'voodoo' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
aerodrome AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__aerodrome_swaps') }}

{% if is_incremental() and 'aerodrome' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
aerodrome_slipstream AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__aerodrome_slipstream_swaps') }}

{% if is_incremental() and 'aerodrome_slipstream' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
all_dex AS (
  SELECT
    *
  FROM
    balancer
  UNION ALL
  SELECT
    *
  FROM
    curve
  UNION ALL
  SELECT
    *
  FROM
    hashflow
  UNION ALL
  SELECT
    *
  FROM
    hashflow_v3
  UNION ALL
  SELECT
    *
  FROM
    quickswap_v2
  UNION ALL
  SELECT
    *
  FROM
    woofi
  UNION ALL
  SELECT
    *
  FROM
    kyberswap_v1_dynamic
  UNION ALL
  SELECT
    *
  FROM
    kyberswap_v1_static
  UNION ALL
  SELECT
    *
  FROM
    kyberswap_v2_elastic
  UNION ALL
  SELECT
    *
  FROM
    dodo_v1
  UNION ALL
  SELECT
    *
  FROM
    dodo_v2
  UNION ALL
  SELECT
    *
  FROM
    swap_evt_v3
  UNION ALL
  SELECT
    *
  FROM
    swap_evt_v2
  UNION ALL
  SELECT
    *
  FROM
    dexalot
  UNION ALL
  SELECT
    *
  FROM
    gmx
  UNION ALL
  SELECT
    *
  FROM
    gmx_v2
  UNION ALL
  SELECT
    *
  FROM
    pharaoh_v1
  UNION ALL
  SELECT
    *
  FROM
    sushiswap
  UNION ALL
  SELECT
    *
  FROM
    platypus
  UNION ALL
  SELECT
    *
  FROM
    trader_joe_v2
  UNION ALL
  SELECT
    *
  FROM
    trader_joe_v2_1
  UNION ALL
  SELECT
    *
  FROM
    velodrome_v1
  UNION ALL
  SELECT
    *
  FROM
    velodrome_v2
  UNION ALL
  SELECT
    *
  FROM
    synthetix
  UNION ALL
  SELECT
    *
  FROM
    maverick
  UNION ALL
  SELECT
    *
  FROM
    maverick_v2
  UNION ALL
  SELECT
    *
  FROM
    pancakeswap_v2_ss
  UNION ALL
  SELECT
    *
  FROM
    pancakeswap_v2_mm
  UNION ALL
  SELECT
    *
  FROM
    pancakeswap_v3
  UNION ALL
  SELECT
    *
  FROM
    dackie
  UNION ALL
  SELECT
    *
  FROM
    uniswap_v4
  UNION ALL
  SELECT
    *
  FROM
    bitflux
  UNION ALL
  SELECT
    *
  FROM
    glyph_v4
  UNION ALL
  SELECT
    *
  FROM
    levelfi
  UNION ALL
  SELECT
    *
  FROM
    camelot_v2
  UNION ALL
  SELECT
    *
  FROM
    zyberswap_v2
  UNION ALL
  SELECT
    *
  FROM
    voodoo
  UNION ALL
  SELECT
    *
  FROM
    dackie
  UNION ALL
  SELECT
    *
  FROM
    aerodrome
  UNION ALL
  SELECT
    *
  FROM
    aerodrome_slipstream
),
complete_dex_swaps AS (
  SELECT
    s.block_number,
    s.block_timestamp,
    s.tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    s.contract_address,
    s.pool_id,
    event_name,
    token_in,
    p1.is_verified AS token_in_is_verified,
    c1.token_decimals AS decimals_in,
    c1.token_symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    CASE
      WHEN decimals_in IS NOT NULL THEN amount_in * p1.price
      ELSE NULL
    END AS amount_in_usd,
    token_out,
    p2.is_verified AS token_out_is_verified,
    c2.token_decimals AS decimals_out,
    c2.token_symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    CASE
      WHEN decimals_out IS NOT NULL THEN amount_out * p2.price
      ELSE NULL
    END AS amount_out_usd,
    CASE
      WHEN lp.pool_name IS NULL THEN CONCAT(
        LEAST(
          COALESCE(
            symbol_in,
            CONCAT(SUBSTRING(token_in, 1, 5), '...', SUBSTRING(token_in, 39, 42))
          ),
          COALESCE(
            symbol_out,
            CONCAT(SUBSTRING(token_out, 1, 5), '...', SUBSTRING(token_out, 39, 42))
          )
        ),
        '-',
        GREATEST(
          COALESCE(
            symbol_in,
            CONCAT(SUBSTRING(token_in, 1, 5), '...', SUBSTRING(token_in, 39, 42))
          ),
          COALESCE(
            symbol_out,
            CONCAT(SUBSTRING(token_out, 1, 5), '...', SUBSTRING(token_out, 39, 42))
          )
        )
      )
      ELSE lp.pool_name
    END AS pool_name,
    sender,
    tx_to,
    event_index,
    s.platform,
    s.protocol,
    s.version,
    s.type,
    s._log_id,
    s._inserted_timestamp
  FROM
    all_dex s
    LEFT JOIN contracts
    c1
    ON s.token_in = c1.contract_address
    LEFT JOIN contracts
    c2
    ON s.token_out = c2.contract_address
    LEFT JOIN prices
    p1
    ON s.token_in = p1.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p1.hour
    LEFT JOIN prices
    p2
    ON s.token_out = p2.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p2.hour
    LEFT JOIN {{ ref('silver_dex__complete_dex_liquidity_pools') }}
    lp
    ON s.contract_address = lp.pool_address
    AND s.pool_id = lp.pool_id
),

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
heal_model AS (
  SELECT
    t0.block_number,
    t0.block_timestamp,
    t0.tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    t0.contract_address,
    t0.pool_id,
    event_name,
    token_in,
    p1.is_verified AS token_in_is_verified,
    c1.token_decimals AS decimals_in,
    c1.token_symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN c1.token_decimals IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, c1.token_decimals))
    END AS amount_in_heal,
    CASE
      WHEN c1.token_decimals IS NOT NULL THEN amount_in_heal * p1.price
      ELSE NULL
    END AS amount_in_usd_heal,
    token_out,
    p2.is_verified AS token_out_is_verified,
    c2.token_decimals AS decimals_out,
    c2.token_symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN c2.token_decimals IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, c2.token_decimals))
    END AS amount_out_heal,
    CASE
      WHEN c2.token_decimals IS NOT NULL THEN amount_out_heal * p2.price
      ELSE NULL
    END AS amount_out_usd_heal,
    CASE
      WHEN lp.pool_name IS NULL THEN CONCAT(
        LEAST(
          COALESCE(
            c1.token_symbol,
            CONCAT(SUBSTRING(token_in, 1, 5), '...', SUBSTRING(token_in, 39, 42))
          ),
          COALESCE(
            c2.token_symbol,
            CONCAT(SUBSTRING(token_out, 1, 5), '...', SUBSTRING(token_out, 39, 42))
          )
        ),
        '-',
        GREATEST(
          COALESCE(
            c1.token_symbol,
            CONCAT(SUBSTRING(token_in, 1, 5), '...', SUBSTRING(token_in, 39, 42))
          ),
          COALESCE(
            c2.token_symbol,
            CONCAT(SUBSTRING(token_out, 1, 5), '...', SUBSTRING(token_out, 39, 42))
          )
        )
      )
      ELSE lp.pool_name
    END AS pool_name_heal,
    sender,
    tx_to,
    event_index,
    t0.platform,
    t0.protocol,
    t0.version,
    t0.type,
    t0._log_id,
    t0._inserted_timestamp
  FROM
    {{ this }}
    t0
    LEFT JOIN contracts
    c1
    ON t0.token_in = c1.contract_address
    LEFT JOIN contracts
    c2
    ON t0.token_out = c2.contract_address
    LEFT JOIN prices
    p1
    ON t0.token_in = p1.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p1.hour
    LEFT JOIN prices
    p2
    ON t0.token_out = p2.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p2.hour
    LEFT JOIN {{ ref('silver_dex__complete_dex_liquidity_pools') }}
    lp
    ON t0.contract_address = lp.pool_address
    AND t0.pool_id = lp.pool_id
  WHERE
    CONCAT(
      t0.block_number,
      '-',
      t0.platform,
      '-',
      t0.version
    ) IN (
      SELECT
        CONCAT(
          t1.block_number,
          '-',
          t1.platform,
          '-',
          t1.version
        )
      FROM
        {{ this }}
        t1
      WHERE
        t1.decimals_in IS NULL
        AND t1._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
          FROM
            {{ this }}
        )
        AND EXISTS (
          SELECT
            1
          FROM
            contracts C
          WHERE
            C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
            AND C.token_decimals IS NOT NULL
            AND C.contract_address = t1.token_in)
          GROUP BY
            1
        )
        OR CONCAT(
          t0.block_number,
          '-',
          t0.platform,
          '-',
          t0.version
        ) IN (
          SELECT
            CONCAT(
              t2.block_number,
              '-',
              t2.platform,
              '-',
              t2.version
            )
          FROM
            {{ this }}
            t2
          WHERE
            t2.decimals_out IS NULL
            AND t2._inserted_timestamp < (
              SELECT
                MAX(
                  _inserted_timestamp
                ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
              FROM
                {{ this }}
            )
            AND EXISTS (
              SELECT
                1
              FROM
                contracts C
              WHERE
                C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                AND C.token_decimals IS NOT NULL
                AND C.contract_address = t2.token_out)
              GROUP BY
                1
            )
            OR CONCAT(
              t0.block_number,
              '-',
              t0.platform,
              '-',
              t0.version
            ) IN (
              SELECT
                CONCAT(
                  t3.block_number,
                  '-',
                  t3.platform,
                  '-',
                  t3.version
                )
              FROM
                {{ this }}
                t3
              WHERE
                t3.amount_in_usd IS NULL
                AND t3._inserted_timestamp < (
                  SELECT
                    MAX(
                      _inserted_timestamp
                    ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
                  FROM
                    {{ this }}
                )
                AND EXISTS (
                  SELECT
                    1
                  FROM
                    prices
                    p
                  WHERE
                    p._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                    AND p.price IS NOT NULL
                    AND p.token_address = t3.token_in
                    AND p.hour = DATE_TRUNC(
                      'hour',
                      t3.block_timestamp
                    )
                )
              GROUP BY
                1
            )
            OR CONCAT(
              t0.block_number,
              '-',
              t0.platform,
              '-',
              t0.version
            ) IN (
              SELECT
                CONCAT(
                  t4.block_number,
                  '-',
                  t4.platform,
                  '-',
                  t4.version
                )
              FROM
                {{ this }}
                t4
              WHERE
                t4.amount_out_usd IS NULL
                AND t4._inserted_timestamp < (
                  SELECT
                    MAX(
                      _inserted_timestamp
                    ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
                  FROM
                    {{ this }}
                )
                AND EXISTS (
                  SELECT
                    1
                  FROM
                    prices
                    p
                  WHERE
                    p._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                    AND p.price IS NOT NULL
                    AND p.token_address = t4.token_out
                    AND p.hour = DATE_TRUNC(
                      'hour',
                      t4.block_timestamp
                    )
                )
              GROUP BY
                1
            )
            OR     
            CONCAT(
              t0.block_number,
              '-',
              t0.platform,
              '-',
              t0.version
            ) IN (
                select concat(
                  t5.block_number,
                  '-',
                  t5.platform,
                  '-',
                  t5.version
                )
                from {{ this }} t5
                where t5.token_in in (
                  select token_address
                  from {{ ref('price__ez_asset_metadata') }}
                  where ifnull(is_verified_modified_timestamp, '1970-01-01' :: TIMESTAMP) > dateadd('day', -10, SYSDATE())
                )
              )
            OR concat(
              t0.block_number,
              '-',
              t0.platform,
              '-',
              t0.version
            ) IN (  
              select concat(
                t6.block_number,
                '-',
                t6.platform,
                '-',
                t6.version
              )
              from {{ this }} t6
              where t6.token_out in (
                select token_address
                from {{ ref('price__ez_asset_metadata') }}
                where ifnull(is_verified_modified_timestamp, '1970-01-01' :: TIMESTAMP) > dateadd('day', -10, SYSDATE())
              )
            )
        ),
      {% endif %}

      FINAL AS (
        SELECT
          *
        FROM
          complete_dex_swaps

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_id,
  event_name,
  token_in,
  token_in_is_verified,
  decimals_in,
  symbol_in,
  amount_in_unadj,
  amount_in_heal AS amount_in,
  amount_in_usd_heal AS amount_in_usd,
  token_out,
  token_out_is_verified,
  decimals_out,
  symbol_out,
  amount_out_unadj,
  amount_out_heal AS amount_out,
  amount_out_usd_heal AS amount_out_usd,
  pool_name_heal AS pool_name,
  sender,
  tx_to,
  event_index,
  platform,
  protocol,
  version,
  type,
  _log_id,
  _inserted_timestamp
FROM
  heal_model
{% endif %}
)
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  pool_id,
  event_name,
  amount_in_unadj,
  amount_in,
  amount_in_usd,
  amount_out_unadj,
  amount_out,
  amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  protocol,
  version,
  type,
  token_in,
  IFNULL(
    token_in_is_verified,
    FALSE
  ) AS token_in_is_verified,
  token_out,
  IFNULL(
    token_out_is_verified,
    FALSE
  ) AS token_out_is_verified,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash','event_index']
  ) }} AS complete_dex_swaps_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify (ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
