{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = "vault_address",
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['silver','defi','erc4626','curated','maple']
) }}

{# Get pool manager addresses from mapping #}
WITH pool_managers AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_ERC4626_VAULTS_MAPPING
    ) }}
    WHERE
        type = 'erc4626_pool_managers'
),

{# Find vault contracts created by pool managers #}
vault_contracts AS (
    SELECT
        c.address AS vault_address,
        c.creator_address AS pool_manager,
        c.created_block_number,
        c.created_block_timestamp,
        c.name AS vault_name,
        c.symbol AS vault_symbol,
        c.decimals AS vault_decimals,
        pm.protocol,
        pm.version,
        pm.protocol || '-' || pm.version AS platform,
        c.modified_timestamp
    FROM
        {{ ref('core__dim_contracts') }} c
    INNER JOIN pool_managers pm
        ON c.creator_address = pm.contract_address
    WHERE
        c.decimals IS NOT NULL

{% if is_incremental() %}
    AND c.modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM
            {{ this }}
    )
{% endif %}
),

{# Derive underlying asset from vault symbol (syrupUSDC -> USDC, syrupUSDT -> USDT, etc.) #}
underlying_mapping AS (
    SELECT
        vc.vault_address,
        vc.vault_symbol,
        CASE
            WHEN UPPER(vc.vault_symbol) LIKE '%USDC%' THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
            WHEN UPPER(vc.vault_symbol) LIKE '%USDT%' THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
            WHEN UPPER(vc.vault_symbol) LIKE '%WETH%' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            WHEN UPPER(vc.vault_symbol) LIKE '%DAI%' THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
            ELSE NULL
        END AS underlying_asset_address
    FROM vault_contracts vc
),

underlying_details AS (
    SELECT
        um.vault_address,
        um.underlying_asset_address,
        c.name AS underlying_name,
        c.symbol AS underlying_symbol,
        c.decimals AS underlying_decimals
    FROM
        underlying_mapping um
    LEFT JOIN {{ ref('core__dim_contracts') }} c
        ON um.underlying_asset_address = c.address
)

SELECT
    vc.vault_address,
    vc.pool_manager,
    vc.created_block_number,
    vc.created_block_timestamp,
    vc.vault_name,
    vc.vault_symbol,
    vc.vault_decimals,
    ud.underlying_asset_address,
    ud.underlying_name,
    ud.underlying_symbol,
    ud.underlying_decimals,
    vc.protocol,
    vc.version,
    vc.platform,
    vc.modified_timestamp AS _inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['vc.vault_address']) }} AS maple_pools_id
FROM
    vault_contracts vc
LEFT JOIN underlying_details ud
    ON vc.vault_address = ud.vault_address
QUALIFY(ROW_NUMBER() OVER (PARTITION BY vc.vault_address ORDER BY vc.modified_timestamp DESC)) = 1
