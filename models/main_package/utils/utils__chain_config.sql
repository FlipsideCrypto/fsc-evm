-- models/config/chain_config.sql
{{
  config(
    materialized = 'table',
    alias = 'chain_config',
    schema = 'config'
  )
}}

WITH chain_values AS (
  SELECT 
    chain,
    key,
    parent_key,
    value,
    is_enabled
  FROM {{ ref('bronze__values') }}
  WHERE chain = '{{ target.name }}' -- Use target name as chain identifier
    OR chain = 'fsc_evm' -- Also include global defaults
),

config_keys AS (
  SELECT
    key,
    parent_key,
    data_type,
    package,
    category,
    default_value
  FROM {{ ref('bronze__keys') }}
),

-- Join keys with values, preferring chain-specific values when available
merged_config AS (
  SELECT
    k.key,
    k.parent_key,
    k.data_type,
    k.package,
    k.category,
    COALESCE(v.value, k.default_value) as raw_value,
    COALESCE(v.is_enabled, 'TRUE') as is_enabled
  FROM config_keys k
  LEFT JOIN chain_values v ON k.key = v.key
  WHERE COALESCE(v.is_enabled, 'TRUE') = 'TRUE'
),

-- First pass: Store all raw values in a simple format
config_pass1 AS (
  SELECT
    key,
    parent_key,
    data_type,
    package,
    category,
    raw_value,
    is_enabled,
    -- Check if value is a template expression like {{X * Y}}
    CASE 
      WHEN raw_value LIKE '{{%}}' THEN TRUE
      ELSE FALSE
    END as needs_resolution
  FROM merged_config
),

-- Second pass: Resolve template expressions for numeric values
config_resolved AS (
  SELECT
    key,
    parent_key,
    data_type,
    package,
    category,
    is_enabled,
    CASE
      -- When it's a template expression, extract and evaluate it
      WHEN needs_resolution THEN
        -- Extract the expression between {{ and }}
        {{ process_templates('raw_value') }}
      ELSE
        raw_value
    END as value
  FROM config_pass1
)

SELECT * FROM config_resolved