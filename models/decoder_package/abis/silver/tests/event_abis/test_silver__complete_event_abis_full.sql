{{ config (
    materialized = "view",
    tags = ['full_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__complete_event_abis') }}
