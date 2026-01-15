{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT suck.tx_hash
FROM {{ ref('maker__fact_vat_suck') }} suck
WHERE suck.u_address = '0xa950524441892a31ebddf91d3ceefa04bf454466'
  AND suck.v_address IN (
    '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb',
    '0x2cc583c0aacdac9e23cb601fda8f1a0c56cdcb71',
    '0xa4c22f0e25c6630b2017979acf1f865e94695c4b'
  )
  AND suck.rad != 0
GROUP BY 1