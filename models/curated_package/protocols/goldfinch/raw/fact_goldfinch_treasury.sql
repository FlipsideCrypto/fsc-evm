{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

with treasury as (
    {{ get_treasury_balance('ethereum', '0xBEb28978B2c755155f20fd3d09Cb37e300A6981f', '2020-01-01') }}
)
select * from treasury
where date > date('2022-01-11') -- TGE on 01/10~01/11 2022 so pricing data is bad on this date