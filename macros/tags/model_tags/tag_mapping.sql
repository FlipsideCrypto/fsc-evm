{#
    Sets model specific tags
#}
{%- macro get_tag_dictionary() -%}
    {% set tag_mapping = {
        'complete_native_asset_metadata': ['phase_2'],
        'complete_native_prices': ['phase_2'], 
        'complete_provider_asset_metadata': ['phase_2'],
        'complete_provider_prices': ['phase_2'],
        'complete_token_asset_metadata': ['phase_2'],
        'complete_token_prices': ['phase_2'],
        'labels': ['phase_2'],
        'token_reads': ['phase_2'],
        'dim_contracts': ['phase_2'],
        'ez_native_transfers': ['phase_2'],
        'ez_token_transfers': ['phase_2'], 
        'dim_asset_metadata': ['phase_2'],
        'ez_asset_metadata': ['phase_2'],
        'ez_prices_hourly': ['phase_2'],
        'fact_prices_ohlc_hourly': ['phase_2'],
        'abis': ['phase_2'],
        'bytecode_abis': ['phase_2'],
        'complete_event_abis': ['phase_2'],
        'contracts': ['phase_2'],
        'created_contracts': ['phase_2'],
        'flat_event_abis': ['phase_2'],
        'proxies': ['phase_2'],
        'relevant_contracts': ['phase_2'],
        'user_verified_abis': ['phase_2'],
        'verified_abis': ['phase_2']
    } %}

    {{ return(tag_mapping) }}
{%- endmacro -%}