{% macro call_sample_rpc_node(
    blockchain,
    node_provider,
    network=none,
    random_block_sample_size=none,
    vault_path_override=none,
    node_url_override=none,
    exclude_traces=none
) %}

{% set query %}
CALL {{target.database}}.admin.sample_rpc_node(
    BLOCKCHAIN => '{{ blockchain }}',
    NODE_PROVIDER => '{{ node_provider }}'
    {% if network is not none %},
    NETWORK => '{{ network }}'
    {% endif %}
    {% if random_block_sample_size is not none %},
    RANDOM_BLOCK_SAMPLE_SIZE => {{ random_block_sample_size }}
    {% endif %}
    {% if vault_path_override is not none %},
    VAULT_PATH_OVERRIDE => '{{ vault_path_override }}'
    {% endif %}
    {% if node_url_override is not none %},
    NODE_URL_OVERRIDE => '{{ node_url_override }}'
    {% endif %}
    {% if exclude_traces is not none %},
    EXCLUDE_TRACES => {{ exclude_traces }}
    {% endif %}
)
{% endset %}

{% do run_query(query) %}

{% endmacro %}
