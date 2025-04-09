{% macro call_sample_rpc_node(
    blockchain=none,
    node_provider=none,
    network=none,
    random_block_sample_size=none,
    vault_path_override=none,
    node_url_override=none,
    exclude_traces=none
) %}

{% set vars = return_vars() %}
{% set global_project_name = vars.GLOBAL_PROJECT_NAME.lower() %}
{% set global_node_provider = vars.GLOBAL_NODE_PROVIDER.lower() %}

{% set query %}
CALL {{target.database}}.admin.sample_rpc_node(
    BLOCKCHAIN => {% if blockchain is not none %}
                   '{{ blockchain }}'
                 {% else %}
                   '{{ global_project_name }}'
                 {% endif %},
    NODE_PROVIDER => {% if node_provider is not none %}
                      '{{ node_provider }}'
                    {% else %}
                      '{{ global_node_provider }}'
                    {% endif %}
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
