{% macro run_fsc_evm_dispatch_workflow(workflow_name, input_repos, command=none) %}
  {% set repo_array %}
    ARRAY_CONSTRUCT(
      {%- for repo in input_repos -%}
        '{{ repo }}'
        {%- if not loop.last -%},{%- endif -%}
      {%- endfor -%}
    )
  {% endset %}

  {% set query %}
    CALL utils.dispatch_workflow(
      '{{ workflow_name }}',
      {{ repo_array }},
      {% if command %}
        '{{ command }}'
      {% else %}
        NULL
      {% endif %}
    )
  {% endset %}

  {% do log("Dispatching workflow: " ~ workflow_name, info=true) %}
  {% do log("Target repos: " ~ input_repos | join(", "), info=true) %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {% set dispatch_results = results.columns[0].values()[0] %}
    {% do log("Dispatch results: " ~ dispatch_results, info=true) %}
  {% endif %}

  {{ return(query) }}
{% endmacro %}