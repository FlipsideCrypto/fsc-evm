{% macro enable_change_tracking() %}

{# Get variables #}
{% set vars = return_vars() %}

  {% if 'exclude_change_tracking' not in config.get('tags') and vars.GLOBAL_CHANGE_TRACKING_ENABLED %}
    {% if config.get('materialized') == 'view' %}
      ALTER VIEW {{ this }} SET CHANGE_TRACKING = TRUE;
    {% else %}
      ALTER TABLE {{ this }} SET CHANGE_TRACKING = TRUE;
    {% endif %}
  {% endif %}
{% endmacro %}