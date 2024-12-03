{% macro recent_data_filter() %}
  {% if 'curated' in config.get('tags', []) %}
    and BLOCK_TIMESTAMP >= dateadd('day', -1, current_timestamp())
  {% endif %}
{% endmacro %} 