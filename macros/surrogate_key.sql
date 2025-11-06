{% macro surrogate_key(cols) -%}
    md5(
      {{ cols | map('string') | map('replace', "'", "\\'") | map('replace', '"', '\\"') | join(" || '|' || ") }}
    )
{%- endmacro %}
