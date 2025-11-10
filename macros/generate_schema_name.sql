{% macro generate_schema_name(custom_schema_name, node) -%}
  {#- 
    Macro to override the default generate schema name macro.
    In this custom version, schema is selected based on configuration provided in dbt_project.yml
    - If schema provided in dbt_project.yml, then that schema is picked
    - Else it falls back to profiles.yml file defined schema
   -#}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
