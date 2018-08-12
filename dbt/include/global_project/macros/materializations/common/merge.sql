

{% macro get_merge_sql(target, source, unique_key, dest_columns) -%}
  {{ adapter_macro('get_merge_sql', target, source, unique_key, dest_columns) }}
{%- endmacro %}

{% macro default__get_merge_sql(target, source, unique_key, dest_columns) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute="name") | join(', ') -%}

    merge into {{ target }} as DBT_DEST
    using {{ source }} as DBT_SOURCE

    {% if unique_key %}
        on DBT_SOURCE.{{ unique_key }} = DBT_DEST.{{ unique_key }}
    {% else %}
        on FALSE
    {% endif %}

    {% if unique_key %}
    when matched then update set
        {% for column in dest_columns -%}
            {{ column.name }} = DBT_SOURCE.{{ column.name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{% endmacro %}

{% macro redshift__get_merge_sql(target, source, unique_key, dest_columns) -%}
    {{ exceptions.raise_compiler_error('get_merge_sql is not implemented for Redshift') }}
{% endmacro %}

{% macro postgres__get_merge_sql(target, source, unique_key, dest_columns) -%}
    {{ exceptions.raise_compiler_error('get_merge_sql is not implemented for Postgres') }}
{% endmacro %}
