{%- if is_view is not defined %}{% set is_view = False %}{% endif %}
{%- if check_existence is not defined %}{% set check_existence = False %}{% endif %}
{%- if storage_fmt is not defined %}{% set storage_fmt = 'PARQUET' %}{% endif -%}
CREATE {{ is_view and 'VIEW' or 'TABLE' }} {% if check_existence %}IF NOT EXISTS {% endif %}`{{ name }}`
{%- if storage_fmt and not is_view %}
STORED AS {{ storage_fmt }}
{%- endif %}
AS
{{ select }}