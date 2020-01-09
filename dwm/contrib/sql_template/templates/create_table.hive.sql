{%- if is_external is not defined %}{% set is_external = False %}{% endif %}
{%- if check_existence is not defined %}{% set check_existence = False %}{% endif %}
{%- if partition_fields is not defined %}{% set partition_fields = False %}{% endif %}
{%- if storage_fmt is not defined %}{% set storage_fmt = 'PARQUET' %}{% endif -%}
CREATE {% if is_external %}EXTERNAL{% endif %} TABLE {% if check_existence %}IF NOT EXISTS {% endif %}`{{ name }}`
(
{%- for fname, ftype in fields %}
    `{{ fname }}` {{ ftype }}{% if not loop.last %},{% endif %}
{%- endfor %}
)
{%- if partition_fields %}
PARTITIONED BY (
{%- for fname, ftype in partition_fields %}
    `{{ fname }}` {{ ftype }}{% if not loop.last %},{% endif %}
{%- endfor %}
)
{%- endif %}
{%- if storage_fmt %}
STORED AS {{ storage_fmt }}
{%- endif %}
