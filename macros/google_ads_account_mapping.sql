{% macro map_google_ads_account_name() %}
    case
    {% set account_name_map = {
        'Talend NORAM': 'talend noram',
        'Talend UK RoEMEA APAC': 'talend emea',
        'Talend APAC': 'talend apac',
        'Stitch': 'stitch',
        'Talend Japan': 'talend japan',
    } %}
    {% for original_name, target_name in account_name_map.items() %}
        when account = '{{original_name}}' then '{{target_name}}'
    {% endfor %}
    else 'na' end  
{% endmacro %}