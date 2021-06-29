{% macro map_bing_ads_account_id_to_name() %}
    case
    {% set account_id_map = {
        '3279498': 'talend noram',
        '141239038': 'talend emea',
        '141239048': 'talend apac',
    } %}
    {% for account_id,account_name in account_id_map.items() %}
        when accountid = '{{account_id}}' then '{{account_name}}'
    {% endfor %}
    else 'other' end  
{% endmacro %}



