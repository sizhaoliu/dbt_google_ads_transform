select keyword
  , 'Bing Ads' as channel
  , sum(spend) as spend
  , sum(clicks) as clicks
  , sum(impressions) as impressions
from  {{ var('database_name') }}.{{ var('source_schema_bing_ads') }}."KEYWORD_PERFORMANCE_REPORT"
group by 1
