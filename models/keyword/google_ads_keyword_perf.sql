select keyword
  , 'Google Ads' as channel
  , sum(cost/1000000) as spend
  , sum(clicks) as clicks
  , sum(impressions) as impressions
from  {{ var('database_name') }}.{{ var('source_schema_google_ads') }}."KEYWORDS_PERFORMANCE_REPORT"
group by 1
