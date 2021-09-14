
select * from {{ ref('bing_ads_keyword_perf')}
union all 
select * from {{ ref('google_ads_keyword_perf')}
