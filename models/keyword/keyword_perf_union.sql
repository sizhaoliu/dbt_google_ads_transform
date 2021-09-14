with keyword_perf_union_data as (
    select * from {{ ref('bing_ads_keyword_perf') }}
    union all 
    select * from {{ ref('google_ads_keyword_perf') }}
)
select * from keyword_perf_union_data
