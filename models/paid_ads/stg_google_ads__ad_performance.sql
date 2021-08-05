with add_row_num as (
    select *
    ,row_number() over (partition by campaignid, adgroupid, adid, DAY,clicks order by _sdc_sequence desc) as row_num 
    FROM {{ var('database_name') }}.{{ var('source_schema_ads') }}."AD_PERFORMANCE_REPORT" 
),

final as (
    select 
        to_date(DAY) as date
        ,date_part('MONTH',day)  as month
        ,{{ map_google_ads_account_name() }} as account
        ,'google' as platform 
        ,lower(campaign) as campaign
        ,campaignid as campaign_id
        ,lower(adgroup) as ad_group
        ,adgroupid as ad_group_id
        ,lower(ad) as ad
        ,adid as ad_id
        ,adstate as ad_state
        ,finalurl as final_url
        ,labels as labels
        ,sum((cost/1000000)) as spend
        ,sum(clicks) as clicks
        ,sum(impressions) as impressions
    from add_row_num 
    where 
        row_num = 1 and 
            day >= '{{ var('start_date', '2018-01-01') }}'
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)

select * from final