with add_row_num as (
    select *
    ,row_number() over (partition by campaignid, adgroupid, DAY,clicks order by _sdc_sequence desc) as row_num 
    FROM {{ var('google_adgroup_perf_source') }})
,

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
        ,sum((cost/1000000)) as spend
        ,sum(clicks) as clicks
        ,sum(impressions) as impressions
    from add_row_num 
    where 
        row_num = 1 and 
            day >= '{{ var('start_date', '2018-01-01') }}'
    group by 1,2,3,4,5,6,7,8
)

select * from final