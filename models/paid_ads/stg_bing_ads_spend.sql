with add_row_num as (select *
,row_number() over (partition by campaignid, adgroupid, timeperiod order by _sdc_sequence desc) as row_num 
FROM INCUBATION.DBT_BING_ADS.AD_GROUP_PERFORMANCE_REPORT) 
,

final as ( select 
    to_date(timeperiod) as date
    ,date_part('MONTH',timeperiod)  as month
    --extract(date from timeperiod) as date
    --,cast(datetime_trunc(cast(timeperiod as datetime), month) as date) as month
    ,case when accountid = 3279498 then 'talend noram'
        when accountid = 141239038 then 'talend emea'
        when accountid = 141239048 then 'talend apac'
        else 'other' end as account
    ,'bing' as platform 
    ,lower(campaignname) as campaign
    ,campaignid as campaign_id
    ,lower(adgroupname) as ad_group
    ,adgroupid as ad_group_id
    ,sum(spend) as spend
    ,sum(clicks) as clicks
    ,sum(impressions) as impressions
from add_row_num 
where row_num = 1
and timeperiod >='2019-01-01'
group by 1,2,3,4,5,6,7,8
)

select * from final