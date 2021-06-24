with add_row_num as (
    select *
    ,row_number() over (partition by campaignid, adgroupid, DAY,clicks order by _sdc_sequence desc) as row_num 
    FROM INCUBATION.DBT_GOOGLE_ADS.ADGROUP_PERFORMANCE_REPORT)
,

final as (
    select 
        to_date(DAY) as date
        ,date_part('MONTH',day)  as month
        ,case when account = 'Talend NORAM' then 'talend noram' 
            when account = 'Talend UK RoEMEA APAC' then 'talend emea' 
            when account = 'Talend APAC' then 'talend apac'   
            when account = 'Stitch' then 'stitch'  
            when account = 'Talend Japan' then 'talend japan'  
            else 'na' end as account
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
            day >='2017-12-31'
    group by 1,2,3,4,5,6,7,8
)

select * from final