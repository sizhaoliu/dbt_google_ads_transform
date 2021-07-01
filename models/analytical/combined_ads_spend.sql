WITH COMBINED_SPEND AS 
(select * from  {{ ref('stg_bing_ads_spend') }}
union all
select * from  {{ ref('stg_google_ads_spend') }}),

spend AS (
    SELECT 
        DATE 
        ,ACCOUNT
        ,PLATFORM 
        ,CAMPAIGN 
        ,AD_GROUP 
        ,'AdsSpend' as region
        ,SUM(IMPRESSIONS) IMPRESSIONS
        ,SUM(CLICKS) CLICKS
        ,SUM(SPEND) SPEND
        ,0 as LEADS
        ,0 as BECAME_MQL
        ,0 as BECAME_SAL
        ,0 as BECAME_OPPORTUNITY
        ,0 as BECAME_CLOSED_WON
        ,0 as PIPELINE_GENERATED
        ,0 as PIPELINE_WON
    FROM COMBINED_SPEND
    group by 1,2,3,4,5)

select * from spend