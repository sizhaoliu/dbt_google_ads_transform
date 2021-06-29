with data_clean as ( 
  select  
    id
    ,date(CREATEDDATE) as date 
    ,email
    , date(X1_INQUIRY_DATE__C) as inquiry_date
    , date(X2_READY_FOR_REVIEW_DATE__C) as ready_for_review_date
    , date(X3_WORKING_DATE__C) as working_date
    , date(LEAD_QUALIFICATION_DATE__C) as qualifying_date
    --  ,extract(date from lattice_timestamp) as lattice_timestamp_date
    ,LEAD__C
    ,lower(CAMPAIGN_SOURCE__C) platform
    ,lower(CAMPAIGN_MEDIUM__C) medium
    ,lower(replace(CAMPAIGNID__C,'%2520','')) campaign
    ,lower(replace(replace(replace(replace(replace(CAMPAIGN_CONTENT__C,'%2520',''),' - ','_'),' ','_'),'-','_'),'%20','')) content
    ,lower(CAMPAIGN_TERM__C) as term
    ,lower(LEADSOURCE) lead_source
    ,lower(MOST_RECENT_LEAD_SOURCE_DETAIL__C) lead_source_detail
    ,lower(TITLE) as title
    ,lower(NAMED_ACCOUNT__C) as company_account
    ,lower(COUNTRY) as country
    ,lower(STATUS) as lead_status
    ,lower(ACQUISITION_CAMPAIGN__C) as acquisition_channel
    ,CONVERSION_URL__C
    ,ENTRY_URL__C
    ,CTVT_SYS_URL__C
    ,MQL_DATE_TIME_LAST__C AS mql_last_date
    -- add row number function to remove duplicate email addresses (there are only a handful of these but they will impact the joining of salesforce opportunities by duplicating the opptys data)
    ,row_number() over (partition by email order by CREATEDDATE desc) as row_num
   from {{ var('sfdc_lead_source') }} 
   where lower(CAMPAIGN_SOURCE__C) in ('google','bing') 
 ),

final as (
  select 
      id AS lead_id,
      case when  mql_last_date IS NOT NULL  
          then 1 else 0 end as became_mql
      ,case when   regexp_like(lead_status, '5a. Qualified.*','i')
          AND not regexp_like(lead_status, 'partner.*','i')
          AND not regexp_like(lead_status, 'Media/Analyst.*','i')
          AND not regexp_like(lead_status, 'Professor.*','i')
          AND not regexp_like(lead_status, 'Student.*','i')
          and NOT regexp_like(country, 'india.*','i')
          then 1 else 0 end as became_SAL
      ,DATE_PART(week , date ) as lead_created_week
      ,case when regexp_like(campaign,'(stitch)') then 'stitch' else 'talend' end as ACCOUNT
      ,*
  from data_clean
  where row_num = 1
)

select * from final