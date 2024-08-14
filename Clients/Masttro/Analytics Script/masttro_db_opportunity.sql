begin -- db_opportunity_aggregate
  --CREATE OR REPLACE TABLE `x-marketing.masttro.db_opportunity_aggregate` AS

  TRUNCATE TABLE `x-marketing.masttro.db_opportunity_aggregate`;

  INSERT INTO `x-marketing.masttro.db_opportunity_aggregate` (
    _campaignID,
    _campaignName,
    _date,	
    _dealID,
    _amount,	
    _amount_usd,	
    _total_cost,	
    _count_opp,
    _cost
  )

  WITH opps_created AS (
    SELECT
    CAST(deals.dealid AS STRING) AS _dealID,
    deals.property_dealstage.timestamp AS _dealStageTimestamp,

    CAST(deals.property_dealstage.timestamp AS DATE)  AS _date,
    property_campaign_utm.value AS _campaignName,
    deals.property_amount.value AS _amount,
    deals.properties.amount_in_home_currency.value AS _amount_usd,

    FROM `x-marketing.masttro_hubspot.deals` deals

    JOIN (
      SELECT DISTINCT
      stages.value.*,
      label AS _pipeline

      FROM `x-marketing.masttro_hubspot.deal_pipelines` , UNNEST(stages) AS stages
    )stages
    ON deals.property_dealstage.value = stages.stageid

    WHERE deals.property_pipeline.value IS NOT NULL
    AND deals.isdeleted = false

    QUALIFY ROW_NUMBER() OVER(PARTITION BY _dealStageTimestamp, _dealID) = 1
    ORDER BY _dealStageTimestamp DESC
  ),

  campaign_perf AS (
    SELECT
    campaign_id AS _campaignID,
    --campaign_name AS _campaignName,
    campaign.name AS _campaignName, -- use campaign name from google campaign table instead of from perf
    date AS _campaignDate,
    CAST(date AS DATE) AS _date,

    impressions,
    clicks,
    cost_micros/1000000 AS cost,
    
    FROM `x-marketing.masttro_google_ads.campaign_performance_report` report

    LEFT JOIN `x-marketing.masttro_google_ads.campaigns` campaign
    ON report.campaign_id = campaign.id

    --WHERE campaign_name = '2309-Masttro-Branded-Keywords'
    QUALIFY RANK() OVER(PARTITION BY report.date, campaign.name, report.campaign_id ORDER BY report._sdc_received_at DESC) = 1
  ), 

  count_opps AS (
    SELECT 
    _date,
    _campaignName,
    _dealID,

    CASE 
      WHEN _campaignName IS NOT NULL OR _campaignName != '' 
        THEN COUNT(DISTINCT _dealID)
      ELSE 0 END 
    AS _count_opp,

    SUM (_amount) AS _amount,
    SUM (_amount_usd) AS _amount_usd

    FROM opps_created
    GROUP BY 1,2,3
  ), 

  cost_aggregate AS (
    SELECT
    _campaignID,
    _campaignName,
    _date,

    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(cost) AS _total_cost
    
    FROM campaign_perf
    ---WHERE _campaignName = '2309-Masttro-Branded-Keywords'
    GROUP BY 1, 2, 3
  ),

  dates AS (
    SELECT AS STRUCT
      _date
    FROM
      --Format: YYYY-MM-DD
      UNNEST(GENERATE_DATE_ARRAY(CURRENT_DATE(), '2017-10-20', INTERVAL -1 DAY)) AS _date 
    ORDER BY 
      1 DESC
  ),

  --to generate all possible date combinations from google and date list
  campaign_name AS (
    SELECT DISTINCT
    campaign_id AS _campaignID,
    --campaign_name AS _campaignName,
    campaign.name AS _campaignName,
    dates._date

    
    FROM `x-marketing.masttro_google_ads.campaign_performance_report` report

    LEFT JOIN `x-marketing.masttro_google_ads.campaigns` campaign
    ON report.campaign_id = campaign.id

    CROSS JOIN dates
  ),

  combine AS (
    SELECT 
    campaign_name._campaignID,
    campaign_name._campaignName,
    campaign_name._date,
    opps._dealID,
    COALESCE(_amount,0) AS _amount,
    COALESCE(_amount_usd,0) AS _amount_usd,
    
    -- campaign.clicks,
    -- campaign.impressions,
    campaign._total_cost,
    COALESCE(opps._count_opp,0) AS _count_opp

    FROM campaign_name

    LEFT JOIN cost_aggregate campaign 
    ON CONCAT(campaign_name._date,campaign_name._campaignName) = CONCAT(campaign._date,campaign._campaignName)  
      
    LEFT JOIN count_opps opps ON CONCAT(campaign_name._date,campaign_name._campaignName) = CONCAT(opps._date,opps._campaignName)  
    --WHERE campaign_name._campaignName = '2309-Masttro-Branded-Keywords'
  )

  SELECT *, 
  CASE
    WHEN _count_opp > 0 
      THEN SUM(_total_cost) OVER (PARTITION BY _date,_campaignName) / SUM(_count_opp) OVER (PARTITION BY _date,_campaignName)
      ELSE 0
  END
  AS _cost

  FROM combine;
end;


begin -- db_opportunity
  --CREATE OR REPLACE TABLE `x-marketing.masttro.db_opportunity` AS

  TRUNCATE TABLE `x-marketing.masttro.db_opportunity`;

  INSERT INTO `x-marketing.masttro.db_opportunity` (
    _campaignID,
    _campaignName,	
    _date,	
    _dealID,	
    _amount,	
    _amount_usd,	
    _total_cost,	
    _count_opp,
    _cost,	
    _dealName,	
    _campaign,	
    _keywords,	
    _dealPipeline,	
    _dealStage,	
    _currency,	
    _dealCreatedDate,	
    _dealClosedDate,	
    _dealOriginalSource,	
    _dealType,	
    _companyName,	
    _companyIndustry,	
    _prospectID,
    _name,	
    _lifecycleStage	
  )

  WITH opp AS (
    WITH contacts AS (
      SELECT
      contacts.vid AS _prospectID,
      -- contacts.properties.email.value AS _email,
      CONCAT(contacts.properties.firstname.value,' ', contacts.properties.lastname.value) AS _name,
      CASE
        WHEN contacts.property_lifecyclestage.value = 'marketingqualifiedlead' THEN 'Marketing Qualified Lead' 
        WHEN contacts.property_lifecyclestage.value = 'salesqualifiedlead' THEN 'Sales Qualified Lead'
        WHEN contacts.property_lifecyclestage.value = '55758351' THEN 'Sales Accepted Lead'
        WHEN contacts.property_lifecyclestage.value = '161283257' THEN 'Onboarding'
        WHEN contacts.property_lifecyclestage.value = '161201966' THEN 'Client At Risk'
        WHEN contacts.property_lifecyclestage.value = '172403121' THEN 'Churn'
        WHEN contacts.property_lifecyclestage.value = '' THEN 'NULL'
        ELSE
        INITCAP(CAST(contacts.properties.lifecyclestage.value AS STRING))
      END
      AS _lifecycleStage,
      -- contacts.property_campaign_utm.value AS _campaign_utm 

      FROM `x-marketing.masttro_hubspot.contacts` contacts

      QUALIFY ROW_NUMBER() OVER( PARTITION BY property_email.value, CONCAT(properties.firstname.value,' ', properties.lastname.value) ORDER BY vid DESC) = 1
    ),

    opps_created AS (
      SELECT
      CAST(deals.dealid AS STRING) AS _dealID,
      deals.property_dealname.value AS _dealName,
      --INITCAP(deals.property_pipedrive_owner.value) AS _dealOwner,
      deals.properties.hs_analytics_source_data_1.value AS _campaign,
      deals.properties.hs_analytics_source_data_2.value AS _keywords,

      stages._pipeline AS _dealPipeline,
      stages.label AS _dealStage,
      --stages.probability AS _dealStageProbability,
      
      deals.property_dealstage.timestamp AS _dealStageTimestamp,
      CAST(deals.property_dealstage.timestamp AS DATE)  AS _date,
      /* deals.property_amount.value AS _amount, */
      /* deals.properties.amount_in_home_currency.value AS _amount_usd, */
      deals.properties.deal_currency_code.value AS _currency,
      --deals.property_hs_acv.value AS _annualContractValue,
      deals.property_createdate.value AS _dealCreatedDate,
      deals.property_closedate.value AS _dealClosedDate,
      
      --deals.property_closed_lost_reason.value AS _dealClosedLostReason,
      INITCAP(REPLACE(deals.properties.hs_analytics_source.value, "_"," ")) AS _dealOriginalSource,
      
      CASE
        WHEN deals.property_dealtype.value = 'existingbusiness' THEN 'Existing Business'
        WHEN deals.property_dealtype.value = 'newbusiness' THEN 'New Business'
        ELSE deals.property_dealtype.value
      END
      AS _dealType,

      companies.property_name.value AS _companyName,
      INITCAP(REPLACE(companies.properties.industry.value, "_"," ")) AS _companyIndustry,

      deals.property_campaign_utm.value AS _campaignName,
      deals.associations.associatedvids[SAFE_OFFSET(0)].value AS _prospectID

      -- CAST(companies.companyid AS STRING) AS _companyID,
      -- REGEXP_REPLACE(
      --   REGEXP_REPLACE(LOWER(companies.property_domain.value), r'[https|http]+:\/\/|www[\d]*.',''), 
      --   r'\/\S*',
      --   ''
      --   ) 
      -- AS _companyDomain,
      -- INITCAP(companies.properties.type.value) AS _companyType,
      
      FROM `x-marketing.masttro_hubspot.deals` deals

      JOIN (
        SELECT DISTINCT
        stages.value.*,
        label AS _pipeline

        FROM `x-marketing.masttro_hubspot.deal_pipelines` , UNNEST(stages) AS stages
      )stages
      ON deals.property_dealstage.value = stages.stageid

      LEFT JOIN `x-marketing.masttro_hubspot.companies` companies
      ON deals.associations.associatedcompanyids[SAFE_OFFSET(0)].value = companies.companyid

      WHERE deals.property_pipeline.value IS NOT NULL
      AND deals.isdeleted = false

      QUALIFY ROW_NUMBER() OVER(PARTITION BY _dealStageTimestamp, _dealID) = 1
      ORDER BY _dealStageTimestamp DESC
    ),

    combine_all AS (
      SELECT
      opps_created.* EXCEPT(_dealStageTimestamp, _prospectID),
      contacts.*

      FROM opps_created

      LEFT JOIN contacts
      ON opps_created._prospectID = contacts._prospectID
    )
  SELECT * FROM combine_all
  )

  SELECT
  agg.*,
  opp.* EXCEPT(_date,_campaignName,_dealID),

  FROM `x-marketing.masttro.db_opportunity_aggregate` agg

  LEFT JOIN  opp
  ON CONCAT(agg._date,agg._campaignName,agg._dealID) = CONCAT(opp._date,opp._campaignName,opp._dealID)

  ;
end;