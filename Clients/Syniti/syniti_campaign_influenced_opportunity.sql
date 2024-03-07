CREATE OR REPLACE TABLE `x-marketing.syniti.campaign_influenced_opportunity` AS


-- only select campaign, indicated by those who have both campaignid and parentid
-- the logic is we shouldnt remove campaign parent first since we want to join them later.
-- ones with parentid is actually a parent campaign
-- ones with both campaignid and parentid is actually a child

WITH _alldata AS (
  WITH campaign_only AS (
      SELECT
        DISTINCT
        campaign.id AS _campaignid,
        campaign.parentid AS _parentid,
        campaign.name AS _campaign_name,
        campaign.startdate AS _campaign_startdate,
        campaign.status AS _campaign_status,
        campaign.isactive AS _campaign_isactive,
      FROM `x-marketing.syniti_salesforce.Campaign` campaign
      -- filter to retrieve campaign only row
      WHERE parentid IS NOT NULL
      AND isactive = true
      AND status IN ('In Progress', 'Planned')
  ),
  --only get the parent campaign stuff, parentid and also its name
  parent_campaign AS (
    SELECT
      DISTINCT
      parent.parentid AS _parentid,
      parent_campaign.name AS _parent_campaignname
    FROM `x-marketing.syniti_salesforce.Campaign` parent
    JOIN `x-marketing.syniti_salesforce.Campaign` parent_campaign
    ON parent.parentid = parent_campaign.id
    WHERE parent.parentid IS NOT NULL
  ),
  --opportunity stuff
  --amount will be converted into usd
  opp AS (
    WITH closedConversionRate AS (
      SELECT DISTINCT
        opp.id,
        isocode,
        opp.closedate,
        rate.conversionrate,
        opp.amount / rate.conversionrate AS converted
      FROM `x-marketing.syniti_salesforce.DatedConversionRate` rate
      LEFT JOIN `x-marketing.syniti_salesforce.Opportunity` opp
      ON rate.isoCode = opp.currencyisocode
        AND opp.closedate >= rate.startDate
        AND opp.closedate < rate.nextStartDate
      WHERE 
        opp.isclosed = true
      -- ORDER BY rate.startDate DESC
    ),
    openConversionRate AS (
      SELECT 
        * EXCEPT(rownum)
      FROM (
        SELECT DISTINCT
          opp.id,
          isocode,
          rate.conversionrate,
          rate.lastmodifieddate,
          opp.closedate,
          -- opp.total_price__c,
          ROW_NUMBER() OVER(PARTITION BY isocode ORDER BY rate.lastmodifieddate DESC) AS rownum
        FROM `x-marketing.syniti_salesforce.DatedConversionRate` rate
        LEFT JOIN `x-marketing.syniti_salesforce.Opportunity` opp
        ON opp.currencyisocode = rate.isocode
        WHERE opp.isclosed = false
        AND opp.currencyisocode != 'USD'
      )
      WHERE rownum = 1
      ORDER BY isocode 
    ),
    -- main opportunity data, only get 2023 and above
    opps_main AS (
    SELECT DISTINCT
      opp.id AS _oppid,
      opp.name AS _opp_name,
      opp.createddate AS _opp_createddate,
      opp.stagename AS _opp_stage,
      opp.region__c AS _opp_region,
      opp.campaignid,
      opp.amount AS _opp_amount,
      opp.currencyisocode AS currencyisocode,
      opp.fiscal_period__c AS _opp_fiscalperiod,
      opp.isclosed
    FROM `x-marketing.syniti_salesforce.Opportunity` opp
    WHERE EXTRACT(YEAR FROM createddate) = 2023
    )
    -- logic where currency conversion is happening
    -- 
      SELECT
      *
    FROM (
      SELECT DISTINCT
        opps_main.* EXCEPT(_opp_amount),
        -- Opportunity.opportunityID,
        -- Opportunity.createddate,
        -- Opportunity.isclosed,
        -- Opportunity.currencyisocode,
        _opp_amount AS original_amount,
        CASE 
          WHEN isclosed = true AND currencyisocode != 'USD'
          THEN (
            closedConversionRate.conversionRate
          )
          WHEN isclosed = false AND currencyisocode != 'USD'
          THEN (
            openConversionRate.conversionRate 
          )
        END AS conversionRate,
        CASE 
          WHEN isclosed = true AND currencyisocode != 'USD'
          THEN (

            closedConversionRate.converted
          )
          WHEN isclosed = false AND currencyisocode != 'USD'
          THEN (
            (_opp_amount / openConversionRate.conversionrate) 
          )
          ELSE _opp_amount
        END AS _opp_amount,
        -- sfdc_activity_casesafeid__c,
        -- application_specialist__c,
        -- Event_Status__c,
        -- Web_Location__c
      FROM opps_main
      LEFT JOIN closedConversionRate ON closedConversionRate.id = opps_main._oppid
      LEFT JOIN openConversionRate ON openConversionRate.isocode = opps_main.currencyisocode
    )
    WHERE EXTRACT(YEAR FROM _opp_createddate) >= 2023


  ),
  combined_data AS (
    SELECT
    * EXCEPT (_parentid),
      CASE WHEN campaign_only._campaignid IS NOT NULL AND opp._oppid IS NOT NULL THEN 'TRUE'
      ELSE 'FALSE'
      END AS _primary_source_campaign
    FROM campaign_only
    LEFT JOIN parent_campaign
    ON campaign_only._parentid = parent_campaign._parentid
    LEFT JOIN opp
    ON campaign_only._campaignid = opp.campaignid
  ),
  -- getting the created date as memberfirst associated date in member table
  member AS (
    SELECT DISTINCT campaignid, createddate AS _memberfirst_associateddate
    FROM x-marketing.syniti_salesforce.CampaignMember
  )
  -- join all data with member table
  SELECT
    combined_data.* EXCEPT(campaignid, conversionRate),
    member._memberfirst_associateddate
  FROM combined_data
  LEFT JOIN member
  ON combined_data._campaignid = member.campaignid
  ),
  avg_amount_opp AS (
    SELECT *,
        COUNT(_oppid) OVER (
            PARTITION BY _oppid
        ) AS opp_count
    FROM _alldata
    )
SELECT *, 
    CASE WHEN opp_count > 0 THEN _opp_amount/opp_count ELSE 0 END AS _avg_amount
FROM avg_amount_opp



--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

-- CREATE OR REPLACE TABLE `x-marketing.syniti.marketing_created_influence_summarized`  AS

-- SELECT
--     _campaignid,
--     _campaign_name,
--     _campaign_startdate,
--     COUNT(CASE WHEN _primary_source_campaign = 'TRUE' THEN 1 END) AS marketing_created_opp_count,
--     COUNT(CASE WHEN _primary_source_campaign = 'FALSE' THEN 1 END) AS influenced_opp_count,
--     COUNT(*) AS prospecting_opp_count,
-- FROM 
--     `x-marketing.syniti.campaign_influenced_opportunity`
-- GROUP BY 
    -- 1, 2, 3;