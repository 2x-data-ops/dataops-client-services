CREATE OR REPLACE TABLE `x-marketing.syniti.campaign_influenced_opportunity` AS


-- only select campaign, indicated by those who have both campaignid and parentid
-- the logic is we shouldnt remove campaign parent first since we want to join them later.
-- ones with parentid is actually a parent campaign
-- ones with both campaignid and parentid is actually a child
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
opp AS (
  SELECT DISTINCT
    opp.id AS _oppid,
    opp.name AS _opp_name,
    opp.createddate AS _opp_createddate,
    opp.stagename AS _opp_stage,
    opp.region__c AS _opp_region,
    opp.campaignid,
    opp.amount AS _opp_amount,
    -- opp.currencyisocode AS _opp_currency,
    opp.fiscal_period__c AS _opp_fiscalperiod
  FROM `x-marketing.syniti_salesforce.Opportunity` opp
  WHERE EXTRACT(YEAR FROM createddate) = 2023
),
combined_data AS (
  SELECT
  * EXCEPT (_parentid),
    CASE WHEN campaign_only._campaignid IS NOT NULL AND opp.campaignid IS NOT NULL THEN 'TRUE'
    ELSE 'FALSE'
    END AS _primary_source_campaign
  FROM campaign_only
  LEFT JOIN parent_campaign
  ON campaign_only._parentid = parent_campaign._parentid
  LEFT JOIN opp
  ON campaign_only._campaignid = opp.campaignid
),
member AS (
  SELECT DISTINCT campaignid, createddate AS _memberfirst_associateddate
  FROM x-marketing.syniti_salesforce.CampaignMember
)

SELECT
  combined_data.* EXCEPT(campaignid),
  member._memberfirst_associateddate
FROM combined_data
LEFT JOIN member
ON combined_data._campaignid = member.campaignid;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `x-marketing.syniti.marketing_created_influence_summarized`  AS


SELECT
    _campaignid,
    _campaign_name,
    _campaign_startdate,
    COUNT(CASE WHEN _primary_source_campaign = 'TRUE' THEN 1 END) AS marketing_created_opp_count,
    COUNT(CASE WHEN _primary_source_campaign = 'FALSE' THEN 1 END) AS influenced_opp_count,
    COUNT(*) AS prospecting_opp_count,
FROM 
    `x-marketing.syniti.campaign_influenced_opportunity`
GROUP BY 
    1, 2, 3;