--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------ Opportunity Log Script --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
/* 
This script is used to normalize the opportunity data
CRM/Platform/Tools: Hubspot
Data type: Deals/Opportunity
Depedency Table: db_consolidated_engagements, report_overview_engagement_opportunity, report_overview_account_opportunity
Target table: db_opportunity_log
*/
TRUNCATE TABLE `x-marketing.sandler.db_opportunity_log`;

INSERT INTO `x-marketing.sandler.db_opportunity_log` (
  _opportunity_id,
  _account_id,
  _account_name,
  _opportunity_name,
  _current_stage,
  _probability,
  _createdate,
  _close_date,
  _amount,
  _acv,
  _domain,
  _type,
  _lost_reason,
  _last_stage_change_date,
  _days_current_stage,
  _previous_stage
)
WITH deal_pipelines AS (
  SELECT DISTINCT
    stages.value.*
  FROM `x-marketing.sandler_hubspot.deal_pipelines`,
    UNNEST (stages) AS stages
),
opps_created AS (
  SELECT
    CAST(deals.dealid AS STRING) AS _opportunityID,
    CAST(companies.companyid AS STRING) AS _accountID,
    companies.property_name.value AS _accountName,
    deals.property_dealname.value AS _opportunityName,
    stages.label AS _currentStage,
    stages.probability AS _currentStageProbability,
    deals.property_createdate.value AS _createTS,
    deals.property_closedate.value AS _closeTS,
    deals.property_amount.value AS _amount,
    deals.property_hs_acv.value AS _acv,
    companies.property_domain.value AS _domain,
    deals.property_dealtype.value AS _type,
    deals.property_closed_lost_reason.value AS _reason,
    deals.property_dealstage.timestamp AS _oppLastChangeinStage,
    DATE_DIFF(CURRENT_TIMESTAMP(), deals.property_dealstage.timestamp, DAY) AS _daysCurrentStage
  FROM `x-marketing.sandler_hubspot.deals` deals,
    UNNEST (associations.associatedcompanyids) AS deals_company
  LEFT JOIN `x-marketing.sandler_hubspot.companies` companies
    ON deals_company.value = companies.companyid
  JOIN deal_pipelines AS stages
    ON deals.property_dealstage.value = stages.stageid
  WHERE LOWER(deals.property_dealtype.value) NOT LIKE '%renewal%'
    AND LOWER(companies.property_name.value) NOT LIKE '%sandler%'
    AND deals.property_pipeline.value IS NOT NULL
    AND deals.isdeleted = FALSE
),
opp_stage_hist AS (
  SELECT DISTINCT
    _opportunityID,
    _stage AS _currentStage,
    LEAD(_stage) OVER (PARTITION BY _opportunityID ORDER BY _timestamp DESC) AS _previousStage,
    LEAD(_probability) OVER (PARTITION BY _opportunityID ORDER BY _timestamp DESC) AS _previousStageProbability,
  FROM `x-marketing.sandler..hubspot_opportunity_stage_history`
),
opps_history AS (
  SELECT
    main.*,
    side._previousStage,
  FROM opps_created AS main
  LEFT JOIN opp_stage_hist AS side
    ON main._opportunityID = side._opportunityID
    AND main._currentStage = side._currentStage
)
SELECT
  *
FROM opps_history;