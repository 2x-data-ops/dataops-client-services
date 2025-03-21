--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------ Opportunity Log Script --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------


/* 
  This script is used to normalize the opportunity data
  CRM/Platform/Tools: Salesforce
  Data type: Opportunity
  Depedency Table: db_consolidated_engagements, report_overview_engagement_opportunity, report_overview_account_opportunity
  Target table: db_opportunity_log
*/

TRUNCATE TABLE `3x.db_opportunity_log`;
INSERT INTO `3x.db_opportunity_log`
---CREATE OR REPLACE TABLE `3x.db_opportunity_log` AS
WITH icp_account_domain AS (
  SELECT
  DISTINCT 
  _sfdcaccountid,
  _domain
  FROM
  `3x.db_icp_database_log`
  WHERE
  LENGTH(_sfdcaccountid) > 1
  ORDER BY 
  _domain
),
account_domain AS (
  SELECT 
  DISTINCT 
  website, 
  LOWER(REGEXP_EXTRACT(website, r'^(?:https?://)?(?:www\.)?([^/]+)')) AS _domain,
  id as accountid,
  domain__c,  
  FROM `x-marketing.x3x_salesforce.Account` 
),
opps_created AS (
  SELECT 
  opps.id AS _opportunity_id, 
  companies.id AS _account_id, 
  companies.name AS _account_name,
  opps.name AS _opportunity_name, 
  opps.stagename AS _current_stage,
  opps.probability AS _probability,
  opps.createddate AS _createdate,
  opps.closedate AS _close_date,
  opps.amount AS _amount, 
  CAST(NULL AS FLOAT64) AS _acv,
  --account_domain._domain,
  opps.type AS _type,
  opps.leadsource AS _leadsource,
  opps.lost_reason_detail__c AS _lost_reason,
  opps.laststagechangedate AS _last_stage_change_date,
  CAST(NULL AS INT) AS _days_current_stage,
  opps.total_one_time__c AS _total_one_time,
  side.stagename AS current_stage,
  LAG(side.stagename) OVER(
    PARTITION BY opps.id ORDER BY side.createddate
    ) AS _previousStage,
  LAG(side.createddate) OVER(
    PARTITION BY opportunityid ORDER BY side.createddate)
  AS previous_change_status_change_date,
  prevamount,
  side.amount AS _current_amount,
  ROW_NUMBER() OVER(PARTITION BY opportunityid ORDER BY side.createddate ASC) AS _order,
  contactid
  FROM 
  `x-marketing.x3x_salesforce.Opportunity` opps
  LEFT JOIN
  `x-marketing.x3x_salesforce.Account` companies
  ON opps.accountid = companies.id
  JOIN  `x-marketing.x3x_salesforce.OpportunityHistory`side
  ON opps.id = side.opportunityid
  --LEFT JOIN
  --account_domain 
  --ON
  -- opps.accountid = account_domain._account_id
  WHERE 
  LOWER(companies.name) NOT LIKE '%3x%'
  AND 
  opps.isdeleted = false 
  --AND opps.id = '0064P000010M73AQAS'
        ORDER BY side.createddate DESC
),max_amount AS (
  SELECT DISTINCT 
  opportunityid AS _opportunity_id,
  MAX(newvalue) AS _max_amount
  FROM
  `x-marketing.x3x_salesforce.OpportunityFieldHistory`
  WHERE field = 'Amount'
  GROUP BY 1
)--, opps_created_all AS (
  SELECT 
  opps_created.* ,
  account_domain._domain,
  CASE WHEN _previousStage IS NULL THEN _current_stage ELSE _previousStage END AS _previousStaged,
  FROM opps_created 
  LEFT JOIN account_domain ON opps_created._account_id = account_domain.accountid
  LEFT JOIN max_amount  USING(_opportunity_id);