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

--CREATE OR REPLACE TABLE `3x.db_opportunity_log` AS

TRUNCATE TABLE `3x.db_opportunity_log`;
INSERT INTO `3x.db_opportunity_log`
-- (
--     _opportunity_id, 
--     _account_id, 
--     _account_name,
--     _opportunity_name, 
--     _current_stage, 
--     _probability, 
--     _createdate, 
--     _close_date, 
--     _amount, 
--     _acv, 
--     _domain, 
--     _type, 
--     _leadsource,
--     _lost_reason, 
--     _last_stage_change_date, 
--     _days_current_stage, 
--     _total_one_time,
--     _previous_stage
-- )
WITH

  icp_account_domain AS (
    SELECT
      DISTINCT _sfdcaccountid,
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
      DISTINCT acc.id AS _account_id, cnt._domain
    FROM
      `x3x_salesforce.Account` acc
    JOIN
      (SELECT DISTINCT accountid, RIGHT(email, LENGTH(email) - STRPOS(email, '@') ) AS _domain FROM `x3x_salesforce.Contact` WHERE LENGTH(email) > 1) cnt
      ON acc.id = cnt.accountid
    
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
        account_domain._domain,
        opps.type AS _type,
        opps.leadsource AS _leadsource,
        opps.lost_reason_detail__c AS _lost_reason,
        opps.laststagechangedate AS _last_stage_change_date,
        CAST(NULL AS INT) AS _days_current_stage,
        opps.total_one_time__c AS _total_one_time
      FROM 
        `x-marketing.x3x_salesforce.Opportunity` opps
      LEFT JOIN
        `x-marketing.x3x_salesforce.Account` companies
      ON 
        opps.accountid = companies.id
      LEFT JOIN
        account_domain 
      ON
        opps.accountid = account_domain._account_id
      WHERE 
          LOWER(companies.name) NOT LIKE '%3x%'
      AND 
          opps.isdeleted = false

  ),

  opps_history AS (

      SELECT
          main.*,
          side._previous_stage,
      FROM
          opps_created AS main
      LEFT JOIN (
          
          SELECT DISTINCT 
              opportunityid AS _opportunity_id,
              oldvalue__st AS _previous_stage,
              newvalue__st AS _current_stage
          FROM
              `x-marketing.x3x_salesforce.OpportunityFieldHistory`

      ) AS side
      USING(_opportunity_id, _current_stage)

  ),max_amount AS (
      SELECT DISTINCT 
              opportunityid AS _opportunity_id,
             MAX(newvalue) AS _max_amount
          FROM
              `x-marketing.x3x_salesforce.OpportunityFieldHistory`
              WHERE field = 'Amount'
              GROUP BY 1

  )
SELECT
  *
FROM 
  opps_history
LEFT JOIN max_amount  USING(_opportunity_id)
;


