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

    

TRUNCATE TABLE `toolsgroup.db_opportunity_log`;
INSERT INTO `toolsgroup.db_opportunity_log`
-- CREATE OR REPLACE TABLE toolsgroup.db_opportunity_log AS
WITH
  opps_created AS (

      SELECT 
        opps.id AS _opportunity_id, 
        opps.name AS _opportunity_name, 
        owner.name AS _opps_owner_name, 
        customer_project_lead_email__c AS _contact_email,
        companies.id AS _account_id, 
        companies.name AS _account_name,
        companies.industry AS _industry,
        -- accountowner__c AS _account_owner,
        opps.createddate AS _created_date,
        opps.closedate AS _close_date,
        opps.stagename AS _current_stage,
        opps.amount AS _annual_amount, 
        opps.currencyisocode AS _currency,
        (opps.amount / conversionrate) AS _amout_converted_usd,
        opps.probability AS _probability,
        IF(opps.stagename NOT LIKE '%Closed%',DATE_DIFF(CURRENT_DATE(), 
          DATE(opps.createddate), DAY), 
            IF(opps.stagename LIKE '%Closed%', DATE_DIFF(DATE(opps.closedate), DATE(opps.createddate), DAY), 
              NULL)
        ) AS _age,
        -- '' AS _domain,
        opps.leadsource AS _leadsource,
        opps.win_loss_reasons__c AS _reason,
        -- opps.laststagechangedate AS _opp_last_stage_change,
        -- campaign.name AS _primary_campaign_source,
        opps.campaignid AS _campaign_id,
        opps.type AS _type,
        -- record.name AS _opportunity_record_type
      FROM 
        `toolsgroup_salesforce.Opportunity` opps
      LEFT JOIN
        `toolsgroup_salesforce.CurrencyType` currency
        ON 
          currency.isocode = opps.currencyisocode
      LEFT JOIN
        `toolsgroup_salesforce.Account` companies
        ON 
          opps.accountid = companies.id
      LEFT JOIN 
        `toolsgroup_salesforce.User` owner
        ON
          opps.ownerid = owner.id
      -- LEFT JOIN
      --   `toolsgroup_salesforce.Campaign` campaign
      --   ON
      --     campaign.id = opps.campaignid
      WHERE 
          opps.isdeleted = false  

  ),
  opps_history AS (

      SELECT
        *
      FROM
        opps_created AS main
      LEFT JOIN (
          
        SELECT DISTINCT 
          opportunityid AS _opportunity_id,
          oldvalue AS _old_stage,
          newvalue AS _new_stage,
          createddate AS _stage_change_date
        FROM
          `toolsgroup_salesforce.OpportunityFieldHistory`
        WHERE
          field = 'StageName'

      ) AS side
      USING(_opportunity_id)

  )
SELECT
  *,
  ROW_NUMBER() OVER(PARTITION BY _opportunity_id ORDER BY _stage_change_date DESC) AS _stage_change_order
FROM 
  opps_history
;