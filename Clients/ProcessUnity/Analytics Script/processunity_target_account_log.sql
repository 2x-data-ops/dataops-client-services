----------------------------------------------------------
------------------- db_target_accounts -------------------
----------------------------------------------------------

-- CREATE OR REPLACE TABLE `x-marketing.processunity.db_target_accounts` AS 
TRUNCATE TABLE `x-marketing.processunity.db_target_accounts`;
INSERT INTO `x-marketing.processunity.db_target_accounts`(
  _account_owner,
  _accountID,
  _account_name,
  _account_domain,
  _type,
  _ABX_tier,
  _ABX_opt_in,
  _account_tier
)
SELECT DISTINCT
  user.name AS _account_owner,
  account.id AS _accountID,
  account.name AS _account_name,
  account.domain__c AS _account_domain,
  account.type AS _type,
  account.abx_tier__c AS _ABX_tier,
  account.abx_opt_in__c AS _ABX_opt_in,
  account.tier__c AS _account_tier
FROM `x-marketing.processunity_salesforce.Account` account
LEFT JOIN `x-marketing.processunity_salesforce.User` user
  ON user.id = account.ownerid
WHERE abx_opt_in__c IS TRUE
  AND isdeleted IS FALSE;

----------------------------------------------------------
----------------- db_target_opportunity ------------------
----------------------------------------------------------

-- CREATE OR REPLACE TABLE `x-marketing.processunity.db_target_opportunity` AS 
TRUNCATE TABLE `x-marketing.processunity.db_target_opportunity`;
INSERT INTO `x-marketing.processunity.db_target_opportunity`(
  _accountID,
  _opportunityID,
  _opportunity_name,
  _stage_name,
  _opportunity_created_date,
  _opportunity_close_date,
  _lead_source,
  _amount,
  _age_lead_opportunity,
  _opportunity_type,
  _currency,
  _is_closed,
  _account_name,
  _ABX_tier,
  _conversionRate,
  _net_new_ARR,
  _subscription,
  _total_opportunity_amount,
  _services
)
WITH closedConversionRate AS (
  SELECT DISTINCT
    opp.id,
    isocode,
    opp.closedate,
    rate.conversionrate,
    -- opp.amount / rate.conversionrate AS total_price_USD
    opp.net_new_arr__c / rate.conversionrate AS _net_new_ARR,
    opp.subscription__c / rate.conversionrate AS _subscription,
    opp.total_opportunity_amount__c / rate.conversionrate AS _total_opportunity_amount,
    opp.services__c / rate.conversionrate AS _services
  FROM `x-marketing.processunity_salesforce.DatedConversionRate` rate
  LEFT JOIN `x-marketing.processunity_salesforce.Opportunity` opp
    ON rate.isoCode = opp.currencyisocode
    AND opp.closedate >= rate.createddate
    AND opp.closedate < rate.nextStartDate
  WHERE opp.isclosed = true
),
openConversionRate AS (
  SELECT DISTINCT
    opp.id,
    isocode,
    rate.conversionrate,
    rate.lastmodifieddate,
    opp.closedate
  FROM `x-marketing.processunity_salesforce.DatedConversionRate` rate
  LEFT JOIN `x-marketing.processunity_salesforce.Opportunity` opp
    ON opp.currencyisocode = rate.isocode
  WHERE opp.isclosed = false
    AND opp.currencyisocode != 'USD'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY isocode ORDER BY rate.lastmodifieddate DESC) = 1
),
Opportunity AS (
  SELECT
    opp.accountid AS _accountID,
    opp.id AS _opportunityID,
    opp.name AS _opportunity_name,
    opp.stagename AS _stage_name,
    opp.createddate AS _opportunity_created_date,
    opp.closedate AS _opportunity_close_date,
    opp.leadsource AS _lead_source,
    opp.amount AS _amount,
    opp.age_lead_opp__c AS _age_lead_opportunity,
    opp.net_new_arr2__c AS _net_new_ARR,
    opp.type AS _opportunity_type,
    opp.subscription__c AS _subscription,
    opp.total_opportunity_amount__c AS _total_opportunity_amount,
    opp.services__c AS _services,
    opp.currencyisocode AS _currency,
    opp.isclosed AS _is_closed,
    targets._account_name,
    targets._ABX_tier
  FROM `x-marketing.processunity_salesforce.Opportunity` opp
  JOIN `x-marketing.processunity.db_target_accounts` targets
    ON targets._accountID = opp.accountid
  WHERE opp.isdeleted IS FALSE
    AND opp.type = 'New Business'
    AND opp.leadsource IN (
      'ABM', 'Advertisement','Database', 'Event', 'Inbound Call', 'PPC', 'Public Relations',
      'Purchased List', 'Social Media', 'Sponsorship', 'Telemarketing', 'Trade Show', 'Web'
    )
)
SELECT DISTINCT
  Opportunity._accountID,
  Opportunity._opportunityID,
  Opportunity._opportunity_name,
  Opportunity._stage_name,
  Opportunity._opportunity_created_date,
  Opportunity._opportunity_close_date,
  Opportunity._lead_source,
  Opportunity._amount,
  Opportunity._age_lead_opportunity,
  Opportunity._opportunity_type,
  Opportunity._currency,
  Opportunity._is_closed,
  Opportunity._account_name,
  Opportunity._ABX_tier,
  CASE 
    WHEN _is_closed = true AND _currency != 'USD'
    THEN (
      closedConversionRate.conversionRate
    )
    WHEN _is_closed = false AND _currency != 'USD'
    THEN (
      openConversionRate.conversionRate 
    )
  END AS _conversionRate,
  ROUND(CASE 
    WHEN _is_closed = true AND _currency != 'USD'
    THEN (
      closedConversionRate._net_new_ARR
    )
    WHEN _is_closed = false AND _currency != 'USD'
    THEN (
      (Opportunity._net_new_ARR / openConversionRate.conversionrate) 
    )
    ELSE Opportunity._net_new_ARR
  END,4) AS _net_new_ARR,
  ROUND(CASE 
    WHEN _is_closed = true AND _currency != 'USD'
    THEN (
      closedConversionRate._subscription
    )
    WHEN _is_closed = false AND _currency != 'USD'
    THEN (
      (Opportunity._subscription / openConversionRate.conversionrate) 
    )
    ELSE Opportunity._subscription
  END,4) AS _subscription,
  ROUND(CASE 
    WHEN _is_closed = true AND _currency != 'USD'
    THEN (
      closedConversionRate._total_opportunity_amount
    )
    WHEN _is_closed = false AND _currency != 'USD'
    THEN (
      (Opportunity._total_opportunity_amount / openConversionRate.conversionrate) 
    )
    ELSE Opportunity._total_opportunity_amount
  END,4) AS _total_opportunity_amount,
  ROUND(CASE 
    WHEN _is_closed = true AND _currency != 'USD'
    THEN (
      closedConversionRate._services
    )
    WHEN _is_closed = false AND _currency != 'USD'
    THEN (
      (Opportunity._services / openConversionRate.conversionrate) 
    )
    ELSE Opportunity._services
  END,4) AS _services
FROM Opportunity
LEFT JOIN closedConversionRate ON closedConversionRate.id = Opportunity._opportunityID
LEFT JOIN openConversionRate ON openConversionRate.isocode = Opportunity._currency;
----------------------------------------------------------
------------------ db_target_acc_leads -------------------
----------------------------------------------------------

-- CREATE OR REPLACE TABLE `x-marketing.processunity.db_target_acc_leads` AS 
TRUNCATE TABLE `x-marketing.processunity.db_target_acc_leads`;
INSERT INTO `x-marketing.processunity.db_target_acc_leads`(
  _accountID,
  _account_domain,
  _account_owner,
  _account_name,
  _ABX_opt_in,
  _ABX_tier,
  _type,
  _leadID,
  _lead_name,
  _lead_email,
  _lead_source,
  _lead_created_date,
  _WF_become_MQL_date
)
SELECT DISTINCT
  account._accountID,
  account._account_domain,
  account._account_owner,
  account._account_name,
  account._ABX_opt_in,
  account._ABX_tier,
  account._type,
  leads.id AS _leadID,
  leads.name AS _lead_name,
  leads.email AS _lead_email,
  leads.leadsource AS _lead_source,
  leads.createddate AS _lead_created_date,
  leads.wf_became_mql_date__c AS _WF_become_MQL_date
FROM `x-marketing.processunity.db_target_accounts` account
LEFT JOIN `x-marketing.processunity_salesforce.Lead` leads
  ON leads.account__c = account._accountID;
-- WHERE leads.isdeleted IS FALSE;


----------------------------------------------------------
---------------- db_target_consolidation -----------------
----------------------------------------------------------

-- CREATE OR REPLACE TABLE `x-marketing.processunity.db_target_consolidation` AS 
TRUNCATE TABLE `x-marketing.processunity.db_target_consolidation`;
INSERT INTO `x-marketing.processunity.db_target_consolidation`(
  _account_owner, 
  _accountID, 
  _account_name, 
  _domain, 
  _type, 
  _ABX_tier, 
  _ABX_opt_in, 
  _statusID, 
  _remark, 
  _cycle_date, 
  _lead_source, 
  _stage_name, 
  _lead_name, 
  _lead_email, 
  _opportunity_type
)
WITH account_leads AS (
  SELECT
    _accountID,
    _account_domain,
    _account_owner,
    _account_name,
    _ABX_opt_in,
    _ABX_tier,
    _type,
    _leadID,
    _lead_name,
    _lead_email,
    _lead_source,
    _lead_created_date,
    _WF_become_MQL_date
  FROM `x-marketing.processunity.db_target_acc_leads`
),
lead_status AS (
  SELECT DISTINCT
    account_leads._account_owner,
    account_leads._accountID,
    account_leads._account_name,
    account_leads._account_domain AS _domain,
    account_leads._type,
    account_leads._ABX_tier,
    UPPER(CAST(account_leads._ABX_opt_in AS STRING)) AS _ABX_opt_in,
    account_leads._leadID AS _statusID,
    'Lead' AS _remark,
    account_leads._lead_created_date AS _cycle_date,
    account_leads._lead_source,
    '' AS _stage_name,
    _lead_name,
    _lead_email,
    '' AS _opportunity_type
  FROM account_leads
  -- WHERE account_leads._lead_created_date IS NOT NULL
  UNION ALL
  SELECT DISTINCT
    account_leads._account_owner,
    account_leads._accountID,
    account_leads._account_name,
    account_leads._account_domain AS _domain,
    account_leads._type,
    account_leads._ABX_tier,
    UPPER(CAST(account_leads._ABX_opt_in AS STRING)) AS _ABX_opt_in,
    account_leads._leadID AS _statusID,
    'MQL' AS _remark,
    account_leads._WF_become_MQL_date AS _cycle_date,
    account_leads._lead_source,
    '' AS _stage_name,
    _lead_name,
    _lead_email,
    '' AS _opportunity_type
  FROM account_leads
  -- WHERE account_leads._WF_become_MQL_date IS NOT NULL
  UNION ALL
  SELECT DISTINCT
    account_leads._account_owner,
    account_leads._accountID,
    account_leads._account_name,
    account_leads._account_domain AS _domain,
    account_leads._type,
    account_leads._ABX_tier,
    UPPER(CAST(account_leads._ABX_opt_in AS STRING)) AS _ABX_opt_in,
    opp._opportunityID AS _statusID,
    'Opportunity' AS _remark,
    opp._opportunity_created_date AS _cycle_date,
    opp._lead_source,
    opp._stage_name,
    _lead_name,
    _lead_email,
    _opportunity_type
  FROM account_leads
  LEFT JOIN `x-marketing.processunity.db_target_opportunity` opp
    USING(_accountID)
  WHERE opp._opportunityID IS NOT NULL
)
SELECT DISTINCT * FROM lead_status;





