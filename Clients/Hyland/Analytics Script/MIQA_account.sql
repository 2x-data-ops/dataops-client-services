CREATE OR REPLACE TABLE `x-marketing.hyland.MIQA_account` AS
WITH account AS (
  SELECT  
    id_18__c AS _id18,
    name AS _name,
    industry AS _industry,
    COALESCE(billingstate, shippingstate) AS _state,
    COALESCE(billingcountry, shippingcountry) AS _country,
    region__c AS _region,
    site AS _site,
    account_tier__c AS _accountTier,
    account_status__c AS _accountStatus,
    record_type_name__c AS _recordTypeName,
    support_category__c AS _supportCategory,
    is_miqa__c AS _isMIQA,
    product_lines_owned__c AS _productLinesOwned,
    open_opportunities__c AS _openOpp,
    createddate AS _createdDate,
    abm_category__c AS _abmCategory,
    sixsense_segments__c AS _6senseSegments,
    abm_category_enrollment_date__c AS _abmCategoryEnrollmentDate,
    ownerid AS _accountOwnerID,
    owner_name__c AS _accountOwnerName,
    ownership AS _ownership
  FROM `x-marketing.hyland_salesforce.Account` 
  WHERE isdeleted IS FALSE
  QUALIFY ROW_NUMBER() OVER(PARTITION BY id_18__c ORDER BY createddate DESC) = 1
),
/*
contact AS (
  SELECT
    id_18__c AS _contactid18,
    -- id AS _contactID,
    accountid AS _accountID,
    firstname AS _contactFirstName,
    lastname AS _contactLastName,
    COALESCE(name, CONCAT(firstname, ' ',lastname)) AS _contactName,
    title AS _contactTitle,
    email AS _contactEmail,
    COALESCE(phone, mobilephone) AS _contactPhone,
    leadsource AS _contactLeadSource,
    lead_source_detail__c AS _contactLeadSourceDetail,
    most_recent_lead_source__c AS _contactMostRecentLeadSource,
    most_recent_lead_source_detail__c AS _contactMostRecentLeadSourceDetail,
    mailingstate AS _contactState,
    mailingcountry AS _contactCountry,
    region__c AS _contactRegion,
    email_opt_in_status__c AS _contactEmailOptIn,
    createddate AS _contactCreatedDate
  FROM `x-marketing.hyland_salesforce.Contact` 
  WHERE isdeleted IS FALSE
  QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY createddate) = 1
),
*/
opportunity_data AS (
  SELECT
    id_18__c AS _oppid18,
    id AS _oppID,
    name AS _oppName,
    accountid AS _accountID,
    record_type_name__c AS _oppRecordType,
    deal_category__c AS _oppDealType,
    product_line__c AS _oppProductLine,
    expectedrevenue AS _oppExpectedRevenue,
    stagename AS _oppStage,
    stage_duration__c AS _oppStageDuration,
    createddate AS _oppCreatedDate,
    closedate AS _oppCloseDate,
    leadsource AS _oppLeadSource,
    currencyisocode AS _oppCurrency,
    isclosed,
    amount AS _oppAmount,
    amount_in_usd__c AS _oppUSDAmount
  FROM `x-marketing.hyland_salesforce.Opportunity`
  WHERE isdeleted IS FALSE
  QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY createddate) = 1
),
closedConversionRate AS (
  SELECT 
  DISTINCT
    opp._oppID,
    isocode,
    opp._oppCloseDate,
    rate.conversionrate,
    opp._oppAmount / rate.conversionrate AS total_price_USD
  FROM `x-marketing.hyland_salesforce.DatedConversionRate` rate
  LEFT JOIN opportunity_data opp
  ON rate.isoCode = opp._oppCurrency
    AND opp._oppCloseDate >= rate.startDate
    AND opp._oppCloseDate < rate.nextStartDate
  WHERE 
    opp.isclosed = true
),
openConversionRate AS (
  SELECT 
    opp._oppID,
    isocode,
    rate.conversionrate,
    rate.lastmodifieddate,
    opp._oppCloseDate,
  FROM `x-marketing.hyland_salesforce.DatedConversionRate` rate
  LEFT JOIN opportunity_data opp
  ON opp._oppCurrency = rate.isocode
  WHERE opp.isclosed = false
  AND opp._oppCurrency != 'USD'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY isocode ORDER BY rate.lastmodifieddate DESC) = 1
),
convertedAmount AS (
  SELECT
    opportunity_data._oppID,
    CASE 
      WHEN isclosed = true AND _oppCurrency != 'USD'
      THEN (
        closedConversionRate.conversionRate
      )
      WHEN isclosed = false AND _oppCurrency != 'USD'
      THEN (
        openConversionRate.conversionRate 
      )
    END AS conversionRate,
    CASE 
      WHEN isclosed = true AND _oppCurrency != 'USD'
      THEN (

        closedConversionRate.total_price_USD
      )
      WHEN isclosed = false AND _oppCurrency != 'USD'
      THEN (
        (_oppAmount / openConversionRate.conversionrate) 
      )
      ELSE _oppAmount
    END AS total_price
  FROM opportunity_data 
  LEFT JOIN closedConversionRate ON closedConversionRate._oppID = opportunity_data._oppID
  LEFT JOIN openConversionRate ON openConversionRate.isocode = opportunity_data._oppCurrency
  WHERE (_oppUSDAmount IS NULL OR _oppUSDAmount = 0)
  AND _oppAmount != 0
  AND _oppCurrency != 'USD'
),
opportunity AS (
  SELECT 
    opportunity_data.* EXCEPT(_oppUSDAmount,isclosed,_oppID),
    COALESCE(opportunity_data._oppUSDAmount, convertedAmount.total_price) AS _oppUSDAmount
  FROM opportunity_data
  LEFT JOIN convertedAmount ON convertedAmount._oppID = opportunity_data._oppID
)
SELECT
  account.*,
  -- contact.* EXCEPT(_accountID),
  opportunity.* EXCEPT(_accountID),
  -- accountHistory.* EXCEPT(_historyAccountID),
  -- accountActivity.* EXCEPT(_accountID)
FROM account
-- LEFT JOIN contact ON contact._accountID = account._id18
LEFT JOIN opportunity ON opportunity._accountID = account._id18;
-- LEFT JOIN accountHistory ON accountHistory._historyAccountID = account._id18
-- LEFT JOIN accountActivity ON accountActivity._accountID = account._id18

---------------------------------
/*------ Account History ------*/
---------------------------------

CREATE OR REPLACE TABLE `x-marketing.hyland.MIQA_acc_history_activity` AS
WITH account AS (
  SELECT  
    id_18__c AS accountid,
    is_miqa__c AS _isMIQA,
    createddate AS _createdDate
  FROM `x-marketing.hyland_salesforce.Account` 
  WHERE isdeleted IS FALSE
  QUALIFY ROW_NUMBER() OVER(PARTITION BY id_18__c ORDER BY createddate DESC) = 1
), 
accountHistory AS (
  SELECT
    DISTINCT
    history.accountid AS accountid,
    history.createdbyid AS _createdBy,
    history.createddate AS _createdDate,
    history.oldvalue AS _oldValue,
    history.newvalue AS _newValue,
    history.field AS _field
  FROM account 
  LEFT JOIN `x-marketing.hyland_salesforce.AccountHistory` history 
  USING(accountid)
  WHERE
    account._isMIQA IS TRUE
    AND isdeleted IS FALSE
    AND field = 'Is_MIQA__c'
)/*,
activity AS (
  SELECT
    event.accountid AS _accountID,
    event.id AS _activityID,
    event.subject AS _activitySubject,
    event.activityDate AS _activityDate,
    event.type AS _activityType,
    event.createddate AS _activityCreatedDate,
    event.eventsubtype AS _activitySubType,
    'event' AS _isActivity,
  FROM `x-marketing.hyland_salesforce.Event` event
  WHERE isdeleted IS FALSE
  QUALIFY ROW_NUMBER() OVER(PARTITION BY event.accountid,event.id ORDER BY event.createddate) = 1
  UNION ALL
  SELECT
    task.accountid AS _accountID,
    task.id AS _activityID,
    task.subject AS _activitySubject,
    task.activityDate AS _activityDate,
    task.type AS _activityType,
    task.createddate AS _activityCreatedDate,
    task.tasksubtype AS _activitySubType,
    'event' AS _isActivity,
  FROM `x-marketing.hyland_salesforce.Task` task
  WHERE isdeleted IS FALSE
  QUALIFY ROW_NUMBER() OVER(PARTITION BY task.accountid,task.id ORDER BY task.createddate) = 1
)*/
SELECT 
-- DISTINCT
  *
  -- accountHistory.* EXCEPT(_historyAccountID),
  -- activity.* 
FROM accountHistory;