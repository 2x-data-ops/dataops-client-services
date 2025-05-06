TRUNCATE TABLE `x-marketing.faro.silver_leap_opportunity_log`;
INSERT INTO `x-marketing.faro.silver_leap_opportunity_log` 
-- CREATE OR REPLACE TABLE `x-marketing.faro.silver_leap_opportunity_log` AS
WITH closedConversionRate AS (
  SELECT DISTINCT
    opp.id AS _id,
    isocode AS _iso_code,
    opp.closedate AS _close_date,
    rate.conversionrate AS _conversion_rate,
    opp.amount / rate.conversionrate AS _total_price_USD
  FROM `x-marketing.faro_salesforce_2.DatedConversionRate` rate
  LEFT JOIN `x-marketing.faro_salesforce_2.Opportunity` opp
  ON rate.isoCode = opp.currencyisocode
    AND opp.closedate >= rate.startDate
    AND opp.closedate < rate.nextStartDate
  WHERE opp.isclosed = true
  -- ORDER BY rate.startDate DESC
),
openConversionRate AS (
  SELECT DISTINCT
    opp.id AS _id,
    isocode AS _iso_code,
    rate.conversionrate AS _conversion_rate,
    rate.lastmodifieddate AS _last_modified_date,
    opp.closedate AS _close_date
  FROM `x-marketing.faro_salesforce_2.DatedConversionRate` rate
  LEFT JOIN `x-marketing.faro_salesforce_2.Opportunity` opp
  ON opp.currencyisocode = rate.isocode
  WHERE opp.isclosed = false
    AND opp.currencyisocode != 'USD'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY isocode ORDER BY rate.lastmodifieddate DESC) = 1
),
Opportunity AS (
  SELECT DISTINCT
    opp.software__c AS _software,
    opp.hardware__c AS _hardware,
    opp.iswon AS _is_won,
    opp.realterritory_name__c AS _realterritory_name,
    opp.id AS _opportunity_id,
    opp.activity_type__c AS _activity_type,
    opp.primary_contact__c AS _primary_contact,
    opp.createdbyid AS _created_by_id,
    opp.lastmodifieddate AS _last_modified_date,
    opp.iss__c AS _iss,
    opp.name AS _name,
    opp.campaignid AS _campaign_id,
    opp.sfdc_opp_casesafeid__c AS _sfdc_opp_casesafe_id,
    opp.most_recent_lead_source__c AS _most_recent_lead_source,
    opp.opportunity_id__c AS _opportunity_custom_id,
    opp.asset__c AS _asset,
    opp.department__c AS _department,
    opp.division_region__c AS _division_region,
    opp.account_division__c AS _account_division,
    opp.ownerid AS _owner_id,
    opp.amount AS _amount,
    opp.type AS _type,
    opp.accountid AS _account_id,
    opp.closedate AS _close_date,
    opp.leadsource AS _lead_source,
    opp.contactid AS _contact_id,
    opp.application__c AS _application,
    opp.opportunity_type__c AS _opportunity_type,
    opp.stagename AS _stage_name,
    opp.createddate AS _created_date,
    opp.application_industry__c AS _application_industry,
    opp.realterritory_user_director__c AS _realterritory_user_director,
    opp.realterritory_user_rm__c AS _realterritory_user_rm,
    opp.primary_application__c AS _primary_application,
    opp.realterritory_user_iss__c AS _realterritory_user_iss,
    opp.primary_software_interest__c AS _primary_software_interest,
    opp.secondary_application__c AS _secondary_application,
    opp.secondary_hardware__c AS _secondary_hardware,
    opp.secondary_software__c AS _secondary_software,
    opp.currencyisocode AS _currency_iso_code,
    opp.isclosed AS _is_closed,
    acc.name AS _account_name
  FROM `x-marketing.faro_salesforce_2.Opportunity` opp
  LEFT JOIN `x-marketing.faro_salesforce_2.Account` acc
    ON opp.accountid = acc.id
)
SELECT DISTINCT
  Opportunity.* EXCEPT(_amount),
  Opportunity._amount AS _original_amount,
  CASE 
    WHEN Opportunity._is_closed = TRUE AND Opportunity._currency_iso_code != 'USD'
    THEN (
      closedConversionRate._conversion_rate
    )
    WHEN Opportunity._is_closed = FALSE AND Opportunity._currency_iso_code != 'USD'
    THEN (
      openConversionRate._conversion_rate 
    )
  END AS conversionRate,
  CASE 
    WHEN Opportunity._is_closed = TRUE AND Opportunity._currency_iso_code != 'USD'
    THEN (

      closedConversionRate._total_price_USD
    )
    WHEN Opportunity._is_closed = FALSE AND Opportunity._currency_iso_code != 'USD'
    THEN (
      (_amount / openConversionRate._conversion_rate) 
    )
    ELSE _amount
  END AS _total_price
FROM Opportunity
LEFT JOIN closedConversionRate ON closedConversionRate._id = Opportunity._opportunity_id
LEFT JOIN openConversionRate ON openConversionRate._iso_code = Opportunity._currency_iso_code;