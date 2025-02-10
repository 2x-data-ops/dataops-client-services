INSERT INTO  `x-marketing.devo.db_account_lifecycle` (
companyid, _salesforceaccountid, _account_lifecycle, _account_lifecycle_timestamp, _target_account_lifecycle_timestamp, _inactive_account_lifecycle_timestamp, _opportunity_account_lifecycle_timestamp, _aware_account_lifecycle_timestamp, _engaged_account_lifecycle_timestamp, _customer_account_lifecycle_timestamp, _lifecyclestage, _lifecyclestage_timestamp, _opportunity_lifecycle_timestamp, _lead_lifecycle_timestamp, _customer_lifecycle_timestamp, _subscriber_lifecycle_timestamp, _extract_date, _extract_timestamp )
SELECT 
CAST(companyid AS STRING) AS companyid,
property_salesforceaccountid.value AS _salesforceaccountid,
property_account_lifecycle.value AS _account_lifecycle,
property_account_lifecycle.timestamp AS _account_lifecycle_timestamp,
CASE WHEN property_account_lifecycle.value = 'Target' THEN property_account_lifecycle.timestamp END AS _target_account_lifecycle_timestamp,
CASE WHEN property_account_lifecycle.value = 'Inactive' THEN property_account_lifecycle.timestamp END AS _inactive_account_lifecycle_timestamp ,
CASE WHEN property_account_lifecycle.value = 'Opportunity' THEN property_account_lifecycle.timestamp END AS _opportunity_account_lifecycle_timestamp ,
CASE WHEN property_account_lifecycle.value = 'Aware' THEN property_account_lifecycle.timestamp END AS _aware_account_lifecycle_timestamp ,
CASE WHEN property_account_lifecycle.value = 'Engaged' THEN property_account_lifecycle.timestamp END AS _engaged_account_lifecycle_timestamp ,
CASE WHEN property_account_lifecycle.value = 'Customer' THEN property_account_lifecycle.timestamp END AS _customer_account_lifecycle_timestamp ,
property_lifecyclestage.value  AS _lifecyclestage ,
property_lifecyclestage.timestamp AS _lifecyclestage_timestamp,
CASE WHEN property_lifecyclestage.value  = 'opportunity' THEN property_lifecyclestage.timestamp END AS _opportunity_lifecycle_timestamp ,
CASE WHEN property_lifecyclestage.value  = 'lead' THEN property_lifecyclestage.timestamp END AS _lead_lifecycle_timestamp ,
CASE WHEN property_lifecyclestage.value  = 'customer' THEN property_lifecyclestage.timestamp END AS _customer_lifecycle_timestamp ,
CASE WHEN property_lifecyclestage.value  = 'subscriber' THEN property_lifecyclestage.timestamp END AS _subscriber_lifecycle_timestamp ,
CURRENT_DATE() AS _extract_date,
CURRENT_TIMESTAMP AS _extract_timestamp, 
 
FROM `x-marketing.devo_hubspot.companies` ;

CREATE OR REPLACE TABLE `x-marketing.devo.db_account_lifecycle_current` AS 
WITH account AS (
SELECT CAST(companyid AS STRING) AS companyid,
property_name.value AS _account_name, 
property_industry.value AS _industry, 
property_annualrevenue.value _annualrevenue, 
property_annualrevenue.value__st AS _annualrevenue_st,
property_account_record_type.value AS _account_record_type, 
property_type.value AS _type, 
property_hs_last_sales_activity_type.value AS _last_sales_activity_type, 
property_partner_type.value AS _partner_type, 
property_account_priority__sales_.value AS _account_priority__sales, 
property_devo_sales_territory___tier_2.value AS _devo_sales_territory___tier_2, 
property_number_of_sales_engaged_contacts.value AS _number_of_sales_engaged_contacts, 
property_presales_engagement_score.value AS _presales_engagement_score, 
property_linkedin_company_page.value AS _linkedin_company_page, 
property_linkedinbio.value AS _linkedinbio, 
property_website.value AS _website, 
property_bdr_account_status.value AS _bdr_account_status, 
property_marketing_qualified_accounts__mqa_.value AS _marketing_qualified_accounts__mqa_, 
property_hs_is_target_account.value AS _hs_is_target_account, 
property_account_engagement_score.value__fl AS _account_engagement_score, 
property_createdate.value AS _createdate,
property_account_lifecycle.timestamp AS _account_lifecycle_timestamps,
property_lifecyclestage.timestamp AS _lifecyclestage_timestamps,
FROM `x-marketing.devo_hubspot.companies`
), snapshots AS (
  SELECT companyid, _salesforceaccountid,_account_lifecycle, _target_account_lifecycle_timestamp,_inactive_account_lifecycle_timestamp, _opportunity_account_lifecycle_timestamp, _aware_account_lifecycle_timestamp, _engaged_account_lifecycle_timestamp, _customer_account_lifecycle_timestamp, _lifecyclestage,_lifecyclestage_timestamp, _opportunity_lifecycle_timestamp, _lead_lifecycle_timestamp, _customer_lifecycle_timestamp, _subscriber_lifecycle_timestamp, _extract_date, _extract_timestamp 
FROM `x-marketing.devo.db_account_lifecycle` 

QUALIFY ROW_NUMBER() OVER(PARTITION BY companyid, _salesforceaccountid,_account_lifecycle, _target_account_lifecycle_timestamp,_inactive_account_lifecycle_timestamp, _opportunity_account_lifecycle_timestamp, _aware_account_lifecycle_timestamp, _engaged_account_lifecycle_timestamp, _customer_account_lifecycle_timestamp, _lifecyclestage, _opportunity_lifecycle_timestamp, _lead_lifecycle_timestamp, _customer_lifecycle_timestamp, _subscriber_lifecycle_timestamp ORDER BY _extract_timestamp DESC) = 1
) SELECT snapshots.*, 
account.* EXCEPT (companyid)
FROM snapshots
LEFT JOIN account on snapshots.companyid = account.companyid