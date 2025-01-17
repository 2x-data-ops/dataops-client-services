CREATE OR REPLACE TABLE `sdi.db_ads_engagements_log` AS
WITH contacts AS (
   SELECT
      _id,
      _email,
      _name,
      _domain,
      _jobtitle,
      _function,
      _seniority,
      _phone,
      _company,
      _revenue,
      _industry,
      _employee,
      _city,
      _state,
      _country, 
      _persona,
      _lifecycleStage,
      _sfdccontactid, 
      _sfdcaccountid,
      _sfdcleadid
    FROM
      `sdi.db_icp_database_log`
    WHERE
      _domain NOT LIKE '%2x.marketing%' 

)
, google_click_engagement AS (
  SELECT
  property_email.value AS _email,
  RIGHT(property_email.value,LENGTH(property_email.value)-STRPOS(property_email.value,'@')) AS _domain,
  property_createdate.value AS _timestamp,
  DATE_TRUNC(DATE(property_createdate.value), MONTH) AS _month,
  DATE_TRUNC(DATE(property_createdate.value), QUARTER) AS _quater,
  EXTRACT(WEEK FROM property_createdate.value) AS _week,
  EXTRACT(YEAR FROM property_createdate.value) AS _year,
  '' AS _contentTitle,
  "Google Ads Clicked " AS _engagement,
  '' AS _description,
  '' AS _campaignID,
FROM
  `x-marketing.sdi_hubspot.contacts`
WHERE
  --property_hs_linkedin_ad_clicked.value IS NOT NULL vid =67125612648 
  property_hs_google_click_id.value IS NOT NULL
) 
,linkedin_click_engagement AS (
  SELECT
  property_email.value AS _email,
  RIGHT(property_email.value,LENGTH(property_email.value)-STRPOS(property_email.value,'@')) AS _domain,
  property_createdate.value AS _timestamp,
  DATE_TRUNC(DATE(property_createdate.value), MONTH) AS _month,
  DATE_TRUNC(DATE(property_createdate.value), QUARTER) AS _quater,
  EXTRACT(WEEK FROM property_createdate.value) AS _week,
  EXTRACT(YEAR FROM property_createdate.value) AS _year,
  '' AS _contentTitle,
  "LinkedIn Clicked " AS _engagement,
  '' AS _description,
  '' AS _campaignID,
FROM
  `x-marketing.sdi_hubspot.contacts`
WHERE
  property_hs_linkedin_ad_clicked.value IS NOT NULL

), 
combine_engagement AS (
   SELECT * FROM google_click_engagement
   UNION ALL 
   SELECT * FROM linkedin_click_engagement
) SELECT combine_engagement.*,
contacts.* EXCEPT (_email,_domain) 

FROM combine_engagement 
LEFT JOIN  
      contacts USING(_email)