CREATE OR REPLACE TABLE `sdi.db_consolidated_engagements_log` AS
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

), accounts AS (
      SELECT 
        CAST(NULL AS INTEGER) AS _id,
        CAST(NULL AS STRING) AS _email,
        CAST(NULL AS STRING) AS _name,
        _domain,
        CAST(NULL AS STRING) AS _jobtitle,
        CAST(NULL AS STRING) AS _function,
        CAST(NULL AS STRING) AS _seniority,
        _phone,
        _company,
        _revenue,
        _industry,
        _employee,
        _city,
        _state,
        _country,
        CAST(NULL AS STRING) AS _persona,
        CAST(NULL AS STRING) AS _lifecycleStage,
        CAST(NULL AS STRING) AS _sfdccontactid,
        _sfdcaccountid,
        CAST(NULL AS STRING) AS _sfdcleadid, 

      FROM
        contacts
    QUALIFY ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY _company DESC) = 1
) ,
 email_engagement AS (
SELECT
  _email,
  RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain,
  TIMESTAMP(FORMAT_TIMESTAMP('%F %I:%M:%S %Z', _timestamp)) AS _timestamp,
  DATE_TRUNC(DATE(_timestamp), MONTH) AS _month,
  DATE_TRUNC(DATE(_timestamp), QUARTER) AS _quater,
  EXTRACT(WEEK FROM _timestamp) AS _week,
  EXTRACT(YEAR FROM _timestamp) AS _year,
  _utmcampaign AS _contentTitle,
  CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
  _description,
  _campaignID AS _campaignID,
FROM
  `sdi.db_email_engagements_log`
WHERE
  LOWER(_engagement) NOT IN ('sent',
    'downloaded',
    'bounced',
    'unsubscribed',
    'processed',
    'deffered',
    'spam',
    'suppressed',
    'dropped',
    'not sent',
    'false delivered',
    'deferred',
    'hard bounce',
    'soft bounce',
    'delivered')
  AND NOT REGEXP_CONTAINS(_domain,'2x.marketing|sdi')
  AND _domain IS NOT NULL
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
  --vid =67125612648 
  --property_hs_google_click_id.value IS NOT NULL
)
,form_fills AS (

    SELECT 
      _email,
       _domain,
      _timestamp,
  DATE_TRUNC(DATE(_timestamp), MONTH) AS _month,
  DATE_TRUNC(DATE(_timestamp), QUARTER) AS _quater,
  EXTRACT(WEEK FROM _timestamp) AS _week,
  EXTRACT(YEAR FROM _timestamp) AS _year,
      _form_title,
      'Form Filled' AS _engagement,
      _description,
      _utmsource,

    FROM 
      `sdi.db_form_fill_log`
) , 
combine_engagement AS (

SELECT * FROM email_engagement 
UNION ALL 
SELECT * FROM google_click_engagement
UNION ALL 
SELECT * FROM linkedin_click_engagement
UNION ALL 
SELECT * FROM form_fills
) SELECT combine_engagement.*,
contacts.* EXCEPT (_email,_domain) 

FROM combine_engagement 
LEFT JOIN  
      contacts USING(_email)