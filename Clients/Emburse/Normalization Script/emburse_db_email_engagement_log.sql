TRUNCATE TABLE `x-marketing.emburse.db_email_engagements_log`;
INSERT INTO `x-marketing.emburse.db_email_engagements_log` (
  _sdc_sequence,	
  _campaignID,	
  _campaign,	
  _subject,	
  _timestamp,	
  _engagement,	
  _description,	
  _link,	
  _utm_source,	
  _utm_medium,	
  _utm_content,
  _utm_campaign,
  _id,	
  _email,	
  _name,	
  _domain,	
  _job_title,	
  _function,	
  _seniority,	
  _phone,	
  _company,	
  _revenue,	
  _industry,	
  _city,	
  _state,	
  _country,	
  _persona,	
  leadsource
)
WITH prospect_info AS (
SELECT DISTINCT 
  CAST(marketo.id AS STRING) AS _id,
  email AS _email,
  CONCAT(firstname,' ', lastname) AS _name,
  RIGHT(email, LENGTH(email) - STRPOS(email, '@')) AS _domain, 
  title AS _job_title,
  job_function__c AS _function,
  CASE 
    WHEN title LIKE '%Senior Counsel%' THEN "VP"
    WHEN title LIKE '%Assistant General Counsel%' THEN "VP" 
    WHEN title LIKE '%General Counsel%' THEN "C-Level" 
    WHEN title LIKE '%Founder%' THEN "C-Level" 
    WHEN title LIKE '%C-Level%' THEN "C-Level" 
    WHEN title LIKE '%CDO%' THEN "C-Level" 
    WHEN title LIKE '%CIO%' THEN "C-Level"
    WHEN title LIKE '%CMO%' THEN "C-Level"
    WHEN title LIKE '%CFO%' THEN "C-Level" 
    WHEN title LIKE '%CEO%' THEN "C-Level"
    WHEN title LIKE '%Chief%' THEN "C-Level" 
    WHEN title LIKE '%coordinator%' THEN "Non-Manager"
    WHEN title LIKE '%COO%' THEN "C-Level" 
    WHEN title LIKE '%Sr. V.P.%' THEN "Senior VP"
    WHEN title LIKE '%Sr.VP%' THEN "Senior VP"  
    WHEN title LIKE '%Senior-Vice Pres%' THEN "Senior VP"  
    WHEN title LIKE '%srvp%' THEN "Senior VP" 
    WHEN title LIKE '%Senior VP%' THEN "Senior VP" 
    WHEN title LIKE '%SR VP%' THEN "Senior VP"  
    WHEN title LIKE '%Sr Vice Pres%' THEN "Senior VP" 
    WHEN title LIKE '%Sr. VP%' THEN "Senior VP" 
    WHEN title LIKE '%Sr. Vice Pres%' THEN "Senior VP"  
    WHEN title LIKE '%S.V.P%' THEN "Senior VP" 
    WHEN title LIKE '%Senior Vice Pres%' THEN "Senior VP"  
    WHEN title LIKE '%Exec Vice Pres%' THEN "Senior VP" 
    WHEN title LIKE '%Exec Vp%' THEN "Senior VP"  
    WHEN title LIKE '%Executive VP%' THEN "Senior VP" 
    WHEN title LIKE '%Exec VP%' THEN "Senior VP"  
    WHEN title LIKE '%Executive Vice President%' THEN "Senior VP" 
    WHEN title LIKE '%EVP%' THEN "Senior VP"  
    WHEN title LIKE '%E.V.P%' THEN "Senior VP" 
    WHEN title LIKE '%SVP%' THEN "Senior VP" 
    WHEN title LIKE '%V.P%' THEN "VP" 
    WHEN title LIKE '%VP%' THEN "VP" 
    WHEN title LIKE '%Vice Pres%' THEN "VP"
    WHEN title LIKE '%V P%' THEN "VP"
    WHEN title LIKE '%President%' THEN "C-Level"
    WHEN title LIKE '%Director%' THEN "Director"
    WHEN title LIKE '%CTO%' THEN "C-Level"
    WHEN title LIKE '%Dir%' THEN "Director"
    WHEN title LIKE '%MDR%' THEN "Non-Manager"
    WHEN title LIKE '%MD%' THEN "Director"
    WHEN title LIKE '%GM%' THEN "Director"
    WHEN title LIKE '%Head%' THEN "VP"
    WHEN title LIKE '%Manager%' THEN "Manager"
    WHEN title LIKE '%escrow%' THEN "Non-Manager"
    WHEN title LIKE '%cross%' THEN "Non-Manager"
    WHEN title LIKE '%crosse%' THEN "Non-Manager"
    WHEN title LIKE '%Assistant%' THEN "Non-Manager"
    WHEN title LIKE '%Partner%' THEN "C-Level"
    WHEN title LIKE '%CRO%' THEN "C-Level"
    WHEN title LIKE '%Chairman%' THEN "C-Level"
    WHEN title LIKE '%Owner%' THEN "C-Level"
  END AS _seniority,
  phone AS _phone,
  company AS _company,
  CAST(annualrevenue AS STRING) AS _revenue,
  industry__c AS _industry,
  city AS _city,
  state AS _state, 
  country AS _country,
  '' AS _persona,
  leadsource
FROM `x-marketing.emburse_marketo.leads` marketo
WHERE email IS NOT NULL
  AND email NOT LIKE '%2x.marketing'
  AND email NOT LIKE '%emburse.com'
QUALIFY ROW_NUMBER() OVER(
  PARTITION BY email 
  ORDER BY marketo.id DESC) = 1
),
email_sent AS (
SELECT
  _sdc_sequence,
  CAST(primary_attribute_value_id AS STRING) AS _campaignID,
  primary_attribute_value AS _campaign,
  '' AS _subject,
  '' AS _email,
  activitydate AS _timestamp,
  'Sent' AS _engagement,
  '' AS _description,
  CAST(leadid AS STRING) AS _leadid,
  '' AS _link
FROM `x-marketing.emburse_marketo_v2.activities_send_email`
QUALIFY ROW_NUMBER() OVER(
  PARTITION BY leadid, primary_attribute_value_id 
  ORDER BY activitydate DESC) = 1
),
email_delivered AS (
SELECT
  _sdc_sequence,
  CAST(primary_attribute_value_id AS STRING) AS _campaignID,
  primary_attribute_value AS _campaign,
  '' AS _subject,
  '' AS _email,
  activitydate AS _timestamp,
  'Delivered' AS _engagement,
  '' AS _description,
  CAST(leadid AS STRING) AS _leadid,
  '' AS _link
FROM `x-marketing.emburse_marketo_v2.activities_email_delivered`
QUALIFY ROW_NUMBER() OVER(
  PARTITION BY leadid, primary_attribute_value_id 
  ORDER BY activitydate DESC) = 1 
),
email_open AS (
SELECT
  _sdc_sequence,
  CAST(primary_attribute_value_id AS STRING) AS _campaignID,
  primary_attribute_value AS _campaign,
  '' AS _subject,
  '' AS _email,
  activitydate AS _timestamp,
  'Opened' AS _engagement,
  '' AS _description,
  CAST(leadid AS STRING) AS _leadid,
  '' AS _link
FROM `x-marketing.emburse_marketo_v2.activities_open_email`
QUALIFY ROW_NUMBER() OVER(
  PARTITION BY leadid, primary_attribute_value_id 
  ORDER BY activitydate DESC) = 1
),
email_click AS (
SELECT
  _sdc_sequence,
  CAST(primary_attribute_value_id AS STRING) AS _campaignID,
  primary_attribute_value AS _campaign,
  '' AS _subject,
  '' AS _email,
  activitydate AS _timestamp,
  'Clicked' AS _engagement,
  '' AS _description,
  CAST(leadid AS STRING) AS _leadid,
  link AS _link
FROM `x-marketing.emburse_marketo_v2.activities_click_email`
QUALIFY ROW_NUMBER() OVER(
  PARTITION BY leadid, primary_attribute_value_id 
  ORDER BY activitydate DESC) = 1
),
open_click AS ( --merge open and click data
SELECT 
  * 
FROM email_open
UNION ALL
SELECT 
  * 
FROM email_click
),
new_open AS ( --to populate the data in Clicked but not appear in Opened list
SELECT 
  _sdc_sequence,
  _campaignID,
  _campaign,
  _subject,
  _email,
  _timestamp,
  'Opened' AS _engagement,
  _description,
  _leadid,
  _link 
FROM open_click
WHERE _engagement <> 'Opened' 
  AND _engagement = 'Clicked'
QUALIFY ROW_NUMBER() OVER(
  PARTITION BY _leadid, _campaignID 
  ORDER BY _timestamp DESC) = 1
), 
new_open_consolidate AS (
SELECT 
  * 
FROM email_open
UNION ALL
SELECT 
  * 
FROM new_open
),
email_hard_bounce AS (
SELECT
  _sdc_sequence,
  CAST(primary_attribute_value_id AS STRING) AS _campaignID,
  primary_attribute_value AS _campaign,
  '' AS _subject,
  '' AS _email,
  activitydate AS _timestamp,
  'Hard Bounced' AS _engagement,
  details AS _description,
  CAST(leadid AS STRING) AS _leadid,
  '' AS _link
FROM `x-marketing.emburse_marketo_v2.activities_email_bounced` 
QUALIFY ROW_NUMBER() OVER(
  PARTITION BY leadid, primary_attribute_value_id 
  ORDER BY activitydate DESC) = 1
),
email_soft_bounce AS (
SELECT
  _sdc_sequence,
  CAST(primary_attribute_value_id AS STRING) AS _campaignID,
  primary_attribute_value AS _campaign,
  '' AS _subject,
  '' AS _email,
  activitydate AS _timestamp,
  'Soft Bounced' AS _engagement,
  details AS _description,
  CAST(leadid AS STRING) AS _leadid,
  '' AS _link
FROM `x-marketing.emburse_marketo_v2.activities_email_bounced_soft`  
QUALIFY ROW_NUMBER() OVER(
  PARTITION BY leadid, primary_attribute_value_id 
  ORDER BY activitydate DESC) = 1
),
email_soft_hard_bounced AS (
SELECT 
  * 
FROM email_hard_bounce
UNION ALL
SELECT
  * 
FROM email_soft_bounce
),
new_delivered_email AS( --remove soft and hard bounced in delivered list
SELECT 
  del.*
FROM email_delivered del
LEFT JOIN email_soft_hard_bounced bounced 
  ON del._campaignID = bounced._campaignID 
  AND del._leadid = bounced._leadid
WHERE bounced._campaignID IS NULL 
  AND bounced._leadid IS NULL
),
email_download AS (
SELECT
  _sdc_sequence,
  CAST(primary_attribute_value_id AS STRING) AS _campaignID,
  primary_attribute_value AS _campaign,
  '' AS _subject,
  '' AS _email,
  activitydate AS _timestamp,
  'Downloaded' AS _engagement,
  '' AS _description,
  CAST(leadid AS STRING) AS _leadid,
  '' AS _link
FROM `x-marketing.emburse_marketo_v2.activities_fill_out_form`
WHERE primary_attribute_value NOT LIKE '%TEST 2X%'
  AND primary_attribute_value NOT LIKE '%Email Unsubscribe Form%'
QUALIFY ROW_NUMBER() OVER(
  PARTITION BY leadid, primary_attribute_value_id 
  ORDER BY activitydate DESC) = 1
),
email_unsubscribed AS (
SELECT
  _sdc_sequence,
  CAST(primary_attribute_value_id AS STRING) AS _campaignID,
  primary_attribute_value AS _campaign,
  '' AS _subject,
  '' AS _email,
  activitydate AS _timestamp,
  'Unsubscribed' AS _engagement,
  '' AS _description,
  CAST(leadid AS STRING) AS _leadid,
  '' AS _link
FROM `x-marketing.emburse_marketo_v2.activities_unsubscribe_email`
QUALIFY ROW_NUMBER() OVER(
  PARTITION BY leadid, primary_attribute_value_id 
  ORDER BY activitydate DESC) = 1
),
engagements_combined AS (
SELECT 
  * 
FROM email_sent
UNION ALL
SELECT 
  * 
FROM new_delivered_email
UNION ALL
SELECT 
  * 
FROM new_open_consolidate
UNION ALL
SELECT 
  * 
FROM email_click
UNION ALL
SELECT 
  * 
FROM email_hard_bounce
UNION ALL
SELECT 
  * 
FROM email_soft_bounce
UNION ALL
SELECT 
  * 
FROM email_unsubscribed
)
SELECT
  engagements.* EXCEPT(_leadid, _email),
  COALESCE(REGEXP_EXTRACT(_link, r'[?&]utm_source=([^&]+)'), "Email") AS _utm_source,
  REGEXP_EXTRACT(_link, r'[?&]utm_medium=([^&]+)') AS _utm_medium,
  REGEXP_EXTRACT(_link, r'[?&]utm_content=([^&]+)') AS _utm_content,
  REGEXP_EXTRACT(_link, r'[?&]utm_campaign=([^&]+)') AS _utm_campaign,
  prospect_info.*
FROM engagements_combined AS engagements
LEFT JOIN prospect_info
  ON engagements._leadid = prospect_info._id;