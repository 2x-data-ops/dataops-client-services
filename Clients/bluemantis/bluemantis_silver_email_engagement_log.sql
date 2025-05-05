TRUNCATE TABLE `x-marketing.bluemantis.db_email_engagements_log`;
INSERT INTO `x-marketing.bluemantis.db_email_engagements_log` (
  _sdc_sequence,
  _email_campaign_id,
  _campaign,
  _subject,
  _timestamp,
  _engagement,
  _description,
  _link,
  _utm_source,
  _utm_medium,
  _utm_content,
  _prospect_id,
  _email,
  _name,
  _domain,
  _job_title,
  _phone,
  _company,
  _revenue,
  _industry,
  _city,
  _state,
  _country,
  _lead_source,
  _lead_source_description,
  _lead_status,
  _program_name,
  _program_channel,
  _campaign_name,
  _campaign_id,
  _campaign_code,
  _email_name,
  _subject_line,
  _preview,
  _asset_title,
  _asset_type,
  _asset_url,
  _live_date,
  _ad_visual,
  _email_proof,
  _campaign_initiated_by
)
WITH prospect_info AS (
  SELECT DISTINCT 
    CAST(marketo.id AS STRING) AS _prospect_id,
    email AS _email,
    CONCAT(firstname,' ', lastname) AS _name,
    RIGHT(email, LENGTH(email) - STRPOS(email, '@')) AS _domain, 
    title AS _job_title,
    phone AS _phone,
    company AS _company,
    CAST(annualrevenue AS STRING) AS _revenue,
    industry__c AS _industry,
    city AS _city,
    state AS _state, 
    country AS _country,
    leadsource AS _lead_source,
    lead_source_description__c AS _lead_source_description,
    leadstatus AS _lead_status,
    programs.name AS _program_name,
    programs.channel AS _program_channel
  FROM `x-marketing.bluemantis_marketo.leads` marketo
  LEFT JOIN `x-marketing.bluemantis_marketo.programs` programs
    ON marketo.acquisitionprogramid = CAST(programs.id AS STRING)
  WHERE email IS NOT NULL
    AND email NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%bluemantis.com%'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY email ORDER BY marketo.id DESC) = 1
),
campaign_list AS (
  SELECT
    _campaign_name,
    _campaign_id,	
    _campaign_code,
    _email_name,
    CAST(_email_id AS STRING) AS _email_id,
    _subject_line,
    _preview,
    _asset_title,
    _asset_type,
    _asset_url,
    _live_date,
    _ad_visual,
    _email_proof,
    _campaign_initiated_by
  FROM `x-marketing.bluemantis_google_sheets.db_email_campaign`
),
email_sent AS (
  SELECT
    _sdc_sequence,
    primary_attribute_value_id AS _campaignID,
    primary_attribute_value AS _campaign,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Sent' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link
  FROM `x-marketing.bluemantis_marketo.activities_send_email`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value_id ORDER BY activitydate DESC) = 1
),
email_delivered AS (
  SELECT
    _sdc_sequence,
    primary_attribute_value_id AS _campaignID,
    primary_attribute_value AS _campaign,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Delivered' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link
  FROM `x-marketing.bluemantis_marketo.activities_email_delivered`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value_id ORDER BY activitydate DESC) = 1 
),
email_open AS (
  SELECT
    _sdc_sequence,
    primary_attribute_value_id AS _campaignID,
    primary_attribute_value AS _campaign,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Opened' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link
  FROM `x-marketing.bluemantis_marketo.activities_open_email`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value_id ORDER BY activitydate DESC) = 1
),
email_click AS (
  SELECT
    _sdc_sequence,
    primary_attribute_value_id AS _campaignID,
    primary_attribute_value AS _campaign,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Clicked' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    link AS _link
  FROM `x-marketing.bluemantis_marketo.activities_click_email`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value_id ORDER BY activitydate DESC) = 1
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
  QUALIFY ROW_NUMBER() OVER(PARTITION BY _leadid, _campaignID ORDER BY _timestamp DESC) = 1
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
final_open AS (
  SELECT 
    *
  FROM new_open_consolidate
  QUALIFY ROW_NUMBER() OVER(PARTITION BY _leadid, _campaignID ORDER BY _timestamp DESC) = 1
),
email_hard_bounce AS (
  SELECT
    _sdc_sequence,
    primary_attribute_value_id AS _campaignID,
    primary_attribute_value AS _campaign,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Hard Bounced' AS _engagement,
    details AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link
  FROM `x-marketing.bluemantis_marketo.activities_email_bounced` 
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value_id ORDER BY activitydate DESC) = 1
),
email_soft_bounce AS (
  SELECT
    _sdc_sequence,
    primary_attribute_value_id AS _campaignID,
    primary_attribute_value AS _campaign,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Soft Bounced' AS _engagement,
    details AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link
  FROM `x-marketing.bluemantis_marketo.activities_email_bounced_soft`  
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value_id ORDER BY activitydate DESC) = 1
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
    d.*
  FROM email_delivered d
  LEFT JOIN email_soft_hard_bounced b 
    ON d._campaignID = b._campaignID 
    AND d._leadid = b._leadid
  WHERE b._campaignID IS NULL 
    AND b._leadid IS NULL
),
email_download AS (
  SELECT
    _sdc_sequence,
    primary_attribute_value_id AS _campaignID,
    primary_attribute_value AS _campaign,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Downloaded' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link
  FROM `x-marketing.bluemantis_marketo.activities_fill_out_form`
  WHERE primary_attribute_value NOT LIKE '%TEST 2X%'
    AND primary_attribute_value NOT LIKE '%Email Unsubscribe Form%'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value_id ORDER BY activitydate DESC) = 1
),
email_unsubscribed AS (
  SELECT
    _sdc_sequence,
    primary_attribute_value_id AS _campaignID,
    primary_attribute_value AS _campaign,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'Unsubscribed' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid,
    '' AS _link
  FROM `x-marketing.bluemantis_marketo.activities_unsubscribe_email`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value_id ORDER BY activitydate DESC) = 1
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
  FROM final_open
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
  engagements._sdc_sequence,
  engagements._campaignID AS _email_campaign_id,
  engagements._campaign,
  engagements._subject,
  engagements._timestamp,
  engagements._engagement,
  engagements._description,
  engagements._link,
  COALESCE(REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)'), "Email") AS _utm_source,
  REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utm_medium,
  REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utm_content,
  prospect_info.*,
  campaign_list.* EXCEPT (_email_id)
FROM engagements_combined AS engagements
JOIN prospect_info
  ON engagements._leadid = prospect_info._prospect_id
JOIN campaign_list
  ON _email_id = engagements._campaignID;