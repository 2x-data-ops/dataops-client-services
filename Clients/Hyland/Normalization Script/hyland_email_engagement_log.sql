------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------ Email Engagement Log --------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* 
  This script is used typically for the email performance page/dashboard 
  CRM/Platform: Marketo
  Data type: Email Engagement
  Depedency Table: db_tam_database
  Target table: db_email_engagements_log
*/

TRUNCATE TABLE `x-marketing.hyland.db_email_engagements_log`;
INSERT INTO `x-marketing.hyland.db_email_engagements_log` (
  _sdc_sequence,
  _campaignID,
  _utmcampaign,
  _subject,
  _timestamp,
  _engagement,
  _description,
  _utm_source,  
  _utm_medium, 
  _utm_content,
  _prospectID,
  _email,
  _name,
  _domain,
  _title,
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
  _lifecycleStage,
  _leadsourcedetail,
  _mostrecentleadsource,
  _mostrecentleadsourcedetail,
  _programname,
  _programchannel,
  _campaignSentDate,
  EMEAcampaign,
  airtableSegment,
  _hive9owner,
  _campaignowner,
  _campaignstartdate,
  _campaignenddate,
  region__c,
  sub_region__c,
  description,
  _sfdccampaignid
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
    industry AS _industry,
    city AS _city,
    state AS _state, 
    country AS _country,
    "" AS _persona,
    lead_lifecycle_stage__c AS _lifecycle_stage,
    leadsourcedetail AS _lead_source_detail,
    mostrecentleadsource AS _most_recent_lead_source,
    mostrecentleadsourcedetail AS _most_recent_lead_source_detail,
    programs.name AS _program_name,
    programs.channel AS _program_channel
  FROM `x-marketing.hyland_marketo.leads` marketo
  LEFT JOIN `x-marketing.hyland_marketo.programs` programs
    ON marketo.acquisitionprogramid = CAST(programs.id AS STRING)
  WHERE email IS NOT NULL
    AND email NOT LIKE '%2x.marketing%'
    AND email NOT LIKE '%hyland.com%'
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY email
    ORDER BY marketo.id DESC
  ) = 1
),
airtable_emea AS (
  SELECT
    _assetid AS _id,
    IF(
      _senddate = '',
      CAST(null AS TIMESTAMP),
      CAST(_senddate AS TIMESTAMP)
    ) AS _campaign_sent_date,
    IF(
      _assetid IS NOT NULL,
      'Yes',
      'No'
    ) AS _EMEA_campaign,
    'EMEA' AS _airtable_segment,
    _sfcampaignid
  FROM `x-marketing.hyland_mysql.db_airtable_email_emea` 
),
airtable_customermarketing AS (
  SELECT
    _assetid AS _id,
    IF(
      _senddate = '',
      CAST(null AS TIMESTAMP),
      CAST(_senddate AS TIMESTAMP)
    ) AS _campaign_sent_date,
    IF(
      _assetid IS NOT NULL,
      'Yes',
      'No'
    ) AS _EMEA_campaign,
    'Customer Marketing' AS _airtable_segment,
    '' AS _sfcampaignID
  FROM `x-marketing.hyland_mysql.db_airtable_email_customermarketing` 
),
airtable_info AS (
  SELECT * FROM airtable_emea
  UNION ALL
  SELECT* FROM airtable_customermarketing
),
email_sent AS (
  SELECT
    _sdc_sequence,
    CAST(primary_attribute_value_id AS STRING) AS _campaignID,
    primary_attribute_value AS _campaign,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'sent' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid
  FROM `x-marketing.hyland_marketo.activities_send_email`
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC
  ) = 1
),
email_delivered AS (
  SELECT
    _sdc_sequence,
    CAST(primary_attribute_value_id AS STRING) AS _campaignID,
    primary_attribute_value AS _campaign,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'delivered' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid
  FROM `x-marketing.hyland_marketo.activities_email_delivered`
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC
  ) = 1 
),
email_open AS (
  SELECT
    _sdc_sequence,
    CAST(primary_attribute_value_id AS STRING) AS _campaignID,
    primary_attribute_value AS _campaign,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'opened' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid
  FROM `x-marketing.hyland_marketo.activities_open_email`
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC
  ) = 1
),
email_click AS (
  SELECT
    _sdc_sequence,
    CAST(primary_attribute_value_id AS STRING) AS _campaignID,
    primary_attribute_value AS _campaign,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'clicked' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid
  FROM `x-marketing.hyland_marketo.activities_click_email`
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC
  ) = 1
),
unique_click AS (
  SELECT
    DISTINCT
    email_click.*
  FROM email_click
  JOIN email_open 
    ON email_open._leadid = email_click._leadid
),
email_hard_bounce AS (
  SELECT
    _sdc_sequence,
    CAST(primary_attribute_value_id AS STRING) AS _campaignID,
    primary_attribute_value AS _campaign,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'hard_bounced' AS _engagement,
    details AS _description,
    CAST(leadid AS STRING) AS _leadid
  FROM `x-marketing.hyland_marketo.activities_email_bounced` 
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC
  ) = 1
),
email_soft_bounce AS (
  SELECT
    _sdc_sequence,
    CAST(primary_attribute_value_id AS STRING) AS _campaignID,
    primary_attribute_value AS _campaign,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'soft_bounced' AS _engagement,
    details AS _description,
    CAST(leadid AS STRING) AS _leadid
  FROM `x-marketing.hyland_marketo.activities_email_bounced_soft`  
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC
  ) = 1
),
email_download AS (
  SELECT
    _sdc_sequence,
    CAST(primary_attribute_value_id AS STRING) AS _campaignID,
    primary_attribute_value AS _campaign,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'downloaded' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid
  FROM `x-marketing.hyland_marketo.activities_fill_out_form`
  WHERE primary_attribute_value NOT LIKE '%TEST 2X%'
    AND primary_attribute_value NOT LIKE '%Email Unsubscribe Form%'
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC
  ) = 1
),
email_unsubscribed AS (
  SELECT
    _sdc_sequence,
    CAST(primary_attribute_value_id AS STRING) AS _campaignID,
    primary_attribute_value AS _campaign,
    '' AS _subject,
    '' AS _email,
    activitydate AS _timestamp,
    'unsubscribed' AS _engagement,
    '' AS _description,
    CAST(leadid AS STRING) AS _leadid
  FROM `x-marketing.hyland_marketo.activities_unsubscribe_email`
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY leadid, primary_attribute_value_id 
    ORDER BY activitydate DESC
  ) = 1
),
engagements_combined AS (
  SELECT * FROM email_sent
  UNION ALL
  SELECT * FROM email_delivered
  UNION ALL
  SELECT * FROM email_open
  -- UNION ALL
  -- SELECT * FROM email_click
  UNION ALL
  SELECT * FROM unique_click
  UNION ALL
  SELECT * FROM email_hard_bounce
  UNION ALL
  SELECT * FROM email_soft_bounce
  UNION ALL
  SELECT * FROM email_unsubscribed
), 
_all AS (
  SELECT
    engagements.* EXCEPT(_leadid, _email),
    COALESCE(REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)'), "Email") AS _utm_source,
    REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utm_medium,
    REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utm_content,
    prospect_info.*,
    CAST(airtable_info._campaign_sent_date AS TIMESTAMP) AS _campaign_sent_date,
    airtable_info._EMEA_campaign,
    airtable_info._airtable_segment,
    airtable_info._sfcampaignid
  FROM 
    engagements_combined AS engagements
  LEFT JOIN prospect_info
    ON engagements._leadid = prospect_info._id
  JOIN airtable_info
    ON engagements._campaignID = CAST(airtable_info._id AS STRING)
),
user AS (
  SELECT 
    name, 
    id 
  FROM `x-marketing.hyland_salesforce.User`
)
SELECT 
  _all.* EXCEPT (_sfcampaignid),
  hive9_owner__c AS _hive_9owner,
  user.name AS _campaign_owner,
  startdate AS _campaign_start_date,
  enddate AS _campaign_end_date,
  region__c AS _region_c,
  sub_region__c AS _sub_region_c,
  description AS _description,
  sfcampaign.id AS _sfcampaignID
FROM _all
LEFT JOIN `x-marketing.hyland_salesforce.Campaign` sfcampaign 
  ON sfcampaign.id = _all._sfcampaignid
LEFT JOIN user 
  ON user.id = sfcampaign.ownerid;

----------------------------------------------------------Email Campaign Timeline---------------------------------------------------------
CREATE OR REPLACE TABLE `x-marketing.hyland.db_email_details_aggregate` AS
WITH campaign_aggregate AS (
  SELECT 
    _campaignname AS _campaign_name, 
    hive9_owner__c AS _hive_9owner,
    user.name AS _campaign_owner,
    _assetid AS _marketo_id,
    --CASE WHEN _senddate = "" THEN NULL ELSE
    --PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%E*S%Ez",_senddate) END AS _senddate,
    _sfcampaignid AS _sfcampaignID,
    startdate AS _campaign_start_date,
    enddate AS _campaign_end_date,
    region__c AS _region_c,
    sub_region__c AS _sub_region_c,
    description AS _description,
  FROM `x-marketing.hyland_mysql.db_airtable_email_emea` airtable
  LEFT JOIN `x-marketing.hyland_salesforce.Campaign` campaign 
    ON campaign.id = airtable._sfcampaignid
  LEFT JOIN (SELECT name, id FROM `x-marketing.hyland_salesforce.User`) user 
    ON user.id = campaign.ownerid
),
email_aggregate AS(
  SELECT 
    _campaignID,
    _utmcampaign AS _utm_campaign,
    _sfdccampaignid AS _sfdccampaignID,
    SUM(CASE WHEN _engagement = 'sent' THEN 1 ELSE 0 END) AS _sent,
    SUM(CASE WHEN _engagement = 'delivered' THEN 1 ELSE 0 END) AS _delivered,
    SUM(CASE WHEN _engagement = 'soft_bounced' THEN 1 ELSE 0 END) AS _soft_bounced,
    SUM(CASE WHEN _engagement = 'hard_bounced' THEN 1 ELSE 0 END) AS _hard_bounced
  FROM `x-marketing.hyland.db_email_engagements_log` email
  GROUP BY 1,2,3
)

SELECT 
  campaign_aggregate.*, 
  email_aggregate.* EXCEPT(_campaignID,_utm_campaign,_sfdccampaignID)
FROM campaign_aggregate
LEFT JOIN email_aggregate 
  ON email_aggregate._sfdccampaignid = campaign_aggregate._sfcampaignid
WHERE _campaign_start_date IS NOT NULL