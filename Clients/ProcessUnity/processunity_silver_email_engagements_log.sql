TRUNCATE TABLE `x-marketing.processunity.email_engagements_log`;

INSERT INTO `x-marketing.processunity.email_engagements_log` (
  _content_title, 
  _timestamp,
  _leadid, 
  _description,
  _campaignID,
  _engagement,
  _email,
  _name,
  _job_title,
  _persona,
  _account,
  _company,
  _industry,
  _email_activity_count,
  _email_delivered_count,
  _email_opens_count,
  _email_CTA_click_count,
  _content_syndication_activity_count,
  _event_registered,
  _event_attended,
  _webinar_registration_count,
  _website_visit_count,
  _lead_score,
  _lead_nurture_entry,
  _lead_mql_qualified,
  _lead_mql_qualified_date,
  _wf_became_mql_date,
  _sal_qualified_date,
  _leads_sql_qualified_date,
  _lead_source,
  _wf_became_sql_date,
  _wf_became_sal_date,
  _campaign_name,
  _campaign_id,
  _campaign_code,
  _email_name,
  _email_id,
  _subject_line,
  _preview,
  _asset_title,
  _asset_type,
  _landing_page_url,
  _programID,
  _program_name
)
-- CREATE OR REPLACE TABLE `x-marketing.processunity.email_engagements_log` AS
WITH prospect_info AS (
  SELECT
    CAST(_leadID AS STRING) AS _prospectID,
    _email,
    CONCAT(_first_name,' ', _last_name) AS _name,
    _job_title,
    _persona,
    _company_name AS _company,
    _account,
    _industry,
    _email_activity_count,
    _email_delivered_count,
    _email_opens_count,
    _email_CTA_click_count,
    _content_syndication_activity_count,
    _event_registered,
    _event_attended,
    _webinar_registration_count,
    _website_visit_count,
    _lead_score,
    _lead_nurture_entry,
    _lead_mql_qualified,
    _lead_mql_qualified_date,
    _wf_became_mql_date,
    _sal_qualified_date,
    _leads_sql_qualified_date,
    _lead_source,
    _wf_became_sql_date,
    _wf_became_sal_date
  FROM `x-marketing.processunity.marketo_contacts_log` leads
),
sent_email AS (
  SELECT   
    primary_attribute_value AS _content_title, 
    activitydate AS _timestamp, 
    CAST(leadid AS STRING) AS _leadid, 
    '' AS _description,
    CAST(campaignid AS STRING) AS _campaignID,
    primary_attribute_value_id AS _emailID,
    'Sent' AS _engagement,
  FROM `x-marketing.processunity_marketo.activities_send_email`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value ORDER BY activitydate DESC) = 1
),
delivered_email AS (
  SELECT   
    primary_attribute_value AS _content_title, 
    activitydate AS _timestamp, 
    CAST(leadid AS STRING) AS _leadid, 
    '' AS _description,
    CAST(campaignid AS STRING) AS _campaignID,
    primary_attribute_value_id AS _emailID,
    'Delivered' AS _engagement,
  FROM `x-marketing.processunity_marketo.activities_email_delivered`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value ORDER BY activitydate DESC) = 1
),
opened_email AS (
  SELECT   
    primary_attribute_value AS _content_title, 
    activitydate AS _timestamp, 
    CAST(leadid AS STRING) AS _leadid, 
    '' AS _description,
    CAST(campaignid AS STRING) AS _campaignID,
    primary_attribute_value_id AS _emailID,
    'Opened' AS _engagement,
  FROM `x-marketing.processunity_marketo.activities_open_email`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value ORDER BY activitydate DESC) = 1
),
clicked_email AS (
  SELECT   
    primary_attribute_value AS _content_title, 
    activitydate AS _timestamp, 
    CAST(leadid AS STRING) AS _leadid, 
    '' AS _description,
    CAST(campaignid AS STRING) AS _campaignID,
    primary_attribute_value_id AS _emailID,
    'Clicked' AS _engagement,
  FROM `x-marketing.processunity_marketo.activities_click_email`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value ORDER BY activitydate DESC) = 1
),
hard_bounced_email AS (
  SELECT   
    primary_attribute_value AS _content_title, 
    activitydate AS _timestamp, 
    CAST(leadid AS STRING) AS _leadid, 
    '' AS _description,
    CAST(campaignid AS STRING) AS _campaignID,
    primary_attribute_value_id AS _emailID,
    'Hard Bounced' AS _engagement,
  FROM `x-marketing.processunity_marketo.activities_email_bounced`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value ORDER BY activitydate DESC) = 1
),
soft_bounced_email AS (
  SELECT   
    primary_attribute_value AS _content_title, 
    activitydate AS _timestamp, 
    CAST(leadid AS STRING) AS _leadid, 
    '' AS _description,
    CAST(campaignid AS STRING) AS _campaignID,
    primary_attribute_value_id AS _emailID,
    'Soft Bounced' AS _engagement,
  FROM `x-marketing.processunity_marketo.activities_email_bounced_soft`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value ORDER BY activitydate DESC) = 1
),
unsubscribed_email AS (
  SELECT   
    primary_attribute_value AS _content_title, 
    activitydate AS _timestamp, 
    CAST(leadid AS STRING) AS _leadid, 
    '' AS _description,
    CAST(campaignid AS STRING) AS _campaignID,
    primary_attribute_value_id AS _emailID,
    'Unsubscribed' AS _engagement,
  FROM `x-marketing.processunity_marketo.activities_unsubscribe_email`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value ORDER BY activitydate DESC) = 1
),
campaign AS (
  SELECT
    CAST(id AS STRING) AS id,
    name AS _campaign_name,
    CAST(programid AS STRING) AS _programID,
    programname AS _program_name
  FROM `x-marketing.processunity_marketo.campaigns`
),
email_data AS (
  SELECT  
    _campaign_name,
    CAST(_campaign_id AS STRING) AS _campaign_id,
    _campaign_code,
    _email_name,
    CAST(_email_id AS STRING) AS _email_id,
    _subject_line,
    _preview,
    _asset_title,
    _asset_type,
    _landing_page_url
FROM `x-marketing.processunity_google_sheets.db_email_campaign` 
),
engagements_combined AS (
  SELECT * FROM sent_email
  UNION ALL
  SELECT * FROM delivered_email
  UNION ALL
  SELECT * FROM opened_email
  UNION ALL
  SELECT * FROM clicked_email
  UNION ALL
  SELECT * FROM hard_bounced_email
  UNION ALL
  SELECT * FROM soft_bounced_email
  UNION ALL
  SELECT * FROM unsubscribed_email
)
SELECT
  engagements_combined._content_title, 
  engagements_combined._timestamp,
  engagements_combined._leadid, 
  engagements_combined._description,
  engagements_combined._campaignID,
  engagements_combined._engagement,
  prospect_info.* EXCEPT(_prospectID),
  email_data._campaign_name,
  email_data._campaign_id,
  email_data._campaign_code,
  email_data._email_name,
  email_data._email_id,
  email_data._subject_line,
  email_data._preview,
  email_data._asset_title,
  email_data._asset_type,
  email_data._landing_page_url,
  campaign._programID,
  campaign._program_name
FROM engagements_combined
LEFT JOIN prospect_info
  ON engagements_combined._leadid = prospect_info._prospectID
JOIN email_data
  ON engagements_combined._emailID = email_data._email_id
LEFT JOIN campaign
  ON email_data._campaign_id = campaign.id