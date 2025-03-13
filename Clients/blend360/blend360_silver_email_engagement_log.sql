--CREATE OR REPLACE TABLE `x-marketing.blend360.db_email_engagements_log` AS
TRUNCATE TABLE `x-marketing.blend360.db_email_engagements_log`;

INSERT INTO `x-marketing.blend360.db_email_engagements_log` (
  _campaignID,
  _campaign,
  _subject,
  _email,
  _timestamp,
  _engagement,
  _description,
  _devicetype,
  linkid,
  duration,
  response,
  _utmsource,
  _utmmedium,
  _utmcontent,
  _id,
  _name,
  _domain,
  _jobtitle,
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
  _leadScore,
  _formSubmissions,
  _pageViews,
  _unsubscribed,
  _liveDate,
  _contentTitle,
  _screenshot,
  _landingPage,
  _emailfilters,
  _costofevent,
  _preposteventemail,
  _eventlevel,
  _audience
)
WITH prospect_info AS (
  SELECT 
    DISTINCT CAST(_id AS STRING) AS _id,
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
    _city,
    _state,
    _country,
    _persona,
    _lifecycleStage,
    _leadScore,
    -- _jobLevel,
    _formSubmissions,
    _pageViews,
    _unsubscribed -- _emailClicks,
    -- _emailOpens
  FROM `blend360.db_icp_database_log`
  WHERE _email IS NOT NULL
    AND _email NOT LIKE '%2x.marketing%'
    AND _email NOT LIKE '%blend360.com%'
),
airtable_info AS (
  SELECT
    CAST(_campaign_id AS INT64) AS _campaign_id,
    CASE
      WHEN LENGTH(TRIM(_live_date)) = 0 THEN NULL
      ELSE CAST(_live_date AS TIMESTAMP)
    END AS _liveDate,
    _campaign_name AS _contentTitle,
    '' AS _screenshot,
    _landing_page_url,
    _email_filters AS _emailfilters,
    '' AS _costofevent,
    '' AS _preposteventemail,
    '' AS _eventlevel,
    _audience
  FROM `x-marketing.blend360_google_sheets.db_email_campaign`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(_campaign_id AS INT64) ORDER BY _campaign_name DESC) = 1
),
total_sent AS (
  SELECT
    CAST(activity.emailcampaignid AS STRING) AS _campaignID,
    campaign.name AS _campaign,
    activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Sent' AS _engagement,
    url AS _description,
    devicetype AS _devicetype,
    CAST(linkid AS STRING) AS linkid,
    CAST(duration AS STRING) AS duration,
    response
  FROM `x-marketing.blend360_hubspot_v2.email_events` activity
  JOIN `x-marketing.blend360_hubspot_v2.campaigns` campaign
    ON activity.emailcampaignid = campaign.id
  WHERE activity.type = 'SENT'
    AND campaign.name IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
),
email_dropped AS (
  SELECT
    main.recipient AS _email,
    CAST(main.emailcampaignid AS STRING) AS _campaignID,
    side.name AS _contentTitle,
    main.url AS _description,
    main.created AS _timestamp,
    'Dropped' AS _engagement
  FROM `x-marketing.blend360_hubspot_v2.email_events` main
  JOIN `x-marketing.blend360_hubspot_v2.campaigns` side
    ON main.emailcampaignid = side.id
  WHERE main.type = 'DROPPED'
    AND main.recipient NOT LIKE '%2x.marketing%'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY main.recipient, main.emailcampaignid ORDER BY main.created DESC) = 1
),
email_sent AS (
  SELECT
    *
  FROM total_sent
  WHERE CONCAT(_email, _campaignID) NOT IN (
      SELECT
        CONCAT(_email, _campaignID)
      FROM email_dropped
    )
),
total_delivered AS (
  SELECT
    CAST(activity.emailcampaignid AS STRING) AS _campaignID,
    campaign.name AS _campaign,
    activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Delivered' AS _engagement,
    url AS _description,
    devicetype AS _devicetype,
    CAST(linkid AS STRING) AS linkid,
    CAST(duration AS STRING) AS duration,
    response
  FROM `x-marketing.blend360_hubspot_v2.email_events` activity
  JOIN `x-marketing.blend360_hubspot_v2.campaigns` campaign
    ON activity.emailcampaignid = campaign.id
  WHERE activity.type = 'DELIVERED'
    AND campaign.name IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
),
email_bounce AS (
  SELECT
    -- activity._sdc_sequence,
    CAST(activity.emailcampaignid AS STRING) AS _campaignID,
    campaign.name AS _campaign,
    activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Bounced' AS _engagement,
    url AS _description,
    devicetype AS _devicetype,
    CAST(linkid AS STRING) AS linkid,
    --appname,
    CAST(duration AS STRING) AS duration,
    response
  FROM `x-marketing.blend360_hubspot_v2.email_events` activity
  JOIN `x-marketing.blend360_hubspot_v2.campaigns` campaign
    ON activity.emailcampaignid = campaign.id
  WHERE activity.type = 'BOUNCE'
    AND activity.recipient NOT LIKE '%2x.marketing%'
    AND activity.recipient NOT LIKE '%blend360.com%'
    AND campaign.name IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
),
email_delivered AS (
  SELECT
    *
  FROM total_delivered
  WHERE CONCAT(_email, _campaignID) NOT IN (
      SELECT
        CONCAT(_email, _campaignID)
      FROM email_bounce
    )
),
email_open AS (
  SELECT
    -- activity._sdc_sequence,
    CAST(activity.emailcampaignid AS STRING) AS _campaignID,
    campaign.name AS _campaign,
    activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Opened' AS _engagement,
    url AS _description,
    devicetype AS _devicetype,
    CAST(linkid AS STRING) AS linkid,
    --appname,
    CAST(duration AS STRING) AS duration,
    response
  FROM `x-marketing.blend360_hubspot_v2.email_events` activity
  JOIN `x-marketing.blend360_hubspot_v2.campaigns` campaign
    ON activity.emailcampaignid = campaign.id
  WHERE activity.type = 'OPEN'
    AND filteredevent = FALSE
    AND activity.recipient NOT LIKE '%2x.marketing%'
    AND activity.recipient NOT LIKE '%blend360.com%'
    AND campaign.name IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
),
total_clicked AS (
  SELECT
    CAST(activity.emailcampaignid AS STRING) AS _campaignID,
    campaign.name AS _campaign,
    activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Clicked' AS _engagement,
    url AS _description,
    devicetype AS _devicetype,
    CAST(linkid AS STRING) AS linkid,
    CAST(duration AS STRING) AS duration,
    response
  FROM `x-marketing.blend360_hubspot_v2.email_events` activity
  JOIN `x-marketing.blend360_hubspot_v2.campaigns` campaign
    ON activity.emailcampaignid = campaign.id
  WHERE activity.type = 'CLICK'
    AND filteredevent = FALSE
    AND activity.recipient NOT LIKE '%2x.marketing%'
    AND activity.recipient NOT LIKE '%blend360.com%'
    AND campaign.name IS NOT NULL
  QUALIFY ROW_NUMBER() OVER ( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
),
email_click AS (
  SELECT
    *
  FROM total_clicked
  WHERE CONCAT(_email, _campaignID) IN (
      SELECT
        CONCAT(_email, _campaignID)
      FROM email_open
    )
),
form_filled AS (
  SELECT
    c._sdc_sequence,
    CAST(NULL AS STRING) AS devicetype,
    REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_source=([^&]+)') AS _utmsource,
    REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_campaign=([^&]+)') AS _utmcampaign,
    REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
    REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_content=([^&]+)') AS _utmcontent,
    form.value.title AS form_title,
    properties.email.value AS email,
    form.value.timestamp AS TIMESTAMP,
    'Downloaded' AS engagement,
    form.value.page_url AS description,
    campaignguid
  FROM `x-marketing.blend360_hubspot_v2.contacts` c,
    UNNEST (form_submissions) AS form
  JOIN `x-marketing.blend360_hubspot_v2.forms` forms
    ON form.value.form_id = forms.guid
),
total_downloaded AS (
  SELECT
    -- activity._sdc_sequence,
    CAST(campaign.id AS STRING) AS _campaignID,
    COALESCE(form_title, campaign.name) AS _campaign,
    campaign.subject,
    activity.email AS _email,
    activity.timestamp AS _timestamp,
    'Downloaded' AS _engagement,
    activity.description AS _description,
    activity.devicetype,
    '' AS linkid,
    '' AS duration,
    "" AS response,
  FROM form_filled AS activity
  LEFT JOIN `x-marketing.blend360_hubspot_v2.campaigns` campaign
    ON activity._utmcontent = CAST(campaign.id AS STRING)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY email, campaign.name ORDER BY TIMESTAMP DESC) = 1
),
email_download AS (
  SELECT
    side._pardotid AS _campaignID,
    side._code AS _campaign,
    main.subject,
    main._email,
    main._timestamp,
    main._engagement,
    main._description,
    main.devicetype,
    main.linkid,
    main.duration,
    main.response
  FROM total_downloaded AS main
  JOIN `x-marketing.blend360_mysql.db_airtable_email` AS side
    ON main._campaign = side._utm_campaign
  QUALIFY ROW_NUMBER() OVER (PARTITION BY main._email, main._campaign ORDER BY main._timestamp DESC) = 1
),
email_unsubscribed AS (
  SELECT
    CAST(main.emailcampaignid AS STRING) AS _campaignID,
    side.name AS _campaign,
    main.subject AS _subject,
    main.recipient AS _email,
    main.created AS _timestamp,
    'Unsubscribed' AS _engagement,
    url AS _description,
    devicetype AS _devicetype,
    CAST(linkid AS STRING) AS linkid,
    --appname,
    CAST(duration AS STRING) AS duration,
    response
  FROM `x-marketing.blend360_hubspot_v2.email_events` main
  JOIN `x-marketing.blend360_hubspot_v2.campaigns` side
    ON main.emailcampaignid = side.id
  WHERE main.type = 'STATUSCHANGE'
    AND main.recipient NOT LIKE '%2x.marketing%'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY main.recipient, side.name ORDER BY main.created DESC) = 1
),
engagements_combined AS (
  SELECT
    *
  FROM email_sent
  UNION ALL
  SELECT
    *
  FROM email_delivered
  UNION ALL
  SELECT
    *
  FROM email_open
  UNION ALL
  SELECT
    *
  FROM email_click
  UNION ALL
  SELECT
    *
  FROM email_bounce
  UNION ALL
  SELECT
    *
  FROM email_download
  UNION ALL
  SELECT
    *
  FROM email_unsubscribed
)
SELECT
  engagements.*,
  COALESCE(
    REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)'),
    "Email"
  ) AS _utmsource,
  REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
  REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utmcontent,
  prospect_info.* EXCEPT (_email),
  airtable_info.* EXCEPT (_campaign_id)
FROM engagements_combined AS engagements
LEFT JOIN prospect_info
  ON engagements._email = prospect_info._email
JOIN airtable_info
  ON engagements._campaignID = CAST(airtable_info._campaign_id AS STRING);



TRUNCATE TABLE `x-marketing.blend360.db_event_members`;
INSERT INTO `x-marketing.blend360.db_event_members` (
  name,
  email,
  phone,
  contact_owner,
  primary_company,
  lead_status,
  marketing_contact_status,
  form_submitted,
  created_date,
  last_activity_date
)
SELECT 
  DISTINCT CONCAT(
    contact.property_firstname.value, ' ',
    contact.property_lastname.value
  )
  AS name,
  contact.property_email.value AS email,
  contact.property_phone.value AS phone,
  CONCAT(
    owner.firstname, ' ',
    owner.lastname
  )
  AS contact_owner,
  contact.associated_company.properties.name.value AS primary_company,
  INITCAP(contact.property_hs_lead_status.value) AS lead_status,
  IF(
    contact.property_hs_marketable_status.value = 'true', 
    'Marketing contact',
    'Non-marketing contact'
  )  
  AS marketing_contact_status,
  form.value.title AS form_submitted,
  contact.property_createdate.value AS created_date,
  contact.property_notes_last_updated.value AS last_activity_date
FROM `blend360_hubspot_v2.contacts` contact, 
  UNNEST(form_submissions) AS form
LEFT JOIN `blend360_hubspot_v2.owners` owner
  ON contact.property_hubspot_owner_id.value = CAST(owner.ownerid AS STRING)
WHERE form.value.title LIKE '%HubSpot Webinar 2023 LP form January 31, 2023 5:00:44 PM CET%'
  AND property_email.value NOT LIKE '%test%'
ORDER BY email;