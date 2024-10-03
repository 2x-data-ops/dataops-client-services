TRUNCATE TABLE `x-marketing.komodohealth.db_email_engagements_log`;

INSERT INTO `x-marketing.komodohealth.db_email_engagements_log` (
  _sdc_sequence,
  _campaignID,
  _utmcampaign,
  _subject,
  _email,
  _timestamp,
  _week,
  _engagement,
  _description,
  _device_type,
  _linkid,
  _duration,
  _response,
  _utm_source,
  _utm_medium,
  _utm_content,
  _prospectID,
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
  _leadSourceSfdc,
  _industryGroup2X,
  _wbnrLeadSource,
  _leadSourceTypeSfdc,
  _leadSourceConferenceEvent,
  _companyType,
  _optWebEvent,
  _optRepPers,
  _optProdUpdate,
  _optNews,
  _optResearch,
  _createDate,
  _latestSource,
  _originalSource,
  _marketingSegment,
  _latestSourceDrillDown1,
  _latestSourceDrillDown2,
  _recentConversion,
  _dateEnteredMQL
)
WITH prospect_info AS (
    SELECT
      CAST(vid AS STRING) AS _id,
      property_email.value AS _email,
      CONCAT(
        property_firstname.value,
        ' ',
        property_lastname.value
      ) AS _name,
      COALESCE(
        associated_company.properties.domain.value,
        property_hs_email_domain.value
      ) AS _domain,
      properties.jobtitle.value AS _jobtitle,
      properties.job_function.value AS _function,
      property_seniority__2x_grouping_.value AS _seniority,
      property_phone.value AS _phone,
      property_company.value AS _company,
      CAST(
        associated_company.properties.annualrevenue.value AS STRING
      ) AS _revenue,
      property_industry.value AS _industry,
      property_city.value AS _city,
      property_state.value AS _state,
      property_country.value AS _country,
      '' AS _persona,
      property_lifecyclestage.value AS _lifecycleStage,
      property_leadsource.value AS _leadSourceSfdc,
      property_industry__grouped_2x_.value AS _industryGroup2X,
      property_wbnr_lead_source.value AS _wbnrLeadSource,
      property_lead_source_type_sfdc.value AS _leadSourceTypeSfdc,
      property_lead_source_drill_down_1.value AS _leadSourceConferenceEvent,
      associated_company.properties.type.value AS _companyType,
      property_hs_email_optout_14401253.value AS _optWebEvent,
      property_hs_email_optout_14402825.value AS _optRepPers,
      property_hs_email_optout_44383032.value AS _optProdUpdate,
      property_hs_email_optout_7475181.value AS _optNews,
      property_hs_email_optout_7613592.value AS _optResearch,
      properties.createdate.value AS _createDate,
      properties.hs_latest_source.value AS _latestSource,
      properties.hs_analytics_source.value AS _originalSource,
      properties.marketing_segment.value AS _marketingSegment,
      properties.hs_latest_source_data_1.value AS _latestSourceDrillDown1,
      properties.hs_latest_source_data_2.value AS _latestSourceDrillDown2,
      properties.recent_conversion_event_name.value AS _recentConversion,
      properties.hs_v2_date_entered_marketingqualifiedlead.value AS _dateEnteredMQL
    FROM `x-marketing.komodohealth_hubspot.contacts` k
    WHERE property_email.value IS NOT NULL
      AND property_email.value NOT LIKE '%2x.marketing%'
      AND property_email.value NOT LIKE '%komodohealth.com%'
    --QUALIFY ROW_NUMBER() OVER(PARTITION BY property_email.value, CONCAT(property_firstname.value, ' ', property_lastname.value) ORDER BY vid DESC) = 1
  ),
  Sent AS (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      EXTRACT(
        WEEK
        FROM
          activity.created
      ) + 1 AS _week,
      'Sent' AS _engagement,
      url AS _description,
      devicetype AS _devicetype,
      CAST(linkid AS STRING) AS _linkid,
      --appname,
      CAST(duration AS STRING) AS _duration,
      response,
    FROM `x-marketing.komodohealth_hubspot.email_events` activity
    JOIN `x-marketing.komodohealth_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'SENT'
      AND activity.recipient NOT LIKE '%2x.marketing%'
      AND activity.recipient NOT LIKE '%komodohealth.com%'
      AND EXTRACT(
        YEAR
        FROM
          created
      ) >= 2023
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  dropped AS (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      EXTRACT(
        WEEK
        FROM
          activity.created
      ) + 1 AS _week,
      activity.type AS _engagement,
      url AS _description,
      devicetype AS _devicetype,
      CAST(linkid AS STRING) AS _linkid,
      --appname,
      CAST(duration AS STRING) AS _duration,
      response,
    FROM `x-marketing.komodohealth_hubspot.email_events` activity
    JOIN `x-marketing.komodohealth_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'DROPPED'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_sent AS (
    SELECT
      Sent.*
    FROM Sent
    LEFT JOIN dropped
      ON Sent._email = dropped._email
      AND Sent._campaignID = dropped._campaignID
    WHERE dropped._email IS NULL
  ),
  hard_bounced AS (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      EXTRACT(
        WEEK
        FROM
          activity.created
      ) + 1 AS _week,
      activity.type AS _engagement,
      url AS _description,
      devicetype AS _devicetype,
      CAST(CAST(linkid AS STRING) AS STRING) AS _linkid,
      --appname,
      CAST(duration AS STRING) AS _duration,
      response,
    FROM `x-marketing.komodohealth_hubspot.subscription_changes`,
      UNNEST (changes) AS status
    JOIN `x-marketing.komodohealth_hubspot.email_events` activity
      ON status.value.causedbyevent.id = activity.id
    JOIN `x-marketing.komodohealth_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'BOUNCE'
      AND status.value.change = 'BOUNCED'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  delivered AS (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      EXTRACT(
        WEEK
        FROM
          activity.created
      ) + 1 AS _week,
      'Delivered' AS _engagement,
      url AS _description,
      devicetype AS _devicetype,
      CAST(CAST(linkid AS STRING) AS STRING) AS _linkid,
      --appname,
      CAST(duration AS STRING) AS _duration,
      response,
    FROM `x-marketing.komodohealth_hubspot.email_events` activity
    JOIN `x-marketing.komodohealth_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'DELIVERED'
      AND activity.recipient NOT LIKE '%2x.marketing%'
      AND activity.recipient NOT LIKE '%komodohealth.com%'
      AND EXTRACT(
        YEAR
        FROM
          created
      ) >= 2023
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_delivered AS (
    SELECT
      delivered.*
    FROM delivered
    LEFT JOIN dropped
      ON delivered._email = dropped._email
      AND delivered._campaignID = dropped._campaignID
    LEFT JOIN hard_bounced
      ON delivered._email = hard_bounced._email
      AND delivered._campaignID = hard_bounced._campaignID
    WHERE hard_bounced._email IS NULL
      AND dropped._email IS NULL
  ),
  email_open AS (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      EXTRACT(
        WEEK
        FROM
          activity.created
      ) + 1 AS _week,
      'Opened' AS _engagement,
      url AS _description,
      devicetype AS _devicetype,
      CAST(linkid AS STRING) AS _linkid,
      --appname,
      CAST(duration AS STRING) AS _duration,
      response,
    FROM `x-marketing.komodohealth_hubspot.email_events` activity
    JOIN `x-marketing.komodohealth_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'OPEN'
      AND filteredevent = FALSE
      AND activity.recipient NOT LIKE '%2x.marketing%'
      AND activity.recipient NOT LIKE '%komodohealth.com%'
      AND EXTRACT(
        YEAR
        FROM
          created
      ) >= 2023
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_click AS (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      EXTRACT(
        WEEK
        FROM
          activity.created
      ) + 1 AS _week,
      'Clicked' AS _engagement,
      url AS _description,
      devicetype AS _devicetype,
      CAST(linkid AS STRING) AS _linkid,
      --appname,
      CAST(duration AS STRING) AS _duration,
      response,
    FROM `x-marketing.komodohealth_hubspot.email_events` activity
    JOIN `x-marketing.komodohealth_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'CLICK'
      AND filteredevent = FALSE
      AND activity.recipient NOT LIKE '%2x.marketing%'
      AND activity.recipient NOT LIKE '%komodohealth.com%'
      AND EXTRACT(
        YEAR
        FROM
          created
      ) >= 2023
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_bounce AS (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      EXTRACT(
        WEEK
        FROM
          activity.created
      ) + 1 AS _week,
      'Bounced' AS _engagement,
      url AS _description,
      devicetype AS _devicetype,
      CAST(linkid AS STRING) AS _linkid,
      --appname,
      CAST(duration AS STRING) AS _duration,
      response,
    FROM `x-marketing.komodohealth_hubspot.email_events` activity
    JOIN `x-marketing.komodohealth_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'BOUNCE'
      AND activity.recipient NOT LIKE '%2x.marketing%'
      AND activity.recipient NOT LIKE '%komodohealth.com%'
      AND EXTRACT(
        YEAR
        FROM
          created
      ) >= 2023
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_unsubcribed AS (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      activity.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      EXTRACT(
        WEEK
        FROM
          activity.created
      ) + 1 AS _week,
      'Unsubscribed' AS _engagement,
      url AS _description,
      devicetype AS _devicetype,
      CAST(linkid AS STRING) AS _linkid,
      --appname,
      CAST(duration AS STRING) AS _duration,
      response,
    FROM `x-marketing.komodohealth_hubspot.subscription_changes`,
      UNNEST (changes) AS status
    JOIN `x-marketing.komodohealth_hubspot.email_events` activity
      ON status.value.causedbyevent.id = activity.id
    JOIN `x-marketing.komodohealth_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'STATUSCHANGE'
      AND activity.recipient NOT LIKE '%2x.marketing%'
      AND activity.recipient NOT LIKE '%komodohealth.com%'
      AND EXTRACT(
        YEAR
        FROM
          created
      ) >= 2023
      AND status.value.change = 'UNSUBSCRIBED'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
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
      EXTRACT(
        WEEK
        FROM
          form.value.timestamp
      ) AS week,
      'Downloaded' AS engagement,
      form.value.page_url AS description,
      campaignguid,
    FROM `x-marketing.komodohealth_hubspot.contacts` c,
      UNNEST (form_submissions) AS form
    JOIN `x-marketing.komodohealth_hubspot.forms` forms
      ON form.value.form_id = forms.guid
  ),
  email_download AS (
    SELECT
      activity._sdc_sequence,
      CAST(campaign.id AS STRING) AS _campaignID,
      COALESCE(form_title, campaign.name) AS _campaign,
      campaign.subject,
      activity.email AS _email,
      activity.timestamp AS _timestamp,
      activity.week + 1 AS _week,
      'Downloaded' AS _engagement,
      activity.description AS _description,
      activity.devicetype,
      '' AS _linkid,
      '' AS _duration,
      "" AS response,
    FROM form_filled activity
    JOIN `x-marketing.komodohealth_hubspot.campaigns` campaign
      ON activity._utmcontent = CAST(campaign.id AS STRING)
    WHERE activity.email NOT LIKE '%2x.marketing%'
      AND activity.email NOT LIKE '%komodohealth.com%'
      AND EXTRACT(
        YEAR
        FROM
          activity.timestamp
      ) >= 2023
    QUALIFY ROW_NUMBER() OVER(PARTITION BY email, campaign.name ORDER BY TIMESTAMP DESC) = 1
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
    FROM email_unsubcribed
    UNION ALL
    SELECT
      *
    FROM email_download
  )
SELECT
  engagements.*,
  COALESCE(
    REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)'),
    "Email"
  ) AS _utmsource,
  REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
  REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utmcontent,
  prospect_info.* EXCEPT (_email)
FROM engagements_combined AS engagements
LEFT JOIN prospect_info
  ON engagements._email = prospect_info._email;

-- LABEL BOT, FALSE DELIVERED, DROPPED, NOT SENT, SHOW EXPORT
--- Label Bots
UPDATE `x-marketing.komodohealth.db_email_engagements_log` origin
SET origin._isBot = 'Yes'
FROM
  (
    SELECT
      CASE
        WHEN TIMESTAMP_DIFF(click._timestamp, open._timestamp, SECOND) <= 5 THEN click._email
        ELSE NULL
      END AS _email,
      click._utmcampaign
    FROM `x-marketing.komodohealth.db_email_engagements_log` AS click
    JOIN `x-marketing.komodohealth.db_email_engagements_log` AS open
      ON LOWER(click._email) = LOWER(open._email)
      AND click._utmcampaign = open._utmcampaign
    WHERE click._engagement = 'Clicked'
      AND open._engagement = 'Opened'
    EXCEPT DISTINCT
    SELECT
      conversion._email,
      conversion._utmcampaign
    FROM `x-marketing.komodohealth.db_email_engagements_log` AS conversion
    WHERE conversion._engagement IN ('Downloaded')
  ) bot
WHERE origin._email = bot._email
  AND origin._utmcampaign = bot._utmcampaign
  AND origin._engagement IN ('Clicked', 'Opened');