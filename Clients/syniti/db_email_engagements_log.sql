CREATE OR REPLACE TABLE `x-marketing.syniti.db_email_engagements_log`
PARTITION BY DATE(_timestamp)
CLUSTER BY _engagement, _domain
AS

WITH prospect_info AS (
  SELECT
    CAST(vid AS STRING) AS _prospectid,
    CONCAT(properties.firstname.value,' ', properties.lastname.value) AS _name,
    properties.phone.value AS _phone,
    properties.jobtitle.value AS _title,
    associated_company.properties.partner_tier__c.value AS _tier,
    properties.company.value AS _company,
    associated_company.properties.domain.value AS _domain,
    properties.industry.value AS _industry,
    properties.country.value AS _country,
    properties.city.value AS _city,
    CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
    CAST(associated_company.properties.numberofemployees.value AS STRING) AS _employees,
    properties.job_function.value AS _function,
    properties.state.value AS _state,
    CAST(properties.lifecyclestage.value AS STRING) AS _lifecyclestage,
    properties.email.value AS _email
  FROM `x-marketing.syniti_hubspot.contacts`
  WHERE properties.email.value IS NOT NULL
  QUALIFY ROW_NUMBER() OVER( PARTITION BY property_email.value, CONCAT(properties.firstname.value,' ', properties.lastname.value) ORDER BY vid DESC) = 1
),
base_email AS (
  SELECT
    activity.id,
    activity._sdc_sequence AS _sdc_sequence,
    CAST(activity.emailcampaignid AS STRING) AS _campaignid,
    campaign.name AS _campaignname,
    activity.recipient AS _email,
    CAST(activity.created AS TIMESTAMP) AS _timestamp,
    activity.url AS _description,
    activity.devicetype AS _device_type,
    CAST(activity.linkid AS STRING) AS _linkid,
    CAST(activity.duration AS STRING) AS _duration,
    response AS _response,
    activity.type,
    activity.filteredevent,
  FROM `x-marketing.syniti_hubspot.email_events` activity
  LEFT JOIN `x-marketing.syniti_hubspot.campaigns` campaign
  ON activity.emailcampaignid = campaign.id
  AND campaign.name IS NOT NULL
),
email_sent AS (
  SELECT *,
  'Sent' AS _engagement
  FROM base_email
  WHERE type = 'SENT'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaignname ORDER BY _timestamp DESC) = 1
),
email_delivered AS (
  SELECT *,
    'Delivered' AS _engagement
  FROM base_email
  WHERE type = 'DELIVERED'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaignname ORDER BY _timestamp DESC) = 1
),
email_opened AS (
  SELECT *,
    'Opened' AS _engagement
  FROM base_email
  WHERE type = 'OPEN'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaignname ORDER BY _timestamp DESC) = 1
),
email_bounced AS (
  SELECT *,
    'Bounced' AS _engagement
  FROM base_email
  WHERE type = 'BOUNCE'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaignname ORDER BY _timestamp DESC) = 1
),
email_clicked AS (
  SELECT *,
    'Clicked' AS _engagement
  FROM base_email
  WHERE type = 'CLICK'
    AND filteredevent = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaignname ORDER BY _timestamp DESC) = 1
),
email_unsubscribed AS (
  SELECT base_email.*,
    'Unsubcribed' AS _engagement
  FROM `x-marketing.syniti_hubspot.subscription_changes` subs
  CROSS JOIN UNNEST(subs.changes) AS status
  JOIN base_email
    ON status.value.causedbyevent.id = base_email.id
  WHERE type = 'STATUSCHANGE'
    AND status.value.change = 'UNSUBSCRIBED'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaignname ORDER BY _timestamp DESC) = 1
),
email_dropped AS (
  SELECT *,
    'Dropped' AS _engagement
  FROM base_email
    WHERE type = 'DROPPED'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaignname ORDER BY _timestamp DESC) = 1
),
email_deferred AS (
  SELECT *,
    'Deferred' AS _engagement
  FROM base_email
  WHERE type = 'DEFERRED'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaignname ORDER BY _timestamp DESC) = 1
),
email_suppressed AS (
  SELECT *,
    'Suppressed' AS _engagement
  FROM base_email
  WHERE type = 'SUPPRESSED'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaignname ORDER BY _timestamp DESC) = 1
),
engagements_combined AS (
  SELECT * FROM email_sent
  UNION ALL
  SELECT * FROM email_delivered
  UNION ALL
  SELECT * FROM email_opened
  UNION ALL
  SELECT * FROM email_bounced
  UNION ALL
  SELECT * FROM email_clicked
  UNION ALL
  SELECT * FROM email_unsubscribed
  UNION ALL
  SELECT * FROM email_dropped
  UNION ALL
  SELECT * FROM email_deferred
  UNION ALL
  SELECT * FROM email_suppressed
)
SELECT
  _sdc_sequence,
  _campaignid,
  _campaignname,
  engagements_combined._email,
  _timestamp,
  _engagement,
  _description,
  _device_type,
  _linkid,
  _duration,
  _response,
  _prospectid,
  _name,
  _domain,
  _title,
  _function,
  -- _seniority,
  _phone,
  _company,
  _revenue,
  _industry,
  _city,
  _state,
  _country,
  _lifecyclestage,
FROM engagements_combined
LEFT JOIN prospect_info
ON engagements_combined._email = prospect_info._email
