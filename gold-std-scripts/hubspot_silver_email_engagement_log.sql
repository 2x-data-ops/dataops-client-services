--CREATE OR REPLACE TABLE `x-marketing.equiteq.db_email_engagements_log` AS
TRUNCATE TABLE `x-marketing.equiteq.db_email_engagements_log`;

INSERT INTO `x-marketing.equiteq.db_email_engagements_log` (
  _sdc_sequence,
  _email,
  _campaign_id,
  _campaign_name,
  _timestamp,
  _description,
  _device_type,
  _link_id,
  _duration,
  _response,
  _engagement,
  _prospect_id,
  _name,
  _phone,
  _title,
  _company,
  _domain,
  _industry,
  _country,
  _city,
  _revenue,
  _employees,
  _life_cycle_stage,
  _content_title,
  _campaign_subject,
  _campaign_type,
  _email_category
)
WITH prospect_info AS (
  SELECT
    CAST(vid AS STRING) AS _prospect_id,
    properties.email.value AS _email,
    CONCAT(properties.firstname.value, ' ', properties.lastname.value) AS _name,
    properties.phone.value AS _phone,
    properties.jobtitle.value AS _title,
    properties.company.value AS _company,
    associated_company.properties.domain.value AS _domain,
    REPLACE(INITCAP(associated_company.properties.industry.value), '_', ' ') AS _industry,
    properties.country.value AS _country,
    properties.city.value AS _city,
    CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
    CAST(associated_company.properties.numberofemployees.value AS STRING) AS _employees,
    CASE
      WHEN properties.lifecyclestage.value = '' THEN NULL
      WHEN property_lifecyclestage.value = '65401112' THEN 'Nurture'
      WHEN property_lifecyclestage.value = 'marketingqualifiedlead' THEN 'Marketing Qualified Lead'
      WHEN property_lifecyclestage.value = 'salesqualifiedlead' THEN 'Sales Qualified Lead'
      ELSE INITCAP(CAST(properties.lifecyclestage.value AS STRING))
    END AS _life_cycle_stage
  FROM `x-marketing.equiteq_hubspot.contacts`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _name ORDER BY vid DESC) = 1
),
airtable_info AS (
  SELECT
    CAST(campaign.id AS STRING) AS _campaign_id,
    campaign.name AS _content_title,
    campaign.subject AS _campaign_subject,
    REPLACE(INITCAP(campaign.type), '_', ' ') AS _campaign_type,
    IF(airtable._email_id IS NOT NULL, '2X', 'Equiteq') AS _email_category
  FROM `x-marketing.equiteq_hubspot.campaigns` campaign
  LEFT JOIN `x-marketing.equiteq_google_sheets.db_email_campaign` airtable
    ON CAST(campaign.id AS STRING) = CAST(airtable._email_id AS STRING)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _content_title, _campaign_id ORDER BY _campaign_id) = 1
),
shared_fields AS (
  SELECT
    activity.id AS _email_event_id,
    activity._sdc_sequence AS _sdc_sequence,
    activity.recipient AS _email,
    CAST(activity.emailcampaignid AS STRING) AS _campaign_id,
    campaign.name AS _campaign_name,
    CAST(activity.created AS TIMESTAMP) AS _timestamp,
    activity.url AS _description,
    INITCAP(activity.devicetype) AS _device_type,
    CAST(activity.linkid AS STRING) AS _link_id,
    CAST(activity.duration AS STRING) AS _duration,
    activity.response AS _response,
    activity.type AS _type,
    activity.filteredevent AS _filtered_event
  FROM `x-marketing.equiteq_hubspot.email_events` activity
  JOIN `x-marketing.equiteq_hubspot.campaigns` campaign
    ON activity.emailcampaignid = campaign.id
    AND campaign.name IS NOT NULL
),
dropped AS (
  SELECT
    * EXCEPT (_type, _email_event_id, _filtered_event),
    'Dropped' AS _engagement,
  FROM shared_fields
  WHERE _type = 'DROPPED'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaign_name ORDER BY _timestamp DESC) = 1
),
deferred AS (
  SELECT
    * EXCEPT (_type, _email_event_id, _filtered_event),
    'Deferred' AS _engagement,
  FROM shared_fields
  WHERE _type = 'DEFERRED'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaign_name ORDER BY _timestamp DESC) = 1
),
suppressed AS (
  SELECT
    * EXCEPT (_type, _email_event_id, _filtered_event),
    'Suppressed' AS _engagement,
  FROM shared_fields
  WHERE _type = 'SUPPRESSED'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaign_name ORDER BY _timestamp DESC) = 1
),
opened AS (
  SELECT
    * EXCEPT (_type, _email_event_id, _filtered_event),
    'Opened' AS _engagement,
  FROM shared_fields
  WHERE _type = 'OPEN'
    AND _filtered_event = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaign_name ORDER BY _timestamp DESC) = 1
),
clicked AS (
  SELECT
    * EXCEPT (_type, _email_event_id, _filtered_event),
    'Clicked' AS _engagement,
  FROM shared_fields
  WHERE _type = 'CLICK'
    AND _filtered_event = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaign_name ORDER BY _timestamp DESC) = 1
),
unsubscribed AS (
  SELECT
    shared_fields.* EXCEPT (_type, _email_event_id, _filtered_event),
    'Unsubscribed' AS _engagement,
  FROM `x-marketing.equiteq_hubspot.subscription_changes`,
    UNNEST (changes) AS status
  JOIN shared_fields
    ON status.value.causedbyevent.id = shared_fields._email_event_id
  WHERE _type = 'STATUSCHANGE'
    AND status.value.change = 'UNSUBSCRIBED'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaign_name ORDER BY _timestamp DESC) = 1
),
form_filled AS (
  SELECT
    contacts._sdc_sequence,
    contacts.properties.email.value AS email,
    form.value.title AS form_title,
    form.value.timestamp AS timestamp,
    form.value.page_url AS description,
    CAST(NULL AS STRING) AS devicetype,
    REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_source=([^&]+)') AS _utmsource,
    REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_campaign=([^&]+)') AS _utmcampaign,
    REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
    REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_content=([^&]+)') AS _utmcontent,
  FROM `x-marketing.equiteq_hubspot.contacts` contacts,
    UNNEST (form_submissions) AS form
  JOIN `x-marketing.equiteq_hubspot.forms` forms
    ON form.value.form_id = forms.guid
),
downloaded AS (
  SELECT
    activity._sdc_sequence,
    activity.email AS _email,
    CAST(campaign.id AS STRING) AS _campaign_id,
    COALESCE(form_title, campaign.name) AS _campaign_name,
    activity.timestamp AS _timestamp,
    activity.description AS _description,
    activity.devicetype,
    '' AS linkid,
    '' AS duration,
    '' AS response,
    'Downloaded' AS _engagement,
  FROM form_filled activity
  LEFT JOIN `x-marketing.equiteq_hubspot.campaigns` campaign
    ON activity._utmcontent = CAST(campaign.id AS STRING)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaign_name ORDER BY _timestamp DESC) = 1
),
softbounced AS (
  SELECT
    * EXCEPT (_type, _email_event_id, _filtered_event),
    'Soft Bounced' AS _engagement,
  FROM shared_fields
  WHERE _type = 'BOUNCE'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaign_name ORDER BY _timestamp DESC) = 1
),
hardbounced AS (
  SELECT
    shared_fields.* EXCEPT (_type, _email_event_id, _filtered_event),
    'Hard Bounced' AS _engagement
  FROM `x-marketing.equiteq_hubspot.subscription_changes`,
    UNNEST (changes) AS status
  JOIN shared_fields
    ON status.value.causedbyevent.id = shared_fields._email_event_id
  WHERE status.value.change = 'BOUNCED'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaign_name ORDER BY _timestamp DESC) = 1
),
sent AS (
  SELECT
    * EXCEPT (_type, _email_event_id, _filtered_event),
    'Sent' AS _engagement,
  FROM shared_fields
  WHERE _type = 'SENT'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaign_name ORDER BY _timestamp DESC) = 1
),
delivered AS (
  SELECT
    * EXCEPT (_type, _email_event_id, _filtered_event),
    'Delivered' AS _engagement,
  FROM shared_fields
  WHERE _type = 'DELIVERED'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _email, _campaign_name ORDER BY _timestamp DESC) = 1
),
sent_filtered AS (
  SELECT
    sent.*
  FROM sent
  LEFT JOIN dropped
    ON sent._email = dropped._email
    AND sent._campaign_id = dropped._campaign_id
  WHERE dropped._email IS NULL
),
delivered_filtered AS (
  SELECT
    delivered.*
  FROM delivered
  LEFT JOIN hardbounced
    ON delivered._email = hardbounced._email
    AND delivered._campaign_id = hardbounced._campaign_id
  LEFT JOIN dropped
    ON delivered._email = dropped._email
    AND delivered._campaign_id = dropped._campaign_id
  WHERE hardbounced._email IS NULL
    AND dropped._email IS NULL
),
softbounced_filtered AS (
  SELECT
    softbounced.*
  FROM softbounced
  LEFT JOIN hardbounced
    ON softbounced._email = hardbounced._email
    AND softbounced._campaign_id = hardbounced._campaign_id
  LEFT JOIN delivered
    ON softbounced._email = delivered._email
    AND softbounced._campaign_id = delivered._campaign_id
  WHERE hardbounced._email IS NULL
    AND delivered._email IS NULL
),
hardbounced_filtered AS (
  SELECT
    hardbounced.*
  FROM hardbounced
  JOIN softbounced
    ON hardbounced._email = softbounced._email
    AND hardbounced._campaign_id = softbounced._campaign_id
),
engagements AS (
  SELECT
    *
  FROM sent_filtered
  UNION ALL
  SELECT
    *
  FROM dropped
  UNION ALL
  SELECT
    *
  FROM deferred
  UNION ALL
  SELECT
    *
  FROM suppressed
  UNION ALL
  SELECT
    *
  FROM delivered_filtered
  UNION ALL
  SELECT
    *
  FROM opened
  UNION ALL
  SELECT
    *
  FROM clicked
  UNION ALL
  SELECT
    *
  FROM unsubscribed
  UNION ALL
  SELECT
    *
  FROM hardbounced_filtered
  UNION ALL
  SELECT
    *
  FROM softbounced_filtered
  UNION ALL
  SELECT
    *
  FROM downloaded
),
combine_all AS (
  SELECT
    engagements.*,
    prospect_info.* EXCEPT (_email),
    airtable_info.* EXCEPT (_campaign_id),
  FROM engagements
  LEFT JOIN prospect_info
    ON engagements._email = prospect_info._email
  JOIN airtable_info
    ON engagements._campaign_id = CAST(airtable_info._campaign_id AS STRING)
),
removed_emails AS (
  SELECT
    _email,
    _domain
  FROM combine_all
  WHERE LOWER(_email) LIKE '%equiteq.com%'
    OR LOWER(_email) LIKE '%2x.marketing%'
)
SELECT
  combine_all.*
FROM combine_all
LEFT JOIN removed_emails
  ON combine_all._email = removed_emails._email
WHERE removed_emails._email IS NULL;