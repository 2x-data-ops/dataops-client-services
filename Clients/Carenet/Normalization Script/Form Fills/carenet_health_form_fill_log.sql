TRUNCATE TABLE `x-marketing.carenet_health.db_form_fill_log`;
INSERT INTO `x-marketing.carenet_health.db_form_fill_log` 
-- CREATE OR REPLACE TABLE `x-marketing.carenet_health.db_form_fill_log` AS
WITH activity AS (
  SELECT
    CAST(NULL AS STRING) AS devicetype,
    CASE
      WHEN form.value.page_url LIKE '%utm_content%' 
      THEN REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_content=([^&]+)')
      ELSE CAST(NULL AS STRING)
    END AS _campaignID,
    form.value.title AS form_title,
    properties.email.value AS email,
    COALESCE(
      associated_company.properties.domain.value, 
      RIGHT(properties.email.value, LENGTH(properties.email.value)-STRPOS(properties.email.value, '@')) 
    ) AS _domain,
    vid,
    property_salesforceaccountid.value AS salesforceaccountid,
    property_salesforcecontactid.value AS salesforcecontactid,
    form.value.timestamp AS _timestamp,
    form.value.page_url AS description,
    campaignguid,
    property_company.value AS company_name,
    associated_company.properties.name.value AS accociated_company_name
  FROM `x-marketing.carenet_health_hubspot.contacts` contacts,
    UNNEST(form_submissions) AS form
  JOIN `x-marketing.carenet_health_hubspot.forms` forms
    ON form.value.form_id = forms.guid 
),
forms AS (
  SELECT
    activity.email AS _email,
    _domain,
    activity._timestamp AS _timestamp,
    EXTRACT(WEEK
    FROM
      activity._timestamp) AS _week,
    EXTRACT(YEAR
    FROM
      activity._timestamp) AS _year,
    form_title AS _form_title,
    'Form Filled' AS _engagement,
    activity.description AS _description,
    REGEXP_EXTRACT(activity.description, r'[?&]utm_source=([^&]+)') AS _utmsource,
    COALESCE(REGEXP_EXTRACT(activity.description, r'[?&]utm_campaign=([^&]+)'), campaign.name) AS _utmcampaign,
    REGEXP_EXTRACT(activity.description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
    REGEXP_EXTRACT(activity.description, r'[?&]utm_content=([^&]+)') AS _utmcontent,
    activity.description AS _fullurl,
    company_name,
    accociated_company_name,
    salesforceaccountid,
    salesforcecontactid
  FROM activity
  LEFT JOIN `x-marketing.carenet_health_hubspot.campaigns` campaign
    ON activity._campaignID = CAST(campaign.id AS STRING)
  WHERE DATE(_timestamp) >= '2024-03-01'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY email, description ORDER BY _timestamp DESC) = 1 
)
SELECT * FROM forms;