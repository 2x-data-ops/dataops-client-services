TRUNCATE TABLE `x-marketing.devo.db_form_fill_log`;
INSERT INTO `x-marketing.devo.db_form_fill_log`

WITH
  forms AS (
  SELECT
    guid,
    campaignguid,
    name AS _form_name,
    STRING_AGG(LOWER(field.value.label), ', \n') AS _labels
  FROM `x-marketing.devo_hubspot_v2.forms`,
    UNNEST(formfieldgroups) AS fieldgrp,
    UNNEST(fieldgrp.value.fields) AS field
  GROUP BY 1, 2, 3 
) , 
owner AS (
  SELECT
    firstname,
    lastname,
    CONCAT(firstname, ' ', lastname) AS _owner_name,
    id AS _owner_id,
    email AS _owner_email,
    userid
  FROM `x-marketing.devo_hubspot_v2.owners` 
) , 
contacts_form AS (
  SELECT
    CAST(NULL AS STRING) AS devicetype,
    IF ( form.value.page_url LIKE '%utm_content%', 
      SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_content=') + 9), '&')[ORDINAL(1)], 
      CAST(NULL AS STRING) ) AS _campaignID,
    IF ( form.value.page_url LIKE '%utm_campaign%', 
      REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, 'utm_campaign=') + 13), '&')[ORDINAL(1)], '%20', ' '), '%3A',':'),
       CAST(NULL AS STRING) ) AS _campaign,
    IF ( form.value.page_url LIKE '%utm_source%', 
      SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_source=') + 8), '&')[ORDINAL(1)], 
      CAST(NULL AS STRING) ) AS _utm_source,
    IF ( form.value.page_url LIKE '%utm_medium%', 
      SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_medium=') + 8), '&')[ORDINAL(1)], 
      CAST(NULL AS STRING) ) AS _utm_medium,
    form.value.title AS _form_title,
    properties.email.value AS _email,
    associated_company.properties.domain.value AS _domain,
    form.value.timestamp AS _timestamp,
    form.value.page_url AS _description,
    property_hubspot_owner_id.value AS _owner_id,
    form.value.form_id AS _form_id,
    vid AS _id,
    property_firstname.value AS _firstname,
    property_lastname.value AS _lastname,
    CONCAT( property_firstname.value,property_lastname.value) AS _name,
    associated_company.properties.name.value AS _associated_company_name,
    associated_company.properties.first_conversion_event_name.value AS _first_conversion_event_name,
    property_associated_company_name.value AS _company_name,
    property_country.value AS _country,
    property_jobtitle.value AS _jobtitle,
    property_job_function.value AS _job_function,
    property_form_submit_date_datestamp.value AS _form_submit_date_datestamp
  FROM `x-marketing.devo_hubspot_v2.contacts` contacts,
    UNNEST(form_submissions) AS form 
)
SELECT
  _id,
  _firstname,
  _lastname,
  _name,
  _associated_company_name,
  _first_conversion_event_name,
  _company_name,
  _country,
  _jobtitle,
  _job_function,
  _email,
  COALESCE(_domain, RIGHT(_email, LENGTH(_email)-STRPOS(_email, '@'))) AS _domain,
  _timestamp,
  EXTRACT(WEEK
  FROM activity._timestamp) AS _week,
  EXTRACT(YEAR FROM activity._timestamp) AS _year,
  'Form Filled' AS _engagement,
  _form_id,
  _form_title,
  _description,
  _campaignID,
  _campaign,
  REGEXP_EXTRACT(activity._description, r'[?&]utm_source=([^&]+)') AS _utmsource,
  REGEXP_EXTRACT(activity._description, r'[?&]utm_campaign=([^&]+)') AS _utmcampaign,
  REGEXP_EXTRACT(activity._description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
  REGEXP_EXTRACT(activity._description, r'[?&]utm_content=([^&]+)') AS _utmcontent,
  activity._description AS _fullurl,
  activity._owner_id,
  _owner_name,
  _owner_email,
  campaign.name AS _campaign_name,
  subject AS _subject,
  _form_name,
  _labels,
  _form_submit_date_datestamp
FROM contacts_form activity
LEFT JOIN `x-marketing.devo_hubspot_v2.campaigns` campaign
  ON  activity._campaignID = CAST(campaign.id AS STRING)
LEFT JOIN owner
  ON activity._owner_id = owner._owner_id
LEFT JOIN forms
  ON activity._form_id = forms.guid 