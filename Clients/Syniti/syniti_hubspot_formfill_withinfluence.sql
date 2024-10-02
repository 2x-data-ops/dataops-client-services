/*

Scheduler: syniti_hubspot_formfill_withinfluence
Specs:
  - PARTITION BY DATE(_conversion_date)
  - CLUSTER BY _channel, _engagement

*/

TRUNCATE TABLE `x-marketing.syniti.hubspot_formfill_withinfluence`;

INSERT INTO `x-marketing.syniti.hubspot_formfill_withinfluence` (
  _sdc_sequence,
  _email,
  _associated_company_name,
  _associated_company_id,
  _associated_company_domain,
  _first_name,
  _last_name,
  _country,
  _region,
  _utm_source,
  _utm_campaign,
  _utm_medium,
  _utm_content,
  _conversion_title,
  _contact_id,
  _conversion_date,
  _conversion_page,
  _6sense_segment_name_new,
  _channel,
  _salesforce_account_id,
  _salesforce_contact_id,
  _companyname,
  _domain,
  _engagement,
  _influenced_source,
  _is_influenced_formfill
)

WITH contact_details AS (
   SELECT
    contact._sdc_sequence,
    properties.email.value AS _email,
    associated_company.properties.name.value AS _associated_company_name,
    associated_company.company_id AS _associated_company_id,
    associated_company.properties.domain.value AS _associated_company_domain,
    properties.firstname.value AS _first_name ,
    properties.lastname.value AS _last_name,
    COALESCE(associated_company.properties.country.value, properties.country.value) AS _country,
    COALESCE(properties.state.value, associated_company.properties.state.value) AS _region,
    REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_source=([^&]+)') AS _utm_source,
    REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_campaign=([^&]+)') AS _utm_campaign,
    REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_medium=([^&]+)') AS _utm_medium,
    REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_content=([^&]+)') AS _utm_content,
    form.value.title AS _conversion_title,
    contact.vid AS _contact_id,
    form.value.timestamp AS _conversion_date,
    form.value.page_url AS _conversion_page,
    COALESCE(properties.n6sense_segment_name__new_.value, property_n6sense_segment_name__new_.value) AS _6sense_segment_name_new,
    CASE
      WHEN properties.n6sense_segment_name__new_.value IS NOT NULL THEN '6sense'
      WHEN REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_medium=([^&]+)') = 'linkedin' THEN 'linkedin'
      WHEN REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_medium=([^&]+)') = 'cpc' THEN 'google'
    END AS _channel,
    COALESCE(properties.salesforceaccountid.value, property_salesforceaccountid.value) AS _salesforce_account_id,
    COALESCE(properties.salesforcecontactid.value, property_salesforcecontactid.value) AS _salesforce_contact_id
  FROM `x-marketing.syniti_hubspot.contacts` contact, UNNEST(form_submissions) AS form
  JOIN `x-marketing.syniti_hubspot.forms` forms
    ON  form.value.form_id = forms.guid
),
engagement_details AS (
  WITH linkedin_details AS (
    SELECT
      DISTINCT 
      _companyname,
      _domain,
      metric.metric_type AS _engagement,
      "LinkedIn" AS _influenced_source
    FROM `x-marketing.syniti_mysql.syniti_db_linkedin_engaged_accounts`,
      -- using unnest to sort all linkedin engagement into one column
      UNNEST([
        STRUCT('LinkedIn Impression' AS metric_type),
        STRUCT('LinkedIn Clicks' AS metric_type)
      ]) AS metric
  ),
  sixsense_details AS (
    SELECT
      DISTINCT
      _6sensecompanyname,
      _6sensedomain,
      _engagement,
      "6sense" AS _influenced_source
    FROM x-marketing.syniti.db_6sense_engagement_log
  )
  SELECT * FROM linkedin_details
  UNION ALL
  SELECT * FROM sixsense_details
)
SELECT *,
  CASE
    WHEN engagement_details._influenced_source IS NOT NULL 
      OR engagement_details._influenced_source IS NOT NULL THEN 'YES'
    ELSE 'NO'
  END AS _is_influenced_formfill,
FROM contact_details
LEFT JOIN engagement_details
  ON contact_details._associated_company_domain = engagement_details._domain
  OR contact_details._associated_company_name = engagement_details._companyname
-- contact should have a duplication, but it is unique by its associated company name, domain, and its engagement
QUALIFY ROW_NUMBER() OVER (PARTITION BY _sdc_sequence, _contact_id, _domain, _companyname, _engagement) = 1;
