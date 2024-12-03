--------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------- Accounth Health Script ------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
-- CREATE OR REPLACE TABLE `x-marketing.logicsource.db_consolidated_engagements_log` AS 
TRUNCATE TABLE `x-marketing.logicsource.db_consolidated_engagements_log`;

INSERT INTO `x-marketing.logicsource.db_consolidated_engagements_log` (
  _domain,
  _email,
  _week,
  _year,
  _contentTitle,
  _engagement,
  _description,
  frequency,
  _avg_bombora_score,
  _id,
  _name,
  _jobtitle,
  _seniority,
  _jobrole,
  _lead_segment,
  _source,
  _function,
  _phone,
  _company,
  _leadstatus,
  _revenue,
  _industry,
  _city,
  _state,
  _country,
  _persona,
  _lifecycleStage,
  _sfdcaccountid,
  _sfdccontactid,
  _date,
  _extract_date,
  _t90_days_score
)
#Query to pull all the contacts in the leads table from Hubspot
WITH contacts AS (
  SELECT
    CAST(vid AS STRING) AS _id,
    property_email.value AS _email,
    CONCAT(property_firstname.value, ' ', property_lastname.value ) AS _name,
    COALESCE(
      associated_company.properties.domain.value,
      property_hs_email_domain.value,
      RIGHT(property_email.value, LENGTH(property_email.value) - STRPOS(property_email.value, '@'))
    ) AS _domain,
    properties.jobtitle.value AS _jobtitle,
    CASE
      WHEN property_management_level__organic_.value IS NOT NULL THEN property_management_level__organic_.value
      ELSE property_management_level.value
    END AS _seniority,
    CASE
      WHEN property_job_role__organic_.value IS NOT NULL THEN property_job_role__organic_.value
      ELSE property_job_role.value
    END AS _jobrole,
    property_lead_segment.value AS _lead_segment,
    properties.hs_analytics_source.value AS _source,
    properties.job_function.value AS _function,
    property_phone.value AS _phone,
    associated_company.properties.name.value AS _company,
    property_leadstatus.value AS _leadstatus,
    CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
    CASE
      WHEN associated_company.properties.industry.value = 'RETAIL' THEN 'Retail'
      WHEN associated_company.properties.industry.value = 'INSURANCE' THEN 'Insurance'
      WHEN associated_company.properties.industry.value LIKE '%FOOD_PRODUCTION%' THEN 'Manufacturing'
      WHEN associated_company.properties.industry.value = 'HOSPITALITY' THEN 'Hospitality'
      WHEN associated_company.properties.industry.value = 'HOSPITAL_HEALTH_CARE' THEN 'Hospital & Health Care'
      WHEN associated_company.properties.name.value LIKE '%BT%' THEN 'Media & Internet'
      ELSE associated_company.properties.industry.value
    END AS _industry,
    property_city.value AS _city,
    property_state.value AS _state,
    property_country.value AS _country,
    CAST(associated_company.company_id AS STRING) AS _persona,
    property_lifecyclestage.value AS _lifecycleStage,
    form_submissions,
    property_salesforceaccountid.value AS _sfdcaccountid,
    property_salesforcecontactid.value AS _sfdccontactid,
  FROM `x-marketing.logicsource_hubspot.contacts` k
  LEFT JOIN `x-marketing.logicsource_salesforce.Lead` LEAD
    ON k.properties.salesforceleadid.value = LEAD.id
  WHERE --vid = 12346251
    property_email.value IS NOT NULL
    AND property_email.value NOT LIKE '%2x.marketing%'
    AND property_email.value NOT LIKE '%logicsourceworkplace.com%'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      property_email.value,
      CONCAT(property_firstname.value, ' ', property_lastname.value)
    ORDER BY vid DESC
  ) = 1
),
accounts_combined AS (
  SELECT DISTINCT
    properties.domain.value AS _domain,
    CAST(sfdc.companyid AS STRING) AS _id,
    CAST(NULL AS STRING) AS _name,
    -- CAST(NULL AS STRING) AS _lastname,
    CAST(NULL AS STRING) AS _title,
    CAST(NULL AS STRING) AS _seniority,
    CAST(NULL AS STRING) AS _jobrole,
    CAST(NULL AS STRING) AS _lead_segment,
    CAST(NULL AS STRING) AS _source,
    CAST(NULL AS STRING) AS _function,
    _phone,
    _company,
    CAST(NULL AS STRING) AS _leadstatus,
    CAST(_revenue AS STRING) AS _revenue,
    _industry AS _industry,
    _city AS _city,
    _state AS _state,
    _country AS _country,
    CAST(NULL AS STRING) AS _persona,
    CAST(NULL AS STRING) AS _lifecycleStage,
    _sfdcaccountid,
    CAST(NULL AS STRING) _sfdccontactid,
  FROM contacts
  RIGHT JOIN `logicsource_hubspot.companies` sfdc
    ON contacts._persona = CAST(sfdc.companyid AS STRING)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _domain ORDER BY property_hs_lastmodifieddate.value DESC) = 1
  UNION ALL
  SELECT DISTINCT
    _domain AS _domain,
    CAST(NULL AS STRING) AS _id,
    CAST(NULL AS STRING) AS _name,
    -- CAST(NULL AS STRING) AS _lastname,
    CAST(NULL AS STRING) AS _title,
    CAST(NULL AS STRING) AS _seniority,
    CAST(NULL AS STRING) AS _jobrole,
    CAST(NULL AS STRING) AS _lead_segment,
    CAST(NULL AS STRING) AS _source,
    CAST(NULL AS STRING) AS _function,
    _phone,
    _domain,
    CAST(NULL AS STRING) AS _leadstatus,
    CAST(_revenue AS STRING) AS _revenue,
    _industry AS _industry,
    _city AS _city,
    _city AS _state,
    _country AS _country,
    CAST(NULL AS STRING) AS _persona,
    CAST(NULL AS STRING) AS _lifecycleStage,
    CAST(NULL AS STRING) AS _sfdcaccountid,
    CAST(NULL AS STRING) _sfdccontactid,
  FROM `x-marketing.logicsource.dashboard_mouseflow_kickfire`
  WHERE _domain IS NOT NULL
    AND _domain != ''
  UNION ALL
  SELECT DISTINCT
    _accountdomain AS _accountdomain,
    CAST(NULL AS STRING) AS _id,
    CAST(NULL AS STRING) AS _name,
    -- CAST(NULL AS STRING) AS _lastname,
    CAST(NULL AS STRING) AS _title,
    CAST(NULL AS STRING) AS _seniority,
    CAST(NULL AS STRING) AS _jobrole,
    CAST(NULL AS STRING) AS _lead_segment,
    CAST(NULL AS STRING) AS _source,
    CAST(NULL AS STRING) AS _function,
    CAST(NULL AS STRING) AS _phone,
    _accountdomain,
    CAST(NULL AS STRING) AS _leadstatus,
    CAST(NULL AS STRING) AS _revenue,
    _industry AS _industry,
    CAST(NULL AS STRING) AS _city,
    CAST(NULL AS STRING) AS _state,
    CAST(NULL AS STRING) AS _country,
    CAST(NULL AS STRING) AS _persona,
    CAST(NULL AS STRING) AS _lifecycleStage,
    CAST(NULL AS STRING) AS _sfdcaccountid,
    CAST(NULL AS STRING) _sfdccontactid,
  FROM `x-marketing.logicsource_mysql.db_airtable_linkedin_ad_account_engagement`
  WHERE _accountdomain IS NOT NULL
    AND _accountdomain != ''
),
accounts AS (
  SELECT
    *
  FROM accounts_combined
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _domain ORDER BY _id DESC) = 1
),
#Query to pull the email engagement
email_engagement_source AS (
  SELECT DISTINCT
    _email,
    RIGHT(_email, LENGTH(_email) - STRPOS(_email, '@')) AS _domain,
    TIMESTAMP(FORMAT_TIMESTAMP('%F %I:%M:%S %Z', _timestamp)) AS _date,
    EXTRACT(WEEK FROM _timestamp) AS _week,
    EXTRACT(YEAR FROM _timestamp) AS _year,
    _contentTitle AS _contentTitle,
    CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
    _description,
    0 AS frequency,
  FROM `x-marketing.logicsource.db_email_engagements_log`
  WHERE LOWER(_engagement) NOT IN (
    'sent',
    'delivered',
    'downloaded',
    'bounced',
    'unsubscribed',
    'processed',
    'deffered',
    'spam',
    'suppressed',
    'dropped',
    'mql'
  )
),
email_engagement AS (
  SELECT
    *
  FROM email_engagement_source
  WHERE NOT REGEXP_CONTAINS(_domain, '2x.marketing|logicsource')
    AND _domain IS NOT NULL
),
web_engagements AS (
  SELECT DISTINCT
    _visitorid AS _email,
    _domain,
    _timestamp,
    EXTRACT(WEEK FROM _timestamp) AS _week,
    EXTRACT(YEAR FROM _timestamp) AS _year,
    _page AS _webActivity,
    "Web Visit" AS _engagement,
    CONCAT(
      "Engagement Time: ",
      _engagementtime,
      "\n",
      "utm_source: ",
      _utmsource,
      "\n",
      "utm_campaign: ",
      _utmcampaign,
      "\n",
      "utm_medium: ",
      _utmmedium,
      "\n",
      "utm_content: ",
      _utmcontent,
      "\n"
    ) AS _description,
    0 AS frequency,
  FROM `x-marketing.logicsource.db_web_engagements_log`
),
ad_clicks AS (
  SELECT
    _contactemail,
    _accountdomain,
    CAST(_date AS TIMESTAMP) AS _timestamp,
    EXTRACT(WEEK FROM CAST(_date AS TIMESTAMP)) AS _week,
    EXTRACT(YEAR FROM CAST(_date AS TIMESTAMP)) AS _year,
    '' AS _webActivity,
    _medium AS _engagement,
    CONCAT(_engagementtype, "-", _frequency) AS _description,
    SAFE_CAST(_frequency AS INT64) AS frequency,
  FROM `x-marketing.logicsource_mysql.db_airtable_linkedin_ad_account_engagement`
  WHERE _medium = 'Paid Ads'
),
content_engagement AS (
  SELECT DISTINCT
    _visitorID AS _email,
    _domain,
    TIMESTAMP(_visitDate) AS _date,
    EXTRACT(WEEK FROM _visitDate) AS _week,
    EXTRACT(YEAR FROM _visitDate) AS _year,
    _title,
    "Content Engagement" AS _engagement,
    CONCAT("Total Page Views: ", _pageviews) AS _description,
    0 AS frequency,
  FROM `x-marketing.logicsource.db_content_engagements_log`
    /* WHERE 
    REGEXP_CONTAINS(LOWER(_page), '/blog/') */
),
downloaded AS (
  SELECT
    CAST(NULL AS STRING) AS devicetype,
    SPLIT(
      SUBSTR(
        form.value.page_url,
        STRPOS(form.value.page_url, '_hsmi=') + 9
      ),
      '&'
    ) [ORDINAL(1)] AS _campaignID,
    #utm_content
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        SPLIT(
          SUBSTR(
            form.value.page_url,
            STRPOS(form.value.page_url, 'utm_campaign') + 13
          ),
          '&'
        ) [ORDINAL(1)],
        '%20',
        ' '
      ),
      '%3A',
      ':'
    ) AS _campaign,
    SPLIT(
      SUBSTR(
        form.value.page_url,
        STRPOS(form.value.page_url, '_source=') + 8
      ),
      '&'
    ) [ORDINAL(1)] AS _utm_source,
    form.value.title AS form_title,
    properties.email.value AS email,
    form.value.timestamp AS TIMESTAMP,
    'Downloaded' AS engagement,
    form.value.page_url AS description,
    campaignguid,
    COALESCE(
      associated_company.properties.domain.value,
      property_hs_email_domain.value,
      RIGHT(property_email.value, LENGTH(property_email.value) - STRPOS(property_email.value, '@'))
    ) AS _domain,
  FROM `x-marketing.logicsource_hubspot.contacts` contacts,
    UNNEST (form_submissions) AS form
  LEFT JOIN `x-marketing.logicsource_hubspot.forms` forms
    ON form.value.form_id = forms.guid -- WHERE properties.email.value = 'michelle.fuentesfina@roquette.com'
),
form_fills AS (
  SELECT
    activity.email AS _email,
    _domain AS _domain,
    activity.timestamp AS _date,
    EXTRACT(WEEK FROM activity.timestamp) AS _week,
    EXTRACT(YEAR FROM activity.timestamp) AS _year,
    form_title,
    'Form Filled' AS _engagement,
    activity.description AS _description,
    0 AS frequency
  FROM downloaded AS activity
  QUALIFY ROW_NUMBER() OVER (PARTITION BY email, description ORDER BY TIMESTAMP DESC) = 1
  UNION ALL
  SELECT
    _contactemail,
    _accountdomain,
    CAST(_date AS TIMESTAMP) AS _timestamp,
    EXTRACT(WEEK FROM CAST(_date AS TIMESTAMP)) AS _week,
    EXTRACT(YEAR FROM CAST(_date AS TIMESTAMP)) AS _year,
    _engagementtype AS _webActivity,
    'Form Filled' AS _engagement,
    _medium AS _description,
    0 AS frequency,
  FROM `x-marketing.logicsource_mysql.db_airtable_linkedin_ad_account_engagement`
  WHERE _medium = 'content syndication'
  UNION ALL
  SELECT DISTINCT
    property_email.value,
    COALESCE(
      associated_company.properties.domain.value,
      property_hs_email_domain.value,
      RIGHT(property_email.value, LENGTH(property_email.value) - STRPOS(property_email.value, '@'))
    ) AS _domain,
    property_lastmodifieddate.value,
    EXTRACT(WEEK FROM property_lastmodifieddate.value) AS _week,
    EXTRACT(YEAR FROM property_lastmodifieddate.value) AS _year,
    "Webinar Engagement" AS form_title,
    'Form Filled' AS _engagement,
    property_event_activity.value AS _description,
    0 AS frequency,
  FROM `x-marketing.logicsource_hubspot.contacts`
  WHERE property_event_activity.value IN ("Visited booth", "Registered", "Attended event")
),
dummy_dates AS (
  # Each domain needs to be shown regardless if they are part of the bombora report or has 0 engagements
  SELECT
    _date,
    EXTRACT(WEEK FROM _date) AS _week,
    EXTRACT(YEAR FROM _date) AS _year
  FROM UNNEST (
    GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 DAY)
  ) AS _date
),
first_party_score AS (
  SELECT DISTINCT
    _domain,
    _extract_date AS _extract_date,
    EXTRACT(WEEK FROM _extract_date) AS _week,
    -- Minus 1 as the score is referring to the week before.
    EXTRACT(YEAR FROM _extract_date) AS _year,
    (
      COALESCE(_quarterly_email_score, 0) + COALESCE(_quarterly_content_synd_score, 0) +
      COALESCE(_quarterly_organic_social_score, 0) + COALESCE(_quarterly_form_fill_score, 0) +
      COALESCE(_quarterly_paid_ads_score, 0) + COALESCE(_quarterly_web_score, 0) +
      COALESCE(_quarterly_organic_social_score, 0)
    ) AS _t90_days_score
  FROM `x-marketing.logicsource.account_90days_score`
),
# Combining the engagements - Contact based and account based engagements
combine_email_form_fills AS (
  SELECT
    *
  FROM email_engagement
  UNION ALL
  SELECT
    *
  FROM form_fills
),
combine_web_ads AS (
  SELECT
    *,
    CAST(NULL AS INTEGER) AS _avg_bombora_score
  FROM web_engagements
  UNION ALL
  SELECT
    *,
    CAST(NULL AS INTEGER) AS _avg_bombora_score
  FROM ad_clicks
),
engagements AS (
  # Contact based engagement query
  SELECT DISTINCT
    contacts._domain,
    contacts._email,
    dummy_dates.* EXCEPT (_date),
    eng.* EXCEPT (_date, _week, _year, _domain, _email),
    CAST(NULL AS INTEGER) AS _avg_bombora_score,
    contacts.* EXCEPT (_domain, _email, form_submissions),
    eng._date,
    CAST(eng._date AS DATE) AS _extract_date
  FROM dummy_dates
  JOIN combine_email_form_fills AS eng
    USING (_week, _year)
  RIGHT JOIN contacts
    ON eng._email = contacts._email
  UNION ALL
  # Account based engagement query
  SELECT DISTINCT
    accounts._domain,
    CAST(NULL AS STRING) AS _email,
    dummy_dates.* EXCEPT (_date),
    eng.* EXCEPT (_timestamp, _week, _year, _domain, _email),
    accounts.* EXCEPT (_domain),
    eng._timestamp,
    CAST(eng._timestamp AS DATE) AS _extract_date
  FROM dummy_dates
  CROSS JOIN accounts
  JOIN combine_web_ads AS eng
    USING (_domain, _week, _year)
),
sfdc AS (
  SELECT DISTINCT
    accountid,
    acc.name AS _accountname,
    annualrevenue AS _annualrevenue,
    acc.industry AS _industry,
    cnt.id AS contactid,
    RIGHT(email, LENGTH(email) - STRPOS(email, '@')) AS _domain,
    cnt.mailingcity AS _city,
    cnt.mailingstate AS _state,
    cnt.mailingcountry AS _country
  FROM `logicsource_salesforce.Contact` cnt
  JOIN `logicsource_salesforce.Account` acc
    ON cnt.accountid = acc.id
),
opps_created AS (
  SELECT DISTINCT
    main.id AS _opportunityID,
    main.accountid AS _accountid,
    sfdc.contactid AS _contactid,
    _accountname,
    main.name AS _opportunityName,
    stagename AS _currentStage,
    main.createddate AS _createTS,
    -- closedate AS _closeTS,
    amount AS _amount,
    -- acv__c AS _acv,
    _domain,
    _industry,
    CAST(NULL AS INTEGER) AS _tier,
    _annualrevenue,
    main.type AS _type,
    -- reason__c AS _reason,
    laststagechangedate AS _oppLastChangeinStage,
    _city,
    _state,
    _country,
    CAST(NULL AS INT64) AS _t90_days_score
  FROM `logicsource_salesforce.Opportunity` main
  JOIN sfdc
    USING (accountid)
    /* LEFT JOIN
    first_party_score USING(_domain) */
  WHERE main.isdeleted = FALSE
    AND main.type != 'Renewal'
    AND LOWER(_accountname) NOT LIKE '%logicsource%'
    AND EXTRACT(
      YEAR
      FROM main.createddate
    ) >= 2022
),
opp_hist AS (
  SELECT DISTINCT
    opportunityid AS _opportunityid,
    createddate AS _oppLastChangeinStage,
    oldvalue AS _previousstage,
    newvalue AS _currentstage
  FROM `logicsource_salesforce.OpportunityFieldHistory`
  WHERE field = 'StageName'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY opportunityid ORDER BY createddate DESC) = 1
),
opps_stage_change AS (
  SELECT
    _domain,
    _accountid,
    _accountname,
    _opportunityname,
    _amount,
    opps_created._currentStage,
    _oppLastChangeinStage,
    _previousStage,
    _industry,
    _tier,
    _annualrevenue,
    _city,
    _state,
    _country,
    _t90_days_score,
    _contactid,
  FROM opps_created
  JOIN opp_hist
    USING (_opportunityid, _oppLastChangeinStage)
)
SELECT DISTINCT
  engagements.*,
  COALESCE(_t90_days_score, 0) AS _t90_days_score
FROM engagements
LEFT JOIN first_party_score
  USING (_domain, _week, _year, _extract_date)
WHERE LENGTH(_domain) > 1;