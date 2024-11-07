-- No bombora report so it's being excluded.
TRUNCATE TABLE `x-marketing.blend360.db_icp_database_log` ;
INSERT INTO `x-marketing.blend360.db_icp_database_log`(
  _id,
  _email,
  _name,
  _domain,
  _jobtitle,
  _seniority,
  _function,
  _phone,
  _company,
  _industry,
  _revenue,
  _employee,
  _city,
  _state,
  _persona,
  _lifecycleStage,
  _createddate,
  _country,
  _num_tied_contacts,
  _num_form_contacts,
  _target_contacts,
  _target_accounts,
  _leadScore,
  _formSubmissions,
  _pageViews,
  _unsubscribed,
  _hubspotlink,
  _accountScoring
)
WITH contact_base_v2 AS (
SELECT 
  vid AS _id,
  property_email.value AS _email,
  CONCAT(property_firstname.value, ' ', property_lastname.value) AS _name,
  LOWER(COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value)) AS _domain, 
  properties.jobtitle.value AS _jobtitle,
  CASE
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Assistant to%") THEN "Non-Manager" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior Counsel%") THEN "VP"  
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%General Counsel%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Founder%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%C-Level%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CDO%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CIO%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CMO%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CFO%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CEO%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Chief%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%coordinator%") THEN "Non-Manager" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%COO%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr. V.P.%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr.VP%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior-Vice Pres%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%srvp%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior VP%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%SR VP%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr Vice Pres%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr. VP%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr. Vice Pres%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%S.V.P%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior Vice Pres%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Exec Vice Pres%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Exec Vp%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Executive VP%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Exec VP%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Executive Vice President%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%EVP%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%E.V.P%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%SVP%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%V.P%") THEN "VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%VP%") THEN "VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Vice Pres%") THEN "VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%V P%") THEN "VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%President%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Director%") THEN "Director" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CTO%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Dir%") THEN "Director" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Dir.%") THEN "Director" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%MDR%") THEN "Non-Manager" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%MD%") THEN "Director" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%GM%") THEN "Director" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Head%") THEN "VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Manager%") THEN "Manager" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%escrow%") THEN "Non-Manager" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%cross%") THEN "Non-Manager" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%crosse%") THEN "Non-Manager" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Partner%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CRO%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Chairman%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Owner%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Team Lead%") THEN "Manager"
  END AS _seniority,
  properties.job_function.value AS _function,
  property_phone.value AS _phone,
  COALESCE(associated_company.properties.name.value, property_company.value) AS _company,
  associated_company.properties.annualrevenue.value AS _revenue,
  INITCAP(REPLACE(associated_company.properties.industry.value, '_', ' ')) AS _industry,
  associated_company.properties.numberofemployees.value AS _employee,
  COALESCE(property_city.value, associated_company.properties.city.value) AS _city, 
  COALESCE(property_state.value, associated_company.properties.state.value) AS _state,
  COALESCE(property_country.value, associated_company.properties.country.value) AS _country,
  properties.blend360___lead_score.value AS _leadScore,
  '' AS _persona,
  CASE
    WHEN property_lifecyclestage.value = 'marketingqualifiedlead' THEN 'Marketing Qualified Lead' 
    WHEN property_lifecyclestage.value = 'salesqualifiedlead' THEN 'Sales Qualified Lead' 
    ELSE INITCAP(property_lifecyclestage.value)
  END AS _lifecycleStage,
  property_createdate.value AS _createddate,
  CAST(NULL AS STRING) AS _sfdcaccountid,
  CAST(NULL AS STRING) AS _sfdccontactid,
  CAST(NULL AS STRING) AS _sfdcleadid,
  properties.job_level.value AS _jobLevel,
  CASE 
    WHEN form_submissions[SAFE_OFFSET(0)].value.title LIKE "%Unsubscribe%" THEN CAST(NULL AS STRING)
    ELSE form_submissions[SAFE_OFFSET(0)].value.form_id
  END AS _formSubmissions,
  CASE 
    WHEN form_submissions[SAFE_OFFSET(0)].value.title LIKE "%Unsubscribe%" THEN CAST(NULL AS STRING)
    ELSE form_submissions[SAFE_OFFSET(0)].value.title
  END AS _formSubmissionsTitle,
  CASE 
    WHEN form_submissions[SAFE_OFFSET(0)].value.title LIKE "%Unsubscribe%" THEN CAST(NULL AS STRING)
    ELSE form_submissions[SAFE_OFFSET(0)].value.page_url
  END AS _formSubmissionsURL,
  CASE 
    WHEN form_submissions[SAFE_OFFSET(0)].value.title LIKE "%Unsubscribe%" THEN CAST(NULL AS TIMESTAMP)
    ELSE form_submissions[SAFE_OFFSET(0)].value.timestamp
  END AS _formSubmissionsTimestamp,
  properties.hs_analytics_num_page_views.value AS _pageViews,
  properties.unsubscribed_from_marketing_information.value AS _unsubscribed,
  associated_company.properties.blend360___average_marketing_email_clicks.value AS _emailClicks,
  associated_company.properties.blend360___average_marketing_email_opens.value AS _emailOpens,
  CONCAT('https://app.hubspot.com/contacts/8374679/',vid) AS _hubspotlink,
  associated_company.properties.blend360___account_scoring.value AS _accountScoring,
  FROM `x-marketing.blend360_hubspot_v2.contacts` hs
),

contacts_v2 AS (
SELECT 
  * EXCEPT(_country),
  CASE 
    WHEN LOWER(_country) IN ('us',  'usa', 'united states', 'united states of america') THEN 'US' 
    ELSE _country 
  END AS _country
FROM contact_base_v2
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY _email
  ORDER BY _id DESC) = 1
)
SELECT 
  DISTINCT _id,
  _email,
  _name,
  _domain,
  _jobtitle,
  _seniority,
  _function,
  _phone,
  _company,
  _industry,
  CAST(_revenue AS STRING) AS _revenue,
  CAST(_employee AS STRING) AS _employee,
  _city,
  _state,
  _persona,
  _lifecycleStage,
  _createddate,
  _country,
  COUNT(_domain) OVER(PARTITION BY CONCAT(_domain, _company)) AS _num_tied_contacts,
  CAST((COUNT(_formSubmissions) OVER(PARTITION BY CONCAT(_email,_id))) AS STRING) AS _num_form_contacts,
  0 AS _target_contacts,
  0 AS _target_accounts,
  _leadScore AS _leadScore,
  _formSubmissions,
  CAST(_pageViews AS STRING) AS _pageViews,
  _unsubscribed,
  _hubspotlink,
  _accountScoring
FROM contacts_v2;

------------------------------------------------------------------------
------------------------------ Hubspot V1 ------------------------------
------------------------------------------------------------------------

INSERT INTO `x-marketing.blend360.db_icp_database_log`(
  _id,
  _email,
  _name,
  _domain,
  _jobtitle,
  _seniority,
  _function,
  _phone,
  _company,
  _industry,
  _revenue,
  _employee,
  _city,
  _state,
  _persona,
  _lifecycleStage,
  _createddate,
  _country,
  _num_tied_contacts,
  _num_form_contacts,
  _target_contacts,
  _target_accounts,
  _leadScore,
  _formSubmissions,
  _pageViews,
  _hubspotlink
)
WITH contact_base_v1 AS (
SELECT 
  vid AS _id,
  property_email.value AS _email,
  CONCAT(property_firstname.value, ' ', property_lastname.value) AS _name,
  LOWER(COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value)) AS _domain, 
  properties.jobtitle.value AS _jobtitle,
  CASE
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Assistant to%") THEN "Non-Manager" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior Counsel%") THEN "VP"  
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%General Counsel%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Founder%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%C-Level%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CDO%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CIO%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CMO%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CFO%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CEO%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Chief%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%coordinator%") THEN "Non-Manager" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%COO%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr. V.P.%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr.VP%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior-Vice Pres%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%srvp%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior VP%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%SR VP%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr Vice Pres%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr. VP%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr. Vice Pres%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%S.V.P%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior Vice Pres%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Exec Vice Pres%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Exec Vp%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Executive VP%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Exec VP%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Executive Vice President%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%EVP%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%E.V.P%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%SVP%") THEN "Senior VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%V.P%") THEN "VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%VP%") THEN "VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Vice Pres%") THEN "VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%V P%") THEN "VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%President%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Director%") THEN "Director" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CTO%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Dir%") THEN "Director" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Dir.%") THEN "Director" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%MDR%") THEN "Non-Manager" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%MD%") THEN "Director" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%GM%") THEN "Director" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Head%") THEN "VP" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Manager%") THEN "Manager" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%escrow%") THEN "Non-Manager" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%cross%") THEN "Non-Manager" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%crosse%") THEN "Non-Manager" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Partner%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CRO%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Chairman%") THEN "C-Level" 
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Owner%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Team Lead%") THEN "Manager"
  END AS _seniority,
  property_phone.value AS _phone,
  COALESCE(associated_company.properties.name.value, property_company.value) AS _company,
  associated_company.properties.annualrevenue.value AS _revenue,
  INITCAP(REPLACE(associated_company.properties.industry.value, '_', ' ')) AS _industry,
  associated_company.properties.numberofemployees.value AS _employee,
  COALESCE(property_city.value, associated_company.properties.city.value) AS _city, 
  COALESCE(property_state.value, associated_company.properties.state.value) AS _state,
  COALESCE(property_country.value, associated_company.properties.country.value) AS _country,
  '' AS _persona,
  CASE
    WHEN property_lifecyclestage.value = 'marketingqualifiedlead' THEN 'Marketing Qualified Lead' 
    WHEN property_lifecyclestage.value = 'salesqualifiedlead' THEN 'Sales Qualified Lead' 
    ELSE INITCAP(property_lifecyclestage.value)
  END AS _lifecycleStage,
  property_createdate.value AS _createddate,
  CAST(NULL AS STRING) AS _sfdcaccountid,
  CAST(NULL AS STRING) AS _sfdccontactid,
  CAST(NULL AS STRING) AS _sfdcleadid,
  CASE 
    WHEN form_submissions[SAFE_OFFSET(0)].value.title LIKE "%Unsubscribe%" THEN CAST(NULL AS STRING)
    ELSE form_submissions[SAFE_OFFSET(0)].value.form_id
  END AS _formSubmissions,
  CASE 
    WHEN form_submissions[SAFE_OFFSET(0)].value.title LIKE "%Unsubscribe%" THEN CAST(NULL AS STRING)
    ELSE form_submissions[SAFE_OFFSET(0)].value.title
  END AS _formSubmissionsTitle,
  CASE 
    WHEN form_submissions[SAFE_OFFSET(0)].value.title LIKE "%Unsubscribe%" THEN CAST(NULL AS STRING)
    ELSE form_submissions[SAFE_OFFSET(0)].value.page_url
  END AS _formSubmissionsURL,
  CASE 
    WHEN form_submissions[SAFE_OFFSET(0)].value.title LIKE "%Unsubscribe%" THEN CAST(NULL AS TIMESTAMP)
    ELSE form_submissions[SAFE_OFFSET(0)].value.timestamp
  END AS _formSubmissionsTimestamp,
  properties.hs_analytics_num_page_views.value AS _pageViews,
  CONCAT('https://app.hubspot.com/contacts/8374679/',vid) AS _hubspotlink,
FROM `x-marketing.blend360_hubspot.contacts` hs
),

contacts_v1 AS (
SELECT 
  * EXCEPT(_country),
  CASE 
    WHEN LOWER(_country) IN ('us','usa','united states','united states of america') THEN 'US' 
    ELSE _country 
  END AS _country
FROM contact_base_v1
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY _email
  ORDER BY _id DESC) = 1
)
SELECT 
  DISTINCT _id,
  _email,
  _name,
  _domain,
  _jobtitle,
  _seniority,
  CAST(NULL AS STRING) AS _function,
  _phone,
  _company,
  _industry,
  CAST(_revenue AS STRING) AS _revenue,
  CAST(_employee AS STRING) AS _employee,
  _city,
  _state,
  _persona,
  _lifecycleStage,
  _createddate,
  _country,
  COUNT(_domain) OVER(PARTITION BY CONCAT(_domain, _company)) AS _num_tied_contacts,
  CAST((COUNT(_formSubmissions) OVER(PARTITION BY CONCAT(_email,_id))) AS STRING) AS _num_form_contacts,
  0 AS _target_contacts,
  0 AS _target_accounts,
  CAST(NULL AS FLOAT64) AS _leadScore,
  _formSubmissions,
  CAST(_pageViews AS STRING) AS _pageViews,
  _hubspotlink
FROM contacts_v1;