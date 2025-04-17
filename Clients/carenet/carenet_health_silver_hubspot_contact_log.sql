--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------ Database Reporting Script -----------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------


-- No bombora report so it's being excluded.
TRUNCATE TABLE `carenet_health.contact_icp_score` ;
INSERT INTO `carenet_health.contact_icp_score`
-- CREATE OR REPLACE TABLE `carenet_health.contact_icp_score` AS
WITH contacts AS (
  SELECT 
    vid AS _id,
    property_email.value AS _email,
    CONCAT(properties.firstname.value, ' ', COALESCE(properties.lastname.value,'')) AS _name,
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
    COALESCE(
        property_city.value,
        associated_company.properties.city.value
    ) AS _city, 
    COALESCE(
        property_state.value,
        associated_company.properties.state.value
    ) AS _state,
    CASE 
      WHEN LOWER(COALESCE(
        property_country.value, 
        associated_company.properties.country.value
    )) 
      IN ('us',  'usa', 'united states', 'united states of america') 
      THEN 'US' 
      ELSE COALESCE(
        property_country.value, 
        associated_company.properties.country.value
    ) 
    END AS _country,
    properties.hubspot_score_v2.value AS _hubspot_score_v2,
    properties.proposed_qualifications.value AS _proposed_qualifications,
    properties.hubspotscore.value AS _hubspot_score,
    '' AS _persona,
    CASE
        WHEN property_lifecyclestage.value = 'marketingqualifiedlead' THEN 'Marketing Qualified Lead' 
        WHEN property_lifecyclestage.value = 'salesqualifiedlead' THEN 'Sales Qualified Lead' 
        ELSE INITCAP(property_lifecyclestage.value)
    END AS _lifecycleStage,
    property_createdate.value AS _createddate,
    properties.salesforceaccountid.value AS _sfdcaccountid,
    properties.salesforcecontactid.value AS _sfdccontactid,
    properties.salesforceleadid.value AS _sfdcleadid,
    properties.job_function.value AS _jobLevel,
    CASE 
      WHEN form_submissions[SAFE_OFFSET(0)].value.title LIKE "%Unsubscribe%"
      THEN CAST(NULL AS STRING)
      ELSE form_submissions[SAFE_OFFSET(0)].value.form_id
    END AS _formSubmissions,
    CASE 
      WHEN form_submissions[SAFE_OFFSET(0)].value.title LIKE "%Unsubscribe%"
      THEN CAST(NULL AS STRING)
      ELSE form_submissions[SAFE_OFFSET(0)].value.title
    END AS _formSubmissionsTitle,
    CASE 
      WHEN form_submissions[SAFE_OFFSET(0)].value.title LIKE "%Unsubscribe%"
      THEN CAST(NULL AS STRING)
      ELSE form_submissions[SAFE_OFFSET(0)].value.page_url
    END AS _formSubmissionsURL,
    CASE 
      WHEN form_submissions[SAFE_OFFSET(0)].value.title LIKE "%Unsubscribe%"
      THEN CAST(NULL AS TIMESTAMP)
      ELSE form_submissions[SAFE_OFFSET(0)].value.timestamp
    END AS _formSubmissionsTimestamp,
    properties.hs_analytics_num_page_views.value AS _pageViews,
    properties.hs_email_click.value__st AS _emailClicks,
    properties.hs_email_open.value__st AS _emailOpens,
    properties.utm_source.value AS _campaignSource,
    properties.recent_conversion_event_name.value AS _campaignName,
    properties.hs_marketable_status.value AS _hsMarketableStatus,
    properties.hs_analytics_source.value AS _hsAnalyticsSource,
    properties.carenet_based_score.value AS _carenetBasedScore,
    properties.hs_analytics_source_data_1.value AS _analyticsSourceData1,
    properties.hs_object_source_detail_1.value AS _objectSourceDetail1,
    properties.hs_latest_source.value AS _hs_latest_source,
    properties.recent_conversion_date.value AS _recent_conversion_date,
    IF(
      properties.leadstatus.value = 'Qualified',
      'Converted',
      properties.leadstatus.value
    ) AS _leadstatus,
    properties.date_time___contacted.value AS _dateTimeContacted,
    properties.date_time___disqualified.value AS _dateTimeDisqualified,
    properties.date_time___engaged.value AS _dateTimeEngaged,
    properties.date_time___mql.value AS _dateTimeMQL,
    properties.date_time___nurture.value AS _dateTimeNurture,
    properties.date_time___open.value AS _dateTimeOpen,
    properties.date_time___oql.value AS _dateTimeOQL,
    properties.date_time___sales_qualified.value AS _dateTimeSalesQualified,
    properties.found_in_hubspot.value AS _found_in_hubspot
  FROM `x-marketing.carenet_health_hubspot_2.contacts` hs
  QUALIFY ROW_NUMBER() OVER( PARTITION BY property_email.value ORDER BY vid DESC) = 1
)
SELECT DISTINCT 
  _id,
  _email,
  _name,
  _domain,
  _jobtitle AS _job_title,
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
  _lifecycleStage AS _lifecycle_stage,
  _createddate AS _created_date,
  _sfdcaccountid AS _sfdc_account_id,
  _sfdccontactid AS _sfdc_contact_id,
  _sfdcleadid AS _sfdc_lead_id,
  _country,
  COUNT(_domain) OVER(PARTITION BY CONCAT(_domain, _company)) AS _num_tied_contacts,
  CAST((COUNT(_formSubmissions) OVER(PARTITION BY CONCAT(_email,_id))) AS STRING) AS _num_form_contacts,
  -- To be enabled when the airtable is updated > update the values accordingly 
  /* IF(
      (_suppressed IS NULL AND (_revenue >= 50000000 OR _employee >= 500) AND _country = 'US') 
      AND (NOT REGEXP_CONTAINS(LOWER(_jobtitle), r"content|design|art|product|brand|writer|analyst") OR _jobtitle IS NULL)
      AND (_seniority IN ('Director', 'C-Level') OR _seniority LIKE '%VP%')
      AND (LOWER(_function) LIKE '%marketing%' OR LOWER(_jobtitle) LIKE '%marketing%'), 
      1, 
      0
    ) */ 0 AS _target_contacts,
  /* IF(_suppressed IS NULL AND (_revenue >= 50000000 OR _employee >= 500) AND _country = 'US', 1, 0)  */ 0 AS _target_accounts,
  _hubspot_score_v2,
  _hubspot_score,
  _proposed_qualifications,
  _jobLevel,
  _formSubmissions AS _form_submissions,
  _formSubmissionsTitle AS _form_submissions_title,
  _formSubmissionsURL AS _form_submissions_URL,
  _formSubmissionsTimestamp AS _form_submissions_timestamp,
  CAST(_pageViews AS STRING) AS _page_views,
  _emailClicks AS _email_clicks,
  _emailOpens AS _email_opens, 
  _campaignName AS _campaign_name,
  _campaignSource AS _campaign_source,
  _hsMarketableStatus AS _hs_marketable_status,
  _hsAnalyticsSource AS _hs_analytics_source,
  _carenetBasedScore AS _carenet_based_score,
  _analyticsSourceData1 AS _analytics_source_data_1,
  _objectSourceDetail1 AS _object_source_detail_1,
  _hs_latest_source,
  _recent_conversion_date,
  _leadstatus AS _lead_status,
  _dateTimeContacted AS _date_time_contacted,
  _dateTimeDisqualified AS _date_time_disqualified,
  _dateTimeEngaged AS _date_time_engaged,
  _dateTimeMQL AS _date_time_MQL,
  _dateTimeNurture AS _date_time_nurture,
  _dateTimeOpen AS _date_time_open,
  _dateTimeOQL AS _date_time_OQL,
  _dateTimeSalesQualified AS _date_time_sales_qualified,
  _found_in_hubspot
FROM contacts
/* LEFT JOIN
  suppressed_industry USING(_industry) */
;