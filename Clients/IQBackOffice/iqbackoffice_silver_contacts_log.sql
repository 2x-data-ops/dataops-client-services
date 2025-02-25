 TRUNCATE TABLE `x-marketing.iqbackoffice.contacts_log`; 

 INSERT INTO `x-marketing.iqbackoffice.contacts_log`
  SELECT
    CAST(vid AS STRING) AS _prospectID,
    CONCAT(properties.firstname.value,' ', properties.lastname.value) AS _name,
    properties.phone.value AS _phone,
    properties.jobtitle.value AS _title,
    --properties.account_tier.value AS _tier,
    properties.company.value AS _company,
    associated_company.properties.domain.value AS _domain,
    properties.industry.value AS _industry,
    properties.country.value AS _country,
    properties.city.value AS _city,
    CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
    --CAST(properties.numberofemployees.value AS STRING) AS _employees,
    --properties.job_function.value AS _function,
    properties.state.value AS _state,
    properties.lifecyclestage.value AS _lifecycleStage,
    properties.email.value AS _email,
    property_utm_source.value AS _utm_source,
    property_utm_campaign.value AS _utm_campaign,
    property_utm_medium.value AS _utm_medium,
    property_product___service_type.value AS _product_service_type,
    property_jobtitle.value AS _jobtitle,
    property_createdate.value AS _createdate,
    CAST(property_behavioral_score.value AS NUMERIC) AS _behavioral_score,
    CAST(property_hs_analytics_average_page_views.value AS NUMERIC) AS _average_page_views,
    property_hs_analytics_last_url.value AS _last_page_view,
    property_hs_analytics_last_referrer.value AS _last_referrer,
    property_recent_conversion_event_name.value AS _recent_conversion_event_name,
    associated_company.properties.bombora_company_size.value AS _company_size,
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
  FROM`x-marketing.iqbackoffice_hubspot.contacts`
  WHERE properties.email.value IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY vid, property_email.value, CONCAT(properties.firstname.value,' ', properties.lastname.value)
    ORDER BY vid DESC ) = 1