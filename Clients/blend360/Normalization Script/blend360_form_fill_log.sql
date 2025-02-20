TRUNCATE TABLE `x-marketing.blend360.db_form_fill_log`;
INSERT INTO `x-marketing.blend360.db_form_fill_log`
WITH contacts_v2 AS (
  SELECT * EXCEPT (_rownum)
  FROM (
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
      COALESCE(
          property_city.value,
          associated_company.properties.city.value
      ) AS _city, 
      COALESCE(
          property_state.value,
          associated_company.properties.state.value
      ) AS _state,
      COALESCE(
          property_country.value, 
          associated_company.properties.country.value
      ) AS _country,
      CAST(properties.blend360___lead_score.value AS STRING) AS _leadScore,
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
      'Form Filled' AS _engagement, 
      CASE 
        WHEN form.value.title LIKE "%Unsubscribe%"
        THEN CAST(NULL AS STRING)
        ELSE form.value.form_id
      END AS _formID,
      CASE 
        WHEN form.value.title LIKE "%Unsubscribe%"
        THEN CAST(NULL AS STRING)
        ELSE form.value.title
        END AS _formTitle,
      CASE 
        WHEN form.value.title LIKE "%Unsubscribe%"
        THEN CAST(NULL AS STRING)
        ELSE form.value.page_url
        END AS _formURL,
      CASE 
        WHEN form.value.title LIKE "%Unsubscribe%"
        THEN CAST(NULL AS TIMESTAMP)
        ELSE form.value.timestamp
        END AS _formTimestamp,
      properties.hs_analytics_num_page_views.value AS _pageViews,
      properties.unsubscribed_from_marketing_information.value AS _unsubscribed,
      CAST(associated_company.properties.blend360___average_marketing_email_clicks.value AS STRING) AS _emailClicks,
      CAST(associated_company.properties.blend360___average_marketing_email_opens.value AS STRING) AS _emailOpens,
      CONCAT('https://app.hubspot.com/contacts/8374679/',vid) AS _hubspotlink,
      CAST(associated_company.properties.blend360___account_scoring.value AS STRING) AS _accountScoring,
      ROW_NUMBER() OVER( PARTITION BY property_email.value ORDER BY vid DESC) AS _rownum,
    FROM 
      `x-marketing.blend360_hubspot_v2.contacts` contacts, UNNEST(form_submissions) AS form
    JOIN
      `x-marketing.blend360_hubspot_v2.forms` forms
    ON
      form.value.form_id = forms.guid
  )
  WHERE _rownum =1
),
contacts_v1 AS (
  SELECT * EXCEPT (_rownum)
  FROM (
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
      CAST(NULL AS STRING) AS _function,
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
      COALESCE(
          property_country.value, 
          associated_company.properties.country.value
      ) AS _country,
      CAST(NULL AS STRING) AS _leadScore,
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
      CAST(NULL AS STRING) AS _jobLevel,
      'Form Filled' AS _engagement, 
      CASE 
        WHEN form.value.title LIKE "%Unsubscribe%"
        THEN CAST(NULL AS STRING)
        ELSE form.value.form_id
      END AS _formID,
      CASE 
        WHEN form.value.title LIKE "%Unsubscribe%"
        THEN CAST(NULL AS STRING)
        ELSE form.value.title
        END AS _formTitle,
      CASE 
        WHEN form.value.title LIKE "%Unsubscribe%"
        THEN CAST(NULL AS STRING)
        ELSE form.value.page_url
        END AS _formURL,
      CASE 
        WHEN form.value.title LIKE "%Unsubscribe%"
        THEN CAST(NULL AS TIMESTAMP)
        ELSE form.value.timestamp
        END AS _formTimestamp,
      properties.hs_analytics_num_page_views.value AS _pageViews,
      CAST(NULL AS STRING) AS _unsubscribed,
      CAST(NULL AS STRING) AS _emailClicks,
      CAST(NULL AS STRING) AS _emailOpens,
      CONCAT('https://app.hubspot.com/contacts/8374679/',vid) AS _hubspotlink,
      CAST(NULL AS STRING) AS _accountScoring,
      ROW_NUMBER() OVER( PARTITION BY property_email.value ORDER BY vid DESC) AS _rownum,
    FROM 
      `x-marketing.blend360_hubspot.contacts` contacts, UNNEST(form_submissions) AS form
    JOIN
      `x-marketing.blend360_hubspot.forms` forms
    ON
      form.value.form_id = forms.guid
  )
  WHERE _rownum = 1
)
SELECT * FROM contacts_v2
UNION ALL
SELECT * FROM contacts_v1
