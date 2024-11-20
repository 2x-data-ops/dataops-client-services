TRUNCATE TABLE `x-marketing.gibraltar.ate_2025_contact_detail`;

-- PARTITION BY DATE(_timestamp)
-- CLUSTER BY (_email)
-- AS

INSERT INTO `x-marketing.gibraltar.ate_2025_contact_detail` (
_hotel,
_seminar_registered,
_first_name,
_last_name,
_company_name,
_i_am_a,
_address,
_state,
_city,
_zipcode,
_timestamp,
_campaign_id,
_email,
_form_title,
_form_id,
_function,
_job_title,
_name,
_page_url,
_phone,
_seniority
)

WITH main_data AS (
  SELECT 
    properties.ate_2025_contact_property.value AS _hotel,
    properties.recent_conversion_event_name.value AS _seminar_registered,
    INITCAP(property_firstname.value) AS _first_name,
    INITCAP(property_lastname.value) AS _last_name,
    INITCAP(COALESCE(associated_company.properties.name.value, property_company.value)) AS _company_name,
    properties.i_am_a_.value AS _i_am_a,
    properties.address.value AS _address,
    COALESCE(properties.state.value, associated_company.properties.state.value) AS _state,
    INITCAP(COALESCE(properties.city.value, associated_company.properties.city.value)) AS _city,
    COALESCE(CAST(properties.zip_code.value AS STRING), associated_company.properties.zip.value) AS _zipcode,
    form.value.timestamp AS _timestamp,
    forms.campaignguid AS _campaign_id,
    properties.email.value AS _email,
    form.value.title As _form_title,
    form.value.form_id AS _form_id,
    properties.job_function.value AS _function,
    properties.jobtitle.value AS _job_title,
    CONCAT(INITCAP(property_firstname.value), ' ', INITCAP(property_lastname.value)) AS _name,
    form.value.page_url AS _page_url,
    property_phone.value AS _phone,
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
    FROM `x-marketing.gibraltar_hubspot.contacts`,
      UNNEST(form_submissions) AS form
    JOIN `x-marketing.gibraltar_hubspot.forms` forms
      ON form.value.form_id = forms.guid
    -- data filtered to this specific event
    WHERE form.value.title = 'ATE 2025 Registration Form (Event Specific)'
    
)
SELECT
  *
FROM main_data
-- removing test email
WHERE _email NOT LIKE '%@2x.marketing'
    AND _email NOT IN ('pscelsi@gibraltar1.com', 'cobrown@gibraltar1.com', 'kkasumi@gibraltar1.com', 'jache@gibraltar1.com', 'kbooker@gibraltar1.com', 'john.jsilas5@gmail.com', 'johnjj5525@gmail.com')
-- take latest engagement per email, and form url
QUALIFY ROW_NUMBER() OVER(PARTITION BY _email, _page_url ORDER BY _timestamp) = 1

