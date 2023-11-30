CREATE OR REPLACE TABLE `x-marketing.gibraltar.db_icp_database_log` AS
SELECT * EXCEPT(_rownum)
FROM (
  SELECT
    _pageurl,
    _name,
    _first_name,
    _last_name,
    _i_am_a,
    _jobtitle,
    _address,
    _city,
    _state,
    _zipcode,
    _seniority,
    _phone,
    _function,
    _company_name,
    _form_title,
    _formid,
    _campaignid,
    _email,
    _timestamp,
    _seminar_registered,
    ROW_NUMBER() OVER(PARTITION BY _email ORDER BY _timestamp DESC ) AS _rownum
  FROM ( 
    SELECT 
      INITCAP(property_firstname.value) AS _first_name,
      INITCAP(property_lastname.value) AS _last_name,
      CONCAT(INITCAP(property_firstname.value), ' ', INITCAP(property_lastname.value)) AS _name,
      properties.i_am_a_.value AS _i_am_a,
      properties.jobtitle.value AS _jobtitle,
      properties.address.value AS _address,
      INITCAP(COALESCE(properties.city.value, associated_company.properties.city.value)) AS _city,
      COALESCE(properties.state.value, associated_company.properties.state.value) AS _state,
      COALESCE(CAST(properties.zip_code.value AS STRING), associated_company.properties.zip.value) AS _zipcode,
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
      form.value.page_url AS _pageurl,
      properties.which_seminar_would_you_like_to_register_for_.value AS _seminar_registered,
      INITCAP(COALESCE(associated_company.properties.name.value, property_company.value)) AS _company_name,
      form.value.title As _form_title,
      properties.email.value AS _email,
      form.value.timestamp AS _timestamp,
      forms.campaignguid AS _campaignid,
      form.value.form_id AS _formid
    FROM `x-marketing.gibraltar_hubspot.contacts`, UNNEST(form_submissions) AS form, UNNEST (list_memberships) AS membership
    JOIN `x-marketing.gibraltar_hubspot.forms` forms
    ON form.value.form_id = forms.guid
    WHERE
    -- properties.email.value != "kbohlin@gibraltar1.com"
    -- AND
    properties.email.value != "kbooker@gibraltar1.com"
    AND properties.which_seminar_would_you_like_to_register_for_.value IS NOT NULL
    -- AND membership.value.is_member IS NOT NULL
    )
)
WHERE _rownum = 1;


--updating the record to get zipcode since some are null, zipcode data is pulled from hubspot itself

UPDATE `x-marketing.gibraltar.db_icp_database_log` origin
SET origin._zipcode = 
  CASE
    WHEN origin._email = 'garrett.swayne@srsbuildingproducts.com' THEN '77389'
    WHEN origin._email = 'skyshieldco@duck.com' THEN '77341'
    WHEN origin._email = 'jeremy@royalroofingoftexas.com' THEN '77494'
    WHEN origin._email = 'bleecruz@gmail.com' THEN '77441'
    WHEN origin._email = 'bockjason21@yahoo.com' THEN '50312'
    WHEN origin._email = 'vicky_medina_2000@yahoo.com' THEN '60101'
    WHEN origin._email = 'michaela@truenorth-roofing.com' THEN '55016'
    WHEN origin._email = 'maspilk01@gmail.com' THEN '44212'
    WHEN origin._email = 'flipracer218@yahoo.com' THEN '43220'
    WHEN origin._email = 'detra@muthandco.com' THEN '43081'
    WHEN origin._email = 'ericpaulstephens@gmail.com' THEN '43054'
    WHEN origin._email = 'chitwood7321@gmail.com' THEN '43062'
    WHEN origin._email = 'jacob_moeller@yahoo.com' THEN '43015'
    WHEN origin._email = 'johnny@bristlewoodroofing.com' THEN '43213'
    WHEN origin._email = 'luke@versaconks.com' THEN '67110'
    ELSE origin._zipcode
  END
WHERE origin._zipcode IS NULL