--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------ Database Reporting Script -----------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------

-- CREATE OR REPLACE TABLE `x-marketing.3x.db_icp_database_log` AS
TRUNCATE TABLE `x-marketing.3x.db_icp_database_log` ;
INSERT INTO `x-marketing.3x.db_icp_database_log`
WITH
  contacts AS (
    SELECT 
      * EXCEPT( _rownum, _country),
      CASE WHEN LOWER(_country) IN ('us',  'usa', 'united states', 'united states of america', 'ussa') THEN 'US' ELSE _country END AS _country
    FROM (
      SELECT 
      DISTINCT
          CAST(vid AS STRING) AS _id,
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
          COALESCE(
              associated_company.properties.name.value,
              property_company.value
          ) AS _company,
          CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
          INITCAP(REPLACE(associated_company.properties.industry.value, '_', ' ')) AS _industry,
          COALESCE(CAST(associated_company.properties.numberofemployees.value AS STRING),CAST(property_numberofemployees.value AS STRING)) AS _employee,
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
          CASE
              WHEN property_lifecyclestage.value = 'marketingqualifiedlead' THEN 'Marketing Qualified Lead' 
              WHEN property_lifecyclestage.value = 'salesqualifiedlead' THEN 'Sales Qualified Lead' 
              ELSE INITCAP(property_lifecyclestage.value)
          END AS _lifecycleStage,
          property_createdate.value AS _createddate,
          COALESCE(associated_company.properties.salesforceaccountid.value,properties.salesforceaccountid.value) AS _sfdcaccountid,
          COALESCE(property_salesforcecontactid.value, properties.salesforcecontactid.value) AS _sfdccontactid,
          COALESCE(property_salesforceleadid.value, properties.salesforceleadid.value) AS _sfdcleadid,
          ROW_NUMBER() OVER( PARTITION BY property_email.value ORDER BY property_lastmodifieddate.value DESC) AS _rownum,
        FROM 
          `x-marketing.x3x_hubspot.contacts` hs
        WHERE 
          property_email.value IS NOT NULL 
          AND property_email.value NOT LIKE '%2x.marketing%'
        )
    WHERE 
      _rownum = 1
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
  _revenue,
  _industry,
  _employee,
  _city,
  _state,
  _lifecycleStage,
  _createddate,
  _sfdcaccountid,
  _sfdccontactid,
  _sfdcleadid,
  _country,
  -- To be enabled when the airtable is updated > update the values accordingly 
  --  AS _target_contacts,
  --  AS _target_accounts
FROM 
  contacts
/* LEFT JOIN
  suppressed_industry USING(_industry) */
;
  





