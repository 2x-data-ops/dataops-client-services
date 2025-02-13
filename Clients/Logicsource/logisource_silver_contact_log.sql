--CREATE OR REPLACE TABLE `x-marketing.logicsource.contact_icp_score` AS
TRUNCATE TABLE `x-marketing.logicsource.contact_icp_score`;

INSERT INTO `x-marketing.logicsource.contact_icp_score` (
  _prospectid,
  _email,
  _name,
  _domain,
  jobtitle,
  _function,
  _company,
  company_id,
  hubspot_score,
  _jobrole_organic,
  score_job_role_organic,
  _management_level_organic,
  score_management_level_organic,
  _jobrole,
  score_job_role,
  _management_level,
  score_management_level,
  _annualrevenue,
  anualrevenue_range,
  anualrevenue_range_score,
  total_score_ICP
)
WITH contact AS (
  SELECT
    CAST(vid AS STRING) AS _prospectid,
    property_email.value AS _email,
    COALESCE(CONCAT(property_firstname.value, ' ', property_lastname.value), property_firstname.value) AS _name,
    CASE
      WHEN associated_company.properties.domain.value IS NULL THEN property_hs_email_domain.value
      ELSE associated_company.properties.domain.value
    END AS _domain,
    properties.jobtitle.value AS jobtitle,
    properties.job_function.value AS _function,
    CASE
      WHEN associated_company.properties.name.value IS NULL THEN properties.company.value
      ELSE associated_company.properties.name.value
    END AS _company,
    CASE
      WHEN associated_company.company_id IS NULL THEN CAST(properties.associatedcompanyid.value AS INT64)
    END AS company_id,
    property_hubspotscore.value AS hubspot_score,
    --CASE WHEN 
    IF(property_job_role__organic_.value = '', CAST(NULL AS STRING), property_job_role__organic_.value) AS _jobrole_organic,
    CASE
      WHEN property_job_role__organic_.value = '' THEN 0
      WHEN property_job_role__organic_.value LIKE '%Sales%' THEN 0
      WHEN property_job_role__organic_.value LIKE '%Business Development%' THEN 0
      WHEN property_job_role__organic_.value IS NOT NULL THEN 10
      ELSE 0
    END AS score_job_role_organic,
    IF(property_management_level__organic_.value = '', CAST(NULL AS STRING), property_management_level__organic_.value) AS _management_level_organic,
    CASE
      WHEN property_management_level__organic_.value = '' THEN 0
      WHEN property_management_level__organic_.value = 'Non-Manager' THEN 0
      WHEN property_management_level__organic_.value = 'Manager' THEN 0
      WHEN property_management_level__organic_.value IS NOT NULL THEN 10
      ELSE 0
    END AS score_management_level_organic,
    IF(property_job_role.value = '', CAST(NULL AS STRING), property_job_role.value) AS _jobrole,
    CASE
      WHEN property_job_role.value = '' THEN 0
      WHEN property_job_role.value LIKE '%Sales%' THEN 0
      WHEN property_job_role.value LIKE '%Business Development%' THEN 0
      WHEN property_job_role.value IS NOT NULL THEN 5
      ELSE 0
    END AS score_job_role,
    IF(property_management_level.value = '', CAST(NULL AS STRING), property_management_level.value) AS _management_level,
    CASE
      WHEN property_management_level.value = '' THEN 0
      WHEN property_management_level.value = 'Non-Manager' THEN 0
      WHEN property_management_level.value = 'Manager' THEN 0
      WHEN property_management_level.value IS NOT NULL THEN 5
      ELSE 0
    END AS score_management_level,
    CAST(associated_company.properties.annualrevenue.value AS NUMERIC) AS _annualrevenue,
    CASE
      WHEN CAST(associated_company.properties.annualrevenue.value AS NUMERIC) IS NULL THEN "<1 Bil."
      WHEN CAST(associated_company.properties.annualrevenue.value AS NUMERIC) < 1000000000 THEN "<1 Bil."
      ELSE ">1 Bil."
    END anualrevenue_range,
    CASE
      WHEN CAST(associated_company.properties.annualrevenue.value AS NUMERIC) IS NULL THEN 0
      WHEN CAST(associated_company.properties.annualrevenue.value AS NUMERIC) < 1000000000 THEN 0
      ELSE 10
    END anualrevenue_range_score,
  FROM `x-marketing.logicsource_hubspot.contacts` k
  --LEFT JOIN `x-marketing.logicsource_salesforce.Lead` l ON LOWER(l.email) = LOWER(property_email.value)
  --WHERE vid = 279601
)
SELECT
  *,
  SUM(score_job_role_organic) + SUM(score_management_level_organic) +
  SUM(anualrevenue_range_score) + SUM(score_job_role) +
  SUM(score_management_level) AS total_score_ICP
FROM contact
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20;

--  CREATE OR REPLACE TABLE `x-marketing.logicsource.account_icp_score` AS
TRUNCATE TABLE `x-marketing.logicsource.account_icp_score`;

INSERT INTO `x-marketing.logicsource.account_icp_score` (
  _domain,
  total_employee,
  total_score_divide_2,
  total_score,
  max_score
)
SELECT
  _domain,
  COALESCE(COUNT(DISTINCT _prospectid), 0) AS total_employee,
  COALESCE(SUM(total_score_ICP) / 2, 0) AS total_score_divide_2,
  COALESCE(SUM(total_score_ICP), 0) total_score,
  MAX(total_score_ICP) AS max_score
FROM `x-marketing.logicsource.contact_icp_score`
GROUP BY 1;

--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------ Database Reporting Script -----------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
/*
  Platform : hubspot
 */
-- No bombora report so it's being excluded.
-- CREATE OR REPLACE TABLE logicsource.db_icp_database_log AS
TRUNCATE TABLE `x-marketing.logicsource.db_icp_database_log`;

INSERT INTO `x-marketing.logicsource.db_icp_database_log` (
    _id,
    _email,
    _name,
    _domain,
    _jobtitle,
    _seniority,
    _function,
    _jobrole,
    _mqldate,
    _source,
    _latest_source,
    _phone,
    _company,
    _industry,
    _revenue,
    _employee,
    _city,
    _state,
    _persona,
    _lifecycleStage,
    _leadscore,
    _leadstatus,
    _ipqc_check,
    _hubspotScore,
    _companyID,
    _companySegment,
    _leadSegment,
    _segment,
    _propertyLeadstatus,
    _companylinkedinbio,
    _company_linkedin,
    _employee_range,
    _employee_range_c,
    _numberofemployees,
    _annualrevenue,
    _annual_revenue_range,
    _annual_revenue_range_c,
    _createddate,
    _sfdcaccountid,
    _sfdccontactid,
    _sfdcleadid,
    _country,
    _target_contacts,
    _target_accounts,
    _sales_follow_up_progress,
    _leadsource
  )
  WITH contact_details AS (
    SELECT
      vid AS _id,
      property_email.value AS _email,
      CONCAT(property_firstname.value, ' ', property_lastname.value) AS _name,
      LOWER(COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value)) AS _domain,
      properties.jobtitle.value AS _jobtitle,
      CASE
        WHEN property_management_level__organic_.value IS NOT NULL THEN property_management_level__organic_.value
        ELSE property_management_level.value
      END _seniority,
      properties.job_function.value AS _function,
      CASE
        WHEN property_job_role__organic_.value IS NOT NULL THEN property_job_role__organic_.value
        ELSE property_job_role.value
      END AS _jobrole,
      properties.hs_lifecyclestage_marketingqualifiedlead_date.value AS _mqldate,
      properties.hs_analytics_source.value AS _source,
      properties.hs_latest_source.value AS _latest_source,
      property_phone.value AS _phone,
      associated_company.properties.name.value AS _company,
      COALESCE(CAST(associated_company.properties.annualrevenue.value AS STRING), property_annualrevenue.value ) AS _revenue,
      CASE
        WHEN associated_company.properties.industry.value = 'RETAIL' THEN 'Retail'
        WHEN associated_company.properties.industry.value = 'INSURANCE' THEN 'Insurance'
        WHEN associated_company.properties.industry.value LIKE '%FOOD_PRODUCTION%' THEN 'Manufacturing'
        WHEN associated_company.properties.industry.value = 'HOSPITALITY' THEN 'Hospitality'
        WHEN associated_company.properties.industry.value = 'HOSPITAL_HEALTH_CARE' THEN 'Hospital & Health Care'
        WHEN associated_company.properties.name.value LIKE '%BT%' THEN 'Media & Internet'
        ELSE COALESCE(associated_company.properties.industry.value, property_industry.value)
      END AS _industry,
      COALESCE(CAST(associated_company.properties.numberofemployees.value AS STRING), CAST(property_numberofemployees.value AS STRING)) AS _employee,
      property_city.value AS _city,
      property_state.value AS _state,
      COALESCE(property_country.value, associated_company.properties.country.value, property_ip_country_code.value) AS _country,
      '' AS _persona,
      CASE
        WHEN property_lifecyclestage.value = "177399578" THEN 'N/A'
        WHEN property_lifecyclestage.value = "lead" THEN 'Target'
        WHEN property_lifecyclestage.value = "marketingqualifiedlead" THEN 'Marketing Lead (ML)'
        WHEN property_lifecyclestage.value = "salesqualifiedlead" THEN "Marketing Qualified Lead (MQL)"
        WHEN property_lifecyclestage.value = "176142615" THEN 'Qualification'
        WHEN property_lifecyclestage.value = "183947660" THEN 'Assessment Planning'
        WHEN property_lifecyclestage.value = "183930970" THEN 'Assessment Pre-work'
        WHEN property_lifecyclestage.value = "opportunity" THEN 'Assessment Active'
        WHEN property_lifecyclestage.value = "183952263" THEN 'In Contracting'
        WHEN property_lifecyclestage.value = "customer" THEN 'Customer'
        WHEN property_lifecyclestage.value = "178822172" THEN 'Loss'
        ELSE property_lifecyclestage.value
      END AS _lifecycleStage,
      CAST(l.lead_score__c AS INT64) AS _leadscore,
      properties.hs_lead_status.value AS _leadstatus,
      properties.ipqc_check.value AS _ipqc_check,
      property_hubspotscore.value AS _hubspotScore,
      associated_company.company_id AS _companyID,
      associated_company.properties.segment__c.value AS _companySegment,
      property_lead_segment.value AS _leadSegment,
      property_segment__c.value AS _segment,
      property_leadstatus.value AS _propertyLeadstatus,
      associated_company.properties.linkedinbio.value AS _companylinkedinbio,
      associated_company.properties.linkedin_company_page.value AS _company_linkedin,
      associated_company.properties.employee_range.value AS _employee_range,
      associated_company.properties.employee_range_c.value AS _employee_range_c,
      CAST(associated_company.properties.numberofemployees.value AS NUMERIC) AS _numberofemployees,
      CAST(associated_company.properties.annualrevenue.value AS NUMERIC) AS _annualrevenue,
      associated_company.properties.annual_revenue_range.value AS _annual_revenue_range,
      associated_company.properties.annual_revenue_range_c.value AS _annual_revenue_range_c,
      property_createdate.value AS _createddate,
      COALESCE(associated_company.properties.salesforceaccountid.value, properties.salesforceaccountid.value) AS _sfdcaccountid,
      COALESCE(property_salesforcecontactid.value, properties.salesforcecontactid.value) AS _sfdccontactid,
      COALESCE(property_salesforceleadid.value, properties.salesforceleadid.value) AS _sfdcleadid,
      properties.lead_type__c.value AS _sales_follow_up_progress,
      properties.leadsource.value AS _leadsource,
    FROM `x-marketing.logicsource_hubspot.contacts` hs
    LEFT JOIN `x-marketing.logicsource_salesforce.Lead` l
      ON LOWER(l.email) = LOWER(property_email.value)
    WHERE property_email.value IS NOT NULL
      AND property_email.value NOT LIKE '%2x.marketing%'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY property_email.value, CAST(vid AS STRING) ORDER BY property_lastmodifieddate.value DESC) = 1
  ),
  -- To be filled up if required
  contacts AS (
    SELECT
      * EXCEPT (_country),
      CASE
        WHEN LOWER(_country) IN (
          'us',
          'usa',
          'united states',
          'united states of america'
        ) THEN 'US'
        ELSE _country
      END AS _country
    FROM contact_details
  )
SELECT DISTINCT
  _id,
  _email,
  _name,
  _domain,
  _jobtitle,
  _seniority,
  _function,
  _jobrole,
  _mqldate,
  _source,
  _latest_source,
  _phone,
  _company,
  _industry,
  _revenue,
  _employee,
  _city,
  _state,
  _persona,
  _lifecycleStage,
  _leadscore,
  _leadstatus,
  _ipqc_check,
  _hubspotScore,
  _companyID,
  _companySegment,
  _leadSegment,
  _segment,
  _propertyLeadstatus,
  _companylinkedinbio,
  _company_linkedin,
  _employee_range,
  _employee_range_c,
  _numberofemployees,
  _annualrevenue,
  _annual_revenue_range,
  _annual_revenue_range_c,
  _createddate,
  _sfdcaccountid,
  _sfdccontactid,
  _sfdcleadid,
  _country,
  CAST(NULL AS INT64) AS _target_contacts,
  CAST(NULL AS INT64) AS _target_accounts,
  _sales_follow_up_progress,
  _leadsource -- To be enabled when the airtable is updated > update the values accordingly 
FROM contacts;