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