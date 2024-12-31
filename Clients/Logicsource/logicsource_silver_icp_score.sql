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