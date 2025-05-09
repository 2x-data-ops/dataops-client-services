TRUNCATE TABLE `x-marketing.faro.silver_contact_log`;
INSERT INTO `x-marketing.faro.silver_contact_log` 
-- CREATE OR REPLACE TABLE `x-marketing.faro.silver_contact_log` AS
WITH prospect_data AS (
  SELECT
    ld.id AS _prospect_id,
    LEFT(ld.id,15) AS _prospect_id_15,
    ld.name AS _prospect_name,
    'Leads' AS _prospect_type,
    ld.leadsource AS _lead_source,
    ld.country AS _country,
    ld.company AS _company,
    ld.industry AS _industry,
    ld.title AS _title,
    ld.division_region__c AS _division_region,
    ld.email AS _email,
    ld.revenue__c AS _revenue_range,
    ld.employees__c AS _employees,
    ld.city AS _city,
    ld.state AS _state,
    ld.ownerid AS _ownerid,
    ld.initial_opt_in_lead__c AS _initial_opt_in,
    CAST(NULL AS STRING) AS _seniority
  FROM `x-marketing.faro_salesforce_2.Lead` ld 
  WHERE ld.isdeleted IS FALSE
  UNION ALL
  SELECT
    ct.id AS _prospect_id,
    LEFT(ct.id,15) AS _prospect_id_15,
    ct.name AS _prospect_name,
    'Contacts' AS _prospect_type,
    ct.leadsource AS _lead_source,
    ct.mailingcountry AS _country,
    ct.account_name__c AS _company,
    ct.isv_primary_industry__c AS _industry,
    ct.title AS _title,
    ct.division_region__c AS _division_region,
    ct.email AS _email,
    ct.revenue__c AS _revenue_range,
    ct.employees__c AS _employees,
    ct.mailingcity AS _city,
    ct.mailingstate AS _state,
    ct.ownerid AS _ownerid,
    ct.initial_opt_in_contact__c AS _initial_opt_in,
    CAST(NULL AS STRING) AS _seniority
  FROM `x-marketing.faro_salesforce_2.Contact` ct 
  WHERE ct.isdeleted IS FALSE
)
SELECT * FROM prospect_data;

--- Update the seniority

UPDATE `x-marketing.faro.silver_contact_log`
SET _seniority = 
CASE
  WHEN LOWER(_title) LIKE LOWER("%Assistant to%") THEN "Non-Manager" 
  WHEN LOWER(_title) LIKE LOWER("%Senior Counsel%") THEN "VP"  
  WHEN LOWER(_title) LIKE LOWER("%General Counsel%") THEN "C-Level" 
  WHEN LOWER(_title) LIKE LOWER("%Founder%") THEN "C-Level" 
  WHEN LOWER(_title) LIKE LOWER("%C-Level%") THEN "C-Level" 
  WHEN LOWER(_title) LIKE LOWER("%CDO%") THEN "C-Level" 
  WHEN LOWER(_title) LIKE LOWER("%CIO%") THEN "C-Level" 
  WHEN LOWER(_title) LIKE LOWER("%CMO%") THEN "C-Level" 
  WHEN LOWER(_title) LIKE LOWER("%CFO%") THEN "C-Level" 
  WHEN LOWER(_title) LIKE LOWER("%CEO%") THEN "C-Level" 
  WHEN LOWER(_title) LIKE LOWER("%Chief%") THEN "C-Level" 
  WHEN LOWER(_title) LIKE LOWER("%coordinator%") THEN "Non-Manager" 
  WHEN LOWER(_title) LIKE LOWER("%COO%") THEN "C-Level" 
  WHEN LOWER(_title) LIKE LOWER("%Sr. V.P.%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%Sr.VP%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%Senior-Vice Pres%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%srvp%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%Senior VP%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%SR VP%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%Sr Vice Pres%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%Sr. VP%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%Sr. Vice Pres%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%S.V.P%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%Senior Vice Pres%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%Exec Vice Pres%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%Exec Vp%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%Executive VP%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%Exec VP%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%Executive Vice President%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%EVP%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%E.V.P%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%SVP%") THEN "Senior VP" 
  WHEN LOWER(_title) LIKE LOWER("%V.P%") THEN "VP" 
  WHEN LOWER(_title) LIKE LOWER("%VP%") THEN "VP" 
  WHEN LOWER(_title) LIKE LOWER("%Vice Pres%") THEN "VP" 
  WHEN LOWER(_title) LIKE LOWER("%V P%") THEN "VP" 
  WHEN LOWER(_title) LIKE LOWER("%President%") THEN "C-Level" 
  WHEN LOWER(_title) LIKE LOWER("%Director%") THEN "Director" 
  WHEN LOWER(_title) LIKE LOWER("%CTO%") THEN "C-Level" 
  WHEN LOWER(_title) LIKE LOWER("%Dir%") THEN "Director" 
  WHEN LOWER(_title) LIKE LOWER("%Dir.%") THEN "Director" 
  WHEN LOWER(_title) LIKE LOWER("%MDR%") THEN "Non-Manager" 
  WHEN LOWER(_title) LIKE LOWER("%MD%") THEN "Director" 
  WHEN LOWER(_title) LIKE LOWER("%GM%") THEN "Director" 
  WHEN LOWER(_title) LIKE LOWER("%Head%") THEN "VP" 
  WHEN LOWER(_title) LIKE LOWER("%Manager%") THEN "Manager" 
  WHEN LOWER(_title) LIKE LOWER("%escrow%") THEN "Non-Manager" 
  WHEN LOWER(_title) LIKE LOWER("%cross%") THEN "Non-Manager" 
  WHEN LOWER(_title) LIKE LOWER("%crosse%") THEN "Non-Manager" 
  WHEN LOWER(_title) LIKE LOWER("%Partner%") THEN "C-Level" 
  WHEN LOWER(_title) LIKE LOWER("%CRO%") THEN "C-Level" 
  WHEN LOWER(_title) LIKE LOWER("%Chairman%") THEN "C-Level" 
  WHEN LOWER(_title) LIKE LOWER("%Owner%") THEN "C-Level"
  WHEN LOWER(_title) LIKE LOWER("%Team Lead%") THEN "Manager"
END
WHERE _seniority IS NULL 
  AND _title IS NOT NULL;