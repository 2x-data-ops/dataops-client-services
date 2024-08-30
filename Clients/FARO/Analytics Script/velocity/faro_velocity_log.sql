--------------------------------------------------------------
-------------------------- VELOCITY --------------------------
--------------------------------------------------------------

CREATE OR REPLACE TABLE `x-marketing.faro.db_activity_log` AS
WITH activity AS (
  SELECT DISTINCT
    id AS _activity_id,
    createddate AS _activity_date,
    description AS _description,
    -- contact_id__c AS _contact_id_c,
    COALESCE(
      CASE
        WHEN whoid LIKE "00Q%"
        THEN whoid
      END,
      CASE
        WHEN whoid LIKE "003%"
        THEN whoid
      END
    ) AS _prospect_id,
    subject AS _subject,
    event_status__c AS _status,
    type AS _activity_type,
    'Event' AS _activity_category
  FROM `x-marketing.faro_salesforce.Event`
  UNION ALL
  SELECT DISTINCT
    id AS _activity_id,
    createddate AS _activity_date,
    description AS _description,
    -- contact_id__c AS _contact_id_c,
    COALESCE(
      CASE
        WHEN whoid LIKE "00Q%"
        THEN whoid
      END,
      CASE
        WHEN whoid LIKE "003%"
        THEN whoid
      END
    ) AS _prospect_id,
    subject AS _subject,
    event_status__c AS _status,
    type AS _activity_type,
    'Task' AS _activity_category
  FROM `x-marketing.faro_salesforce.Task`
)
SELECT * FROM activity
WHERE EXTRACT(YEAR FROM _activity_date) > 2021;

CREATE OR REPLACE TABLE `x-marketing.faro.db_icp_database_log` AS
WITH prospect_data AS (
  SELECT
    leads.leadid AS _prospect_id,
    LEFT(leads.leadid,15) AS _prospect_id_15,
    ld.name AS _prospect_name,
    -- act.name AS _prospectOwner,
    IF(
    leads.oldvalue IS NULL,
    'None',
    leads.oldvalue
    ) AS _old_value,
    leads.newvalue AS _new_value,
    leads.createddate AS _created_date,
    leads.field AS _field,
    'Leads' AS _prospect_type,
    ld.leadsource AS _leadsource,
    ld.country AS _country,
    ld.assigned_iss__c AS _iss_name,
    ld.company AS _company,
    ld.zprimary_solution_interest__c AS _zprimary_solution_interest,
    ld.id AS _id,
    ld.industry AS _industry,
    ld.primary_hardware_interest__c AS _primary_hardware_interest,
    ld.primary_software_interest__c AS _primary_software_interest,
    ld.web_primary_software_interest__c AS _web_primary_software_interest,
    '' AS _addition_product_interest,
    ld.primary_application__c AS _primary_application,
    ld.product_interest__c AS _product_interest,
    ld.title AS _title,
    '' AS _campaign_product_interest,
    ld.no_current_interest_reason__c AS _no_current_interest_reason,
    ld.secondary_application__c AS _secondary_application,
    ld.vertical__c AS _vertical
  FROM `x-marketing.faro_salesforce.LeadHistory` leads
  LEFT JOIN `x-marketing.faro_salesforce.Lead` ld 
    ON leads.leadid = ld.id
  -- LEFT JOIN `x-marketing.faro_salesforce.Account` act ON ld.convertedaccountid = act.id
  WHERE leads.isdeleted IS FALSE
    AND EXTRACT(YEAR FROM DATE(leads.createddate)) > 2021
    AND leads.field IN ('Waterfall_Stage__c', 'Status')
    AND (leads.field != 'Status' OR leads.newvalue = 'No Current Interest-Recycled')
  -- QUALIFY ROW_NUMBER() OVER(PARTITION BY leads.id ORDER BY leads.createddate) = 1
  UNION ALL
  SELECT
    contacts.contactid AS _prospect_id,
    LEFT(contacts.contactid,15) AS _prospect_id_15,
    ct.name AS _prospect_name,
    -- mbr.name AS _prospectOwner,
    IF(
    contacts.oldvalue IS NULL,
    'None',
    contacts.oldvalue
    ) AS _old_value,
    contacts.newvalue AS _new_value,
    contacts.createddate AS _created_date,
    contacts.field AS _field,
    'Contacts' AS _prospect_type,
    ct.leadsource AS _leadsource,
    ct.mailingcountry AS _country,
    ct.assigned_iss__c AS _iss_name,
    ct.account_name__c AS _company,
    '' AS _zprimary_solution_interest,
    ct.id AS _id,
    ct.isv_primary_industry__c AS _industry,
    '' AS _primary_hardware_interest,
    '' AS _primary_software_interest,
    '' AS _web_primary_software_interest,
    ct.additional_product_interest__c AS _addition_product_interest,
    '' AS _primary_application,
    ct.product_interest__c AS _product_interest,
    ct.title AS _title,
    ct.campaign_product_interest__c AS _campaign_product_interest,
    ct.no_current_interest_reason__c AS _no_current_interest_reason,
    ct.secondary_application__c AS _secondary_application,
    ct.vertical__c AS _vertical
  FROM `x-marketing.faro_salesforce.ContactHistory` contacts
  LEFT JOIN `x-marketing.faro_salesforce.Contact` ct 
    ON contacts.contactid = ct.id
  -- LEFT JOIN `x-marketing.faro_salesforce.CampaignMember` mbr ON ct.ownerid = mbr.leadorcontactownerid
  WHERE contacts.isdeleted IS FALSE
    AND EXTRACT(YEAR FROM DATE(contacts.createddate)) > 2021
    AND contacts.field IN ('Waterfall_Stage__c', 'Status')
    AND (contacts.field != 'Status' OR contacts.newvalue = 'No Current Interest-Recycled')
  -- QUALIFY ROW_NUMBER() OVER(PARTITION BY contacts.id ORDER BY contacts.createddate) = 1
)
SELECT * FROM prospect_data;


CREATE OR REPLACE TABLE `x-marketing.faro.db_velocity_log` AS
WITH stages_data AS (
  SELECT
  DISTINCT
    velocity_data.*,
    LEFT(velocity_data._prospect_id,15) AS _prospect_id15,
    LEAD(velocity_data._created_date) OVER (PARTITION BY velocity_data._prospect_id ORDER BY velocity_data._created_date) AS _next_change_date,
    CASE
      WHEN _old_value = 'Inquiry' AND _new_value = 'Automated Qualified Lead' THEN 'AQL'
      WHEN _old_value IS NULL AND _new_value = 'Inquiry' THEN 'Inquiry'
      WHEN _old_value = 'Automated Qualified Lead' AND _new_value = 'Inside Sales Accepted Lead' THEN 'ISAL'
      WHEN _old_value = 'Automated Qualified Lead' AND _new_value = 'Sales Accepted Lead' THEN 'SAL'
      WHEN _old_value = 'Inside Sales Accepted Lead' AND _new_value = 'Inside Sales Qualified Lead' THEN 'ISQL'
      WHEN _old_value = 'None' AND _new_value = 'Inside Sales Generated Lead' THEN 'ISGL'
      WHEN _old_value = 'Inside Sales Qualified Lead' AND _new_value = 'Sales Accepted Lead' THEN 'SAL'
      WHEN _old_value = 'Inside Sales Generated Lead' AND _new_value = 'Sales Accepted Lead' THEN 'SAL'
      WHEN _old_value = 'Sales Generated Lead' AND _new_value = 'Sales Qualified Lead' THEN 'SQL'
      WHEN _old_value = 'Sales Accepted Lead' AND _new_value = 'Sales Qualified Lead' THEN 'SQL'
      WHEN _old_value = 'None' AND _new_value = 'Sales Generated Lead' THEN 'SGL'
      WHEN (_old_value = 'Sales Accepted Lead' OR _old_value = 'Sales Generated Lead') AND _new_value = 'Sales Qualified Opportunity' THEN 'SQO'
      WHEN _new_value = 'No Current Interest-Recycled' THEN 'NCIR'
      WHEN _new_value = 'Closed Won Opportunity' THEN 'Closed Won Opportunity'
      WHEN _new_value = 'Closed Lost Opportunity' THEN 'Closed Lost Opportunity'
      WHEN _new_value = 'Sales Rejected Lead' THEN 'Rejected'
      WHEN _new_value = 'Inside Sales Rejected Lead' THEN 'Rejected'
      ELSE 'Skipped Stage'
    END AS _2x_stages,
    CONCAT(_old_value,_new_value) AS _old_new
  FROM `x-marketing.faro.db_icp_database_log` velocity_data
),
activity_data AS (
  SELECT 
  DISTINCT
    _prospect_id,
    _subject,
    _activity_date
  FROM `x-marketing.faro.db_activity_log` 
  ORDER BY _activity_date
),
activity AS (
  SELECT
  DISTINCT
    stages_data._prospect_id,
    stages_data._old_new,
    stages_data._created_date,
    stages_data._next_change_date,
    COUNT(activity_data._subject) AS _activity_count
  FROM stages_data
  LEFT JOIN activity_data
    -- ON activity_data._prospect_id = stages_data._prospect_id15
    ON activity_data._prospect_id = stages_data._prospect_id
    AND activity_data._activity_date 
    BETWEEN stages_data._created_date AND stages_data._next_change_date
  GROUP BY 
    stages_data._prospect_id,
    stages_data._old_new,
    stages_data._created_date,
    stages_data._next_change_date
)
SELECT 
DISTINCT
  stages_data._prospect_id,
  stages_data._old_value,
  stages_data._new_value,
  stages_data._created_date,
  stages_data._next_change_date,
  stages_data._field,
  stages_data._prospect_type,
  stages_data._2x_stages,
  stages_data._prospect_name,
  stages_data._leadsource,
  stages_data._country,
  stages_data._iss_name,
  stages_data._company,
  stages_data._zprimary_solution_interest,
  stages_data._id,
  stages_data._industry,
  stages_data._primary_hardware_interest,
  stages_data._primary_software_interest,
  stages_data._web_primary_software_interest,
  stages_data._addition_product_interest,
  stages_data._primary_application,
  stages_data._product_interest,
  stages_data._title,
  stages_data._campaign_product_interest,
  stages_data._no_current_interest_reason,
  stages_data._secondary_application,
  CASE
    WHEN stages_data._next_change_date IS NULL
    THEN COALESCE( 
      DATE_DIFF(CURRENT_DATE(), DATE(stages_data._created_date),DAY),
      0
    )
    ELSE COALESCE(
      DATE_DIFF(DATE(stages_data._next_change_date),DATE(stages_data._created_date),DAY),
      0
    )
  END AS _stage_change_duration_days,
  activity._activity_count
FROM stages_data
LEFT JOIN activity
  ON activity._prospect_id = stages_data._prospect_id
  AND activity._old_new = CONCAT(stages_data._old_value,stages_data._new_value)
  AND activity._created_date = stages_data._created_date;