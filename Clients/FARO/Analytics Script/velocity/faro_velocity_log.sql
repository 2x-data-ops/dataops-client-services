--------------------------------------------------------------
-------------------------- Activity --------------------------
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

--------------------------------------------------------------
------------------------ Member Stat -------------------------
--------------------------------------------------------------

CREATE OR REPLACE TABLE `x-marketing.faro.db_member_log` AS
SELECT DISTINCT
  member.campaignid AS _campaignid,
  member.leadorcontactid AS _leadorcontactid,
  campaign.name AS _campaign_name,
  campaign.type AS _campaign_type,
  member.status AS _member_status,
  member.lastmodifieddate AS _last_modified_date
FROM `x-marketing.faro_salesforce.CampaignMember` member
LEFT JOIN `x-marketing.faro_salesforce.Campaign` campaign
  ON campaign.id = member.campaignid;

--------------------------------------------------------------
-------------------------- Velocity --------------------------
--------------------------------------------------------------
CREATE OR REPLACE TABLE `x-marketing.faro.db_velocity_log` AS
WITH stages_data AS (
  SELECT DISTINCT
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
  SELECT DISTINCT
    _prospect_id,
    _subject,
    _activity_date
  FROM `x-marketing.faro.db_activity_log` 
  ORDER BY _activity_date
),
activity AS (
  SELECT DISTINCT
    stages_data._prospect_id,
    stages_data._old_new,
    stages_data._created_date,
    stages_data._next_change_date,
    COUNT(activity_data._subject) AS _activity_count
  FROM stages_data
  LEFT JOIN activity_data
    ON activity_data._prospect_id = stages_data._prospect_id
    AND activity_data._activity_date 
    BETWEEN stages_data._created_date AND stages_data._next_change_date
  GROUP BY 
    stages_data._prospect_id,
    stages_data._old_new,
    stages_data._created_date,
    stages_data._next_change_date
)
SELECT DISTINCT
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

------------------------- RECENT LOG -------------------------
CREATE OR REPLACE TABLE `x-marketing.faro.db_velocity_recent` AS
SELECT 
  velocity.*,
  member_log.*
FROM `x-marketing.faro.db_velocity_log` velocity
LEFT JOIN `x-marketing.faro.db_member_log` member_log
  ON velocity._prospect_id = member_log._leadorcontactid
  AND member_log._last_modified_date 
  BETWEEN velocity._created_date AND velocity._next_change_date;

------------------------- MEMBER STAT -------------------------
CREATE OR REPLACE TABLE `x-marketing.faro.db_member_stat` AS
SELECT
  member_log._campaignid,
  member_log._leadorcontactid,
  member_log._campaign_name,
  member_log._campaign_type,
  member_log._member_status,
  member_log._last_modified_date,
  velocity._created_date,
  velocity._next_change_date,
  velocity._old_value,
  velocity._new_value,
  velocity._2x_stages
FROM `x-marketing.faro.db_velocity_log` velocity
LEFT JOIN `x-marketing.faro.db_member_log` member_log
  ON velocity._prospect_id = member_log._leadorcontactid