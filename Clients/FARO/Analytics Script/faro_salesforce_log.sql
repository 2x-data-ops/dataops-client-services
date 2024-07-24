-- CREATE OR REPLACE TABLE `x-marketing.faro.db_salesforce_log` AS

TRUNCATE TABLE `x-marketing.faro.db_salesforce_log`;
INSERT INTO `x-marketing.faro.db_salesforce_log`
WITH campaignMembers AS (
  SELECT
    id,
    campaignid,
    status,
    createddate,
    lastmodifieddate,
    --name,
    --firstname,
    --lastname,
    --title,
  --street,
    city,
    state,
    postalcode,
    country,
    --email,
    --phone,
    --fax,
    --mobilephone,
    --description,
    --leadsource,
    --companyoraccount,
    type,
    leadorcontactid,
    sfdc_campmem_casesafeid__c,
    first_associated_date__c,
    activity__c,
    is_duplicate__c,
    --assigned_iss__c,
    --current_waterfall_stage_date_time__c AS WaterfallStageDate__c,
    account_manager__c,
    inside_sales_specialis__c,
    language__c,
    --current_waterfall_stage_date_time__c,
    --lead_contact_no_current_interest_reason__c,
    --lead_contact_source__c,
    --lead_contact_status__c,
    --lead_contact_waterfalls_stage__c,
    --most_recent_lead_source__c,
    --reason_for_disqualification__c,
    --pull_market_segment__c,
    --region__c
  FROM `x-marketing.faro_salesforce.CampaignMember`
),  
lead_contact AS (
  SELECT 
    leads.id,
    leads.firstname,
    leads.lastname,
    leads.name,
    leads.title,
    leads.street,
    ---leads.city,
    --leads.state,
    --leads.postalcode,
    --leads.country,
    leads.phone,
    leads.fax,
    leads.mobilephone,
    leads.email,
    description,
    leadsource,
    company  AS companyoraccount,
    current_waterfall_stage_date_time__c AS WaterfallStageDate__c,
    OwnerId AS _manager__c,
    owner.name AS account_manager__c_name,
    users.name AS assigned_iss_name__c,
    assigned_iss__c, 
    current_waterfall_stage_date_time__c,
    no_current_interest_reason__c AS lead_contact_no_current_interest_reason__c,
    leadsource AS lead_contact_source__c ,
    Status AS lead_contact_status__c,
    Waterfall_Stage__c AS lead_contact_waterfalls_stage__c,
    most_recent_lead_source__c,
    reason_for_disqualification__c,
    leads.Vertical__c AS pull_market_segment__c,
    leads.Division_Region__c  AS region__c,
    leads.primary_hardware_interest__c AS hardwareInterest,
    leads.Additional_Product_Interest__c AS additionalProductInterest,
    leads.primary_software_interest__c AS primarySoftwareInterest,
    leads.web_primary_software_interest__c AS webPrimarySoftwareInterest,
    leads.trial_software_interest__c AS trialSoftwareInterest,
    leads.product_interest__c AS productInterest,
    -- distribution__c,
    distributor__c,
    media_code__c
 FROM `x-marketing.faro_salesforce.Lead` leads
 LEFT JOIN `x-marketing.faro_salesforce.User` users ON leads.assigned_iss__c = users.id
 LEFT JOIN `x-marketing.faro_salesforce.User` Owner ON leads.OwnerId = Owner.id
 UNION ALL 
 SELECT 
    leads.id,
    leads.firstname,
    leads.lastname,
    leads.name,
    leads.title,
    leads.otherstreet,
    ---leads.city,
    --leads.state,
    --leads.postalcode,
    --leads.country,
    leads.phone,
    leads.fax,
    leads.mobilephone,
    leads.email,
    leads.description,
    leadsource,
    account_name__c,
    current_waterfall_stage_date_time__c AS WaterfallStageDate__c,
    leads.OwnerId AS _manager__c,
    owner.name AS account_manager__c_name,
    users.name,
    leads.assigned_iss__c, 
    --assigned_iss_name__c,
    current_waterfall_stage_date_time__c,
    no_current_interest_reason__c,
    leadsource,
    leads.status__c,
    Waterfall_Stage__c,
    most_recent_lead_source__c,
    reason_for_disqualification__c,
    leads.Vertical__c ,
    leads.Division_Region__c ,
    leads.primary_hardware_interest__c AS hardwareInterest,
    leads.Additional_Product_Interest__c AS additionalProductInterest,
    leads.primary_software_interest__c AS primarySoftwareInterest,
    leads.web_primary_software_interest__c AS webPrimarySoftwareInterest,
    leads.trial_software_interest__c AS trialSoftwareInterest,
    leads.product_interest__c AS productInterest,
    -- distribution__c,
    distributor__c,
    media_code__c
  FROM `x-marketing.faro_salesforce.Contact` leads
  LEFT JOIN `x-marketing.faro_salesforce.User` users ON leads.assigned_iss__c = users.id
  LEFT JOIN `x-marketing.faro_salesforce.User` Owner ON leads.OwnerId = Owner.id
  LEFT JOIN `x-marketing.faro_salesforce.Account` account ON leads.accountid = account.id
), 
campaigns AS (
  SELECT
    id,
    name,
    type
  FROM `x-marketing.faro_salesforce.Campaign`
)
SELECT
  campaignMembers.*,
  leads.* EXCEPT (id),
  campaigns.name AS campaignName,
  campaigns.type AS campaignType
FROM campaignMembers
LEFT JOIN lead_contact  leads
ON campaignMembers.leadorcontactid = leads.id
LEFT JOIN campaigns
ON campaignMembers.campaignid = campaigns.id;

--------------------------------------------------------------
-------------------------- VELOCITY --------------------------
--------------------------------------------------------------

CREATE OR REPLACE TABLE `x-marketing.faro.db_activity_log` AS
WITH activity AS (
  SELECT DISTINCT
    id AS _activityID,
    createddate AS _activityDate,
    description AS _description,
    contact_id__c AS _contactID_C,
    subject AS _subject,
    event_status__c AS _status,
    type AS _activityType,
    'Event' AS _activityCategory
  FROM `x-marketing.faro_salesforce.Event`
  UNION ALL
  SELECT DISTINCT
    id AS _activityID,
    createddate AS _activityDate,
    description AS _description,
    contact_id__c AS _contactID_C,
    subject AS _subject,
    event_status__c AS _status,
    type AS _activityType,
    'Task' AS _activityCategory
  FROM `x-marketing.faro_salesforce.Task`
)
SELECT * FROM activity
WHERE EXTRACT(YEAR FROM _activityDate) > 2021;


CREATE OR REPLACE TABLE `x-marketing.faro.db_velocity_log` AS
WITH velocity_data AS (
  SELECT
    leads.leadid AS _prospectID,
    -- ld.name AS _prospectName,
    -- act.name AS _prospectOwner,
    leads.oldvalue AS _oldValue,
    leads.newvalue AS _newValue,
    leads.createddate AS _createdDate,
    leads.field AS _field,
    'Leads' AS _prospectType
  FROM `x-marketing.faro_salesforce.LeadHistory` leads
  -- LEFT JOIN `x-marketing.faro_salesforce.Lead` ld ON leads.leadid = ld.id
  -- LEFT JOIN `x-marketing.faro_salesforce.Account` act ON ld.convertedaccountid = act.id
  WHERE leads.isdeleted IS FALSE
  AND EXTRACT(YEAR FROM DATE(leads.createddate)) > 2021
  AND leads.field IN (
    'Waterfall_Stage__c', 'Status'
  )
  AND (leads.field != 'Status' OR leads.newvalue = 'No Current Interest-Recycled')
  -- QUALIFY ROW_NUMBER() OVER(PARTITION BY leads.id ORDER BY leads.createddate) = 1
  UNION ALL
  SELECT
    contacts.contactid AS _prospectID,
    -- ct.name AS _prospectName,
    -- mbr.name AS _prospectOwner,
    contacts.oldvalue AS _oldValue,
    contacts.newvalue AS _newValue,
    contacts.createddate AS _createdDate,
    contacts.field AS _field,
    'Contacts' AS _prospectType
  FROM `x-marketing.faro_salesforce.ContactHistory` contacts
  -- LEFT JOIN `x-marketing.faro_salesforce.Contact` ct ON contacts.contactid = ct.id
  -- LEFT JOIN `x-marketing.faro_salesforce.CampaignMember` mbr ON ct.ownerid = mbr.leadorcontactownerid
  WHERE contacts.isdeleted IS FALSE
  AND EXTRACT(YEAR FROM DATE(contacts.createddate)) > 2021
  AND contacts.field IN (
    'Waterfall_Stage__c', 'Status'
  )
  AND (contacts.field != 'Status' OR contacts.newvalue = 'No Current Interest-Recycled')
  -- QUALIFY ROW_NUMBER() OVER(PARTITION BY contacts.id ORDER BY contacts.createddate) = 1
),
stages_data AS (
  SELECT
    velocity_data.*,
    LEFT(velocity_data._prospectID,15) AS _prospectID15,
    LEAD(velocity_data._createdDate) OVER (PARTITION BY velocity_data._prospectID ORDER BY velocity_data._createdDate) AS _next_change_date,
    CASE
      WHEN _oldValue = 'Inquiry' AND _newValue = 'Automated Qualified Lead' THEN 'AQL'
      WHEN _oldValue IS NULL AND _newValue = 'Inquiry' THEN 'Inquiry'
      WHEN _oldValue = 'Automated Qualified Lead' AND _newValue = 'Inside Sales Accepted Lead' THEN 'ISAL'
      WHEN _oldValue = 'Automated Qualified Lead' AND _newValue = 'Sales Accepted Lead' THEN 'SAL'
      WHEN _oldValue = 'Inside Sales Accepted Lead' AND _newValue = 'Inside Sales Qualified Lead' THEN 'ISQL'
      WHEN _oldValue IS NULL AND _newValue = 'Inside Sales Generated Lead' THEN 'ISGL'
      WHEN _oldValue = 'Inside Sales Qualified Lead' AND _newValue = 'Sales Accepted Lead' THEN 'SAL'
      WHEN _oldValue = 'Inside Sales Generated Lead' AND _newValue = 'Sales Accepted Lead' THEN 'SAL'
      WHEN _oldValue = 'Sales Generated Lead' AND _newValue = 'Sales Qualified Lead' THEN 'SQL'
      WHEN _oldValue = 'Sales Accepted Lead' AND _newValue = 'Sales Qualified Lead' THEN 'SQL'
      WHEN _newValue = 'No Current Interest-Recycled' THEN 'NCIR'
      WHEN _newValue = 'Closed Won Opportunity' THEN 'Closed Won Opportunity'
      WHEN _newValue = 'Closed Lost Opportunity' THEN 'Closed Lost Opportunity'
      WHEN _newValue = 'Sales Rejected Lead' THEN 'Rejected'
      WHEN _newValue = 'Inside Sales Rejected Lead' THEN 'Rejected'
      ELSE 'Skipped Stage'
    END AS _2x_stages,
    CONCAT(_oldValue,_newValue) AS _old_new
  FROM velocity_data
),
activity_data AS (
  SELECT DISTINCT
    _contactID_C AS _prospectID,
    _subject,
    _activityDate
  FROM `x-marketing.faro.db_activity_log` 
  ORDER BY _activityDate
),
activity AS (
  SELECT
    stages_data._prospectID,
    stages_data._old_new,
    stages_data._createdDate,
    stages_data._next_change_date,
    COUNT(activity_data._subject) AS _activity_count
  FROM 
    stages_data 
  LEFT JOIN 
    activity_data
  ON 
    activity_data._prospectID = stages_data._prospectID15
    AND activity_data._activityDate 
    BETWEEN stages_data._createdDate AND stages_data._next_change_date
  GROUP BY 
    stages_data._prospectID,
    stages_data._old_new,
    stages_data._createdDate,
    stages_data._next_change_date
)
SELECT
  stages_data._prospectID,
  stages_data._oldValue,
  stages_data._newValue,
  stages_data._createdDate,
  stages_data._field,
  stages_data._prospectType,
  stages_data._2x_stages,
  COALESCE(
    DATE_DIFF(
      DATE(stages_data._createdDate), 
      LAG(DATE(stages_data._createdDate)) OVER (PARTITION BY stages_data._prospectID 
      ORDER BY DATE(stages_data._createdDate)), DAY),
      0
  ) AS stage_change_duration_days,
  activity._activity_count
FROM stages_data
LEFT JOIN activity
ON activity._prospectID = stages_data._prospectID
AND activity._old_new = CONCAT(stages_data._oldValue,stages_data._newValue);









