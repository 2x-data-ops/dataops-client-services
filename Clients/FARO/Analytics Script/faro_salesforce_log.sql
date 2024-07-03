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

CREATE OR REPLACE TABLE `x-marketing.faro.db_velocity_log` AS
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