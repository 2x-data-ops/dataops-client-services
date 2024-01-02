
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
),  lead_contact AS (
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
    primary_hardware_interest__c AS hardwareInterest,
    distribution__c,
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
    description,
    leadsource,
    account_name__c,
    current_waterfall_stage_date_time__c AS WaterfallStageDate__c,
    OwnerId AS _manager__c,
    owner.name AS account_manager__c_name,
    users.name,
    assigned_iss__c, 
    --assigned_iss_name__c,
    current_waterfall_stage_date_time__c,
    no_current_interest_reason__c,
    leadsource,
    status__c,
    Waterfall_Stage__c,
    most_recent_lead_source__c,
    reason_for_disqualification__c,
    leads.Vertical__c ,
    leads.Division_Region__c ,
    primary_hardware_interest__c ,
    distribution__c,
  FROM `x-marketing.faro_salesforce.Contact` leads
  LEFT JOIN `x-marketing.faro_salesforce.User` users ON leads.assigned_iss__c = users.id
  LEFT JOIN `x-marketing.faro_salesforce.User` Owner ON leads.OwnerId = Owner.id
), 
campaigns AS (
  SELECT
    id,
    name
  FROM `x-marketing.faro_salesforce.Campaign`
)
SELECT
  campaignMembers.*,
  leads.* EXCEPT (id),
  campaigns.name AS campaignName
  FROM campaignMembers
  LEFT JOIN lead_contact  leads
  ON campaignMembers.leadorcontactid = leads.id
  LEFT JOIN campaigns
  ON campaignMembers.campaignid = campaigns.id