
-- Get all requested fields for campaign members 

CREATE OR REPLACE TABLE `thunder.db_sf_campaign_members` AS

SELECT  

    member.id AS member_id,
    member.email,
    member.title,
    member.companyoraccount AS company_or_account,
    member.type AS member_type,
    member.leadorcontactid AS lead_or_contact_id,
    member.createddate AS created_date,
    member.firstname AS first_name,
    member.lastname AS last_name,
    member.firstrespondeddate AS first_responded_date,
    member.lastmodifieddate AS last_modified_date,
    member.leadsource AS lead_source,
    member.mobilephone AS mobile_phone,
    member.phone,
    member.state,
    member.postalcode AS postal_code,
    member.street,
    member.status,
    member.hasresponded AS has_responded,
    member.createdbyid AS creator_id,
    owner.name AS creator_name,
    member.campaignid AS campaign_id,
    campaign.name AS campaign_name,
    campaign.type AS campaign_type,
    member.accountid AS account_id,
    account.name AS account_name,
    account.annualrevenue AS annual_revenue,
    account.opportunity__c AS opportunity,
    account.sector__c AS sector

FROM 
    `thunder_salesforce.CampaignMember` AS member

LEFT JOIN 
    `thunder_salesforce.User` AS owner
ON 
    member.createdbyid = owner.id

LEFT JOIN 
    `thunder_salesforce.Campaign` AS campaign
ON 
    member.campaignid = campaign.id

LEFT JOIN 
    `thunder_salesforce.Account` AS account
ON 
    member.accountid = account.id

WHERE 
    member.isdeleted = false;


----------------------------------------------------------------------------------
----------------------------------------------------------------------------------
----------------------------------------------------------------------------------

-- Complile all leads and contacts into the same table

CREATE OR REPLACE TABLE `thunder.db_sf_lead_contacts` AS

WITH all_contacts AS (

    SELECT

        'Contact' AS people_type,
        contact.id AS lead_or_contact_id,
        contact.email,
        contact.mobilephone AS mobile_phone,
        contact.name,
        contact.title,
        contact.industry__c AS industry,
        contact.leadsource AS lead_source,
        contact.contact_status__c AS lead_or_contact_status,
        contact.accountid AS account_id,
        account.name AS company_or_account,
        contact.ownerid AS owner_id,
        owner.name AS owner_name,
        owner.userroleid AS owner_role_id,
        role.name AS owner_role_name,
        contact.linkedin_profile__c AS linkedin_url
    
    FROM 
        `thunder_salesforce.Contact` contact
    
    LEFT JOIN 
        `thunder_salesforce.Account` AS account
    ON 
        contact.accountid = account.id
    
    LEFT JOIN 
        `thunder_salesforce.User` AS owner
    ON 
        contact.createdbyid = owner.id
    
    LEFT JOIN 
        `thunder_salesforce.UserRole` AS role
    ON 
        owner.userroleid = role.id
    
    WHERE 
        contact.isdeleted = false

),

all_leads AS (

    SELECT

        'Lead' AS people_type,
        lead.id AS lead_or_contact_id,
        lead.email,
        lead.mobilephone AS mobile_phone,
        lead.name,
        lead.title,
        lead.industry,
        lead.leadsource AS lead_source,
        lead.status AS lead_or_contact_status,
        CAST(NULL AS STRING) AS account_id,
        lead.company AS company_or_account,
        lead.ownerid AS owner_id,
        owner.name AS owner_name,
        owner.userroleid AS owner_role_id,
        role.name AS owner_role_name,
        lead.linkedin_url__c AS linkedin_url
    
    FROM 
        `thunder_salesforce.Lead` lead
    
    LEFT JOIN 
        `thunder_salesforce.User` AS owner
    ON 
        lead.createdbyid = owner.id
    
    LEFT JOIN 
        `thunder_salesforce.UserRole` AS role
    ON 
        owner.userroleid = role.id
    
    WHERE 
        lead.isdeleted = false 
    AND 
        lead.isconverted = false

)

SELECT * FROM all_leads

UNION ALL

SELECT * FROM all_contacts;


----------------------------------------------------------------------------------
----------------------------------------------------------------------------------
----------------------------------------------------------------------------------

-- Get all requested field for opportunities

CREATE OR REPLACE TABLE `thunder.db_sf_opportunities` AS

SELECT  
    opp.id AS opp_id,
    dascoopcomposer__domain_1__c AS domain,
    opp.type AS opp_type,
    opp.name AS opp_name,
    stagename AS opp_stage,
    amount AS opp_amount,
    closedate AS opp_close_date,
    opp.ownerid AS owner_id,
    owner_name__c AS owner_name,
    products__c AS products,
    created_date_2__c AS opp_created_date,
    leadsource AS lead_source,
    account_annual_revenue__c AS account_annual_revenue,
    account_employee_count__c AS account_employee_count,
    account_name__c AS account_name,
    accountid AS account_id,
    account_sector__c AS account_sector
    
FROM 
    `thunder_salesforce.Opportunity` opp
LEFT JOIN
    `thunder_salesforce.Account` account
ON
    opp.accountid = account.id
WHERE 
    opp.isdeleted = false;


----------------------------------------------------------------------------------
----------------------------------------------------------------------------------
----------------------------------------------------------------------------------

-- SF Campaign + Leads + Contacts + Opps


CREATE OR REPLACE TABLE thunder.db_sf_campaign_leads_contacts_opps AS 

WITH sf_campaign_members AS (
SELECT
  campaign_name,
  campaign_id
  created_date,
  first_responded_date,
  lead_or_contact_id
FROM `thunder.db_sf_campaign_members`
),
sf_lead_contacts AS (
SELECT * FROM `thunder.db_sf_lead_contacts`
),
opp_contact_role AS (
  WITH oppcontact AS (
    SELECT
      opportunityid,
      role,
      contactid

    FROM `thunder_salesforce.OpportunityContactRole`
  ),
  opp AS (
    SELECT name,
    id,
    account_name__c AS account_name,
    owner_name__c AS opp_owner,
    amount,
    createddate AS opp_createddate,
    closedate AS opp_closedate,
    stagename AS stage
    FROM `thunder_salesforce.Opportunity`
  )
  SELECT
    oppcontact.opportunityid,
    opp.name AS opp_name,
    opp.account_name,
    opp.opp_owner,
    oppcontact.role,
    oppcontact.contactid,
    opp.opp_createddate,
    opp.opp_closedate,
    opp.stage,
    opp.amount
  FROM oppcontact
  JOIN opp
  ON oppcontact.opportunityid = opp.id
)
SELECT * FROM sf_campaign_members
JOIN sf_lead_contacts
USING (lead_or_contact_id)
JOIN opp_contact_role
ON sf_lead_contacts.lead_or_contact_id = opp_contact_role.contactid

