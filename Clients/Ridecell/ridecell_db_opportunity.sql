CREATE OR REPLACE TABLE `x-marketing.ridecell.db_opportunity` AS 
WITH _all AS (
WITH oppscontact AS (
    SELECT 
        contactid, 
        opportunityid,
        isprimary
    FROM `x-marketing.ridecell_salesforce.OpportunityContactRole`
    WHERE isdeleted IS FALSE
),
opps AS (
    SELECT 
        opp.stagename,
        opp.amount AS opportunity_amount,
        opp.id AS opportunity_id,
        opp.name AS opportunity_name,
        opp.type,
        contactid,
        opp.createddate,
        number_of_days_to_close__c,
        days_in_engaged__c
    FROM `x-marketing.ridecell_salesforce.Opportunity` opp
    WHERE opp.amount > 0
),
contact AS (
    SELECT 
        id AS contactid,
        firstname,
        lastname,
        title,
        email,
        ownerid,
        accountid,
        original_lead_source_details__c AS original_lead_source,
        contact_linkedin_url__c AS contact_linkedin_url
    FROM `x-marketing.ridecell_salesforce.Contact` contact
)
SELECT 
    opps.* EXCEPT (contactid),
    contact.*,
    oppscontact.isprimary AS is_primary,
    lead.contact_linkedin_url__c AS lead_contact_linkedin_url,
    user.name AS user_name,
    user.id AS user_id,
    user.title AS user_title,
    acc.name AS account_name,
    acc.ridecell_industry__c AS ridecell_industry,
    acc.billingcountry AS billing_country,
    acc.company_linkedin_url__c AS company_linkedin_url,
    LAG(side.stagename) OVER (PARTITION BY opps.opportunity_id,opps.createddate ORDER BY side.createddate) AS from_stage,
FROM oppscontact 
JOIN  opps ON opps.opportunity_id = oppscontact.opportunityid
JOIN contact ON contact.contactid = oppscontact.contactid
LEFT JOIN `x-marketing.ridecell_salesforce.Lead` lead ON lead.convertedcontactid = oppscontact.contactid
LEFT JOIN `x-marketing.ridecell_salesforce.User` user ON user.id = contact.ownerid
LEFT JOIN `x-marketing.ridecell_salesforce.Account` acc ON acc.id = contact.accountid
LEFT JOIN `x-marketing.ridecell_salesforce.OpportunityHistory` side ON side.opportunityid = opps.opportunity_id
WHERE acc.account_tier__c IN ('Tier 1', 'Tier 2')
QUALIFY ROW_NUMBER() OVER (PARTITION BY opps.stagename, opps.opportunity_id, contact.contactid, opps.createddate ORDER BY side.createddate DESC) = 1

),
avg_amount_opp AS (
    SELECT *,
        COUNT(opportunity_id) OVER (
            PARTITION BY opportunity_id 
        ) AS opp_count
    FROM _all
)
SELECT *, 
    CASE WHEN opp_count > 0 THEN SAFE_DIVIDE(opportunity_amount, opp_count) ELSE 0 END AS average_amount
FROM avg_amount_opp;