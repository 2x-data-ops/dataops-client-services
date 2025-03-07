TRUNCATE TABLE `x-marketing.ridecell.db_opportunity`;
INSERT INTO `x-marketing.ridecell.db_opportunity` (
    stagename,
    opportunity_amount,
    opportunity_id,
    opportunity_name,
    type,
    createddate,
    number_of_days_to_close,
    days_engaged,
    contactid,
    firstname,
    lastname,
    title,
    email,
    ownerid,
    accountid,
    original_lead_source,
    contact_linkedin_url,
    is_primary,
    lead_contact_linkedin_url,
    user_name,
    user_id,
    user_title,
    account_name,
    ridecell_industry,
    billing_country,
    company_linkedin_url,
    from_stage,
    opp_count,
    average_amount

)
WITH oppscontact AS (
    SELECT 
        contactid, 
        opportunityid,
        isprimary
    FROM `x-marketing.ridecell_salesforce.OpportunityContactRole`
    WHERE isdeleted IS FALSE

), opps AS (
    SELECT 
        opp.stagename,
        opp.amount AS opportunity_amount,
        opp.id AS opportunity_id,
        opp.name AS opportunity_name,
        opp.type,
        contactid,
        opp.createddate,
        number_of_days_to_close__c AS number_of_days_to_close,
        days_in_engaged__c AS days_engaged
    FROM `x-marketing.ridecell_salesforce.Opportunity` opp
    WHERE opp.amount > 0

), contact AS (
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

), opp_base AS (
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
    JOIN  opps 
        ON opps.opportunity_id = oppscontact.opportunityid
    JOIN contact 
        ON contact.contactid = oppscontact.contactid
    LEFT JOIN `x-marketing.ridecell_salesforce.Lead` lead 
        ON lead.convertedcontactid = oppscontact.contactid
    LEFT JOIN `x-marketing.ridecell_salesforce.User` user 
        ON user.id = contact.ownerid
    LEFT JOIN `x-marketing.ridecell_salesforce.Account` acc 
        ON acc.id = contact.accountid
    LEFT JOIN `x-marketing.ridecell_salesforce.OpportunityHistory` side 
        ON side.opportunityid = opps.opportunity_id
    WHERE acc.account_tier__c IN ('Tier 1', 'Tier 2')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY opps.stagename, opps.opportunity_id, contact.contactid, opps.createddate ORDER BY side.createddate DESC) = 1

), final_opp_base AS (
    SELECT *,
        COUNT(opportunity_id) OVER (
            PARTITION BY opportunity_id 
        ) AS opp_count
    FROM opp_base
)
SELECT *, 
    CASE WHEN opp_count > 0 
        THEN SAFE_DIVIDE(opportunity_amount, opp_count) 
        ELSE 0 
        END AS average_amount
FROM final_opp_base;