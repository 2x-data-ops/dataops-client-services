TRUNCATE TABLE `x-marketing.brp.sf_opportunity_ads`;
INSERT INTO `x-marketing.brp.sf_opportunity_ads` (
    stagename,	
    opportunity_amount,	
    opportunity_id,	
    opportunity_name,	
    type,	
    createddate,	
    closedate,	
    opp_type,	
    opp_record_type,	
    accountid,	
    laststagechangedate,	
    amount,	
    converted_amount__c,	
    pl_inside_sales__c,	
    lead_id,	
    firstname,	
    lastname,	
    title,	
    email,	
    ownerid,	
    leadsource,	
    gclid__c,	
    company,	
    pi__utm_campaign__c,	
    pi__utm_medium__c,	
    pi__utm_source__c,	
    pi__utm_term__c,	
    pi__utm_content__c,	
    lead_created_date,	
    lead_status,	
    lead_status_date__c,	
    reason_unqualified__c,	
    industry,	
    state,	
    country,	
    lastactivity__c,	
    inquiry_type__c,	
    owner_name,	
    from_stage,	
    opp_count,	
    average_amount
)
WITH oppscontact AS (
    SELECT 
        contactid, 
        opportunityid,
        isprimary
    FROM `x-marketing.brp_salesforce.OpportunityContactRole`
    WHERE isdeleted IS FALSE
      AND isprimary IS TRUE
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
        opp.closedate,
        type AS opp_type,
        r.name AS opp_record_type,
        opp.accountid,
        laststagechangedate,
        amount,
        converted_amount__c,
        user.name AS owner_name,
        pl_inside_sales__c,
        gclid__c
    FROM `x-marketing.brp_salesforce.Opportunity` opp
    LEFT JOIN `x-marketing.brp_salesforce.RecordType` r
    ON r.id = opp.recordtypeid
    LEFT JOIN `x-marketing.brp_salesforce.User` user
        ON user.id = opp.ownerid
    WHERE pl_inside_sales__c IS TRUE
      AND gclid__c IS NOT NULL 
), 
leads AS (
    SELECT 
        leads.id AS lead_id,
        leads.firstname,
        leads.lastname,
        leads.title,
        leads.email,
        ownerid,
        convertedaccountid,
        convertedopportunityid,
        leadsource,
        gclid__c,
        company,
        pi__utm_campaign__c,
        pi__utm_medium__c,
        pi__utm_source__c,
        pi__utm_term__c,
        pi__utm_content__c,
        leads.createddate AS lead_created_date,
        status AS lead_status,
        lead_status_date__c,
        reason_unqualified__c,
        industry,
        leads.state,
        leads.country,
        lastactivity__c,
        inquiry_type__c,
        user.name AS owner_name
    FROM `x-marketing.brp_salesforce.Lead` leads
    LEFT JOIN `x-marketing.brp_salesforce.User` user
        ON user.id = ownerid
    WHERE gclid__c IS NOT NULL
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
        pi__utm_campaign__c
    FROM `x-marketing.brp_salesforce.Contact` contact
),
opp_base AS (
    SELECT 
        opps.* EXCEPT (contactid,accountid,owner_name),
        contact.*,
        oppscontact.isprimary AS is_primary,
        user.name AS owner_name,
        LAG(side.stagename) OVER (PARTITION BY opps.opportunity_id,opps.createddate ORDER BY side.createddate) AS from_stage,
    FROM oppscontact 
    JOIN  opps 
        ON opps.opportunity_id = oppscontact.opportunityid
    JOIN contact 
        ON contact.contactid = oppscontact.contactid
    LEFT JOIN `x-marketing.brp_salesforce.User` user 
        ON user.id = contact.ownerid
    LEFT JOIN `x-marketing.brp_salesforce.Account` acc 
        ON acc.id = contact.accountid
    LEFT JOIN `x-marketing.brp_salesforce.OpportunityHistory` side 
        ON side.opportunityid = opps.opportunity_id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY opps.stagename, opps.opportunity_id, contact.contactid, opps.createddate 
        ORDER BY side.createddate DESC) = 1
),
opp_base_v2 AS (
    SELECT 
        opps.* EXCEPT (contactid, gclid__c, owner_name),
        leads.* EXCEPT(convertedaccountid,convertedopportunityid),
        LAG(side.stagename) OVER (PARTITION BY opps.opportunity_id,opps.createddate ORDER BY side.createddate) AS from_stage,
    FROM leads
    LEFT JOIN  opps 
        ON opps.gclid__c = leads.gclid__c
    LEFT JOIN `x-marketing.brp_salesforce.Account` acc 
        ON acc.id = leads.convertedaccountid
    LEFT JOIN `x-marketing.brp_salesforce.OpportunityHistory` side 
        ON side.opportunityid = opps.opportunity_id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY opps.stagename, opps.opportunity_id, leads.lead_id, opps.createddate 
        ORDER BY side.createddate DESC) = 1
), 
final_opp_base AS (
    SELECT 
        *,
        COUNT(opportunity_id) OVER (PARTITION BY opportunity_id) AS opp_count
    FROM opp_base_v2
), 
final_base AS (
    SELECT 
        *, 
        IF(opp_count > 0, opportunity_amount / opp_count, 0) AS average_amount
    FROM final_opp_base
)
SELECT *
FROM final_base;


TRUNCATE TABLE `x-marketing.brp.sf_opportunity_created`;
INSERT INTO `x-marketing.brp.sf_opportunity_created` (
    stagename,	
    opportunity_amount,	
    opportunity_id,	
    opportunity_name,	
    type,	
    contactid,	
    _date,	
    opp_type,	
    opp_record_type,	
    accountid,	
    laststagechangedate,	
    amount,	
    converted_amount__c,	
    owner_name,	
    pl_inside_sales__c,	
    gclid__c,
    _platform
)
WITH google_ads AS (
    SELECT 
        opp.stagename,
        opp.amount AS opportunity_amount,
        opp.id AS opportunity_id,
        opp.name AS opportunity_name,
        opp.type,
        opp.contactid,
        opp.createddate AS _date,
        opp.type AS opp_type,
        r.name AS opp_record_type,
        opp.accountid,
        laststagechangedate,
        amount,
        converted_amount__c,
        user.name AS owner_name,
        pl_inside_sales__c,
        gclid__c,
        'Google' AS _platform
    FROM `x-marketing.brp_salesforce.Opportunity` opp
    LEFT JOIN `x-marketing.brp_salesforce.RecordType` r
        ON r.id = opp.recordtypeid
    LEFT JOIN `x-marketing.brp_salesforce.User` user
        ON user.id = opp.ownerid
    LEFT JOIN `x-marketing.brp_salesforce.CampaignMember` campaignmember
        ON campaignmember.campaignid = opp.campaignid
    WHERE gclid__c IS NOT NULL
      AND opp.isdeleted IS FALSE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY opp.id 
        ORDER BY opp.createddate DESC) = 1
),
facebook_ads AS (
    SELECT 
        opp.stagename,
        opp.amount AS opportunity_amount,
        opp.id AS opportunity_id,
        opp.name AS opportunity_name,
        opp.type,
        contactid,
        opp.createddate AS _date,
        type AS opp_type,
        r.name AS opp_record_type,
        opp.accountid,
        laststagechangedate,
        amount,
        converted_amount__c,
        user.name AS owner_name,
        pl_inside_sales__c,
        gclid__c,
        'Facebook' AS _platform
    FROM `x-marketing.brp_salesforce.Opportunity` opp
    LEFT JOIN `x-marketing.brp_salesforce.RecordType` r
        ON r.id = opp.recordtypeid
    LEFT JOIN `x-marketing.brp_salesforce.User` user
        ON user.id = opp.ownerid
    WHERE opp.isdeleted IS FALSE
      AND campaignid = '701US000008frVEYAY'
)
SELECT 
    * 
FROM google_ads
UNION ALL
SELECT
    *
FROM facebook_ads;


TRUNCATE TABLE `x-marketing.brp.sf_opportunity_closed`;
INSERT INTO `x-marketing.brp.sf_opportunity_closed` (
    stagename,	
    opportunity_amount,	
    opportunity_id,	
    opportunity_name,	
    type,	
    contactid,	
    _date,	
    opp_type,	
    opp_record_type,	
    accountid,	
    laststagechangedate,	
    amount,	
    converted_amount__c,	
    owner_name,	
    pl_inside_sales__c,	
    gclid__c,
    _platform
)
WITH google_ads AS (
    SELECT 
        opp.stagename,
        opp.amount AS opportunity_amount,
        opp.id AS opportunity_id,
        opp.name AS opportunity_name,
        opp.type,
        contactid,
        opp.closedate AS _date,
        type AS opp_type,
        r.name AS opp_record_type,
        opp.accountid,
        laststagechangedate,
        amount,
        converted_amount__c,
        user.name AS owner_name,
        pl_inside_sales__c,
        gclid__c,
        'Google' AS _platform
    FROM `x-marketing.brp_salesforce.Opportunity` opp
    LEFT JOIN `x-marketing.brp_salesforce.RecordType` r
    ON r.id = opp.recordtypeid
    LEFT JOIN `x-marketing.brp_salesforce.User` user
        ON user.id = opp.ownerid
    WHERE gclid__c IS NOT NULL
      AND isdeleted IS FALSE
),
facebook_ads AS (
    SELECT 
        opp.stagename,
        opp.amount AS opportunity_amount,
        opp.id AS opportunity_id,
        opp.name AS opportunity_name,
        opp.type,
        contactid,
        opp.closedate AS _date,
        type AS opp_type,
        r.name AS opp_record_type,
        opp.accountid,
        laststagechangedate,
        amount,
        converted_amount__c,
        user.name AS owner_name,
        pl_inside_sales__c,
        gclid__c,
        'Facebook' AS _platform
    FROM `x-marketing.brp_salesforce.Opportunity` opp
    LEFT JOIN `x-marketing.brp_salesforce.RecordType` r
        ON r.id = opp.recordtypeid
    LEFT JOIN `x-marketing.brp_salesforce.User` user
        ON user.id = opp.ownerid
    WHERE opp.isdeleted IS FALSE
      AND campaignid = '701US000008frVEYAY'
)
SELECT
    *
FROM google_ads
UNION ALL
SELECT
    *
FROM facebook_ads;


TRUNCATE TABLE `x-marketing.brp.sf_leads`;
INSERT INTO `x-marketing.brp.sf_leads`  (
    lead_id,	
    firstname,	
    lastname,	
    title,	
    email,	
    ownerid,	
    convertedaccountid,	
    convertedopportunityid,	
    leadsource,	
    gclid__c,	
    company,	
    pi__utm_campaign__c,	
    pi__utm_medium__c,	
    pi__utm_source__c,	
    pi__utm_term__c,	
    pi__utm_content__c,	
    _date,	
    lead_status,	
    lead_status_date__c,	
    reason_unqualified__c,	
    industry,	
    state,	
    country,	
    lastactivity__c,	
    inquiry_type__c,	
    owner_name,
    _platform
)
WITH google_ads AS (
    SELECT 
        leads.id AS lead_id,
        leads.firstname,
        leads.lastname,
        leads.title,
        leads.email,
        ownerid,
        convertedaccountid,
        convertedopportunityid,
        leadsource,
        gclid__c,
        company,
        pi__utm_campaign__c,
        pi__utm_medium__c,
        pi__utm_source__c,
        pi__utm_term__c,
        pi__utm_content__c,
        leads.createddate AS _date,
        status AS lead_status,
        lead_status_date__c,
        reason_unqualified__c,
        industry,
        leads.state,
        leads.country,
        lastactivity__c,
        inquiry_type__c,
        user.name AS owner_name,
        'Google' AS _platform
    FROM `x-marketing.brp_salesforce.Lead` leads
    LEFT JOIN `x-marketing.brp_salesforce.User` user
        ON user.id = ownerid
    WHERE gclid__c IS NOT NULL
      AND isdeleted IS FALSE
),
facebook_ads AS (
    SELECT 
        leads.id AS lead_id,
        leads.firstname,
        leads.lastname,
        leads.title,
        leads.email,
        ownerid,
        convertedaccountid,
        convertedopportunityid,
        leads.leadsource,
        gclid__c,
        company,
        pi__utm_campaign__c,
        pi__utm_medium__c,
        pi__utm_source__c,
        pi__utm_term__c,
        pi__utm_content__c,
        leads.createddate AS _date,
        leads.status AS lead_status,
        lead_status_date__c,
        reason_unqualified__c,
        industry,
        leads.state,
        leads.country,
        lastactivity__c,
        inquiry_type__c,
        user.name AS owner_name,
        'Facebook' AS _platform
    FROM `x-marketing.brp_salesforce.Lead` leads
    LEFT JOIN `x-marketing.brp_salesforce.User` user
        ON user.id = ownerid
    LEFT JOIN `x-marketing.brp_salesforce.CampaignMember` campaignmember
        ON campaignmember.leadid = leads.id
    WHERE leads.isdeleted IS FALSE
      AND campaignmember.campaignid = '701US000008frVEYAY'
)
SELECT
    *
FROM google_ads
UNION ALL
SELECT
    *
FROM facebook_ads;


TRUNCATE TABLE `x-marketing.brp.sf_opportunity_created_with_utm`;
INSERT INTO `x-marketing.brp.sf_opportunity_created_with_utm` (
    stagename,	
    opportunity_amount,	
    opportunity_id,	
    opportunity_name,	
    type,	
    contactid,	
    _date,	
    opp_type,	
    opp_record_type,	
    accountid,	
    laststagechangedate,	
    amount,	
    converted_amount__c,	
    owner_name,	
    pl_inside_sales__c,	
    gclid__c,	
    pi__utm_campaign__c,	
    pi__utm_medium__c,	
    pi__utm_source__c,	
    pi__utm_term__c,	
    pi__utm_content__c,
    _platform
)
WITH google_ads AS (
    SELECT 
        opp.stagename,
        opp.amount AS opportunity_amount,
        opp.id AS opportunity_id,
        opp.name AS opportunity_name,
        opp.type,
        contactid,
        opp.createddate AS _date,
        --opp.closedate,
        type AS opp_type,
        r.name AS opp_record_type,
        opp.accountid,
        laststagechangedate,
        amount,
        converted_amount__c,
        user.name AS owner_name,
        pl_inside_sales__c,
        opp.gclid__c,
        pi__utm_campaign__c,
        pi__utm_medium__c,
        pi__utm_source__c,
        pi__utm_term__c,
        pi__utm_content__c,  
        'Google' AS _platform 
    FROM `x-marketing.brp_salesforce.Opportunity` opp
    LEFT JOIN `x-marketing.brp_salesforce.RecordType` r
        ON r.id = opp.recordtypeid
    LEFT JOIN `x-marketing.brp_salesforce.User` user
        ON user.id = opp.ownerid
    LEFT JOIN `x-marketing.brp_salesforce.Lead` leads
        ON leads.gclid__c = opp.gclid__c
    WHERE opp.gclid__c IS NOT NULL
      AND opp.isdeleted IS FALSE
),
facebook_ads AS (
    SELECT 
        opp.stagename,
        opp.amount AS opportunity_amount,
        opp.id AS opportunity_id,
        opp.name AS opportunity_name,
        opp.type,
        opp.contactid,
        opp.createddate AS _date,
        opp.type AS opp_type,
        r.name AS opp_record_type,
        opp.accountid,
        laststagechangedate,
        amount,
        converted_amount__c,
        user.name AS owner_name,
        pl_inside_sales__c,
        opp.gclid__c,
        pi__utm_campaign__c,
        pi__utm_medium__c,
        pi__utm_source__c,
        pi__utm_term__c,
        pi__utm_content__c,
        'Facebook' AS _platform   
    FROM `x-marketing.brp_salesforce.Opportunity` opp
    LEFT JOIN `x-marketing.brp_salesforce.RecordType` r
        ON r.id = opp.recordtypeid
    LEFT JOIN `x-marketing.brp_salesforce.User` user
        ON user.id = opp.ownerid
    LEFT JOIN `x-marketing.brp_salesforce.Lead` leads
        ON leads.gclid__c = opp.gclid__c
    LEFT JOIN `x-marketing.brp_salesforce.CampaignMember` campaignmember
        ON campaignmember.campaignid = opp.campaignid
    WHERE opp.isdeleted IS FALSE
      AND campaignmember.campaignid = '701US000008frVEYAY'
)
SELECT
    *
FROM google_ads
UNION ALL
SELECT
    *
FROM facebook_ads;


TRUNCATE TABLE `x-marketing.brp.sf_opportunity_closed_with_utm`;
INSERT INTO `x-marketing.brp.sf_opportunity_closed_with_utm` (
    stagename,	
    opportunity_amount,	
    opportunity_id,	
    opportunity_name,	
    type,	
    contactid,	
    _date,	
    opp_type,	
    opp_record_type,	
    accountid,	
    laststagechangedate,	
    amount,	
    converted_amount__c,	
    owner_name,	
    pl_inside_sales__c,	
    gclid__c,	
    pi__utm_campaign__c,	
    pi__utm_medium__c,	
    pi__utm_source__c,	
    pi__utm_term__c,	
    pi__utm_content__c,
    _platform
)
WITH google_ads AS (
    SELECT 
        opp.stagename,
        opp.amount AS opportunity_amount,
        opp.id AS opportunity_id,
        opp.name AS opportunity_name,
        opp.type,
        contactid,
        opp.closedate AS _date,
        type AS opp_type,
        r.name AS opp_record_type,
        opp.accountid,
        laststagechangedate,
        amount,
        converted_amount__c,
        user.name AS owner_name,
        pl_inside_sales__c,
        opp.gclid__c,
        pi__utm_campaign__c,
        pi__utm_medium__c,
        pi__utm_source__c,
        pi__utm_term__c,
        pi__utm_content__c,  
        'Google' AS _platform 
    FROM `x-marketing.brp_salesforce.Opportunity` opp
    LEFT JOIN `x-marketing.brp_salesforce.RecordType` r
        ON r.id = opp.recordtypeid
    LEFT JOIN `x-marketing.brp_salesforce.User` user
        ON user.id = opp.ownerid
    LEFT JOIN `x-marketing.brp_salesforce.Lead` leads
        ON leads.gclid__c = opp.gclid__c
    WHERE opp.gclid__c IS NOT NULL
      AND opp.isdeleted IS FALSE
),
facebook_ads AS (
    SELECT 
        opp.stagename,
        opp.amount AS opportunity_amount,
        opp.id AS opportunity_id,
        opp.name AS opportunity_name,
        opp.type,
        opp.contactid,
        opp.closedate AS _date,
        opp.type AS opp_type,
        r.name AS opp_record_type,
        opp.accountid,
        laststagechangedate,
        amount,
        converted_amount__c,
        user.name AS owner_name,
        pl_inside_sales__c,
        opp.gclid__c,
        pi__utm_campaign__c,
        pi__utm_medium__c,
        pi__utm_source__c,
        pi__utm_term__c,
        pi__utm_content__c,  
        'Facebook' AS _platform 
    FROM `x-marketing.brp_salesforce.Opportunity` opp
    LEFT JOIN `x-marketing.brp_salesforce.RecordType` r
        ON r.id = opp.recordtypeid
    LEFT JOIN `x-marketing.brp_salesforce.User` user
        ON user.id = opp.ownerid
    LEFT JOIN `x-marketing.brp_salesforce.Lead` leads
        ON leads.gclid__c = opp.gclid__c
    LEFT JOIN `x-marketing.brp_salesforce.CampaignMember` campaignmember
        ON campaignmember.campaignid = opp.campaignid
    WHERE opp.isdeleted IS FALSE
      AND campaignmember.campaignid = '701US000008frVEYAY'
)
SELECT
    *
FROM google_ads
UNION ALL
SELECT
    *
FROM facebook_ads;