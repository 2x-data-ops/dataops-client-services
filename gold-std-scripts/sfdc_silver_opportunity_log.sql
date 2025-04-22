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
  laststagechangedate,
  amount,
  converted_amount__c,
  pl_inside_sales__c,
  gclid__c,
  contactid,
  firstname,
  lastname,
  title,
  email,
  ownerid,
  accountid,
  pi__utm_campaign__c,
  is_primary,
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
    opps.* EXCEPT (contactid, accountid, owner_name),
    contact.*,
    oppscontact.isprimary AS is_primary,
    user.name AS owner_name,
    LAG(side.stagename) OVER (
      PARTITION BY opps.opportunity_id,
        opps.createddate
      ORDER BY side.createddate
    ) AS from_stage,
  FROM oppscontact
  JOIN opps
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
    ORDER BY side.createddate DESC
  ) = 1
),
final_opp_base AS (
  SELECT
    *,
    COUNT(opportunity_id) OVER (PARTITION BY opportunity_id) AS opp_count
  FROM opp_base
),
final_base AS (
  SELECT
    *,
    IF(opp_count > 0, opportunity_amount / opp_count, 0) AS average_amount
  FROM final_opp_base
)
SELECT
  *
FROM final_base;