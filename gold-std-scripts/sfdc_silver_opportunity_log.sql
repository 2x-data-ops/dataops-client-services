TRUNCATE TABLE `x-marketing.brp.sf_opportunity_ads`;

INSERT INTO `x-marketing.brp.sf_opportunity_ads` (
  _stage_name,
  _opportunity_amount,
  _opportunity_id,
  _opportunity_name,
  _type,
  _created_date,
  _close_date,
  _opp_type,
  _opp_record_type,
  _last_stage_change_date,
  _amount,
  _converted_amount__c,
  _pl_inside_sales__c,
  _gclid__c,
  _contact_id,
  _first_name,
  _last_name,
  _title,
  _email,
  _owner_id,
  _account_id,
  _pi__utm_campaign__c,
  _is_primary,
  _owner_name,
  _from_stage,
  _opp_count,
  _average_amount
)
WITH oppscontact AS (
  SELECT
    contactid AS _contact_id,
    opportunityid AS _opportunity_id,
    isprimary AS _is_primary
  FROM `x-marketing.brp_salesforce.OpportunityContactRole`
  WHERE isdeleted IS FALSE
    AND isprimary IS TRUE
),
opps AS (
  SELECT
    opp.stagename AS _stage_name,
    opp.amount AS _opportunity_amount,
    opp.id AS _opportunity_id,
    opp.name AS _opportunity_name,
    opp.type AS _type,
    contactid AS _contact_id,
    opp.createddate AS _created_date,
    opp.closedate AS _close_date,
    type AS _opp_type,
    r.name AS _opp_record_type,
    opp.accountid AS _account_id,
    laststagechangedate AS _last_stage_change_date,
    amount AS _amount,
    converted_amount__c AS _converted_amount__c,
    user.name AS _owner_name,
    pl_inside_sales__c AS _pl_inside_sales__c,
    gclid__c AS _gclid__c
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
    id AS _contact_id,
    firstname AS _first_name,
    lastname AS _last_name,
    title AS _title,
    email AS _email,
    ownerid AS _owner_id,
    accountid AS _account_id,
    pi__utm_campaign__c AS _pi__utm_campaign__c
  FROM `x-marketing.brp_salesforce.Contact` contact
),
opp_base AS (
  SELECT
    -- opps.* EXCEPT (contactid, accountid, owner_name),
    opps._stage_name,
    opps._opportunity_amount,
    opps._opportunity_id,
    opps._opportunity_name,
    opps._type,
    opps._created_date,
    opps._close_date,
    opps._opp_type,
    opps._opp_record_type,
    opps._last_stage_change_date,
    opps._amount,
    opps._converted_amount__c,
    opps._pl_inside_sales__c,
    opps._gclid__c,
    contact.*,
    oppscontact._is_primary,
    user.name AS _owner_name,
    LAG(side.stagename) OVER (
      PARTITION BY opps._opportunity_id,
        opps._created_date
      ORDER BY side.createddate
    ) AS _from_stage,
  FROM oppscontact
  JOIN opps
    ON opps._opportunity_id = oppscontact._opportunity_id
  JOIN contact
    ON contact._contact_id = oppscontact._contact_id
  LEFT JOIN `x-marketing.brp_salesforce.User` user
    ON user.id = contact._owner_id
  LEFT JOIN `x-marketing.brp_salesforce.Account` acc
    ON acc.id = contact._account_id
  LEFT JOIN `x-marketing.brp_salesforce.OpportunityHistory` side
    ON side.opportunityid = opps._opportunity_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY opps._stage_name, opps._opportunity_id, contact._contact_id, opps._created_date
    ORDER BY side.createddate DESC
  ) = 1
),
final_opp_base AS (
  SELECT
    *,
    COUNT(_opportunity_id) OVER (PARTITION BY _opportunity_id) AS _opp_count
  FROM opp_base
),
final_base AS (
  SELECT
    *,
    IF(_opp_count > 0, _opportunity_amount / _opp_count, 0) AS _average_amount
  FROM final_opp_base
)
SELECT
  *
FROM final_base;