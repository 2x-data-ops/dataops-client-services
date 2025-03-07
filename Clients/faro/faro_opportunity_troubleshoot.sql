--////////////////////////////////////////////////--
----------------------------------------------------
-- ----------// FARO OPPORTUNITY_RAW //---------- --
----------------------------------------------------
--////////////////////////////////////////////////--
TRUNCATE TABLE `faro.opportunity_raw` ;
INSERT INTO `faro.opportunity_raw` (
  opportunityID,
  ownerid,
  opportunity_type__c,
  recordtypeid,
  faro_industry__c,
  stagename,
  probability, 
  opportunity_age__c, 
  closing_confidence__c,
  sfdc_opp_casesafeid__c,
  accountid, 
  createddate, 
  closedate,
  lead_created_date_formula_global__c, 
  converted_created_date__c, 
  division_region__c, 
  campaignid,
  leadsource,
  most_recent_lead_source__c, 
  demo_type__c, 
  type,
  software__c, 
  hardware__c, 
  realzip__realzip_country__c, 
  _real_territory_user_am_dm,
  iss__c,
  createdbyid, 
  lead_group_source__c, 
  _real_territory_user_director, 
  currencyisocode,
  isclosed, 
  realterritory_name__c, 
  vertical__c,
  division_region_from_account__c, 
  _accountName, 
  _userID,
  _createdBy, 
  _createdByTitle, 
  _lastModifiedDate,
  _contactID,
  applicationSpecialist,
  actualOwner,
  opportunityName,
  marketSegment,
  accountType,
  _workflow,
  _products,
  _sub_products,
  _asset,
  _application,
  _primary_application,
  _secondary_application,
  original_amount,
  conversionRate, 
  total_price, 
  _campaignName,
  _real_territory_user_rm
)
-- CREATE OR REPLACE TABLE `faro.opportunity_raw` AS
WITH closedConversionRate AS (
  SELECT DISTINCT
    opp.id,
    isocode,
    opp.closedate,
    rate.conversionrate,
    opp.amount / rate.conversionrate AS total_price_USD
  FROM `x-marketing.faro_salesforce.DatedConversionRate` rate
  LEFT JOIN `x-marketing.faro_salesforce.Opportunity` opp
  ON rate.isoCode = opp.currencyisocode
    AND opp.closedate >= rate.startDate
    AND opp.closedate < rate.nextStartDate
  WHERE 
    opp.isclosed = true
  -- ORDER BY rate.startDate DESC
),
openConversionRate AS (
  SELECT 
    * EXCEPT(rownum)
  FROM (
    SELECT DISTINCT
      opp.id,
      isocode,
      rate.conversionrate,
      rate.lastmodifieddate,
      opp.closedate,
      -- opp.total_price__c,
      ROW_NUMBER() OVER(PARTITION BY isocode ORDER BY rate.lastmodifieddate DESC) AS rownum
    FROM `x-marketing.faro_salesforce.DatedConversionRate` rate
    LEFT JOIN `x-marketing.faro_salesforce.Opportunity` opp
    ON opp.currencyisocode = rate.isocode
    WHERE opp.isclosed = false
    AND opp.currencyisocode != 'USD'
  )
  WHERE rownum = 1
  ORDER BY isocode 
),
user AS (
  SELECT DISTINCT
    id AS _user_id,
    name AS _user_name,
    title AS _user_title
  FROM `x-marketing.faro_salesforce.User`
),
real_territory AS (
  SELECT DISTINCT 
    name,
    user_director__c,
    user_am_dm__c,
    user_rm__c
  FROM `x-marketing.faro_salesforce.RealTerritory__c` 
),
real_territory_user AS (
  SELECT 
  rt.name,
  MAX(CASE WHEN u._user_id = rt.user_director__c THEN u._user_name END) AS user_director_name,
  MAX(CASE WHEN u._user_id = rt.user_am_dm__c THEN u._user_name END) AS user_am_dm_name,
  MAX(CASE WHEN u._user_id = rt.user_rm__c THEN u._user_name END) AS user_rm_name
FROM real_territory rt
LEFT JOIN user u
  ON u._user_id IN (rt.user_director__c, rt.user_am_dm__c, rt.user_rm__c)
GROUP BY 
  rt.name, 
  rt.user_director__c, 
  rt.user_am_dm__c, 
  rt.user_rm__c
),
Opportunity AS (
  SELECT DISTINCT
    opp.id AS opportunityID,
    opp.ownerid,
    opp.opportunity_type__c,
    opp.recordtypeid,
    opp.faro_industry__c,
    opp.stagename,
    opp.probability,
    opp.opportunity_age__c,
    opp.closing_confidence__c,
    opp.sfdc_opp_casesafeid__c,
    opp.accountid,
    opp.createddate,
    opp.closedate,
    opp.lead_created_date_formula_global__c,
    opp.converted_created_date__c,
    acc.division_region__c,
    opp.campaignid,
    opp.leadsource,
    opp.most_recent_lead_source__c,
    opp.demo_type__c,
    opp.type,
    opp.software__c,
    opp.hardware__c,
    opp.realzip__realzip_country__c,
    -- opp.realterritory_user_am_dm__c,
    real_territory.user_am_dm_name AS _real_territory_user_am_dm,
    opp.iss__c,
    opp.createdbyid,
    opp.lead_group_source__c,
    -- opp.realterritory_user_director__c,
    real_territory.user_director_name AS _real_territory_user_director,
    opp.currencyisocode, 
    opp.amount,
    opp.isclosed,
    opp.realterritory_name__c,
    opp.vertical__c,
    opp.division_region_from_account__c,
    -- opp.realterritory_user_rm__c,
    real_territory.user_rm_name AS _real_territory_user_rm,
    acc.name AS _accountName,
    user._user_id AS _userID,
    user._user_name AS _createdBy,
    user._user_title AS _createdByTitle,
    opp.lastmodifieddate AS _lastModifiedDate,
    campaign.name AS _campaignName,
    opp.contactid AS _contactID,
    opp.application_specialist__c AS applicationSpecialist,
    opp.actual_owner__c AS actualOwner,
    opp.name AS opportunityName,
    opp.market_segment_read_only__c AS marketSegment,
    acc.type AS accountType,
    opp.workflow__c AS _workflow,
    opp.Products__c AS _products,
    opp.SubProduct__c AS _sub_products,
    opp.asset__c AS _asset,
    opp.application__c AS _application,
    opp.primary_application__c AS _primary_application,
    opp.secondary_application__c AS _secondary_application
  FROM `x-marketing.faro_salesforce.Opportunity` opp
  LEFT JOIN `x-marketing.faro_salesforce.Account` acc 
    ON acc.id = opp.accountid
  LEFT JOIN real_territory_user real_territory
    ON real_territory.name = opp.realterritory_name__c
  LEFT JOIN user 
    ON user._user_id = opp.createdbyid
  -- LEFT JOIN `x-marketing.faro_salesforce.User` user 
  -- ON user.id = opp.createdbyid
  LEFT JOIN `x-marketing.faro_salesforce.Campaign` campaign
    ON campaign.id = opp.campaignid
  -- LEFT JOIN `x-marketing.faro_salesforce.Event` event
  -- ON event.createdbyid = user.id
  WHERE opp.isdeleted = false
  -- WHERE isclosed = false
  -- AND total_price__c IS NOT NULL
  -- AND currencyisocode != 'USD'
)
SELECT DISTINCT
  Opportunity.* EXCEPT(amount, _campaignName,_real_territory_user_rm),
  -- Opportunity.opportunityID,
  -- Opportunity.createddate,
  -- Opportunity.isclosed,
  -- Opportunity.currencyisocode,
  amount AS original_amount,
  CASE 
    WHEN isclosed = true AND currencyisocode != 'USD'
    THEN (
      closedConversionRate.conversionRate
    )
    WHEN isclosed = false AND currencyisocode != 'USD'
    THEN (
      openConversionRate.conversionRate 
    )
  END AS conversionRate,
  CASE 
    WHEN isclosed = true AND currencyisocode != 'USD'
    THEN (

      closedConversionRate.total_price_USD
    )
    WHEN isclosed = false AND currencyisocode != 'USD'
    THEN (
      (amount / openConversionRate.conversionrate) 
    )
    ELSE amount
  END AS total_price,
  _campaignName,
  _real_territory_user_rm
FROM Opportunity
LEFT JOIN closedConversionRate ON closedConversionRate.id = Opportunity.opportunityID
LEFT JOIN openConversionRate ON openConversionRate.isocode = Opportunity.currencyisocode;
-- WHERE EXTRACT(YEAR FROM createddate) >= 2022;
-- AND opportunityID = '0063p000010tU94AAE'
-- LIMIT 1000

-- TRUNCATE TABLE `x-marketing.faro.event_raw`;
-- INSERT INTO `x-marketing.faro.event_raw` (
--   id,
--   createdbyid,
--   sfdc_activity_casesafeid__c, 
--   application_specialist__c, 
--   Event_Status__c, 
--   Web_Location__c, 
--   createddate
-- )


--////////////////////////////////////////////////--
----------------------------------------------------
-- -------------// FARO FEEDBACK //-------------- --
----------------------------------------------------
--////////////////////////////////////////////////--

-- TRUNCATE TABLE `x-marketing.faro.feedback`;
-- INSERT INTO `x-marketing.faro.feedback` 
CREATE OR REPLACE TABLE `x-marketing.faro.feedback` AS 
WITH feedback AS (
  SELECT 
    *
  FROM `x-marketing.faro_salesforce.Feedback__c`
  WHERE isdeleted IS false
),
opportunity AS (
  SELECT 
    opp.id AS opportunity_id,
    opp.name AS opportunity_name,
  FROM `x-marketing.faro_salesforce.Opportunity` opp
),
contact AS (
  SELECT 
    cont.id AS contact_id,
    cont.name AS contact_name
  FROM `x-marketing.faro_salesforce.Contact` cont
)
SELECT
  feedback.*,
  opportunity.opportunity_name,
  contact.contact_name
FROM feedback
LEFT JOIN opportunity ON feedback.opportunity__c = opportunity.opportunity_id
LEFT JOIN contact ON feedback.contact__c = contact.contact_id