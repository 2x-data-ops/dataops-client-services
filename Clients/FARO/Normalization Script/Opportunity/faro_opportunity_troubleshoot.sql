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
  realterritory_user_am_dm__c,
  iss__c,
  createdbyid, 
  lead_group_source__c, 
  realterritory_user_director__c, 
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
  contactID,
  applicationSpecialist,
  actualOwner,
  opportunityName,
  marketSegment,
  accountType,
  original_amount,
  conversionRate, 
  total_price, 
  _campaignName
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
    opp.division_region__c,
    opp.campaignid,
    opp.leadsource,
    opp.most_recent_lead_source__c,
    opp.demo_type__c,
    opp.type,
    opp.software__c,
    opp.hardware__c,
    opp.realzip__realzip_country__c,
    opp.realterritory_user_am_dm__c,
    opp.iss__c,
    opp.createdbyid,
    opp.lead_group_source__c,
    opp.realterritory_user_director__c,
    opp.currencyisocode, 
    opp.amount,
    opp.isclosed,
    opp.realterritory_name__c,
    opp.vertical__c,
    opp.division_region_from_account__c,
    acc.name AS _accountName,
    user.id AS _userID,
    user.name AS _createdBy,
    user.title AS _createdByTitle,
    opp.lastmodifieddate AS _lastModifiedDate,
    campaign.name AS _campaignName,
    opp.contactid AS _contactID,
    opp.application_specialist__c AS applicationSpecialist,
    opp.actual_owner__c AS actualOwner,
    opp.name AS opportunityName,
    opp.market_segment_read_only__c AS marketSegment,
    acc.type AS accountType
    -- createddate,
    -- lastactivitydate,
    -- event.sfdc_activity_casesafeid__c,
    -- event.application_specialist__c,
    -- event.Event_Status__c,
    -- event.Web_Location__c
  FROM `x-marketing.faro_salesforce.Opportunity` opp
  LEFT JOIN `x-marketing.faro_salesforce.Account` acc 
  ON acc.id = opp.accountid
  LEFT JOIN `x-marketing.faro_salesforce.User` user 
  ON user.id = opp.createdbyid
  LEFT JOIN `x-marketing.faro_salesforce.Campaign` campaign
  ON campaign.id = opp.campaignid
  -- LEFT JOIN `x-marketing.faro_salesforce.Event` event
  -- ON event.createdbyid = user.id
  WHERE opp.isdeleted = false
  -- WHERE isclosed = false
  -- AND total_price__c IS NOT NULL
  -- AND currencyisocode != 'USD'
)

SELECT
  *
FROM (
  SELECT DISTINCT
    Opportunity.* EXCEPT(amount, _campaignName),
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
    -- sfdc_activity_casesafeid__c,
    -- application_specialist__c,
    -- Event_Status__c,
    -- Web_Location__c
  FROM Opportunity
  LEFT JOIN closedConversionRate ON closedConversionRate.id = Opportunity.opportunityID
  LEFT JOIN openConversionRate ON openConversionRate.isocode = Opportunity.currencyisocode
);
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
-- -------------// FARO EVENT_RAW //------------- --
----------------------------------------------------
--////////////////////////////////////////////////--

TRUNCATE TABLE `x-marketing.faro.event_raw`;
INSERT INTO `x-marketing.faro.event_raw` 
-- CREATE OR REPLACE TABLE `x-marketing.faro.event_raw` AS
WITH event_raw AS(
  SELECT 
    DISTINCT
    event.id,
    event.createdbyid,
    event.sfdc_activity_casesafeid__c,
    event.application_specialist__c,
    event.Event_Status__c,
    event.Web_Location__c,
    event.createddate,
    event.activity_type_2__c AS activityType,
    event.eventsubtype AS eventSubtype,
    event.recordtypeid,
    event.activitydate AS activityDate,
    event.enddate AS endDate,
    event.account_manager_present__c AS accountManagerPresent,
    event.additional_hardware__c AS additionalHardware,
    event.hardware__c AS hardware,
    event.additional_software__c AS additionalSoftware,
    event.software__c AS software,
    event.country__c AS country,
    event.state_region__c AS region,
    event.event_status__c AS eventStatus,
    event.opportunity__c AS opportunityID,
    event.created_by_role__c AS roleCreatedBy,
    event.subject AS subject,
    event.division_region__c,
    event.vertical__c,
    event.lastmodifieddate,
    event.accountid,
    event.contact_id__c
  FROM `x-marketing.faro_salesforce.Event` event
  WHERE isdeleted IS false
),
event_relation AS (
  SELECT
    eventid,
    isinvitee
  FROM `x-marketing.faro_salesforce.EventRelation` e
  JOIN `x-marketing.faro_salesforce.Contact` c ON e.relationid = c.id 
),
user AS (
  SELECT
    id,
    name
  FROM `x-marketing.faro_salesforce.User` 
),
id_name AS (
  SELECT 
    opp.id AS opportunity_id,
    opp.name AS opportunity_name,
    cont.id AS contact_id,
    cont.name AS contact_name,
    acc.id AS account_id,
    acc.name AS account_name,
    leads.id AS lead_id,
    leads.name AS lead_name,
    cont.mailingcountry
  FROM `x-marketing.faro_salesforce.Opportunity` opp
  LEFT JOIN `x-marketing.faro_salesforce.Contact` cont
  ON opp.contact__c = cont.id
  LEFT JOIN `x-marketing.faro_salesforce.Account` acc
  ON opp.accountid = acc.id
  LEFT JOIN `x-marketing.faro_salesforce.Lead` leads
  ON cont.sfdc_lead_id__c = leads.id
)
SELECT 
  event_raw.*,
  user.name AS createdByName,
  event_relation.isinvitee,
  id_name.* EXCEPT(opportunity_id)
FROM event_raw
LEFT JOIN user ON event_raw.createdbyid = user.id
LEFT JOIN event_relation ON event_relation.eventid = event_raw.sfdc_activity_casesafeid__c
LEFT JOIN id_name ON event_raw.opportunityID = id_name.opportunity_id
WHERE 
-- EXTRACT(YEAR FROM createddate) >= 2022
-- AND 
sfdc_activity_casesafeid__c NOT IN (
  '00U5d00000gWXaWEAW',
  '00U5d00000gXNpmEAG',
  '00U5d00000gWPIPEA4',
  '00U5d00000gWKn0EAG',
  '00U5d00000gX3drEAC',
  '00U5d00000gWkJSEA0'
);

TRUNCATE TABLE `x-marketing.faro.event_relation`;
INSERT INTO `x-marketing.faro.event_relation` 
SELECT id,
eventid, 
relationid,
iswhat, 
isparent, 
isinvitee, 
status, 
accountid, 
response, 
systemmodstamp, 
createddate, 
respondeddate 
FROM `x-marketing.faro_salesforce.EventRelation` 
WHERE isdeleted IS FALSE;

--////////////////////////////////////////////////--
----------------------------------------------------
-- -----------// FARO OPP_EVENT_RAW //----------- --
----------------------------------------------------
--////////////////////////////////////////////////--

/*TRUNCATE TABLE `x-marketing.faro.opp_event_raw`;
INSERT INTO `x-marketing.faro.opp_event_raw` 
-- CREATE OR REPLACE TABLE `x-marketing.faro.opp_event_raw` AS 
SELECT
  opp.opportunityID,
  opp.opportunityName,
  opp.stagename AS opportunityStage,
  opp.applicationSpecialist,
  opp.createddate AS opportunityCreatedDate,
  opp.closedate AS opportunityCloseDate,
  opp.actualOwner,
  opp.ownerid,
  opp.accountid,
  opp._accountName AS accountName,
  opp.contactID,
  contact.name AS contactName,
  contact.email AS contactEmail,
  opp._createdBy,
  event.activityType,
  event.eventsubtype,
  event.recordtypeid,
  event.activitydate,
  event.enddate,
  event.accountManagerPresent,
  event.additionalHardware,
  event.hardware,
  event.additionalSoftware,
  event.software,
  event.country,
  event.region,
  event.eventStatus,
  event.sfdc_activity_casesafeid__c AS activityID,
  event.roleCreatedBy,
  event.subject,
  opp.marketSegment,
  opp.vertical__c AS vertical,
  user.name AS createdByName,
  event.isinvitee,
  event.lastmodifieddate
FROM `x-marketing.faro.opportunity_raw` opp
LEFT JOIN `x-marketing.faro_salesforce.Contact` contact
ON contact.id = opp.contactID
LEFT JOIN `x-marketing.faro.event_raw` event
ON event.createdbyid = opp._userID
LEFT JOIN `x-marketing.faro_salesforce.User` user
ON event.createdbyid = user.id
-- ON event.opportunityID = opp.opportunityID
-- WHERE event.eventStatus = 'Complete'
;*/

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