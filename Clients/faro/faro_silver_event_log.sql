
--////////////////////////////////////////////////--
----------------------------------------------------
-- -------------// FARO EVENT_RAW //------------- --
----------------------------------------------------
--////////////////////////////////////////////////--

TRUNCATE TABLE `x-marketing.faro.event_raw`;
INSERT INTO `x-marketing.faro.event_raw` 
-- CREATE OR REPLACE TABLE `x-marketing.faro.event_raw` AS
WITH event_raw AS(
  SELECT DISTINCT
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
    event.opp_id__c AS opportunityID,
    event.created_by_role__c AS roleCreatedBy,
    event.subject AS subject,
    event.division_region__c,
    event.vertical__c,
    event.lastmodifieddate,
    event.accountid AS accountID,
    event.contact_id__c AS contactID,
    event.ownerid AS _owner_id,
    event.ischild AS _is_child,
    event.isgroupevent AS _is_group_event
  FROM `x-marketing.faro_salesforce.Event` event
  WHERE isdeleted IS false
),
event_relation AS (
  SELECT
    eventid,
    isinvitee
  FROM `x-marketing.faro_salesforce.EventRelation` e
  JOIN `x-marketing.faro_salesforce.Contact` c 
    ON e.relationid = c.id 
),
user AS (
  SELECT
    id AS _user_id,
    name,
    username AS _username,
    manager__c AS _manager_id,
    title AS _workday_position
  FROM `x-marketing.faro_salesforce.User` 
),
manager AS (
  SELECT
    id AS _manager_id,
    name AS _manager_name,
    title AS _manager_workday_position
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
    ON opp.contactid = cont.id
  LEFT JOIN `x-marketing.faro_salesforce.Account` acc
    ON opp.accountid = acc.id
  LEFT JOIN `x-marketing.faro_salesforce.Lead` leads
    ON cont.sfdc_lead_id__c = leads.id
),
user_info AS (
  SELECT
    event_raw.id AS event_id,
    MAX(CASE WHEN event_raw.createdbyid = user._user_id THEN user._user_id END) AS _created_by_id,
    MAX(CASE WHEN event_raw.createdbyid = user._user_id THEN user.name END) AS _created_by_name,
    MAX(CASE WHEN event_raw.createdbyid = user._user_id THEN user._workday_position END) AS _created_by_workday_position,
    MAX(CASE WHEN event_raw._owner_id = user._user_id THEN user._user_id END) AS _owner_id,
    MAX(CASE WHEN event_raw._owner_id = user._user_id THEN user.name END) AS _owner_name,
    MAX(CASE WHEN event_raw._owner_id = user._user_id THEN user._workday_position END) AS _owner_workday_position,
    MAX(CASE WHEN user._manager_id = manager._manager_id THEN manager._manager_name END) AS _manager_name,
    MAX(CASE WHEN user._manager_id = manager._manager_id THEN manager._manager_workday_position END) AS _manager_workday_position
  FROM event_raw
  LEFT JOIN user ON event_raw.createdbyid = user._user_id OR event_raw._owner_id = user._user_id
  LEFT JOIN manager USING(_manager_id)
  -- WHERE conditions if needed
  GROUP BY event_raw.id
)
SELECT 
  event_raw.* EXCEPT(opportunityID),
  -- CASE 
  --   WHEN event_raw.createdbyid = user._user_id
  --   THEN user.name 
  -- END AS createdByName,
  -- CASE 
  --   WHEN event_raw._owner_id = user._user_id
  --   THEN user.name 
  -- END AS ownerName,
  -- user.* EXCEPT(name),
  user_info._created_by_name,
  user_info._created_by_workday_position,
  user_info._owner_name,
  user_info._owner_workday_position,
  user_info._manager_name,
  user_info._manager_workday_position,
  event_relation.isinvitee,
  id_name.*
FROM event_raw
-- LEFT JOIN user 
--   ON event_raw.createdbyid = user._user_id
--   OR event_raw._owner_id = user._user_id
LEFT JOIN event_relation 
  ON event_relation.eventid = event_raw.sfdc_activity_casesafeid__c
LEFT JOIN id_name 
  ON event_raw.opportunityID = LEFT(id_name.opportunity_id, 15)
LEFT JOIN user_info
  ON event_raw.id = user_info.event_id
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