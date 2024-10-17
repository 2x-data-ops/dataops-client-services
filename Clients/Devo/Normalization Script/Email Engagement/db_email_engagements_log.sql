--CREATE OR REPLACE TABLE `x-marketing.devo.db_email_engagements_log` AS

TRUNCATE TABLE `x-marketing.devo.db_email_engagements_log`;

INSERT INTO `x-marketing.devo.db_email_engagements_log`(
_sdc_sequence, 
_campaignID,
_campaign,
_engagement,
_email,
_timestamp, 
_description,
_device_type,
_linkid,
_duration,
_response,
_prospectID,
_name, 
_phone,
_title,
_tier,
_company,
_domain,
_industry,
_country,
_city,
_revenue,
_employees,
_function,
_state,
_lifecycleStage,
_contentTitle,
_contentID,
_landingPage,
_screenshot,
_subject,
_2X_campaign
)
WITH prospect_info AS(
  SELECT 
    CAST(vid AS STRING) AS _prospectID,
    CONCAT(properties.firstname.value,' ', properties.lastname.value) AS _name,
    properties.phone.value AS _phone,
    properties.jobtitle.value AS _title,
    properties.account_tier.value AS _tier,
    properties.company.value AS _company,
    associated_company.properties.domain.value AS _domain,
    properties.industry.value AS _industry,
    properties.country.value AS _country,
    properties.city.value AS _city,
    CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
    CAST(properties.numberofemployees.value AS STRING) AS _employees,
    properties.job_function.value AS _function,
    properties.state.value AS _state,
    CAST(properties.lifecyclestage.value AS STRING) AS _lifecycleStage,
    properties.email.value AS _email,

    FROM `x-marketing.devo_hubspot.contacts`
    WHERE properties.email.value IS NOT NULL
    QUALIFY ROW_NUMBER() OVER( PARTITION BY property_email.value, CONCAT(properties.firstname.value,' ', properties.lastname.value) ORDER BY vid DESC) = 1
),

airtable_info AS(
  SELECT
    campaign.name AS _contentTitle,
    CAST(campaign.contentid AS STRING) AS _contentID,
    airtable._landingpage AS _landingPage,
    airtable._screenshot AS _screenshot,
    campaign.subject AS _subject,
    CAST(campaign.id AS STRING) as _campaignID, ---get the id from hubspot id to join with the engagement data. not airtableid,
    CASE WHEN _emailid IS NULL THEN 'Not 2X Campaign' ELSE '2X Campaign' END AS _2X_campaign,
    FROM `x-marketing.devo_hubspot.campaigns` campaign
    LEFT JOIN `x-marketing.devo_mysql.db_airtable_email` airtable
    ON CAST(campaign.contentid AS STRING) = _emailid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign.name, campaign.id ORDER BY campaign.id) = 1

),

Dropped AS(
    SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Dropped' AS _engagement,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      FROM `x-marketing.devo_hubspot.email_events` activity
      JOIN `x-marketing.devo_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'DROPPED'
      AND campaign.name IS NOT NULL
      --AND activity.recipient NOT LIKE '%devo.%'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),

  Sent AS(
    SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Sent' AS _engagement,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      FROM `x-marketing.devo_hubspot.email_events` activity
      JOIN `x-marketing.devo_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'SENT'
      AND campaign.name IS NOT NULL
      --AND activity.recipient NOT LIKE '%2x.marketing%'
      --AND activity.recipient NOT LIKE '%devo.%'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1

  ),

email_sent AS(
  SELECT Sent.* FROM Sent
  LEFT JOIN Dropped
  ON Sent._email = Dropped._email AND Sent._campaignID = Dropped._campaignID
  WHERE Dropped._email IS NULL
),

Delivered AS(
    SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Delivered' AS _engagement,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      FROM `x-marketing.devo_hubspot.email_events` activity
      JOIN `x-marketing.devo_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'DELIVERED'
      AND campaign.name IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1

  ),

  HardBounced AS (
    SELECT 
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Hard Bounced' AS _engagement,
      activity.recipient AS _email,
      CAST(subs.timestamp AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      FROM `x-marketing.devo_hubspot.subscription_changes` subs, UNNEST(changes) AS status
      JOIN `x-marketing.devo_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.devo_hubspot.campaigns` campaign ON activity.emailcampaignid = campaign.id
      WHERE status.value.change = 'BOUNCED'
      QUALIFY ROW_NUMBER() OVER(PARTITION BY subs.recipient, activity.emailcampaigngroupid ORDER BY subs.timestamp DESC) = 1

  ),

  SoftBounced AS (
    SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Soft Bounced' AS _engagement,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      FROM `x-marketing.devo_hubspot.email_events` activity
      JOIN `x-marketing.devo_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'BOUNCE'
      AND campaign.name IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1

  ),

  hardbounced_mailbox AS (
    SELECT
      *
    FROM HardBounced
    WHERE _response NOT LIKE "%mailbox full%"
  ),

email_delivered AS(
  SELECT Delivered.* FROM Delivered
  LEFT JOIN Dropped ON Delivered._email = Dropped._email AND Delivered._campaignID = Dropped._campaignID
  LEFT JOIN hardbounced_mailbox AS HardBounced ON Delivered._email = HardBounced._email AND Delivered._campaignID = HardBounced._campaignID ---devo exclude hardbounce if they have respond mailbox full from hardbounce. include into delivered instead. 
  LEFT JOIN SoftBounced ON Delivered._email = SoftBounced._email AND Delivered._campaignID = SoftBounced._campaignID
  WHERE Dropped._email IS NULL AND HardBounced._email IS NULL AND SoftBounced._email IS NULL
),

email_soft_bounced AS (
  SELECT SoftBounced.* FROM SoftBounced
  LEFT JOIN HardBounced ON SoftBounced._email = HardBounced._email AND SoftBounced._campaignID = HardBounced._campaignID
  LEFT JOIN Delivered ON SoftBounced._email = Delivered._email AND SoftBounced._campaignID = Delivered._campaignID
  WHERE HardBounced._email IS NULL AND Delivered._email IS NULL 
),

email_hard_bounced AS (
   SELECT HardBounced.* 
   FROM HardBounced
  JOIN SoftBounced ON HardBounced._email = Softbounced._email AND HardBounced._campaignID = SoftBounced._campaignID
   --LEFT JOIN Delivered ON HardBounced._email = Delivered._email AND HardBounced._campaignID = Delivered._campaignID
   WHERE HardBounced._response NOT LIKE "%mailbox full%"  ---- we remove the mailbox full and dont join with delivered
),

email_opened AS(
  SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Opened' AS _engagement,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      FROM `x-marketing.devo_hubspot.email_events` activity
      JOIN `x-marketing.devo_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'OPEN'
      AND activity.filteredevent = FALSE
      AND campaign.name IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1

),

email_clicked AS(
  SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Clicked' AS _engagement,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      FROM `x-marketing.devo_hubspot.email_events` activity
      JOIN `x-marketing.devo_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'CLICK'
      AND activity.filteredevent = FALSE
      AND campaign.name IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1

),

email_dropped AS (
  SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Dropped' AS _engagement,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      FROM `x-marketing.devo_hubspot.email_events` activity
      JOIN `x-marketing.devo_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'DROPPED'
      AND campaign.name IS NOT NULL
      --AND activity.recipient NOT LIKE '%devo.%'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1

),

email_deferred AS(
  SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Deferred' AS _engagement,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      FROM `x-marketing.devo_hubspot.email_events` activity
      JOIN `x-marketing.devo_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'DEFERRED'
      AND campaign.name IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1

),

email_suppressed AS(
  SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Suppressed' AS _engagement,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      FROM `x-marketing.devo_hubspot.email_events` activity
      JOIN `x-marketing.devo_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'SUPPRESSED'
      AND campaign.name IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1

),

email_unsubscribed AS(
  SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Unsubscribed' AS _engagement,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      FROM `x-marketing.devo_hubspot.subscription_changes` , UNNEST(changes) AS status
      JOIN `x-marketing.devo_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.devo_hubspot.campaigns` campaign ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'STATUSCHANGE'
      AND status.value.change = 'UNSUBSCRIBED'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1

),

engagements AS (
  SELECT * FROM email_sent
    UNION ALL
  SELECT * FROM email_delivered
    UNION ALL
  SELECT * FROM email_soft_bounced
    UNION ALL
  SELECT * FROM email_hard_bounced
  UNION ALL
  SELECT * FROM email_opened
    UNION ALL
  SELECT * FROM email_clicked
    UNION ALL
  SELECT * FROM email_dropped
    UNION ALL
  SELECT * FROM email_deferred
    UNION ALL
  SELECT * FROM email_unsubscribed
  UNION ALL 
  SELECT * FROM email_suppressed
)

SELECT 
  engagements.*,
  prospect_info.* EXCEPT (_email),
  airtable_info.* EXCEPT (_campaignID),
FROM engagements

LEFT JOIN prospect_info ON engagements._email = prospect_info._email
LEFT JOIN airtable_info ON engagements._campaignid = CAST(airtable_info._campaignID AS STRING);
 --WHERE engagements._campaignID = '295019700';

/*
ALTER TABLE `x-marketing.devo.db_email_engagements_log`
ADD COLUMN _isBot STRING;

--- Label Bots ---
UPDATE `x-marketing.devo.db_email_engagements_log`origin  
SET origin._isBot = 'true'
FROM (
    SELECT DISTINCT

        _email,
        _contentTitle

    FROM 
        `x-marketing.devo.db_email_engagements_log`
    WHERE 
        _description LIKE '%https://3x.wise-portal.com/iclick/iclick.php%'
) bot
WHERE 
    origin._email = bot._email
AND origin._contentTitle = bot._contentTitle
AND origin._engagement IN ('Clicked');

*/


