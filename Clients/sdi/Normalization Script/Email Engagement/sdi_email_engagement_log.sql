TRUNCATE TABLE `x-marketing.sdi.db_email_engagements_log`;

INSERT INTO `x-marketing.sdi.db_email_engagements_log`(
_sdc_sequence, 
_campaignID,
_utmcampaign,
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
_subject,
_campaign_name, 
_campaign_status, 
_campaign_start_date, 
_campaign_end_date, 
_campaign_note

)
WITH prospect_info AS(
  SELECT * EXCEPT (_rownum)
  FROM(
    SELECT 
    CAST(vid AS STRING) AS _prospectID,
    CONCAT(properties.firstname.value,' ', properties.lastname.value) AS _name,
    properties.phone.value AS _phone,
    properties.jobtitle.value AS _title,
    --properties.account_tier.value AS _tier,
    properties.company.value AS _company,
    associated_company.properties.domain.value AS _domain,
    properties.industry.value AS _industry,
    properties.country.value AS _country,
    properties.city.value AS _city,
    CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
    CAST(properties.numberofemployees.value AS STRING) AS _employees,
    properties.job_function.value AS _function,
    properties.state.value AS _state,
    properties.lifecyclestage.value AS _lifecycleStage,
    properties.email.value AS _email,
    ROW_NUMBER() OVER( PARTITION BY vid,property_email.value, CONCAT(properties.firstname.value,' ', properties.lastname.value) ORDER BY vid DESC) AS _rownum
    FROM `x-marketing.sdi_hubspot.contacts`
    WHERE properties.email.value IS NOT NULL

  )WHERE _rownum = 1
) 
, campaign_info AS (

  SELECT 
  `Campaign Name` AS _campaign_name, 
  GuiID AS _campaign_id, 
  Status AS _status, 
  `Start Date` AS _start_date, 
  `End Date` AS _end_date, 
  Notes AS _notes
  FROM `x-marketing.sdi.campaign`

) 
, email_campaign_info AS (

  SELECT 
  campaign_id AS _campaign_id, 
  email_campaign_id  AS _campaignID,
  _campaign_name,
  _status, 
  CASE WHEN _start_date = 'No Start Date' THEN NULL
  ELSE CAST(_start_date  AS DATE) END AS _start_date  , 
  CASE WHEN _end_date = 'No End Date' THEN NULL
  ELSE CAST(_end_date  AS DATE) END AS _end_date,
  _notes
  FROM `x-marketing.sdi.email_campaign` email
  LEFT JOIN campaign_info ON email.campaign_id = campaign_info._campaign_id

) 
, email_info AS (

   SELECT
    campaign.name AS _contentTitle,
    CAST(campaign.contentid AS STRING) AS _contentID,
    campaign.id AS _campaignID,
    campaign.subject AS _subject,

    FROM `x-marketing.sdi_hubspot.campaigns` campaign

    QUALIFY ROW_NUMBER() OVER(PARTITION BY campaign.name, campaign.id ORDER BY campaign.id) = 1


)
,airtable_info AS(
    SELECT
    _contentTitle,
    campaign._contentID,
    campaign._campaignID,
    --airtable._landingpage AS _landingPage,
    --airtable._screenshot AS _screenshot,
    _subject,
    --airtable._livedate AS _campaignSentDate,
    _campaign_name,
    _status,
    _start_date,
    _end_date,
    _notes

    FROM email_info campaign
    LEFT JOIN email_campaign_info ON campaign._campaignID = email_campaign_info._campaignID
    --JOIN `x-marketing.sdi_mysql.db_airtable_email` airtable
    --ON LOWER(campaign.name) = LOWER(airtable._code)

)
,sent AS(
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Sent' AS _engagement,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sdi_hubspot.email_events` activity
      JOIN `x-marketing.sdi_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'SENT'
      AND campaign.name IS NOT NULL

    )WHERE _rownum = 1
)
,dropped AS(
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Dropped' AS _engagement,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sdi_hubspot.email_events` activity
      JOIN `x-marketing.sdi_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'DROPPED'
      AND campaign.name IS NOT NULL
      --AND activity.recipient NOT LIKE '%sdi.%'
    )WHERE _rownum = 1
)
,delivered AS(
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Delivered' AS _engagement,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sdi_hubspot.email_events` activity
      JOIN `x-marketing.sdi_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'DELIVERED'
      AND campaign.name IS NOT NULL
    )WHERE _rownum = 1
)
,HardBounced AS (
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT 
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Hard Bounce' AS _engagement,
      activity.recipient AS _email,
      subs.timestamp AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER(PARTITION BY subs.recipient, activity.emailcampaigngroupid ORDER BY subs.timestamp DESC) AS _rownum
      FROM `x-marketing.sdi_hubspot.subscription_changes` subs, UNNEST(changes) AS status
      JOIN `x-marketing.sdi_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.sdi_hubspot.campaigns` campaign ON activity.emailcampaignid = campaign.id
      WHERE status.value.change = 'BOUNCED'
    )WHERE _rownum = 1
)
,SoftBounced AS (
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Soft Bounce' AS _engagement,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sdi_hubspot.email_events` activity
      JOIN `x-marketing.sdi_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'BOUNCE'
      AND campaign.name IS NOT NULL
    )WHERE _rownum = 1
)
,email_opened AS(
  SELECT * EXCEPT (_rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Opened' AS _engagement,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sdi_hubspot.email_events` activity
      JOIN `x-marketing.sdi_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'OPEN'
      AND activity.filteredevent = FALSE
      AND campaign.name IS NOT NULL
  )WHERE _rownum = 1
)
,email_clicked AS(
  SELECT * EXCEPT (_rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Clicked' AS _engagement,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sdi_hubspot.email_events` activity
      JOIN `x-marketing.sdi_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'CLICK'
      AND activity.filteredevent = FALSE
      AND campaign.name IS NOT NULL
  )WHERE _rownum = 1
)
,email_deferred AS(
  SELECT * EXCEPT (_rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Deferred' AS _engagement,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sdi_hubspot.email_events` activity
      JOIN `x-marketing.sdi_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'DEFERRED'
      AND campaign.name IS NOT NULL
  )WHERE _rownum = 1
)
,email_unsubscribed AS(
  SELECT * EXCEPT (_rownum)
  FROM (
    SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Unsubscribed' AS _engagement,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sdi_hubspot.subscription_changes` , UNNEST(changes) AS status
      JOIN `x-marketing.sdi_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.sdi_hubspot.campaigns` campaign ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'STATUSCHANGE'
      AND status.value.change = 'UNSUBSCRIBED'
  )WHERE _rownum = 1
)
, email_sent AS (
   SELECT Sent.* 
   FROM Sent
   LEFT JOIN Dropped
   ON Sent._email = Dropped._email AND Sent._campaignID = Dropped._campaignID
   WHERE Dropped._email IS NULL
)
,email_delivered AS (
    SELECT Delivered.* 
   FROM Delivered
   LEFT JOIN Dropped ON Delivered._email = Dropped._email AND Delivered._campaignID = Dropped._campaignID
   LEFT JOIN (SELECT * FROM HardBounced WHERE _response NOT LIKE "%mailbox full%") HardBounced ON Delivered._email = HardBounced._email AND Delivered._campaignID = HardBounced._campaignID
   LEFT JOIN SoftBounced ON Delivered._email = SoftBounced._email AND Delivered._campaignID = SoftBounced._campaignID
   WHERE Dropped._email IS NULL 
  AND HardBounced._email IS NULL 
  --AND SoftBounced._email IS  NULL 
) 
,email_soft_bounced AS (
  SELECT SoftBounced.* 
  FROM SoftBounced
  LEFT JOIN HardBounced ON SoftBounced._email = HardBounced._email AND SoftBounced._campaignID = HardBounced._campaignID
  LEFT JOIN Delivered ON SoftBounced._email = Delivered._email AND SoftBounced._campaignID = Delivered._campaignID
  WHERE HardBounced._email IS NULL
  AND Delivered._email IS NULL 
)
,email_hard_bounce AS (
   SELECT HardBounced.* 
   FROM HardBounced
  JOIN SoftBounced ON HardBounced._email = Softbounced._email AND HardBounced._campaignID = SoftBounced._campaignID
   LEFT JOIN Delivered ON HardBounced._email = Delivered._email AND HardBounced._campaignID = Delivered._campaignID
   WHERE HardBounced._response NOT LIKE "%mailbox full%" 
   --AND CONCAT(Delivered._email,Delivered._campaignID) IS NULL
)
,combine_bounce AS (
   SELECT
    * EXCEPT(_rownum)
  FROM (
  SELECT *, 
  ROW_NUMBER() OVER(PARTITION BY _email, _campaignID ORDER BY _timestamp DESC) AS _rownum 
  FROM (
    SELECT * FROM email_soft_bounced
    UNION ALL 
    SELECT * FROM email_hard_bounce
    )
  ) WHERE
    _rownum = 1
), email_false_delivered AS (
  SELECT 
   email_bounce ._sdc_sequence,
   email_bounce ._campaignID,
   email_bounce ._campaign,
   "False Delivered" AS _engagement,
   email_bounce ._email,
   email_bounce ._timestamp,
   email_bounce ._description,
   email_bounce ._device_type,
   email_bounce ._linkid,
   email_bounce ._duration,
   email_bounce ._response,
   FROM delivered 
   LEFT JOIN dropped ON delivered._email = dropped._email and delivered._campaignID = dropped._campaignID
   --LEFT JOIN bounced ON delivered._email = bounced._email and delivered._campaignID = bounced._campaignID
   JOIN  combine_bounce email_bounce ON delivered._email = email_bounce._email and delivered._campaignID = email_bounce._campaignID
   WHERE  
   dropped._email IS NULL 
), email_not_sent AS (
  SELECT 
   dropped ._sdc_sequence,
   dropped ._campaignID,
   dropped ._campaign,
   "Not Sent" AS _engagement,
   dropped ._email,
   dropped ._timestamp,
   dropped ._description,
   dropped ._device_type,
   dropped ._linkid,
   dropped ._duration,
   dropped ._response,
   FROM dropped 
   --LEFT JOIN Sent ON dropped._email = Sent._email and dropped._campaignID = Sent._campaignID

  --  WHERE  
  --  Sent._email IS NULL 
), engagement_combine AS (
  SELECT * FROM email_not_sent
  UNION ALL 
  SELECT * FROM email_sent 
  UNION ALL 
  SELECT * FROM email_delivered
  UNION ALL
  SELECT * FROM email_soft_bounced
  UNION ALL 
  SELECT * FROM email_hard_bounce
  UNION ALL
  SELECT * FROM email_opened 
  UNION ALL 
  SELECT * FROM email_clicked 
  UNION ALL
  SELECT * FROM email_false_delivered
  UNION ALL 
  SELECT * FROM email_unsubscribed
  UNION ALL 
  SELECT * FROM email_deferred
  UNION ALL 
  SELECT * FROM email_not_sent
) 
SELECT engagements.* ,
prospect_info.* EXCEPT(_email),
airtable_info.* EXCEPT(_campaignID)
FROM engagement_combine AS engagements
LEFT JOIN
  prospect_info
ON
  engagements._email = prospect_info._email
LEFT JOIN
  airtable_info
ON
  engagements._campaignID = CAST(airtable_info._campaignID AS STRING)