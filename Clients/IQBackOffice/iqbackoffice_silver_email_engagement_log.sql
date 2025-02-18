

TRUNCATE TABLE `x-marketing.iqbackoffice.email_engagement_log`;

INSERT INTO `x-marketing.iqbackoffice.email_engagement_log`(


_sdc_sequence, 
_campaignID,
_campaign_name,
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


_state,
_lifecycleStage,
_utm_source, 
_utmcampaign, 
_utm_medium,
_product_service_type, 
_jobtitle, 
_createdate, 
_behavioral_score, 
_average_page_views, 
_last_page_view, 
_last_page_referrer, 
_recent_conversion_event_name, 
_company_size,
_seniority,




_contentTitle,
_contentID,
_landingPage, 
_screenshot,

_subject,
_campaignSentDate,
_subCampaign,
 
_campaign_status, 
_campaign_start_date, 
_campaign_end_date, 
_campaign_note,
_preview,
_asset_type,
_segment_campaign,
_category

)

WITH prospect_info AS(

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
  --CAST(properties.numberofemployees.value AS STRING) AS _employees,
  --properties.job_function.value AS _function,
  properties.state.value AS _state,
  properties.lifecyclestage.value AS _lifecycleStage,
  properties.email.value AS _email,

  property_utm_source.value AS _utm_source,
  property_utm_campaign.value AS _utm_campaign,
  property_utm_medium.value AS _utm_medium,
  property_product___service_type.value AS _product_service_type,
  property_jobtitle.value AS _jobtitle,
  property_createdate.value AS _createdate,
  CAST(property_behavioral_score.value  AS NUMERIC) AS _behavioral_score,
  CAST(property_hs_analytics_average_page_views.value AS NUMERIC) AS _average_page_views,
  property_hs_analytics_last_url.value AS _last_page_view,
  property_hs_analytics_last_referrer.value AS _last_referrer,
  property_recent_conversion_event_name.value AS _recent_conversion_event_name,

  associated_company.properties.bombora_company_size.value AS _company_size,
  CASE
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Assistant to%") THEN "Non-Manager"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior Counsel%") THEN "VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%General Counsel%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Founder%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%C-Level%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CDO%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CIO%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CMO%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CFO%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CEO%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Chief%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%coordinator%") THEN "Non-Manager"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%COO%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr. V.P.%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr.VP%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior-Vice Pres%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%srvp%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior VP%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%SR VP%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr Vice Pres%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr. VP%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr. Vice Pres%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%S.V.P%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior Vice Pres%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Exec Vice Pres%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Exec Vp%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Executive VP%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Exec VP%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Executive Vice President%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%EVP%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%E.V.P%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%SVP%") THEN "Senior VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%V.P%") THEN "VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%VP%") THEN "VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Vice Pres%") THEN "VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%V P%") THEN "VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%President%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Director%") THEN "Director"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CTO%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Dir%") THEN "Director"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Dir.%") THEN "Director"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%MDR%") THEN "Non-Manager"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%MD%") THEN "Director"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%GM%") THEN "Director"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Head%") THEN "VP"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Manager%") THEN "Manager"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%escrow%") THEN "Non-Manager"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%cross%") THEN "Non-Manager"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%crosse%") THEN "Non-Manager"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Partner%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CRO%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Chairman%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Owner%") THEN "C-Level"
    WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Team Lead%") THEN "Manager"
    END AS _seniority,
FROM
  `x-marketing.iqbackoffice_hubspot.contacts`
WHERE
  properties.email.value IS NOT NULL
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY vid, property_email.value, CONCAT(properties.firstname.value,' ', properties.lastname.value)
  ORDER BY
    vid DESC ) = 1
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

    FROM `x-marketing.iqbackoffice_hubspot.campaigns` campaign

    QUALIFY ROW_NUMBER() OVER(PARTITION BY campaign.name, campaign.id ORDER BY campaign.id) = 1


)
, google_sheet AS (
  SELECT
    _campaign_name,
    _email_id,
    _preview,
    _asset_type,
    PARSE_TIMESTAMP('%m/%d/%Y %H:%M', CONCAT(_live_date, ' ', _send_time)) AS _campaignSentDate,
    _email_segment,
    _landing_page_url,
    _landing_page_url AS _landingpage,
    _ad_visual AS _screenshot
  FROM
    `x-marketing.iqbackoffice_google_sheets.db_email_campaign`
)
,airtable_info AS(
    SELECT
    _contentTitle,
    campaign._contentID,
    campaign._campaignID,
    airtable._landingpage AS _landingPage,
    airtable._screenshot AS _screenshot,
    _subject,
    _campaignSentDate AS _campaignSentDate,
    airtable._campaign_name,
    _status,
    CAST(_campaignSentDate AS DATE) AS _start_date,
    _end_date,
    _notes,
    _preview,
    _asset_type,
    _email_segment,
     CASE WHEN _email_id IS NULL THEN 'Non-Marketing Email' ELSE "Marketing Email" END AS _campaign_note 
  FROM
    email_info campaign
  LEFT JOIN email_campaign_info 
    ON campaign._campaignID = email_campaign_info._campaignID
  LEFT JOIN google_sheet airtable
    ON campaign._campaignID = _email_id

)
,sent AS(

      SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Sent' AS _engagement,
      activity.recipient AS _email,
      TIMESTAMP(DATETIME(activity.created, 'America/New_York')) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
     
      FROM `x-marketing.iqbackoffice_hubspot.email_events` activity
      JOIN `x-marketing.iqbackoffice_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'SENT'
      AND campaign.name IS NOT NULL 
      QUALIFY  ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name,activity.emailcampaignid ORDER BY activity.created DESC)  = 1

)
,dropped AS(

      SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Dropped' AS _engagement,
      activity.recipient AS _email,
      TIMESTAMP(DATETIME(activity.created, 'America/New_York')) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      
      FROM `x-marketing.iqbackoffice_hubspot.email_events` activity
      JOIN `x-marketing.iqbackoffice_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'DROPPED'
      AND campaign.name IS NOT NULL
      --AND activity.recipient NOT LIKE '%sdi.%'
   QUALIFY  ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name,activity.emailcampaignid ORDER BY activity.created DESC)  = 1
)
,delivered AS(

      SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Delivered' AS _engagement,
      activity.recipient AS _email,
      TIMESTAMP(DATETIME(activity.created, 'America/New_York')) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      
      FROM `x-marketing.iqbackoffice_hubspot.email_events` activity
      JOIN `x-marketing.iqbackoffice_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'DELIVERED'
      AND campaign.name IS NOT NULL
    QUALIFY  ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name,activity.emailcampaignid ORDER BY activity.created DESC)  = 1
)
,HardBounced AS (

      SELECT 
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Hard Bounce' AS _engagement,
      activity.recipient AS _email,
      TIMESTAMP(DATETIME(activity.created, 'America/New_York')) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
     
      FROM `x-marketing.iqbackoffice_hubspot.subscription_changes` subs, UNNEST(changes) AS status
      JOIN `x-marketing.iqbackoffice_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.iqbackoffice_hubspot.campaigns` campaign ON activity.emailcampaignid = campaign.id
      WHERE status.value.change = 'BOUNCED'
   QUALIFY ROW_NUMBER() OVER(PARTITION BY subs.recipient, activity.emailcampaigngroupid,activity.emailcampaignid ORDER BY subs.timestamp DESC) = 1
)
,SoftBounced AS (
 
      SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Soft Bounce' AS _engagement,
      activity.recipient AS _email,
      TIMESTAMP(DATETIME(activity.created, 'America/New_York')) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
    
      FROM `x-marketing.iqbackoffice_hubspot.email_events` activity
      JOIN `x-marketing.iqbackoffice_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'BOUNCE'
      AND campaign.name IS NOT NULL
     QUALIFY  ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name,activity.emailcampaignid ORDER BY activity.created DESC)  = 1
)
,email_opened AS(
 
    SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Opened' AS _engagement,
      activity.recipient AS _email,
      TIMESTAMP(DATETIME(activity.created, 'America/New_York')) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      
      FROM `x-marketing.iqbackoffice_hubspot.email_events` activity
      JOIN `x-marketing.iqbackoffice_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'OPEN'
      AND activity.filteredevent = FALSE
      AND campaign.name IS NOT NULL
   QUALIFY  ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name,activity.emailcampaignid ORDER BY activity.created DESC)  = 1
)
,email_clicked AS(

    SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Clicked' AS _engagement,
      activity.recipient AS _email,
      TIMESTAMP(DATETIME(activity.created, 'America/New_York')) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      
      FROM `x-marketing.iqbackoffice_hubspot.email_events` activity
      JOIN `x-marketing.iqbackoffice_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'CLICK'
      AND activity.filteredevent = FALSE
      AND campaign.name IS NOT NULL
   QUALIFY  ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name,activity.emailcampaignid ORDER BY activity.created DESC)  = 1
)
,email_deferred AS(

    SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Deferred' AS _engagement,
      activity.recipient AS _email,
      TIMESTAMP(DATETIME(activity.created, 'America/New_York')) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      
      FROM `x-marketing.iqbackoffice_hubspot.email_events` activity
      JOIN `x-marketing.iqbackoffice_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'DEFERRED'
      AND campaign.name IS NOT NULL
   QUALIFY  ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name,activity.emailcampaignid ORDER BY activity.created DESC)  = 1
)
,email_unsubscribed AS(

    SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Unsubscribed' AS _engagement,
      activity.recipient AS _email,
      TIMESTAMP(DATETIME(activity.created, 'America/New_York')) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      
      FROM `x-marketing.iqbackoffice_hubspot.subscription_changes` , UNNEST(changes) AS status
      JOIN `x-marketing.iqbackoffice_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.iqbackoffice_hubspot.campaigns` campaign ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'STATUSCHANGE'
      AND status.value.change = 'UNSUBSCRIBED'
   QUALIFY  ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name,activity.emailcampaignid ORDER BY activity.created DESC)  = 1
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