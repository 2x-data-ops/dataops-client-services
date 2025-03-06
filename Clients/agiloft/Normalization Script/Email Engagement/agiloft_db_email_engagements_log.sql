------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------ Email Engagement Log --------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* 
  This script is used typically for the email performance page/dashboard 
  CRM/Platform: Hubspot
  Data type: Email Engagement
  Depedency Table: db_tam_database
  Target table: db_email_engagements_log
*/
CREATE OR REPLACE TABLE `x-marketing.agiloft.db_email_engagements_log` AS
-- TRUNCATE TABLE `x-marketing.agiloft.db_email_engagements_log`;
-- INSERT INTO `x-marketing.agiloft.db_email_engagements_log` (
--     _sdc_sequence,
--     _campaignID,
--     _utmcampaign,
--     _subject,
--     _email,
--     _timestamp,
--     _engagement,
--     _description,
--     _device_type,
--     _linkid,
--     _duration,
--     _response,
--     _utm_source,  
--     _utm_medium, 
--     _utm_content,
--     -------------airtable 
--   _campaignSentDate,
--   _plan_date,
--   _campaignName, 
--   _campaign_subject, 
--   _what_we_do, 
--   _campaign_type,
--   _email_name,
--   _landingPage, 
--   _email_sequence, 
--   _email_sequence_st, 
--   _asset_title, 
--   _persona_campaign,
--   _screenshot,
--   ------------prospect----
--     _prospectID,
--     _name,
--     _domain,
--     _title,
--     _function,
--     _seniority,
--     _phone,
--     _company,
--     _revenue,
--     _industry,
--     _city,
--     _state,
--     _country,
--     _persona,
--     _lifecycleStage
-- )
WITH prospect_info AS (
  SELECT  
    DISTINCT 
    CAST(_id AS STRING) AS _id,
    _email,
    _name,
    _domain,
    _jobtitle,
    _seniority,
    _function,
    _phone,
    _company,
    _industry,
    _revenue,
    _employee,
    _city,
    _state,
    _persona,
    _lifecycleStage,
    _createddate,
    _sfdcaccountid,
    _sfdccontactid,
    _sfdcleadid,
    _country
  FROM
    -- `agiloft.db_tam_database`
    `agiloft.db_icp_database_log`
  WHERE
    _email IS NOT NULL
    -- AND _email NOT LIKE '%2x.marketing%'
    -- AND _email NOT LIKE '%agiloft.com%'
),
/*airtable_info AS (
 SELECT  
    CAST(id AS STRING) AS _pardotid,
    name AS _code , 
    subject AS _subject, 
    _assettitle, 
    _screenshot, 
    _assetType,
    _requesterName,
    CASE WHEN id = 262574330 THEN "LSI MI TOFU01 #1" ELSE  _email END _email,
    _campaign,
    _campaignid,
    CASE WHEN _emailid = "" THEN NULL ELSE CAST(_emailid AS INT64) END AS _emailid,
    CASE WHEN _livedate = "" THEN NULL ELSE CAST(_livedate AS DATE) END AS _livedate,
    CASE WHEN  _senddate = "" THEN NULL ELSE CAST(_senddate AS DATE) END AS _senddate,
    email._cihomeurl,
    email._code AS _campaignCode
   -- id, name, subject, type, email.* 
FROM `x-marketing.agiloft_hubspot.campaigns` campaign
 JOIN `x-marketing.agiloft_mysql.db_airtable_email` email ON CAST(campaign.id AS STRING) =   _pardotid --END
),*/
hardbounced AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.emailcampaignid AS STRING) AS _campaignID, 
    campaign.name AS _campaign,
    campaign.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Hard Bounce' AS _engagement,
    url AS _description,
    devicetype AS _device_type,
    CAST(linkid AS STRING) _linkid,
    CAST(duration AS STRING) _duration,
    response AS _response,
    ROW_NUMBER() OVER( 
      PARTITION BY activity.recipient, campaign.name 
      ORDER BY activity.created DESC) AS _rownum
  FROM `x-marketing.agiloft_hubspot.subscription_changes`, UNNEST(changes) AS status 
  JOIN `x-marketing.agiloft_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
  JOIN `x-marketing.agiloft_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
  WHERE 
    activity.type = 'BOUNCE'
  AND 
    status.value.change = 'BOUNCED'  
),
Sent AS (
  SELECT
    * EXCEPT(_rownum)
  FROM (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Sent' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.agiloft_hubspot.email_events` activity
    JOIN
      `x-marketing.agiloft_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'SENT'  
    )
  WHERE
    _rownum = 1 
),
email_sent AS (
  SELECT 
    Sent.* 
  FROM Sent
  LEFT JOIN hardbounced ON Sent._email = hardbounced._email 
  AND Sent._campaignID = hardbounced._campaignID
  WHERE hardbounced._email IS NULL 
),
delivered AS (
  SELECT
    * EXCEPT(_rownum)
  FROM (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name  AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Delivered' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.agiloft_hubspot.email_events` activity
    JOIN
      `x-marketing.agiloft_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'DELIVERED'    
    AND 
      campaign.name IS NOT NULL  
  )
  WHERE
    _rownum = 1
),
email_delivered AS (
  SELECT 
    delivered .* 
  FROM delivered 
  LEFT JOIN hardbounced ON delivered._email = hardbounced._email 
  AND delivered._campaignID = hardbounced._campaignID
  WHERE hardbounced._email IS NULL
),
email_open AS (
  SELECT
    * EXCEPT(_rownum)
  FROM (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Opened' AS _engagement,
      url AS _description,
      devicetype AS _devicetype,
      CAST(linkid AS STRING),
      --appname,
      CAST(duration AS STRING),
      response,
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.agiloft_hubspot.email_events` activity
    JOIN
      `x-marketing.agiloft_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'OPEN'
      AND filteredevent = FALSE
      AND campaign.name IS NOT NULL )
  WHERE
    _rownum = 1 
),
email_click AS (
  SELECT
    * EXCEPT(_rownum)
  FROM (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Clicked' AS _engagement,
      url AS _description,
      devicetype AS _devicetype,
      CAST(linkid AS STRING),
      --appname,
      CAST(duration AS STRING),
      response,
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.agiloft_hubspot.email_events` activity
    JOIN
      `x-marketing.agiloft_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'CLICK'
      AND filteredevent = FALSE
      --AND activity.recipient NOT LIKE '%2x.marketing%'
      --AND activity.recipient NOT LIKE '%agiloft.com%'
      AND campaign.name IS NOT NULL )
  WHERE
    _rownum = 1
),
softbounced AS (
  SELECT
    * EXCEPT(_rownum)
  FROM (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name  AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Soft Bounced' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.agiloft_hubspot.email_events` activity
    JOIN
      `x-marketing.agiloft_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'BOUNCE'  
  )
  WHERE
    _rownum = 1
),
email_softbounce AS (
  SELECT 
    softbounced.* 
  FROM softbounced
  LEFT JOIN hardbounced ON softbounced._email = hardbounced._email 
  AND softbounced._campaignID = hardbounced._campaignID
  LEFT JOIN delivered ON softbounced._email = delivered._email 
  AND softbounced._campaignID = delivered._campaignID
  WHERE  hardbounced._email IS NULL 
  AND delivered._email IS NULL
),
email_hardBounce AS (
  SELECT 
    softbounced .* 
  FROM softbounced
  JOIN hardbounced ON softbounced._email = hardbounced._email 
  AND softbounced._campaignID = hardbounced._campaignID
  JOIN delivered ON softbounced._email = delivered._email 
  AND softbounced._campaignID = delivered._campaignID
),
unsubsribe AS (
  SELECT * EXCEPT(_rownum) 
  FROM (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID, 
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Unsubscribed' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM `x-marketing.agiloft_hubspot.subscription_changes`, UNNEST(changes) AS status 
    JOIN `x-marketing.agiloft_hubspot.email_events` activity 
    ON status.value.causedbyevent.id = activity.id
    JOIN `x-marketing.agiloft_hubspot.campaigns` campaign 
    ON activity.emailcampaignid = campaign.id
    WHERE 
      activity.type = 'STATUSCHANGE'
    AND status.value.change = 'UNSUBSCRIBED' 
  )
  WHERE _rownum = 1
),
email_download AS (
  SELECT
    * EXCEPT (rownum)
  FROM (
    SELECT
      activity._sdc_sequence,
      CAST(campaign.id AS STRING) AS _campaignID,
      COALESCE(form_title, campaign.name) AS _campaign,
      campaign.subject,
      activity.email AS _email,
      activity.timestamp AS _timestamp,
      'Downloaded' AS _engagement,
      activity.description AS _description,
      activity.devicetype,
      '' AS linkid,
      '' AS duration,
      "" AS response,
      ROW_NUMBER() OVER(PARTITION BY email, campaign.name ORDER BY timestamp DESC) AS rownum
    FROM (
      SELECT
        c._sdc_sequence,
        CAST(NULL AS STRING) AS devicetype,
        REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_source=([^&]+)') AS _utmsource,
        REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_campaign=([^&]+)') AS _utmcampaign,
        REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
        REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_content=([^&]+)') AS _utmcontent,
        form.value.title AS form_title,
        properties.email.value AS email,
        form.value.timestamp AS timestamp,
        'Downloaded' AS engagement,
        form.value.page_url AS description,
        -- campaignguid,
      FROM
        `x-marketing.agiloft_hubspot.contacts` c,
        UNNEST(form_submissions) AS form
      -- JOIN
      --   `x-marketing.agiloft_hubspot.forms` forms
      -- ON
      --   form.value.form_id = forms.guid
      ) activity
    LEFT JOIN
      `x-marketing.agiloft_hubspot.campaigns` campaign
    ON
      activity._utmcontent = CAST(campaign.id AS STRING) 
  )
  WHERE
    rownum = 1 
),
engagements_combined AS (
    SELECT
      *
    FROM
      email_sent
    UNION ALL
    SELECT
      *
    FROM
      email_delivered
    UNION ALL 
    SELECT
      *
    FROM
      email_open
    UNION ALL
    SELECT
      *
    FROM
      email_click
    UNION ALL
    SELECT
      * FROM
      email_softbounce
    UNION ALL 
    SELECT 
      * 
    FROM email_download
    UNION ALL
    SELECT * FROM unsubsribe
    UNION ALL 
    SELECT * 
    FROM email_hardBounce
  )
SELECT
  engagements.*,
  COALESCE(REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)'), "Email") AS _utmsource,
  REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
  REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utmcontent,
  -- airtable_info.* EXCEPT (id),
  prospect_info.* EXCEPT (_email),
  /*airtable_info.subtype, 
    airtable_info.name, 
    CAST(airtable_info.contentid AS STRING), 
    airtable_info.type,
    airtable_info._landingpage, 
    airtable_info._utm_medium, 
    airtable_info._utm_source, 
    CAST(airtable_info._livedate AS TIMESTAMP), 
    airtable_info._code, 
    airtable_info._whatwedo, 
    airtable_info._assettitle, 
    airtable_info._screenshot, 
        airtable_info._trimcode,
    airtable_info._progress, 
 
    airtable_info._url_param, 
    airtable_info._launched,
    airtable_info._2x_campaign*/
FROM 
  engagements_combined AS engagements
LEFT JOIN
  prospect_info
ON
  engagements._email = prospect_info._email
/*JOIN
  airtable_info
ON
  engagements._campaignID = CAST(airtable_info.id AS STRING)*/
;


-- Label Clicks That Are Visits and Set their Page Views
UPDATE `sbi.db_email_engagements_log` origin
SET 
    origin._isPageView = true, 
    origin._totalPageViews = scenario.pageviews,
    origin._averagePageViews = scenario.pageviews / scenario.visitors
FROM (
    SELECT  
        CONCAT(_email, _campaignid, _engagement, email._timestamp) AS _key,
        COUNT(DISTINCT web._visitorid) AS visitors,
        SUM(web._totalsessionviews) AS pageviews
    FROM 
        `x-marketing.sbi.db_email_engagements_log` email 
    JOIN (
        SELECT DISTINCT
            _timestamp,
            _visitorid,
            _utmcampaign,
            _totalsessionviews,
            _utmmedium,
            _utmsource,
        FROM 
          `x-marketing.sbi.db_web_engagements_log`
    ) web
    ON DATE(email._timestamp) = DATE(web._timestamp)
    AND email._utmcampaign = web._utmcampaign
    WHERE 
        email._engagement = 'Clicked'
        AND LOWER(web._utmsource) LIKE '%email%'
    GROUP BY 
        1
) scenario
WHERE CONCAT(_email, _campaignid, _engagement, _timestamp) = scenario._key;

