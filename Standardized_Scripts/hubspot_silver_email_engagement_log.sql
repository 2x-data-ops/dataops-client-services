-- 3X Email Engagements

-- Schedule : every day 19:00 UTC /  3:00 AM UTC+8

TRUNCATE TABLE `x-marketing.3x.db_email_engagements`;
INSERT INTO `x-marketing.3x.db_email_engagements` (
  _sdc_sequence,
  _campaignID,
  _utmcampaign,
  _subject,
  _email,
  _timestamp,
  _engagement,
  _description,
  _device_type,
  _linkid,
  _duration,
  _response,
  _utm_source,  
  _utm_medium, 
  _utm_content,
  _prospectID,
  _name,
  _domain,
  _title,
  _function,
  _seniority,
  _phone,
  _company,
  _revenue,
  _industry,
  _city,
  _state,
  _country,
  _lifecycleStage,
  _campaignSentDate,
  _contentTitle,
  _preview,
  _screenshot, 
  _landingPage,
  _segment_campaign,
  _3x_campaign
)

WITH 
prospect_info AS (
  SELECT DISTINCT 
    CAST(_id AS STRING) AS _id,
    _name,
    _email,
    _domain,
    _jobtitle,
    _function,
    _seniority,
    _phone,
    _company,
    CAST(_revenue AS STRING) AS _revenue,
    _industry,
    -- _employee,
    _city,
    _state,
    _country,
    _lifecycleStage
  
  FROM `3x.db_icp_database_log`
),

airtable_info AS (
  SELECT 
    CAST(campaign.id AS STRING) AS id,
    SAFE_CAST(airtable._livedate AS TIMESTAMP) AS _liveDate,
    name AS _contentTitle,
    airtable._subject,
    airtable._screenshot,
    airtable._landingPage, _segment,
    CASE WHEN name IS NULL THEN 'Not 3X campaign' ELSE '3X campaign' END AS _3x_campaign

  FROM `x-marketing.x3x_hubspot.campaigns` campaign
  LEFT JOIN `x-marketing.x_mysql.db_airtable_3x_email` airtable
    ON CAST(campaign.id AS STRING) = airtable._emailid

  QUALIFY ROW_NUMBER() OVER(PARTITION BY campaign.id ORDER BY campaign.id DESC) = 1
),

dropped AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.emailcampaignid AS STRING) AS _campaignID,
    campaign.name AS _contentTitle,
    campaign.contentid AS _contentID,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    activity.type AS _engagement,
    url AS _description,
    devicetype AS _device_type,
    CAST(linkid AS STRING) _linkid,
    CAST(duration AS STRING) _duration,
    response AS _response,
    '',
    
  FROM `x-marketing.x3x_hubspot.email_events` activity
  JOIN `x-marketing.x3x_hubspot.campaigns` campaign
    ON activity.emailcampaignid = campaign.id
  
  WHERE activity.type = 'DROPPED' 
),

Sent AS (
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
  
  FROM `x-marketing.x3x_hubspot.email_events` activity
  JOIN `x-marketing.x3x_hubspot.campaigns` campaign
    ON activity.emailcampaignid = campaign.id
  
  WHERE activity.type = 'SENT'
  
  QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
)

, email_sent AS (
  SELECT 
    Sent .* 
  
  FROM Sent
  LEFT JOIN dropped  
    ON Sent._email = dropped._email 
    AND Sent._campaignID = dropped._campaignID
  
  WHERE dropped._email IS NULL 
),

bounced AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.emailcampaignid AS STRING) AS _campaignID, 
    campaign.name AS _campaign,
    campaign.subject AS _subject,
    activity.recipient AS _email,
    c.timestamp AS _timestamp,
    'Hard Bounce' AS _engagement,
    url AS _description,
    devicetype AS _device_type,
    CAST(linkid AS STRING) _linkid,
    --appname,
    CAST(duration AS STRING) _duration,
    response AS _response

  FROM `x-marketing.x3x_hubspot.subscription_changes` c, UNNEST(changes) AS status 
  JOIN `x-marketing.x3x_hubspot.email_events` activity 
    ON status.value.causedbyevent.id = activity.id
  JOIN `x-marketing.x3x_hubspot.campaigns` campaign 
    ON  activity.emailcampaignid = campaign.id
  
  WHERE status.value.change = 'BOUNCED'

  QUALIFY ROW_NUMBER() OVER( PARTITION BY c.recipient, activity.emailcampaignid  ORDER BY c.timestamp DESC) = 1
), 

email_bounce AS (
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
    --appname,
    CAST(duration AS STRING) _duration,
    response AS _response,
    
  FROM `x-marketing.x3x_hubspot.email_events` activity
  JOIN `x-marketing.x3x_hubspot.campaigns` campaign
    ON activity.emailcampaignid = campaign.id
  
  WHERE activity.type = 'BOUNCE'  
  
  QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),

delivered AS (
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
    
  FROM `x-marketing.x3x_hubspot.email_events` activity
  JOIN `x-marketing.x3x_hubspot.campaigns` campaign
    ON activity.emailcampaignid = campaign.id
  
  WHERE activity.type = 'DELIVERED'
  
  QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
)

, email_delivered AS (
SELECT 
  delivered .* 

FROM delivered 
LEFT JOIN dropped 
  ON delivered._email = dropped._email 
  AND delivered._campaignID = dropped._campaignID
LEFT JOIN bounced 
  ON delivered._email = bounced._email 
  AND delivered._campaignID = bounced._campaignID
LEFT JOIN email_bounce 
  ON delivered._email = email_bounce._email 
  AND delivered._campaignID = email_bounce._campaignID

WHERE bounced._email IS NULL 
  AND dropped._email IS NULL 
  AND email_bounce._email IS NULL 
),


email_open AS (
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
    CAST(duration AS STRING),
    response,
    
  FROM `x-marketing.x3x_hubspot.email_events` activity
  JOIN `x-marketing.x3x_hubspot.campaigns` campaign
    ON activity.emailcampaignid = campaign.id
  
  WHERE activity.type = 'OPEN'
    AND filteredevent = FALSE

  QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
),

email_click AS (
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
    CAST(linkid AS STRING) AS _linkid ,
    --appname,
    CAST(duration AS STRING) AS  _duration,
    response AS _response,
    
  FROM `x-marketing.x3x_hubspot.email_events` activity
  JOIN `x-marketing.x3x_hubspot.campaigns` campaign
    ON activity.emailcampaignid = campaign.id

  WHERE activity.type = 'CLICK'
    AND filteredevent = FALSE 

  QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  )

  , email_softbounce AS (
  SELECT 
    email_bounce .* 

  FROM email_bounce
  LEFT JOIN bounced 
    ON email_bounce._email = bounced._email 
    AND email_bounce._campaignID = bounced._campaignID
  LEFT JOIN delivered 
    ON email_bounce._email = delivered._email 
    AND email_bounce._campaignID = delivered._campaignID

  WHERE  bounced._email IS NULL 
)
  
  
 , email_hardBounce AS (
SELECT 
  email_bounce .* 

FROM email_bounce
JOIN bounced 
  ON email_bounce._email = bounced._email 
  AND email_bounce._campaignID = bounced._campaignID
LEFT JOIN delivered 
  ON email_bounce._email = delivered._email 
  AND email_bounce._campaignID = delivered._campaignID
),


unsubsribe AS (
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

  FROM `x-marketing.x3x_hubspot.subscription_changes`, UNNEST(changes) AS status 
  JOIN `x-marketing.x3x_hubspot.email_events` activity 
    ON status.value.causedbyevent.id = activity.id
  JOIN `x-marketing.x3x_hubspot.campaigns` campaign 
    ON  activity.emailcampaignid = campaign.id
  WHERE activity.type = 'STATUSCHANGE'
    AND status.value.change = 'UNSUBSCRIBED' 

  QUALIFY ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
)

, email_download_base AS (
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
    campaignguid,

  FROM `x-marketing.x3x_hubspot.contacts` c, UNNEST(form_submissions) AS form
  JOIN `x-marketing.x3x_hubspot.forms` forms
    ON form.value.form_id = forms.guid
)

, email_download AS (
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
    
  FROM email_download_base AS activity
  LEFT JOIN `x-marketing.x3x_hubspot.campaigns` campaign
    ON activity._utmcontent = CAST(campaign.id AS STRING) 

  QUALIFY ROW_NUMBER() OVER(PARTITION BY email, campaign.name ORDER BY timestamp DESC) = 1
)
  
, customer_townhall_reg AS (
  SELECT  CAST(vid AS STRING) AS _id,
    property_email.value AS _email,
    CONCAT(property_firstname.value, ' ', property_lastname.value) AS _name,
    properties.hs_email_domain.value AS _domain,
    property_jobtitle.value AS _jobTitle,
    properties.job_function.value AS _function,
    list_memberships.value.timestamp AS _timestamp,
    activity._sdc_sequence,
    list_memberships.value.static_list_id,
    CASE 
      WHEN list_memberships.value.static_list_id = 1976 THEN "295023697" 
      ELSE NULL 
    END AS _campaignid

  FROM `x-marketing.x3x_hubspot.contacts` activity, 
    Unnest (list_memberships) list_memberships
  LEFT JOIN `x-marketing.x3x_hubspot.contact_lists` list 
    ON list_memberships.value.static_list_id = list.listid

  WHERE list_memberships.value.static_list_id IN (1976)
)
  
, customer_marketing_activity AS (
  SELECT 
    activity._sdc_sequence,
    CAST(activity.emailcampaignid AS STRING) AS _campaignID,
    campaigns.name  AS _campaign,
    campaigns.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    activity.type AS _engagement,
    url AS _description,
    devicetype AS _devicetype,
    CAST(linkid AS STRING) AS linkid,
    CAST(duration AS STRING) AS duration,
    response,
  
  FROM  `x-marketing.x3x_hubspot.email_events` activity
  LEFT JOIN `x-marketing.x3x_hubspot.campaigns` campaigns
    ON activity.emailcampaignid = campaigns.id
  JOIN airtable_info campaign
    ON CAST(activity.emailcampaignid AS STRING) = campaign.id
  
  WHERE activity.type IN ( 'CLICK')
    AND filteredevent = FALSE 
    AND _segment = 'Customer Marketing'
)

, customer_reg AS (
  SELECT
    COALESCE(activity._sdc_sequence, reg._sdc_sequence) AS _sdc_sequence,
    CAST(campaigns.id AS STRING) AS _campaignid ,
    campaigns.name  AS _campaign,
    campaigns.subject AS _subject,
    reg._email,
    reg._timestamp,
    "Register" AS _engagement,
    _description,
    _devicetype,
    linkid,
    duration,
    response,

  FROM customer_townhall_reg AS reg 
  LEFT JOIN customer_marketing_activity AS activity
    ON  reg._email = activity._email
  LEFT JOIN `x-marketing.x3x_hubspot.campaigns` campaigns
    ON reg._campaignid = CAST(campaigns.id AS STRING)

  QUALIFY ROW_NUMBER() OVER(PARTITION BY reg._email  ORDER BY reg._timestamp DESC) = 1
), 


customer_sent_register AS (
  SELECT
    email_click. _sdc_sequence,
    email_click._campaignid ,
    email_click._campaign,
    email_click._subject,
    email_click._email,
    email_click._timestamp,
    "Sent + Register" AS _engagement,
    email_click._description,
    email_click. _devicetype,
    _linkid,
    _duration,
    _response,
  
  FROM email_click
  LEFT JOIN airtable_info campaign  
    ON email_click._campaignID = campaign.id
  JOIN customer_reg  customer_townhall_reg 
    ON email_click._email = customer_townhall_reg._email 
  
  WHERE _segment = 'Customer Marketing'
), 

customer_do_not_reg AS (
  SELECT
    email_delivered. _sdc_sequence,
    email_delivered._campaignid ,
    email_delivered._campaign,
    email_delivered._subject,
    email_delivered._email,
    email_delivered._timestamp,
    "Do Not Register" AS _engagement,
    email_delivered._description,
    _device_type,
    _linkid,
    _duration,
    _response,
  
  FROM email_delivered
  JOIN airtable_info campaign
    ON email_delivered._campaignID = campaign.id
  LEFT JOIN customer_reg customer_townhall_reg 
    ON email_delivered._email = customer_townhall_reg._email 
  
  WHERE customer_townhall_reg._email  IS NULL 
    AND  _segment = 'Customer Marketing'
)

,combine_bounce_base AS (

SELECT * FROM email_bounce 
UNION ALL 
SELECT * FROM bounced
)

,combine_bounce AS (
SELECT
*

FROM combine_bounce_base

QUALIFY ROW_NUMBER() OVER(PARTITION BY _email, _campaignID ORDER BY _timestamp DESC) = 1
)


, false_delivered AS ( 
SELECT 
  delivered .* 

FROM delivered 
LEFT JOIN dropped 
  ON delivered._email = dropped._email 
  AND delivered._campaignID = dropped._campaignID
JOIN combine_bounce
  ON delivered._email = combine_bounce._email 
  AND delivered._campaignID = combine_bounce._campaignID
  
WHERE dropped._email IS NULL 
),

engagements_combined AS (
SELECT *
FROM email_sent
UNION ALL
SELECT *
FROM email_delivered
UNION ALL
SELECT *
FROM email_open
UNION ALL
SELECT *
FROM email_click
UNION ALL
SELECT *
FROM email_softbounce
UNION ALL
SELECT *
FROM email_download
UNION ALL
SELECT *
FROM unsubsribe
UNION ALL
SELECT *
FROM email_hardBounce
UNION ALL
SELECT *
FROM customer_reg
UNION ALL
SELECT *
FROM customer_sent_register
UNION ALL
SELECT *
FROM customer_do_not_reg
UNION ALL
SELECT *
FROM false_delivered
)

SELECT
  engagements.*,
  COALESCE(REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)'), "Email") AS _utmsource,
  REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
  REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utmcontent,
  prospect_info.* EXCEPT (_email),
  airtable_info.* EXCEPT (id),

FROM 
  engagements_combined AS engagements
LEFT JOIN prospect_info
  ON engagements._email = prospect_info._email
LEFT JOIN airtable_info
  ON engagements._campaignID = CAST(airtable_info.id AS STRING)
;

--- Label Bots ---
UPDATE `x-marketing.3x.db_email_engagements` origin  
SET origin._isBot = 'true'
FROM (
    SELECT DISTINCT

        _email,
        _contentTitle

    FROM `x-marketing.3x.db_email_engagements`
    WHERE _description LIKE '%https://3x.wise-portal.com/iclick/iclick.php%'
) bot
WHERE origin._email = bot._email
  AND origin._contentTitle = bot._contentTitle
  AND origin._engagement IN ('Clicked');

-- Label Clicks That Are Visits and Set their Page Views
UPDATE`x-marketing.3x.db_email_engagements` origin
SET 
  origin._isPageView = true, 
  origin._totalPageViews = scenario.pageviews,
  origin._averagePageViews = scenario.pageviews / scenario.visitors
FROM (
    SELECT  
      CONCAT(_email, _campaignid, _engagement, email._timestamp) AS _key,
      COUNT(DISTINCT web._visitorid) AS visitors,
      SUM(web._totalsessionviews) AS pageviews
    FROM `x-marketing.3x.db_email_engagements` AS email 
    JOIN (
        SELECT DISTINCT
            _timestamp,
            _visitorid,
            _utmcampaign,
            _totalsessionviews,
            _utmmedium,
            _utmsource,
        FROM `x-marketing.3x.db_web_engagements_log`) AS web
      ON DATE(email._timestamp) = DATE(web._timestamp)
      AND email._utmcampaign = web._utmcampaign
    WHERE email._engagement = 'Clicked'
      AND LOWER(web._utmsource) LIKE '%email%'
    GROUP BY 1
) scenario

WHERE CONCAT(_email, _campaignid, _engagement, _timestamp) = scenario._key;

------------------------------------------------
-------------- Content Analytics ---------------
------------------------------------------------
TRUNCATE TABLE `x-marketing.3x.email_content_analytics`;
INSERT INTO `x-marketing.3x.email_content_analytics` 
SELECT  
  email.* EXCEPT(_landingpage, _persona),
  content._contentitem,
  content._contenttype,
  content._gatingstrategy,
  content._homeurl,
  content._summary,
  content._status,
  content._buyerstage,
  content._vertical,
  content._persona
FROM `x-marketing.3x.db_email_engagements` email 
JOIN `x-marketing.x_mysql.db_airtable_3x_content_inventory` content 
  ON email._landingpage = content._homeurl