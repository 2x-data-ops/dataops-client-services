-- 3X Email Engagements

-- Schedule : every day 19:00 UTC /  3:00 AM UTC+8

-- CREATE OR REPLACE TABLE `x-marketing.3x.db_email_engagements_log` AS

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
  SELECT  
  DISTINCT 
    _id,
    _email,
    _name,
    _domain,
    _jobtitle,
    _function,
    _seniority,
    _phone,
    _company,
    _revenue,
    _industry,
    _employee,
    _city,
    _state,
    _lifecycleStage
  FROM
    `3x.db_icp_database_log`
),
airtable_info AS (
    SELECT 
        * EXCEPT(_rownum), CASE WHEN _contentTitle IS NULL THEN 'Not 3X campaign' ELSE '3X campaign' END AS _3x_campaign
    FROM (

        SELECT 

            CAST(campaign.id AS STRING) AS id,
            SAFE_CAST(airtable._livedate AS TIMESTAMP) AS _liveDate,
            airtable._code  AS _contentTitle,
            airtable._subject,
            airtable._screenshot,
            airtable._landingPage, _segment,

            ROW_NUMBER() OVER(
                PARTITION BY campaign.id
                ORDER BY campaign.id DESC
            ) 
            AS _rownum
        
        FROM 
            `x-marketing.x3x_hubspot.campaigns` campaign
        LEFT JOIN
             `x-marketing.x_mysql.db_airtable_3x_email` airtable
        ON 
             campaign.name = airtable._code 

    ) 
    WHERE _rownum = 1
),
email_sent AS (
    WITH bounced AS (
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
        --appname,
        CAST(duration AS STRING) _duration,
        response AS _response,
        ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
        FROM `x-marketing.x3x_hubspot.subscription_changes`, UNNEST(changes) AS status 
        JOIN `x-marketing.x3x_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
        JOIN `x-marketing.x3x_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
        WHERE 
        activity.type = 'BOUNCE' 
        AND 
        status.value.change = 'BOUNCED'  
        --AND activity.recipient NOT LIKE '%2x.marketing%'
       
        ), Sent AS (
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
      `x-marketing.x3x_hubspot.email_events` activity
    JOIN
      `x-marketing.x3x_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'SENT'  
      --AND activity.recipient NOT LIKE '%2x.marketing%'
     
     )
  WHERE
    _rownum = 1 
) SELECT Sent .* FROM Sent
LEFT JOIN bounced ON Sent._email = bounced._email and Sent._campaignID = bounced._campaignID
WHERE bounced._email IS NULL 
--AND Sent._campaignID = "279480425"
  ),
email_delivered AS (

  WITH bounced AS (
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
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.x3x_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.x3x_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.x3x_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
      WHERE 
      activity.type = 'BOUNCE'
      AND 
      status.value.change = 'BOUNCED'  
      --AND activity.recipient NOT LIKE '%2x.marketing%'
    ), delivered AS (
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
        `x-marketing.x3x_hubspot.email_events` activity
        JOIN
        `x-marketing.x3x_hubspot.campaigns` campaign
        ON
        activity.emailcampaignid = campaign.id
        WHERE
        activity.type = 'DELIVERED'  
        --AND activity.recipient NOT LIKE '%2x.marketing%'
    
        --AND campaign.name IS NOT NULL 
         )
    WHERE
        _rownum = 1
    ) SELECT delivered .* FROM delivered 
    LEFT JOIN bounced ON delivered._email = bounced._email and delivered._campaignID = bounced._campaignID
    WHERE bounced._email IS NULL

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
        `x-marketing.x3x_hubspot.email_events` activity
      JOIN
        `x-marketing.x3x_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'OPEN'
        AND filteredevent = FALSE
        --AND campaign.name IS NOT NULL 
        --AND activity.recipient NOT LIKE '%2x.marketing%'
        )
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
        CAST(linkid AS STRING) AS _linkid ,
        --appname,
        CAST(duration AS STRING) AS  _duration,
        response AS _response,
        ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM
        `x-marketing.x3x_hubspot.email_events` activity
      JOIN
        `x-marketing.x3x_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'CLICK'
        AND filteredevent = FALSE 
       --AND activity.recipient NOT LIKE '%2x.marketing%'
        --AND campaign.name IS NOT NULL
        )
    WHERE
      _rownum = 1
  ),
  email_softbounce AS (

        WITH bounced AS (
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
        --appname,
        CAST(duration AS STRING) _duration,
        response AS _response,
        ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
        FROM `x-marketing.x3x_hubspot.subscription_changes`, UNNEST(changes) AS status 
        JOIN `x-marketing.x3x_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
        JOIN `x-marketing.x3x_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
        WHERE 
        activity.type = 'BOUNCE'
        AND 
        status.value.change = 'BOUNCED'  
        --AND activity.recipient NOT LIKE '%2x.marketing%'
  ), email_bounce AS (
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
        --appname,
        CAST(duration AS STRING) _duration,
        response AS _response,
        ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM
        `x-marketing.x3x_hubspot.email_events` activity
      JOIN
        `x-marketing.x3x_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'BOUNCE'  
    
      --AND activity.recipient NOT LIKE '%2x.marketing%'
        )
    WHERE
      _rownum = 1
  ),delivered AS (
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
        `x-marketing.x3x_hubspot.email_events` activity
      JOIN
        `x-marketing.x3x_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'DELIVERED'  
    
       -- AND campaign.name IS NOT NULL  
       -- AND activity.recipient NOT LIKE '%2x.marketing%'
        )
    WHERE
      _rownum = 1
  ) SELECT email_bounce .* FROM email_bounce
  LEFT JOIN bounced ON email_bounce._email = bounced._email and email_bounce._campaignID = bounced._campaignID
  LEFT JOIN delivered ON email_bounce._email = delivered._email and email_bounce._campaignID = delivered._campaignID
  WHERE  bounced._email IS NULL AND delivered._email IS NULL
  ),email_hardBounce AS (
     WITH bounced AS (
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
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.x3x_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.x3x_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.x3x_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
      WHERE 
      activity.type = 'BOUNCE'
      AND 
      status.value.change = 'BOUNCED'  
     -- AND activity.recipient NOT LIKE '%2x.marketing%'
      ), email_bounce AS (
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
      'Hard Bounced' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.x3x_hubspot.email_events` activity
    JOIN
      `x-marketing.x3x_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'BOUNCE'  
   
     -- AND campaign.name IS NOT NULL  
      --AND activity.recipient NOT LIKE '%2x.marketing%'
      )
  WHERE
    _rownum = 1
    ),delivered AS (
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
        CAST(duration AS STRING) _duration,
        response AS _response,
        ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
        FROM
        `x-marketing.x3x_hubspot.email_events` activity
        JOIN
        `x-marketing.x3x_hubspot.campaigns` campaign
        ON
        activity.emailcampaignid = campaign.id
        WHERE
        activity.type = 'DELIVERED'  
        --AND campaign.name IS NOT NULL  
        --AND activity.recipient NOT LIKE '%2x.marketing%'
        )
        WHERE
        _rownum = 1
        ) SELECT email_bounce .* FROM email_bounce
        JOIN bounced ON email_bounce._email = bounced._email and email_bounce._campaignID = bounced._campaignID
        JOIN delivered ON email_bounce._email = delivered._email and email_bounce._campaignID = delivered._campaignID

),
unsubsribe AS (
     SELECT * EXCEPT(_rownum) 
    FROM
    (
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
      FROM `x-marketing.x3x_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.x3x_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.x3x_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
      WHERE 
      activity.type = 'STATUSCHANGE'
      AND status.value.change = 'UNSUBSCRIBED' 
      --AND activity.recipient NOT LIKE '%2x.marketing%'
    
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
          campaignguid,
        FROM
          `x-marketing.x3x_hubspot.contacts` c,
          UNNEST(form_submissions) AS form
        JOIN
          `x-marketing.x3x_hubspot.forms` forms
        ON
          form.value.form_id = forms.guid
          ) activity
      LEFT JOIN
        `x-marketing.x3x_hubspot.campaigns` campaign
      ON
        activity._utmcontent = CAST(campaign.id AS STRING) 
    )
    WHERE
      rownum = 1 
  ),customer_reg AS (
    WITH  
    customer_townhall_reg AS (
      SELECT  CAST(vid AS STRING) AS _id,
      property_email.value AS _email,
      CONCAT(property_firstname.value, ' ', property_lastname.value) AS _name,
      properties.hs_email_domain.value AS _domain,
      property_jobtitle.value AS _jobTitle,
      properties.job_function.value AS _function,
      list_memberships.value.timestamp AS _timestamp,
      activity._sdc_sequence,
    list_memberships.value.static_list_id,
    CASE WHEN list_memberships.value.static_list_id = 1976 THEN "295023697" 
    ELSE NULL 
    END AS _campaignid
    FROM `x-marketing.x3x_hubspot.contacts` activity, 
    Unnest (list_memberships) list_memberships
    LEFT JOIN `x-marketing.x3x_hubspot.contact_lists` list ON list_memberships.value.static_list_id = list.
    listid
    WHERE /*properties.email.value = '1chimneymanplus@gmail.com'*/ list_memberships.value.static_list_id IN
    (1976)
    ) 
    SELECT
     *
    EXCEPT(_rownum)
    FROM (
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
           --appname,
           --CAST(duration AS STRING),
           --response,
           ROW_NUMBER() OVER(PARTITION BY reg._email  ORDER BY reg._timestamp DESC) AS _rownum
           FROM
           customer_townhall_reg  reg 
           LEFT JOIN (
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
            --appname,
            CAST(duration AS STRING) AS duration,
            response,
            FROM  `x-marketing.x3x_hubspot.email_events` activity
            LEFT JOIN
            `x-marketing.x3x_hubspot.campaigns` campaigns
            ON activity.emailcampaignid = campaigns.id
            JOIN airtable_info campaign  ON 
            CAST(activity.emailcampaignid AS STRING) = campaign.id
            WHERE
            activity.type IN ( 'CLICK')
            AND filteredevent = FALSE 
            --AND activity.recipient NOT LIKE '%2x.marketing%'
            --AND campaigns.name IS NOT NULL 
            AND _segment = 'Customer Marketing')  activity
            ON  reg._email = activity._email
            LEFT JOIN
            `x-marketing.x3x_hubspot.campaigns` campaigns
            ON reg._campaignid = CAST(campaigns.id AS STRING)
            )
    WHERE
     _rownum = 1
), customer_sent_register AS (
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
    LEFT JOIN airtable_info campaign  ON 
           email_click._campaignID
 = campaign.id
    JOIN customer_reg  customer_townhall_reg ON email_click._email = customer_townhall_reg._email 
    WHERE 
    --customer_townhall_reg._email  IS NULL AND 
     _segment = 'Customer Marketing'

), customer_do_not_reg AS (
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
    JOIN airtable_info campaign  ON 
            email_delivered._campaignID
 = campaign.id
    LEFT JOIN customer_reg customer_townhall_reg ON email_delivered._email = customer_townhall_reg._email 
    WHERE customer_townhall_reg._email  IS NULL AND  _segment = 'Customer Marketing'
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
    FROM 
      email_download
    UNION ALL
    SELECT * 
    FROM 
    unsubsribe
    UNION ALL 
    SELECT * 
    FROM 
    email_hardBounce
     UNION ALL 
    SELECT * 
    FROM 
    customer_reg
    UNION ALL 
    SELECT * 
    FROM 
    customer_sent_register
    UNION ALL 
    SELECT * 
    FROM 
   customer_do_not_reg
  )
SELECT
  engagements.*,
  COALESCE(REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)'), "Email") AS _utmsource,
  REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
  REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utmcontent,
  prospect_info.* EXCEPT (_email),
  airtable_info.* EXCEPT (id),
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
LEFT JOIN
  airtable_info
ON
  engagements._campaignID = CAST(airtable_info.id AS STRING)
;

--- Label Bots ---
UPDATE `x-marketing.3x.db_email_engagements` origin  
SET origin._isBot = 'true'
FROM (
    SELECT DISTINCT

        _email,
        _contentTitle

    FROM 
         `x-marketing.3x.db_email_engagements`
    WHERE 
        _description LIKE '%https://3x.wise-portal.com/iclick/iclick.php%'
) bot
WHERE 
    origin._email = bot._email
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
    FROM 
        `x-marketing.3x.db_email_engagements` email 
    JOIN (
        SELECT DISTINCT
            _timestamp,
            _visitorid,
            _utmcampaign,
            _totalsessionviews,
            _utmmedium,
            _utmsource,
        FROM 
           `x-marketing.3x.db_web_engagements_log`
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


------------------------------------------------
-------------- Content Analytics ---------------
------------------------------------------------

-- CREATE OR REPLACE TABLE `x-marketing.3x.email_content_analytics` AS
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
FROM 
  `x-marketing.3x.db_email_engagements` email 
JOIN 
  `x-marketing.x_mysql.db_airtable_3x_content_inventory` content 
ON email._landingpage = content._homeurl
