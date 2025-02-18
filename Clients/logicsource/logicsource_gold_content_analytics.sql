
-- CREATE OR REPLACE TABLE `x-marketing.logicsource.content_analytics` AS
TRUNCATE TABLE `x-marketing.logicsource.content_analytics`;
INSERT INTO `x-marketing.logicsource.content_analytics` (
    _sdc_sequence,
    _campaignID,
    _contentTitle,
    _email,
    _timestamp,
    _engagement,
    _description,
    _device_type,
    _linkid,
    _duration,
    _response,
    _id,
    _name,
    _domain,
    _jobtitle,
    _function,
    _jobrole,
    _mqldate,
    _source,
    _latest_source,
    _seniority,
    _phone,
    _company,
    _revenue,
    _industry,
    _city,
    _state,
    _country,
    _lifecycleStage,
    _leadscore,
    _leadstatus,
    _hubspotScore,
    _emailSentDate,
    _contentitem,
    _contenttype,
    _gatingstrategy,
    _homeurl,
    _summary,
    _status,
    _buyerstage,
    _vertical,
    _persona,
    _contentID
)
WITH prospect_info AS (
  SELECT  
    DISTINCT 
    CAST(_id AS STRING) AS _id,
    _email,
    _name,
    _domain,
    _jobtitle,
    _function,
    _jobrole,
    _mqldate,
    _source,
    _latest_source,
    _seniority,
    _phone,
    _company,
    _revenue,
    _industry,
    _city,
    _state,
    _country,
    -- _persona,
    _lifecycleStage,
    _leadscore,
    _leadstatus,
    _hubspotScore
  FROM
    -- `logicsource.db_tam_database`
    `logicsource.db_icp_database_log`
  WHERE
    _email IS NOT NULL
    AND _email NOT LIKE '%2x.marketing%'
    AND _email NOT LIKE '%logicsource.com%'
),
content_info AS (
    SELECT  
        email._pardotid,
        email._code AS _campaignCode,
        email._senddate AS _emailSentDate,
        content._contentitem,
        content._contenttype,
        content._gatingstrategy,
        content._homeurl,
        content._summary,
        content._status,
        content._buyerstage,
        content._vertical,
        content._persona,
        content._id AS _contentID
    FROM 
    -- `x-marketing.logicsource_hubspot.campaigns` campaign
    -- JOIN 
    `x-marketing.logicsource_mysql.db_airtable_email` email 
    -- ON CAST(campaign.id AS STRING) = _pardotid 
    JOIN `x-marketing.logicsource_mysql.db_airtable_content_inventory` content on email._cihomeurl = content._homeurl
),
email_sent AS (
  WITH bounced AS (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID, 
      campaign.name AS _contentTitle,
      campaign.contentid AS _contentID,
      --activity.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Hard Bounce' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      
      CAST(duration AS STRING) _duration,
      response AS _response,
      
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM `x-marketing.logicsource_hubspot.subscription_changes`, 
      UNNEST(changes) AS status 
    JOIN `x-marketing.logicsource_hubspot.email_events` activity 
      ON status.value.causedbyevent.id = activity.id
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign 
      ON  activity.emailcampaignid = campaign.id
    WHERE 
      activity.type = 'BOUNCE' 
    ---AND emailcampaignid = 269760036
    AND status.value.change = 'BOUNCED'  
  ), 
  Sent AS (
  SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        activity._sdc_sequence,
        CAST(activity.emailcampaignid AS STRING) AS _campaignID,
        campaign.name AS _contentTitle,
        campaign.contentid AS _contentID,
        --activity.subject AS _subject,
        activity.recipient AS _email,
        activity.created AS _timestamp,
        'Sent' AS _engagement,
        url AS _description,
        devicetype AS _device_type,
        CAST(linkid AS STRING) _linkid,
        
        CAST(duration AS STRING) _duration,
        response AS _response,
        
        ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM
        `x-marketing.logicsource_hubspot.email_events` activity
      JOIN
        `x-marketing.logicsource_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'SENT'  
        ---AND emailcampaignid = 269760036
        AND activity.recipient NOT IN ('colingilmore2@gmail.com','x@gmail.com') AND activity.recipient NOT LIKE '%2x.marketing' AND activity.recipient NOT LIKE '%logicsource%' AND activity.recipient NOT LIKE '%medifastinc.com' AND activity.recipient NOT LIKE '%@ckr.com%' AND activity.recipient NOT LIKE '%@ircinc.com%' AND activity.recipient NOT LIKE '%finnpartners.com%' AND activity.recipient NOT LIKE '%oceanstatejoblot.com%' AND activity.recipient NOT LIKE '%@osjl.com%'
        AND campaign.name IS NOT NULL  )
      WHERE
      _rownum = 1 
  ) SELECT Sent .* FROM Sent
  LEFT JOIN bounced 
    ON Sent._email = bounced._email 
    AND Sent._campaignID = bounced._campaignID
  WHERE bounced._email IS NULL 
),
email_delivered AS (
    WITH bounced AS ( 
      SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID, 
      campaign.name AS _contentTitle,
      campaign.contentid AS _contentID,
      --activity.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Hard Bounce' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      
      CAST(duration AS STRING) _duration,
      response AS _response,
      
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.logicsource_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.logicsource_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.logicsource_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
      WHERE 
      activity.type = 'BOUNCE'
      AND 
      status.value.change = 'BOUNCED'  
  ), 
  delivered AS (
  SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        activity._sdc_sequence,
        CAST(activity.emailcampaignid AS STRING) AS _campaignID,
        campaign.name AS _contentTitle,
        campaign.contentid AS _contentID,
        --activity.subject AS _subject,
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
        `x-marketing.logicsource_hubspot.email_events` activity
      JOIN
        `x-marketing.logicsource_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'DELIVERED'  
        AND activity.recipient NOT IN ('colingilmore2@gmail.com','x@gmail.com') AND activity.recipient NOT LIKE '%2x.marketing' AND activity.recipient NOT LIKE '%logicsource%' AND activity.recipient NOT LIKE '%medifastinc.com' AND activity.recipient NOT LIKE '%@ckr.com%' AND activity.recipient NOT LIKE '%@ircinc.com%' AND activity.recipient NOT LIKE '%finnpartners.com%' AND activity.recipient NOT LIKE '%oceanstatejoblot.com%' AND activity.recipient NOT LIKE '%@osjl.com%'
        AND campaign.name IS NOT NULL  )
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
      campaign.name AS _contentTitle,
      campaign.contentid AS _contentID,
      --activity.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Opened' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      
      CAST(duration AS STRING) _duration,
      response AS _response,
      
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.logicsource_hubspot.email_events` activity
    JOIN
      `x-marketing.logicsource_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
       activity.type = 'OPEN'
      AND filteredevent = FALSE
      AND activity.recipient NOT IN ('colingilmore2@gmail.com','x@gmail.com') AND activity.recipient NOT LIKE '%2x.marketing' AND activity.recipient NOT LIKE '%logicsource%' AND activity.recipient NOT LIKE '%medifastinc.com' AND activity.recipient NOT LIKE '%@ckr.com%' AND activity.recipient NOT LIKE '%@ircinc.com%' AND activity.recipient NOT LIKE '%finnpartners.com%' AND activity.recipient NOT LIKE '%oceanstatejoblot.com%' AND activity.recipient NOT LIKE '%@osjl.com%'
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
      campaign.name AS _contentTitle,
      campaign.contentid AS _contentID,
      --activity.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Clicked' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      
      CAST(duration AS STRING) _duration,
      response AS _response,
      
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.logicsource_hubspot.email_events` activity
    JOIN
      `x-marketing.logicsource_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'CLICK'
      AND filteredevent = FALSE
      AND activity.recipient NOT IN ('colingilmore2@gmail.com','x@gmail.com') AND activity.recipient NOT LIKE '%2x.marketing' AND activity.recipient NOT LIKE '%logicsource%' AND activity.recipient NOT LIKE '%medifastinc.com' AND activity.recipient NOT LIKE '%@ckr.com%' AND activity.recipient NOT LIKE '%@ircinc.com%' AND activity.recipient NOT LIKE '%finnpartners.com%' AND activity.recipient NOT LIKE '%oceanstatejoblot.com%' AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL  )
  WHERE
    _rownum = 1
),
email_bounce AS (
  SELECT
    * EXCEPT(_rownum)
  FROM (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _contentTitle,
      campaign.contentid AS _contentID,
      --activity.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Bounced' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      
      CAST(duration AS STRING) _duration,
      response AS _response,
      
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.logicsource_hubspot.email_events` activity
    JOIN
      `x-marketing.logicsource_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'BOUNCE' 
      ---AND emailcampaignid = 269760036
    AND activity.recipient NOT IN ('colingilmore2@gmail.com','x@gmail.com') AND activity.recipient NOT LIKE '%2x.marketing' AND activity.recipient NOT LIKE '%logicsource%' AND activity.recipient NOT LIKE '%medifastinc.com' AND activity.recipient NOT LIKE '%@ckr.com%' AND activity.recipient NOT LIKE '%@ircinc.com%' AND activity.recipient NOT LIKE '%finnpartners.com%' AND activity.recipient NOT LIKE '%oceanstatejoblot.com%' AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL  )
  WHERE
    _rownum = 1
),
email_defferred AS (
  SELECT * EXCEPT(_rownum) 
  FROM
  (
    SELECT
    activity._sdc_sequence,
    CAST(activity.emailcampaignid AS STRING) AS _campaignID, 
    campaign.name AS _contentTitle,
    campaign.contentid AS _contentID,
    --activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Deffered' AS _engagement,
    url AS _description,
    devicetype AS _device_type,
    CAST(linkid AS STRING) _linkid,
      
      CAST(duration AS STRING) _duration,
    response AS _response,
    
    ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
    WHERE 
    activity.type = 'DEFERRED' 
     AND activity.recipient NOT IN ('colingilmore2@gmail.com','x@gmail.com') AND activity.recipient NOT LIKE '%2x.marketing' AND activity.recipient NOT LIKE '%logicsource%' AND activity.recipient NOT LIKE '%medifastinc.com' AND activity.recipient NOT LIKE '%@ckr.com%' AND activity.recipient NOT LIKE '%@ircinc.com%' AND activity.recipient NOT LIKE '%finnpartners.com%' AND activity.recipient NOT LIKE '%oceanstatejoblot.com%' AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL 
  )
WHERE _rownum = 1
),
email_dropped AS (
  SELECT * EXCEPT(_rownum) 
  FROM
  (
    SELECT
    activity._sdc_sequence,
    CAST(activity.emailcampaignid AS STRING) AS _campaignID, 
    campaign.name AS _contentTitle,
    campaign.contentid AS _contentID,
    --activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Dropped' AS _engagement,
    url AS _description,
    devicetype AS _device_type,
   CAST(linkid AS STRING) _linkid,
      
      CAST(duration AS STRING) _duration,
    response AS _response,
    
    ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
    WHERE 
    activity.type = 'DROPPED' 
     AND activity.recipient NOT IN ('colingilmore2@gmail.com','x@gmail.com') AND activity.recipient NOT LIKE '%2x.marketing' AND activity.recipient NOT LIKE '%logicsource%' AND activity.recipient NOT LIKE '%medifastinc.com' AND activity.recipient NOT LIKE '%@ckr.com%' AND activity.recipient NOT LIKE '%@ircinc.com%' AND activity.recipient NOT LIKE '%finnpartners.com%' AND activity.recipient NOT LIKE '%oceanstatejoblot.com%' AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL 
  )
  WHERE _rownum = 1
), 
email_suppressed AS (
  SELECT * EXCEPT(_rownum) 
  FROM
  (
    SELECT
    activity._sdc_sequence,
    CAST(activity.emailcampaignid AS STRING) AS _campaignID, 
    campaign.name AS _contentTitle,
    campaign.contentid AS _contentID,
    --activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Suppressed' AS _engagement,
    url AS _description,
    devicetype AS _device_type,
    CAST(linkid AS STRING) _linkid,
      
      CAST(duration AS STRING) _duration,
    response AS _response,
    
    ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
    WHERE 
    activity.type = 'SUPPRESSED'
     AND activity.recipient NOT IN ('colingilmore2@gmail.com','x@gmail.com') AND activity.recipient NOT LIKE '%2x.marketing' AND activity.recipient NOT LIKE '%logicsource%' AND activity.recipient NOT LIKE '%medifastinc.com' AND activity.recipient NOT LIKE '%@ckr.com%' AND activity.recipient NOT LIKE '%@ircinc.com%' AND activity.recipient NOT LIKE '%finnpartners.com%' AND activity.recipient NOT LIKE '%oceanstatejoblot.com%' AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL 
  )
  WHERE _rownum = 1
), 
email_processed AS (
  SELECT * EXCEPT(_rownum) 
  FROM
  (
    SELECT
    activity._sdc_sequence,
    CAST(activity.emailcampaignid AS STRING) AS _campaignID, 
    campaign.name AS _contentTitle,
    campaign.contentid AS _contentID,
    --activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Processed' AS _engagement,
    url AS _description,
    devicetype AS _device_type,
    CAST(linkid AS STRING) _linkid,
      
      CAST(duration AS STRING) _duration,
    response AS _response,
    
    ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
    WHERE 
    activity.type = 'PROCESSED'
     AND activity.recipient NOT IN ('colingilmore2@gmail.com','x@gmail.com') AND activity.recipient NOT LIKE '%2x.marketing' AND activity.recipient NOT LIKE '%logicsource%' AND activity.recipient NOT LIKE '%medifastinc.com' AND activity.recipient NOT LIKE '%@ckr.com%' AND activity.recipient NOT LIKE '%@ircinc.com%' AND activity.recipient NOT LIKE '%finnpartners.com%' AND activity.recipient NOT LIKE '%oceanstatejoblot.com%' AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL 
  )
  WHERE _rownum = 1
), 
email_forward AS (
  SELECT * EXCEPT(_rownum) 
  FROM
  (
    SELECT
    activity._sdc_sequence,
    CAST(activity.emailcampaignid AS STRING) AS _campaignID, 
    campaign.name AS _contentTitle,
    campaign.contentid AS _contentID,
    --activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Forward' AS _engagement,
    url AS _description,
    devicetype AS _device_type,
    CAST(linkid AS STRING) _linkid,
      
      CAST(duration AS STRING) _duration,
    response AS _response,
    
    ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
    WHERE 
    activity.type = 'FORWARD'
    AND activity.recipient NOT IN ('colingilmore2@gmail.com','x@gmail.com') AND activity.recipient NOT LIKE '%2x.marketing' AND activity.recipient NOT LIKE '%logicsource%' AND activity.recipient NOT LIKE '%medifastinc.com' AND activity.recipient NOT LIKE '%@ckr.com%' AND activity.recipient NOT LIKE '%@ircinc.com%' AND activity.recipient NOT LIKE '%finnpartners.com%' AND activity.recipient NOT LIKE '%oceanstatejoblot.com%' AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL 
  )
  WHERE _rownum = 1
), 
email_spam AS (
  SELECT * EXCEPT(_rownum) 
  FROM
  (
    SELECT
    activity._sdc_sequence,
    CAST(activity.emailcampaignid AS STRING) AS _campaignID, 
    campaign.name AS _contentTitle,
    campaign.contentid AS _contentID,
    --activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Spam' AS _engagement,
    url AS _description,
    devicetype AS _device_type,
    CAST(linkid AS STRING) _linkid,
      
    CAST(duration AS STRING) _duration,
    response AS _response,
    
    ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
    WHERE 
    activity.type = 'SPAMREPORT'
    AND activity.recipient NOT IN ('colingilmore2@gmail.com','x@gmail.com') AND activity.recipient NOT LIKE '%2x.marketing' AND activity.recipient NOT LIKE '%logicsource%' AND activity.recipient NOT LIKE '%medifastinc.com' AND activity.recipient NOT LIKE '%@ckr.com%' AND activity.recipient NOT LIKE '%@ircinc.com%' AND activity.recipient NOT LIKE '%finnpartners.com%' AND activity.recipient NOT LIKE '%oceanstatejoblot.com%' AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL 
  )
  WHERE _rownum = 1
), 
email_print AS (
  SELECT * EXCEPT(_rownum) 
  FROM
  (
    SELECT
    activity._sdc_sequence,
    CAST(activity.emailcampaignid AS STRING) AS _campaignID, 
    campaign.name AS _contentTitle,
    campaign.contentid AS _contentID,
    --activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Print' AS _engagement,
    url AS _description,
    devicetype AS _device_type,
    CAST(linkid AS STRING) _linkid,
    
    CAST(duration AS STRING) _duration,
    response AS _response,
    
    ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
    WHERE 
    activity.type = 'PRINT'
     AND activity.recipient NOT IN ('colingilmore2@gmail.com','x@gmail.com') AND activity.recipient NOT LIKE '%2x.marketing' AND activity.recipient NOT LIKE '%logicsource%' AND activity.recipient NOT LIKE '%medifastinc.com' AND activity.recipient NOT LIKE '%@ckr.com%' AND activity.recipient NOT LIKE '%@ircinc.com%' AND activity.recipient NOT LIKE '%finnpartners.com%' AND activity.recipient NOT LIKE '%oceanstatejoblot.com%' AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL 
  )
  WHERE _rownum = 1
), 
email_unsubcribed AS (
  SELECT * EXCEPT(_rownum) 
    FROM
    (
      SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID, 
      campaign.name AS _contentTitle,
      campaign.contentid AS _contentID,
      --activity.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Unsubscribed' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      
      CAST(duration AS STRING) _duration,
      response AS _response,
      
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.logicsource_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.logicsource_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.logicsource_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
      WHERE 
      activity.type = 'STATUSCHANGE'
      AND status.value.change = 'UNSUBSCRIBED' 
       AND activity.recipient NOT IN ('colingilmore2@gmail.com','x@gmail.com') AND activity.recipient NOT LIKE '%2x.marketing' AND activity.recipient NOT LIKE '%logicsource%' AND activity.recipient NOT LIKE '%medifastinc.com' AND activity.recipient NOT LIKE '%@ckr.com%' AND activity.recipient NOT LIKE '%@ircinc.com%' AND activity.recipient NOT LIKE '%finnpartners.com%' AND activity.recipient NOT LIKE '%oceanstatejoblot.com%' AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL 
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
      COALESCE(form_title, campaign.name) AS _contentTitle,
      campaign.contentid AS _contentID,
      --campaign.subject,
      activity.email AS _email,
      activity.timestamp AS _timestamp,
      'Downloaded' AS _engagement,
      activity.description AS _description,
      activity.devicetype,
      '' AS linkid,
      '' AS duration,
      "" AS _response,
      _utm_source,
      ROW_NUMBER() OVER(PARTITION BY email, description
 ORDER BY timestamp DESC) AS rownum
    FROM (
      SELECT
        c._sdc_sequence,
        CAST(NULL AS STRING) AS devicetype,
        SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_hsmi=') + 6), '&')[ORDINAL(1)] AS _campaignID,
        #utm_content
        REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%20', ' '), '%3A',':') AS _contentTitle,
        SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_source=') + 8), '&')[ORDINAL(1)] AS _utm_source,
        form.value.title AS form_title,
        properties.email.value AS email,
        form.value.timestamp AS timestamp,
        'Downloaded' AS engagement,
        form.value.page_url AS description,
        campaignguid,
        
      FROM
        `x-marketing.logicsource_hubspot.contacts` c,
        UNNEST(form_submissions) AS form
      JOIN
        `x-marketing.logicsource_hubspot.forms` forms
      ON
        form.value.form_id = forms.guid
        ) activity
    JOIN
      `x-marketing.logicsource_hubspot.campaigns` campaign
    ON
      activity._campaignID = CAST(campaign.id AS STRING) )
  WHERE
    rownum = 1 
    --AND _email NOT LIKE '%2x.marketing%'
     -- AND _email NOT LIKE '%logicsource%'
), 
mql AS (
   
 SELECT
    c._sdc_sequence,
    _campaignID,
    #utm_content
        _contentTitle,
        NULL AS _contentID,
        --" " AS subject,
        list.email,
    _created_date,
    'MQL' AS engagement,
    description,
    devicetype,
        '' AS linkid,
    '' AS duration,
    "" AS _response,
    _utm_source,
 FROM (
 SELECT
        vid,
        properties.email.value AS email,property_recent_conversion_event_name.value,property_createdate.value AS _created_date
      FROM `x-marketing.logicsource_hubspot.contacts`c
--Unnest (list_memberships) list_memberships
--LEFT JOIN `x-marketing.logicsource_hubspot.contact_lists` list ON list_memberships.value.static_list_id = list.listid
WHERE properties.hs_lifecyclestage_marketingqualifiedlead_date.value >= '2023-04-01' AND properties.email.value NOT LIKE '%2x.marketing%' 
 AND vid <> 943501 
     AND properties.email.value NOT LIKE '%logicsource%'
     AND properties.email.value NOT LIKE '%@test.com%'
     AND properties.email.value NOT LIKE '%x@gmail.com%'
 ) list 
 LEFT JOIN ( SELECT
        c._sdc_sequence,vid,
        CAST(NULL AS STRING) AS devicetype,
        SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url,  '_hsmi=') + 9), '&')[ORDINAL(1)] AS _campaignID,
        #utm_content
        properties.utm_campaign.value AS _contentTitle,
        --REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%20', ' '), '%3A',':') AS _contentTitle,
        SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_source=') + 8), '&')[ORDINAL(1)] AS _utm_source,
        form.value.title AS form_title,
        properties.email.value AS email,
        form.value.timestamp AS timestamp,
        'MQL' AS engagement,
        form.value.page_url AS description,
        campaignguid,
      FROM
        `x-marketing.logicsource_hubspot.contacts` c,
        UNNEST(form_submissions) AS form
      JOIN
        `x-marketing.logicsource_hubspot.forms` forms
      ON
        form.value.form_id = forms.guid) c ON list.vid = c.vid
)
SELECT
  engagements.* EXCEPT (_contentid),
  prospect_info.* EXCEPT (_email),
  content_info.* EXCEPT(_pardotid,_campaignCode)
FROM (
  SELECT * FROM email_sent
  UNION ALL
  SELECT * FROM email_delivered
  UNION ALL
  SELECT * FROM email_open
  UNION ALL
  SELECT * FROM email_click
  UNION ALL
  SELECT * FROM email_bounce
  UNION ALL
  SELECT * FROM email_unsubcribed
  UNION ALL
  SELECT * FROM email_defferred
  UNION ALL 
  SELECT * FROM email_dropped
  UNION ALL 
  SELECT * FROM email_suppressed 
  UNION ALL 
  SELECT * FROM email_processed
  UNION ALL 
  SELECT * FROM email_forward
  UNION ALL 
  SELECT * FROM email_spam
  UNION ALL 
  SELECT * FROM email_print
) AS engagements
LEFT JOIN
  prospect_info
ON
  engagements._email = prospect_info._email
JOIN
  content_info
ON
  engagements._campaignID = content_info._pardotid;
/*UNION ALL
SELECT
  engagements.* EXCEPT(_contentid),
  prospect_info.* EXCEPT (_email),
  content_info.* EXCEPT(_pardotid,_campaignCode)
FROM (
  SELECT * FROM email_download 
  UNION ALL 
  SELECT * FROM mql
  ) AS engagements
LEFT JOIN
  prospect_info
ON
  engagements._email = prospect_info._email
LEFT JOIN
  content_info
ON
  engagements._campaignID  = content_info._pardotid; */