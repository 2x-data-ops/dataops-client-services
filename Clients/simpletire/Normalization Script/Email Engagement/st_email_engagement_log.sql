CREATE OR REPLACE TABLE `x-marketing.simpletire.db_email_engagements_log` AS
-- TRUNCATE TABLE `x-marketing.simpletire.db_email_engagements_log`;
/*INSERT INTO `x-marketing.simpletire.db_email_engagements_log` (
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
  -------------airtable 
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
 ------------prospect----
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
  _persona,
  _lifecycleStage
)*/
WITH prospect_info AS (
    SELECT
        _id,
        _email,
        _name,
        _domain,
        _jobTitle,
        _function,
        _seniority,
        _phone,
        _company,
        _revenue,
        _industry,
        _city,
        _state,
        _country,
        _persona,
        _lifecycleStage,
    FROM
    `x-marketing.simpletire.db_icp_database_log`
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
      FROM `x-marketing.simpletire_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.simpletire_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.simpletire_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
            `x-marketing.simpletire_hubspot.email_events` activity
            JOIN
            `x-marketing.simpletire_hubspot.campaigns` campaign
            ON
            activity.emailcampaignid = campaign.id
            WHERE
            activity.type = 'SENT'  
        )
        WHERE _rownum = 1 
    ) 
    SELECT Sent .* FROM Sent
    LEFT JOIN bounced ON Sent._email = bounced._email and Sent._campaignID = bounced._campaignID
    WHERE bounced._email IS NULL 
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
        FROM `x-marketing.simpletire_hubspot.subscription_changes`, UNNEST(changes) AS status 
        JOIN `x-marketing.simpletire_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
        JOIN `x-marketing.simpletire_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
            `x-marketing.simpletire_hubspot.email_events` activity
            JOIN
            `x-marketing.simpletire_hubspot.campaigns` campaign
            ON
            activity.emailcampaignid = campaign.id
            WHERE
            activity.type = 'DELIVERED'  
            AND campaign.name IS NOT NULL  
        )
        WHERE
            _rownum = 1
    ) 
    SELECT delivered .* FROM delivered 
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
            `x-marketing.simpletire_hubspot.email_events` activity
        JOIN
            `x-marketing.simpletire_hubspot.campaigns` campaign
        ON
            activity.emailcampaignid = campaign.id
        WHERE
            activity.type = 'OPEN'
            AND filteredevent = FALSE
            AND campaign.name IS NOT NULL 
    )
    WHERE _rownum = 1 
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
            `x-marketing.simpletire_hubspot.email_events` activity
        JOIN
            `x-marketing.simpletire_hubspot.campaigns` campaign
        ON
            activity.emailcampaignid = campaign.id
        WHERE
            activity.type = 'CLICK'
            AND filteredevent = FALSE
            --AND activity.recipient NOT LIKE '%2x.marketing%'
            --AND activity.recipient NOT LIKE '%simpletire.com%'
            AND campaign.name IS NOT NULL 
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
        FROM `x-marketing.simpletire_hubspot.subscription_changes`, UNNEST(changes) AS status 
        JOIN `x-marketing.simpletire_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
        JOIN `x-marketing.simpletire_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
        WHERE 
        activity.type = 'BOUNCE'
        AND 
        status.value.change = 'BOUNCED'  
    ), 
    email_bounce AS (
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
                `x-marketing.simpletire_hubspot.email_events` activity
            JOIN
                `x-marketing.simpletire_hubspot.campaigns` campaign
            ON
                activity.emailcampaignid = campaign.id
            WHERE
                activity.type = 'BOUNCE'
        )
        WHERE
            _rownum = 1
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
                `x-marketing.simpletire_hubspot.email_events` activity
            JOIN
                `x-marketing.simpletire_hubspot.campaigns` campaign
            ON
                activity.emailcampaignid = campaign.id
            WHERE
                activity.type = 'DELIVERED'  
            AND campaign.name IS NOT NULL  
        )
        WHERE
            _rownum = 1
    ) 
    SELECT email_bounce .* FROM email_bounce
    LEFT JOIN bounced ON email_bounce._email = bounced._email and email_bounce._campaignID = bounced._campaignID
    LEFT JOIN delivered ON email_bounce._email = delivered._email and email_bounce._campaignID = delivered._campaignID
    WHERE  bounced._email IS NULL AND delivered._email IS NULL
),
email_hardBounce AS (
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
        FROM `x-marketing.simpletire_hubspot.subscription_changes`, UNNEST(changes) AS status 
        JOIN `x-marketing.simpletire_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
        JOIN `x-marketing.simpletire_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
        WHERE 
        activity.type = 'BOUNCE'
        AND 
        status.value.change = 'BOUNCED'     
    ), 
    email_bounce AS (
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
        `x-marketing.simpletire_hubspot.email_events` activity
        JOIN
        `x-marketing.simpletire_hubspot.campaigns` campaign
        ON
        activity.emailcampaignid = campaign.id
        WHERE
        activity.type = 'BOUNCE'
        AND campaign.name IS NOT NULL  
    )
    WHERE
        _rownum = 1
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
        `x-marketing.simpletire_hubspot.email_events` activity
        JOIN
        `x-marketing.simpletire_hubspot.campaigns` campaign
        ON
        activity.emailcampaignid = campaign.id
        WHERE
        activity.type = 'DELIVERED'
        AND campaign.name IS NOT NULL  
    )
    WHERE
        _rownum = 1
    ) 
    SELECT email_bounce .* FROM email_bounce
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
        FROM `x-marketing.simpletire_hubspot.subscription_changes`, UNNEST(changes) AS status 
        JOIN `x-marketing.simpletire_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
        JOIN `x-marketing.simpletire_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
                campaignguid,
            FROM
            `x-marketing.simpletire_hubspot.contacts` c,
            UNNEST(form_submissions) AS form
            JOIN
            `x-marketing.simpletire_hubspot.forms` forms
            ON
            form.value.form_id = forms.guid
        ) activity
        LEFT JOIN
            `x-marketing.simpletire_hubspot.campaigns` campaign
        ON
            activity._utmcontent = CAST(campaign.id AS STRING) 
    )
    WHERE
      rownum = 1 
),
email_spam AS (
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
            'Spam Reports' AS _engagement,
            url AS _description,
            devicetype AS _devicetype,
            CAST(linkid AS STRING),
            --appname,
            CAST(duration AS STRING),
            response,
            ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
        FROM
            `x-marketing.simpletire_hubspot.email_events` activity
        JOIN
            `x-marketing.simpletire_hubspot.campaigns` campaign
        ON
            activity.emailcampaignid = campaign.id
        WHERE
            activity.type = 'SPAMREPORT'
    )
    WHERE
      _rownum = 1 
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
    UNION ALL 
    SELECT * 
    FROM email_spam
)
SELECT
  engagements.*,
  COALESCE(REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)'), "Email") AS _utmsource,
  REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
  REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utmcontent,
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
;