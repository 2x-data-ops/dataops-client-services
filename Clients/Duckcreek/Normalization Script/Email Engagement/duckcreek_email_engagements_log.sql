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
CREATE OR REPLACE TABLE `x-marketing.duckcreek.db_email_engagements_log` AS
WITH prospect_info AS (
    SELECT 
        _id AS _prospectid,
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
        _city,
        _state,
        _country,
        _persona,
        _lifecycleStage
    FROM 
        `x-marketing.duckcreek.db_icp_database_log`
),
airtable_info AS (
    SELECT
        id,
        subtype,
        subject,
        contentid,
        type,
        name,
        CASE
            WHEN id IN (247413984, 247366742, 247239377, 245978650, 245935666, 245977325, 245936306, 245369808, 245369806, 245369807, 244174255) 
            THEN '2X'
        END AS _2x_campaign
    FROM
        `x-marketing.duckcreek_hubspot.campaigns` campaign
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
        FROM `x-marketing.duckcreek_hubspot.subscription_changes`, UNNEST(changes) AS status 
        JOIN `x-marketing.duckcreek_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
        JOIN `x-marketing.duckcreek_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
            `x-marketing.duckcreek_hubspot.email_events` activity
            JOIN
            `x-marketing.duckcreek_hubspot.campaigns` campaign
            ON
            activity.emailcampaignid = campaign.id
            WHERE
            activity.type = 'SENT'  
            
            )
        WHERE
            _rownum = 1 
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
      FROM `x-marketing.duckcreek_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.duckcreek_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.duckcreek_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
        `x-marketing.duckcreek_hubspot.email_events` activity
        JOIN
        `x-marketing.duckcreek_hubspot.campaigns` campaign
        ON
        activity.emailcampaignid = campaign.id
        WHERE
        activity.type = 'DELIVERED'  
    
        AND campaign.name IS NOT NULL  )
    WHERE
        _rownum = 1
    ) 
    SELECT delivered .* FROM delivered 
    LEFT JOIN bounced ON delivered._email = bounced._email and delivered._campaignID = bounced._campaignID
    WHERE bounced._email IS NULL
    --and delivered._campaignID = "279480425"
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
        `x-marketing.duckcreek_hubspot.email_events` activity
      JOIN
        `x-marketing.duckcreek_hubspot.campaigns` campaign
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
        `x-marketing.duckcreek_hubspot.email_events` activity
      JOIN
        `x-marketing.duckcreek_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'CLICK'
        AND filteredevent = FALSE
        AND campaign.name IS NOT NULL )
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
        FROM `x-marketing.duckcreek_hubspot.subscription_changes`, UNNEST(changes) AS status 
        JOIN `x-marketing.duckcreek_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
        JOIN `x-marketing.duckcreek_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
            `x-marketing.duckcreek_hubspot.email_events` activity
        JOIN
            `x-marketing.duckcreek_hubspot.campaigns` campaign
        ON
            activity.emailcampaignid = campaign.id
        WHERE
            activity.type = 'BOUNCE'  
        
        -- AND campaign.name IS NOT NULL AND campaign.id = 279480425  
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
            `x-marketing.duckcreek_hubspot.email_events` activity
        JOIN
            `x-marketing.duckcreek_hubspot.campaigns` campaign
        ON
            activity.emailcampaignid = campaign.id
        WHERE
            activity.type = 'DELIVERED'  
        
            AND campaign.name IS NOT NULL  )
        WHERE
        _rownum = 1
    )
    SELECT 
        email_bounce .* 
    FROM email_bounce
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
        FROM `x-marketing.duckcreek_hubspot.subscription_changes`, UNNEST(changes) AS status 
        JOIN `x-marketing.duckcreek_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
        JOIN `x-marketing.duckcreek_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
        `x-marketing.duckcreek_hubspot.email_events` activity
        JOIN
        `x-marketing.duckcreek_hubspot.campaigns` campaign
        ON
        activity.emailcampaignid = campaign.id
        WHERE
        activity.type = 'BOUNCE'  
    
        AND campaign.name IS NOT NULL  )
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
        `x-marketing.duckcreek_hubspot.email_events` activity
        JOIN
        `x-marketing.duckcreek_hubspot.campaigns` campaign
        ON
        activity.emailcampaignid = campaign.id
        WHERE
        activity.type = 'DELIVERED'  
        AND campaign.name IS NOT NULL  )
        WHERE
            _rownum = 1
    ) 
    SELECT 
        email_bounce .* 
    FROM email_bounce
    JOIN bounced ON email_bounce._email = bounced._email and email_bounce._campaignID = bounced._campaignID
    JOIN delivered ON email_bounce._email = delivered._email and email_bounce._campaignID = delivered._campaignID
),
unsubscribe AS (
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
      FROM `x-marketing.duckcreek_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.duckcreek_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.duckcreek_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
          `x-marketing.sbi_hubspot.contacts` c,
          UNNEST(form_submissions) AS form
        JOIN
          `x-marketing.sbi_hubspot.forms` forms
        ON
          form.value.form_id = forms.guid
          ) activity
      LEFT JOIN
        `x-marketing.sbi_hubspot.campaigns` campaign
      ON
        activity._utmcontent = CAST(campaign.id AS STRING) 
    )
    WHERE
      rownum = 1 
),
email_defferred AS (
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
      'Deffered' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
        FROM `x-marketing.duckcreek_hubspot.email_events` activity
        JOIN `x-marketing.duckcreek_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
        WHERE 
        activity.type = 'DEFERRED' 
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
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Dropped' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
        FROM `x-marketing.duckcreek_hubspot.email_events` activity
        JOIN `x-marketing.duckcreek_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
        WHERE 
        activity.type = 'DROPPED' 
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
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Suppressed' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
        FROM `x-marketing.duckcreek_hubspot.email_events` activity
        JOIN `x-marketing.duckcreek_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
        WHERE 
        activity.type = 'SUPPRESSED' 
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
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Processed' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
        FROM `x-marketing.duckcreek_hubspot.email_events` activity
        JOIN `x-marketing.duckcreek_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
        WHERE 
        activity.type = 'PROCESSED' 
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
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Forward' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
        FROM `x-marketing.duckcreek_hubspot.email_events` activity
        JOIN `x-marketing.duckcreek_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
        WHERE 
        activity.type = 'FORWARD' 
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
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Spam' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
        FROM `x-marketing.duckcreek_hubspot.email_events` activity
        JOIN `x-marketing.duckcreek_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
        WHERE 
        activity.type = 'SPAMREPORT' 
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
      campaign.name AS _campaign,
      campaign.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Print' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
        FROM `x-marketing.duckcreek_hubspot.email_events` activity
        JOIN `x-marketing.duckcreek_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
        WHERE 
        activity.type = 'PRINT' 
    )
    WHERE _rownum = 1
),
engagements_combined AS (
    SELECT * FROM email_sent
    UNION ALL
    SELECT * FROM email_delivered
    UNION ALL 
    SELECT * FROM email_open
    UNION ALL
    SELECT * FROM email_click
    UNION ALL
    SELECT * FROM email_softbounce
    UNION ALL 
    SELECT * FROM email_hardBounce
    UNION ALL
    SELECT * FROM unsubscribe
    UNION ALL 
    SELECT * FROM email_download
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
)
SELECT
    engagements.*,
    COALESCE(REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)'), "Email") AS _utmsource,
    REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
    REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utmcontent,
    airtable_info.* EXCEPT (id),
    prospect_info.* EXCEPT (_email)
FROM 
  engagements_combined AS engagements
LEFT JOIN
  prospect_info
ON
  engagements._email = prospect_info._email
JOIN
  airtable_info
ON
  engagements._campaignID = CAST(airtable_info.id AS STRING)
;


