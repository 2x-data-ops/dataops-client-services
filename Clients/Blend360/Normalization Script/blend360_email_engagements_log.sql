CREATE OR REPLACE TABLE `x-marketing.blend360.db_email_engagements_log` AS
WITH
  prospect_info AS (
      SELECT
        DISTINCT CAST(_id AS STRING) AS _id,
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
        _lifecycleStage,
        _leadScore,
        -- _jobLevel,
        _formSubmissions,
        _pageViews,
        _unsubscribed
        -- _emailClicks,
        -- _emailOpens
      FROM
        -- `blend360.db_tam_database`
        `blend360.db_icp_database_log`
      WHERE
        _email IS NOT NULL
        AND _email NOT LIKE '%2x.marketing%'
        AND _email NOT LIKE '%blend360.com%'
  ),
  airtable_info AS (
    SELECT 
      * EXCEPT(_rownum)
    FROM (
      SELECT 
          _pardotid,
          CASE 
              WHEN LENGTH(TRIM(_livedate)) = 0 THEN NULL
              ELSE CAST(_livedate AS TIMESTAMP)
          END 
          AS _liveDate,
          _code AS _contentTitle,
          -- _subject,
          _screenshot,
          _landingPage,
          _emailfilters,
          _costofevent,
          _preposteventemail,
          _eventlevel,
          ROW_NUMBER() OVER(
              PARTITION BY _pardotid 
              ORDER BY _id DESC
          ) _rownum
      FROM 
          `x-marketing.blend360_mysql.db_airtable_email` 

    ) 
    WHERE _rownum = 1
  ),
  total_sent AS (
    WITH sent_v2 AS (
      SELECT
        * EXCEPT(_rownum)
      FROM (
        SELECT
          -- activity._sdc_sequence,
          CAST(activity.emailcampaignid AS STRING) AS _campaignID,
          campaign.name AS _campaign,
          activity.subject AS _subject,
          activity.recipient AS _email,
          activity.created AS _timestamp,
          'Sent' AS _engagement,
          url AS _description, 
          devicetype AS _devicetype,
          CAST(linkid AS STRING) AS linkid,
          --appname,
          CAST(duration AS STRING) AS duration,
          response,
          ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
        FROM
          `x-marketing.blend360_hubspot_v2.email_events` activity
        JOIN
          `x-marketing.blend360_hubspot_v2.campaigns` campaign
        ON
          activity.emailcampaignid = campaign.id
        WHERE
          activity.type = 'SENT' /*AND activity.recipient NOT LIKE '%2x.marketing%'
        AND activity.recipient NOT LIKE '%blend360.com%'*/
          AND campaign.name IS NOT NULL 
      )
      WHERE 
        _rownum = 1 
    ),
    sent_v1 AS (
      SELECT
        * EXCEPT(_rownum)
      FROM (
        SELECT
          -- activity._sdc_sequence,
          CAST(activity.emailcampaignid AS STRING) AS _campaignID,
          CAST(NULL AS STRING) AS _campaign,
          activity.subject AS _subject,
          activity.recipient AS _email,
          activity.created AS _timestamp,
          'Sent' AS _engagement,
          url AS _description, 
          devicetype AS _devicetype,
          CAST(linkid AS STRING) AS linkid,
          --appname,
          CAST(duration AS STRING) AS duration,
          response,
          ROW_NUMBER() OVER(PARTITION BY activity.recipient, activity.emailcampaignid ORDER BY activity.created DESC) AS _rownum
        FROM
          `x-marketing.blend360_hubspot.email_events` activity
        WHERE
          activity.type = 'SENT' /*AND activity.recipient NOT LIKE '%2x.marketing%'
        AND activity.recipient NOT LIKE '%blend360.com%'*/
      )
      WHERE 
        _rownum = 1 
    )
    SELECT 
      *
    FROM sent_v2
    UNION ALL
    SELECT 
      *
    FROM sent_v1
    
  ),
  email_dropped AS (
    WITH dropped_v2 AS (
      SELECT 
          * EXCEPT(_rownum) 
      FROM (

          SELECT
              main.recipient AS _email,
              CAST(main.emailcampaignid AS STRING) AS _campaignID,
              side.name AS _contentTitle,
              main.url AS _description,
              main.created AS _timestamp,
              'Dropped' AS _engagement,
              ROW_NUMBER() OVER(
                  PARTITION BY main.recipient, main.emailcampaignid
                  ORDER BY main.created DESC
              ) AS _rownum
          FROM 
              `x-marketing.blend360_hubspot_v2.email_events` main
          JOIN
              `x-marketing.blend360_hubspot_v2.campaigns` side
          ON
              main.emailcampaignid = side.id
          WHERE 
              main.type = 'DROPPED' 
          AND 
              main.recipient NOT LIKE '%2x.marketing%'

      )
      WHERE _rownum = 1
    ),
    dropped_v1 AS (
      SELECT 
          * EXCEPT(_rownum) 
      FROM (

          SELECT
              recipient AS _email,
              CAST(emailcampaignid AS STRING) AS _campaignID,
              CAST(NULL AS STRING) AS _campaign,
              url AS _description,
              created AS _timestamp,
              'Dropped' AS _engagement,
              ROW_NUMBER() OVER(
                  PARTITION BY recipient, emailcampaignid
                  ORDER BY created DESC
              ) AS _rownum
          FROM 
              `x-marketing.blend360_hubspot.email_events` 
          WHERE 
              type = 'DROPPED' 
          AND 
              recipient NOT LIKE '%2x.marketing%'

      )
      WHERE _rownum = 1
    )
    SELECT
      *
    FROM dropped_v2
    UNION ALL
    SELECT
      *
    FROM dropped_v1
  ),
  email_sent AS (
    SELECT
      *
    FROM total_sent
    WHERE
      CONCAT(_email, _campaignID) NOT IN (
        SELECT 
            CONCAT(_email, _campaignID)
        FROM email_dropped
      ) 
  ),
  total_delivered AS (
    WITH delivered_v2 AS (
      SELECT
        * EXCEPT(_rownum)
      FROM (
        SELECT
          -- activity._sdc_sequence,
          CAST(activity.emailcampaignid AS STRING) AS _campaignID,
          campaign.name AS _campaign,
          activity.subject AS _subject,
          activity.recipient AS _email,
          activity.created AS _timestamp,
          'Delivered' AS _engagement,
          url AS _description,
          devicetype AS _devicetype,
          CAST(linkid AS STRING) AS linkid,
          --appname,
          CAST(duration AS STRING) AS duration,
          response,
          ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
        FROM
          `x-marketing.blend360_hubspot_v2.email_events` activity
        JOIN
          `x-marketing.blend360_hubspot_v2.campaigns` campaign
        ON
          activity.emailcampaignid = campaign.id
        WHERE
          activity.type = 'DELIVERED' /*AND activity.recipient NOT LIKE '%2x.marketing%'
        AND activity.recipient NOT LIKE '%blend360.com%'*/
          AND campaign.name IS NOT NULL )
      WHERE
        _rownum = 1 
    ),
    delivered_v1 AS (
      SELECT 
        * EXCEPT(_rownum) 
      FROM (
          SELECT
            CAST(emailcampaignid AS STRING) AS _campaignID,
            CAST(NULL AS STRING) AS _campaign,
            subject AS _subject,
            recipient AS _email,
            created AS _timestamp,
            'Delivered' AS _engagement,
            url AS _description,
            devicetype AS _devicetype,
            CAST(linkid AS STRING) AS linkid,
            --appname,
            CAST(duration AS STRING) AS duration,
            response,
            ROW_NUMBER() OVER(
                PARTITION BY recipient, emailcampaignid
                ORDER BY created DESC
            ) AS _rownum
          FROM 
            `x-marketing.blend360_hubspot.email_events` 
          WHERE 
            type = 'DELIVERED' 
          AND 
            recipient NOT LIKE '%2x.marketing%'
      )
      WHERE _rownum = 1
    )
    SELECT
      *
    FROM delivered_v2
    UNION ALL
    SELECT
      *
    FROM delivered_v1
  ),
  email_bounce AS (
    WITH bounce_v2 AS (
      SELECT
        * EXCEPT(_rownum)
      FROM (
        SELECT
          -- activity._sdc_sequence,
          CAST(activity.emailcampaignid AS STRING) AS _campaignID,
          campaign.name AS _campaign,
          activity.subject AS _subject,
          activity.recipient AS _email,
          activity.created AS _timestamp,
          'Bounced' AS _engagement,
          url AS _description,
          devicetype AS _devicetype,
          CAST(linkid AS STRING) AS linkid,
          --appname,
          CAST(duration AS STRING) AS duration,
          response,
          ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
        FROM
          `x-marketing.blend360_hubspot_v2.email_events` activity
        JOIN
          `x-marketing.blend360_hubspot_v2.campaigns` campaign
        ON
          activity.emailcampaignid = campaign.id
        WHERE
          activity.type = 'BOUNCE'
          AND activity.recipient NOT LIKE '%2x.marketing%'
          AND activity.recipient NOT LIKE '%blend360.com%'
          AND campaign.name IS NOT NULL )
      WHERE
        _rownum = 1
    ),
    bounce_v1 AS (
      SELECT 
        * EXCEPT(_rownum) 
      FROM (

          SELECT
              CAST(emailcampaignid AS STRING) AS _campaignID,
              CAST(NULL AS STRING) AS _campaign,
              subject AS _subject,
              recipient AS _email,
              created AS _timestamp,
              'Bounced' AS _engagement,
              url AS _description,
              devicetype AS _devicetype,
              CAST(linkid AS STRING) AS linkid,
              --appname,
              CAST(duration AS STRING) AS duration,
              response,
              ROW_NUMBER() OVER(
                  PARTITION BY recipient, emailcampaignid
                  ORDER BY created DESC
              ) AS _rownum
          FROM 
              `x-marketing.blend360_hubspot.email_events` 
          WHERE 
              type = 'BOUNCE' 
          AND 
              recipient NOT LIKE '%2x.marketing%'

      )
      WHERE _rownum = 1 
    )
    SELECT
      *
    FROM bounce_v2
    UNION ALL
    SELECT
      *
    FROM bounce_v1
  ),
  email_delivered AS (
    SELECT 
        *
    FROM 
        total_delivered
    WHERE 
    CONCAT(_email, _campaignID) NOT IN (
      SELECT 
          CONCAT(_email, _campaignID)
      FROM email_bounce
    ) 
  ),
  email_open AS (
    WITH open_v2 AS (
      SELECT
        * EXCEPT(_rownum)
      FROM (
        SELECT
          -- activity._sdc_sequence,
          CAST(activity.emailcampaignid AS STRING) AS _campaignID,
          campaign.name AS _campaign,
          activity.subject AS _subject,
          activity.recipient AS _email,
          activity.created AS _timestamp,
          'Opened' AS _engagement,
          url AS _description,
          devicetype AS _devicetype,
          CAST(linkid AS STRING) AS linkid,
          --appname,
          CAST(duration AS STRING) AS duration,
          response,
          ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
        FROM
          `x-marketing.blend360_hubspot_v2.email_events` activity
        JOIN
          `x-marketing.blend360_hubspot_v2.campaigns` campaign
        ON
          activity.emailcampaignid = campaign.id
        WHERE
          activity.type = 'OPEN'
          AND filteredevent = FALSE
          AND activity.recipient NOT LIKE '%2x.marketing%'
          AND activity.recipient NOT LIKE '%blend360.com%'
          AND campaign.name IS NOT NULL )
      WHERE
        _rownum = 1 
    ),
    open_v1 AS (
      SELECT 
        * EXCEPT(_rownum) 
      FROM (

          SELECT
            CAST(emailcampaignid AS STRING) AS _campaignID,
            CAST(NULL AS STRING) AS _campaign,
            subject AS _subject,
            recipient AS _email,
            created AS _timestamp,
            'Opened' AS _engagement,
            url AS _description,
            devicetype AS _devicetype,
            CAST(linkid AS STRING) AS linkid,
            --appname,
            CAST(duration AS STRING) AS duration,
            response,
            ROW_NUMBER() OVER(
                PARTITION BY recipient, emailcampaignid
                ORDER BY created DESC
            ) AS _rownum
          FROM 
              `x-marketing.blend360_hubspot.email_events` 
          WHERE 
              type = 'OPEN' 
          AND 
              recipient NOT LIKE '%2x.marketing%'
          AND 
              filteredevent = false

      )
      WHERE _rownum = 1
    )
    
    SELECT
      *
    FROM open_v2
    UNION ALL
    SELECT
      *
    FROM open_v1
  ),
  total_clicked AS (
    WITH click_v2 AS (
      SELECT
        * EXCEPT(_rownum)
      FROM (
        SELECT
          -- activity._sdc_sequence,
          CAST(activity.emailcampaignid AS STRING) AS _campaignID,
          campaign.name AS _campaign,
          activity.subject AS _subject,
          activity.recipient AS _email,
          activity.created AS _timestamp,
          'Clicked' AS _engagement,
          url AS _description,
          devicetype AS _devicetype,
          CAST(linkid AS STRING) AS linkid,
          --appname,
          CAST(duration AS STRING) AS duration,
          response,
          ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
        FROM
          `x-marketing.blend360_hubspot_v2.email_events` activity
        JOIN
          `x-marketing.blend360_hubspot_v2.campaigns` campaign
        ON
          activity.emailcampaignid = campaign.id
        WHERE
          activity.type = 'CLICK'
          AND filteredevent = FALSE
          AND activity.recipient NOT LIKE '%2x.marketing%'
          AND activity.recipient NOT LIKE '%blend360.com%'
          AND campaign.name IS NOT NULL )
      WHERE
        _rownum = 1
    ),
    click_v1 AS (
      SELECT 
        * EXCEPT(_rownum) 
      FROM (
        SELECT
            CAST(emailcampaignid AS STRING) AS _campaignID,
            CAST(NULL AS STRING) AS _campaign,
            subject AS _subject,
            recipient AS _email,
            created AS _timestamp,
            'Clicked' AS _engagement,
            url AS _description,
            devicetype AS _devicetype,
            CAST(linkid AS STRING) AS linkid,
            --appname,
            CAST(duration AS STRING) AS duration,
            response,
            ROW_NUMBER() OVER(
                PARTITION BY recipient, emailcampaignid
                ORDER BY created DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot.email_events` 
        WHERE 
            type = 'CLICK' 
        AND 
            recipient NOT LIKE '%2x.marketing%'
        AND 
            filteredevent = false
      )
      WHERE _rownum = 1
    )
    
    SELECT
      *
    FROM click_v2
    UNION ALL
    SELECT
      *
    FROM click_v1
  ),
  email_click AS (
    SELECT
      *
    FROM total_clicked
    WHERE
      CONCAT(_email, _campaignID) IN (
        SELECT 
            CONCAT(_email, _campaignID)
        FROM email_open
      ) 
  ),
  total_downloaded AS (
    WITH download_v2 AS (
      SELECT
        * EXCEPT (rownum)
      FROM (
        SELECT
          -- activity._sdc_sequence,
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
            `x-marketing.blend360_hubspot_v2.contacts` c,
            UNNEST(form_submissions) AS form
          JOIN
            `x-marketing.blend360_hubspot_v2.forms` forms
          ON
            form.value.form_id = forms.guid
            ) activity
        LEFT JOIN
          `x-marketing.blend360_hubspot_v2.campaigns` campaign
        ON
          activity._utmcontent = CAST(campaign.id AS STRING) 
      )
      WHERE
        rownum = 1
    ),
    download_v1 AS (
      SELECT
          CAST(NULL AS STRING) AS _campaignID,
          CASE 
              WHEN form.value.page_url IS NOT NULL
              THEN SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_campaign=') + 10), '&')[ORDINAL(1)]
              ELSE NULL
          END AS _utm_campaign,
          CAST(NULL AS STRING) AS _subject,
          contact.properties.email.value AS _email,
          form.value.timestamp AS _timestamp,
          'Downloaded' AS _engagement,
          form.value.page_url AS _description, 
          CAST(NULL AS STRING) AS devicetype,
          '' AS linkid,
          '' AS duration,
          "" AS response,
      FROM 
          `x-marketing.blend360_hubspot.contacts` contact, 
          UNNEST(form_submissions) AS form
      WHERE 
          contact.properties.email.value NOT LIKE '%2x.marketing%'
      AND 
          form.value.page_url LIKE '%utm_campaign%'
    )
    
    SELECT
      *
    FROM download_v2
    UNION ALL
    SELECT
      *
    FROM download_v1
  ),
  email_download AS (
    SELECT 
        * EXCEPT(_rownum) 
    FROM (
        SELECT
            side._pardotid AS _campaignID,
            side._code AS _campaign,
            main.subject,
            main._email,
            main._timestamp, 
            main._engagement,
            main._description,
            main.devicetype,
            main.linkid,
            main.duration,
            main.response,
            ROW_NUMBER() OVER(
                PARTITION BY main._email, main._campaign
                ORDER BY main._timestamp DESC
            ) AS _rownum
        FROM 
            total_downloaded AS main
        JOIN 
            `x-marketing.blend360_mysql.db_airtable_email` AS side
        ON 
            main._campaign = side._utm_campaign
    )
    WHERE _rownum = 1
  ),
  email_unsubscribed AS (
    WITH unsub_v2 AS (
      SELECT 
          * EXCEPT(_rownum) 
      FROM (

          SELECT
            CAST(main.emailcampaignid AS STRING) AS _campaignID,
            side.name AS _campaign,
            main.subject AS _subject,
            main.recipient AS _email,
            main.created AS _timestamp,
            'Unsubscribed' AS _engagement,
            url AS _description,
            devicetype AS _devicetype,
            CAST(linkid AS STRING) AS linkid,
            --appname,
            CAST(duration AS STRING) AS duration,
            response,
            ROW_NUMBER() OVER(
                PARTITION BY main.recipient, side.name
                ORDER BY main.created DESC
            ) AS _rownum
          FROM 
              `x-marketing.blend360_hubspot_v2.email_events` main
          JOIN
              `x-marketing.blend360_hubspot_v2.campaigns` side
          ON
              main.emailcampaignid = side.id
          WHERE 
              main.type = 'STATUSCHANGE' 
          AND 
              main.recipient NOT LIKE '%2x.marketing%'

      )
      WHERE _rownum = 1
    ),
    unsub_v1 AS (
      SELECT 
        * EXCEPT(_rownum) 
      FROM (
        SELECT
          CAST(emailcampaignid AS STRING) AS _campaignID,
          CAST(NULL AS STRING) AS _campaign,
          subject AS _subject,
          recipient AS _email,
          created AS _timestamp,
          'Unsubscribed' AS _engagement,
          url AS _description,
          devicetype AS _devicetype,
          CAST(linkid AS STRING) AS linkid,
          --appname,
          CAST(duration AS STRING) AS duration,
          response,
          ROW_NUMBER() OVER(
              PARTITION BY recipient, emailcampaignid
              ORDER BY created DESC
          ) AS _rownum
        FROM 
          `x-marketing.blend360_hubspot.email_events` 
        WHERE 
          type = 'STATUSCHANGE' 
        AND 
          recipient NOT LIKE '%2x.marketing%'
      )
      WHERE _rownum = 1 
    )

    SELECT
      *
    FROM unsub_v2
    UNION ALL
    SELECT
      *
    FROM unsub_v1
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
      email_bounce
    UNION ALL 
    SELECT 
      * 
    FROM email_download
    UNION ALL 
    SELECT 
      * 
    FROM email_unsubscribed
  )
SELECT
  engagements.*,
  COALESCE(REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)'), "Email") AS _utmsource,
  REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
  REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utmcontent,
  prospect_info.* EXCEPT (_email),
  airtable_info.* EXCEPT (_pardotid)
FROM 
  engagements_combined AS engagements
LEFT JOIN
  prospect_info
ON
  engagements._email = prospect_info._email
JOIN
  airtable_info
ON
  engagements._campaignID = CAST(airtable_info._pardotid AS STRING)
;