------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------ Email Engagement Log --------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* 
  This script is used typically for the email performance page/dashboard 
  CRM/Platform: Hubspot
  Data type: Email Engagement
  Depedency Table: db_tam_database
  Target table: ddb_email_campaign_engagement
*/

TRUNCATE TABLE `x-marketing.sandler.db_email_campaign_engagement`;
INSERT INTO `x-marketing.sandler.db_email_campaign_engagement` (
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
  _persona,
  _lifecycleStage
)
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
      FROM
        `sandler.db_tam_database`
      -- WHERE
      --   _email IS NOT NULL
      --   AND _email NOT LIKE '%2x.marketing%'
      --   AND _email NOT LIKE '%sandler.com%'
  ),
  airtable_info AS (
    SELECT
      id,
      '' AS subtype,
      subject,
      contentid,
      type,
      name,
      /*_landingpage, 
    _pardotid, 
    _utm_medium, 
    _utm_source, 
    _livedate, 
    _code, 
    _utm_campaign, 
    _whatwedo, 
    _subject, _assettitle, _screenshot, _progress, _trimcode, _url_param, 
    _launched, */
      /* CASE
        WHEN id IN (247413984, 247366742, 247239377, 245978650, 245935666, 245977325, 245936306, 245369808, 245369806, 245369807, 244174255) THEN '2X'
    END
      AS */ 
      '' AS _2x_campaign
    FROM
      `x-marketing.sandler_hubspot.campaigns` campaign
      -- WHERE id IN (247413984,247366742,247239377,245978650,245935666,245977325,245936306,245369808,245369806,245369807,244174255)
      --LEFT JOIN `x-marketing.toolsgroup_mysql.db_airtable_email` airtable ON CAST(campaign.id AS STRING) = airtable._pardotid
      -- WHERE _pardotid IS NOT NULL
  ),
  email_sent AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        activity._sdc_sequence,
        CAST(activity.emailcampaignid AS STRING) AS _campaignID,
        campaign.name AS _campaign,
        activity.subject AS _subject,
        activity.recipient AS _email,
        activity.created AS _timestamp,
        'Sent' AS _engagement,
        url AS _description, 
        devicetype AS _devicetype,
        CAST(linkid AS STRING),
        --appname,
        CAST(duration AS STRING),
        response,
        ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM
        `x-marketing.sandler_hubspot.email_events` activity
      JOIN
        `x-marketing.sandler_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'SENT' /*AND activity.recipient NOT LIKE '%2x.marketing%'
      AND activity.recipient NOT LIKE '%sandler.com%'*/
        AND campaign.name IS NOT NULL )
    WHERE
      _rownum = 1 
  ),
  email_delivered AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        activity._sdc_sequence,
        CAST(activity.emailcampaignid AS STRING) AS _campaignID,
        campaign.name AS _campaign,
        activity.subject AS _subject,
        activity.recipient AS _email,
        activity.created AS _timestamp,
        'Delivered' AS _engagement,
        url AS _description,
        devicetype AS _devicetype,
        CAST(CAST(linkid AS STRING) AS STRING),
        --appname,
        CAST(duration AS STRING),
        response,
        ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM
        `x-marketing.sandler_hubspot.email_events` activity
      JOIN
        `x-marketing.sandler_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'DELIVERED' /*AND activity.recipient NOT LIKE '%2x.marketing%'
      AND activity.recipient NOT LIKE '%sandler.com%'*/
        AND campaign.name IS NOT NULL )
    WHERE
      _rownum = 1 
  ),
  email_open AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        activity._sdc_sequence,
        CAST(activity.emailcampaignid AS STRING) AS _campaignID,
        campaign.name AS _campaign,
        activity.subject AS _subject,
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
        `x-marketing.sandler_hubspot.email_events` activity
      JOIN
        `x-marketing.sandler_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'OPEN'
        AND filteredevent = FALSE
        AND activity.recipient NOT LIKE '%2x.marketing%'
        AND activity.recipient NOT LIKE '%sandler.com%'
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
        activity.subject AS _subject,
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
        `x-marketing.sandler_hubspot.email_events` activity
      JOIN
        `x-marketing.sandler_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'CLICK'
        AND filteredevent = FALSE
        AND activity.recipient NOT LIKE '%2x.marketing%'
        AND activity.recipient NOT LIKE '%sandler.com%'
        AND campaign.name IS NOT NULL )
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
        campaign.name AS _campaign,
        activity.subject AS _subject,
        activity.recipient AS _email,
        activity.created AS _timestamp,
        'Bounced' AS _engagement,
        url AS _description,
        devicetype AS _devicetype,
        CAST(linkid AS STRING),
        --appname,
        CAST(duration AS STRING),
        response,
        ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM
        `x-marketing.sandler_hubspot.email_events` activity
      JOIN
        `x-marketing.sandler_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'BOUNCE'
        AND activity.recipient NOT LIKE '%2x.marketing%'
        AND activity.recipient NOT LIKE '%sandler.com%'
        AND campaign.name IS NOT NULL )
    WHERE
      _rownum = 1
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
          `x-marketing.sandler_hubspot.contacts` c,
          UNNEST(form_submissions) AS form
        JOIN
          `x-marketing.sandler_hubspot.forms` forms
        ON
          form.value.form_id = forms.guid
          ) activity
      LEFT JOIN
        `x-marketing.sandler_hubspot.campaigns` campaign
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
      email_bounce
    UNION ALL 
    SELECT 
      * 
    FROM email_download
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
JOIN
  airtable_info
ON
  engagements._campaignID = CAST(airtable_info.id AS STRING)
;


-- Label Clicks That Are Visits and Set their Page Views
UPDATE `sandler.db_email_campaign_engagement` origin
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
        `x-marketing.sandler.db_email_campaign_engagement` email 
    JOIN (
        SELECT DISTINCT
            _timestamp,
            _visitorid,
            _utmcampaign,
            _totalsessionviews,
            _utmmedium,
            _utmsource,
        FROM 
          `x-marketing.sandler.web_metrics`
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




