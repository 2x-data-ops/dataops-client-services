------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------ Email Engagement Log --------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/* 
  This script is used typically for the email performance page/dashboard 
  CRM/Platform: Marketo
  Data type: Email Engagement
  Depedency Table: db_tam_database
  Target table: db_email_engagements_log
*/

TRUNCATE TABLE `x-marketing.hyland.db_email_engagements_log`;
INSERT INTO `x-marketing.hyland.db_email_engagements_log` (
  _sdc_sequence,
  _campaignID,
  _utmcampaign,
  _subject,
  -- _email,
  _timestamp,
  _engagement,
  _description,
  -- _device_type,
  -- _linkid,
  -- _duration,
  -- _response,
  _utm_source,  
  _utm_medium, 
  _utm_content,
  _prospectID,
  _email,
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
  _lifecycleStage,
  _leadsourcedetail,
  _mostrecentleadsource,
  _mostrecentleadsourcedetail,
  _programname,
  _programchannel,
  _campaignSentDate,
  EMEAcampaign
)
WITH
  prospect_info AS ( # TO BE REPLACED WITH CONTACTS TABLE FROM ICP DATABASE TABLE
    SELECT * EXCEPT(rownum)
    FROM (
        SELECT DISTINCT 
            CAST(marketo.id AS STRING) AS _id,
            email AS _email,
            CONCAT(firstname,' ', lastname) AS _name,
            RIGHT(email, LENGTH(email) - STRPOS(email, '@')) AS _domain, 
            title AS _jobtitle,
            job_function__c AS _function,
            CASE 
              WHEN title LIKE '%Senior Counsel%' THEN "VP"
              WHEN title LIKE '%Assistant General Counsel%' THEN "VP" 
              WHEN title LIKE '%General Counsel%' THEN "C-Level" 
              WHEN title LIKE '%Founder%' THEN "C-Level" 
              WHEN title LIKE '%C-Level%' THEN "C-Level" 
              WHEN title LIKE '%CDO%' THEN "C-Level" 
              WHEN title LIKE '%CIO%' THEN "C-Level"
              WHEN title LIKE '%CMO%' THEN "C-Level"
              WHEN title LIKE '%CFO%' THEN "C-Level" 
              WHEN title LIKE '%CEO%' THEN "C-Level"
              WHEN title LIKE '%Chief%' THEN "C-Level" 
              WHEN title LIKE '%coordinator%' THEN "Non-Manager"
              WHEN title LIKE '%COO%' THEN "C-Level" 
              WHEN title LIKE '%Sr. V.P.%' THEN "Senior VP"
              WHEN title LIKE '%Sr.VP%' THEN "Senior VP"  
              WHEN title LIKE '%Senior-Vice Pres%' THEN "Senior VP"  
              WHEN title LIKE '%srvp%' THEN "Senior VP" 
              WHEN title LIKE '%Senior VP%' THEN "Senior VP" 
              WHEN title LIKE '%SR VP%' THEN "Senior VP"  
              WHEN title LIKE '%Sr Vice Pres%' THEN "Senior VP" 
              WHEN title LIKE '%Sr. VP%' THEN "Senior VP" 
              WHEN title LIKE '%Sr. Vice Pres%' THEN "Senior VP"  
              WHEN title LIKE '%S.V.P%' THEN "Senior VP" 
              WHEN title LIKE '%Senior Vice Pres%' THEN "Senior VP"  
              WHEN title LIKE '%Exec Vice Pres%' THEN "Senior VP" 
              WHEN title LIKE '%Exec Vp%' THEN "Senior VP"  
              WHEN title LIKE '%Executive VP%' THEN "Senior VP" 
              WHEN title LIKE '%Exec VP%' THEN "Senior VP"  
              WHEN title LIKE '%Executive Vice President%' THEN "Senior VP" 
              WHEN title LIKE '%EVP%' THEN "Senior VP"  
              WHEN title LIKE '%E.V.P%' THEN "Senior VP" 
              WHEN title LIKE '%SVP%' THEN "Senior VP" 
              WHEN title LIKE '%V.P%' THEN "VP" 
              WHEN title LIKE '%VP%' THEN "VP" 
              WHEN title LIKE '%Vice Pres%' THEN "VP"
              WHEN title LIKE '%V P%' THEN "VP"
              WHEN title LIKE '%President%' THEN "C-Level"
              WHEN title LIKE '%Director%' THEN "Director"
              WHEN title LIKE '%CTO%' THEN "C-Level"
              WHEN title LIKE '%Dir%' THEN "Director"
              WHEN title LIKE '%MDR%' THEN "Non-Manager"
              WHEN title LIKE '%MD%' THEN "Director"
              WHEN title LIKE '%GM%' THEN "Director"
              WHEN title LIKE '%Head%' THEN "VP"
              WHEN title LIKE '%Manager%' THEN "Manager"
              WHEN title LIKE '%escrow%' THEN "Non-Manager"
              WHEN title LIKE '%cross%' THEN "Non-Manager"
              WHEN title LIKE '%crosse%' THEN "Non-Manager"
              WHEN title LIKE '%Assistant%' THEN "Non-Manager"
              WHEN title LIKE '%Partner%' THEN "C-Level"
              WHEN title LIKE '%CRO%' THEN "C-Level"
              WHEN title LIKE '%Chairman%' THEN "C-Level"
              WHEN title LIKE '%Owner%' THEN "C-Level"
            END AS _seniority,
            phone AS _phone,
            company AS _company,
            CAST(annualrevenue AS STRING) AS _revenue,
            industry AS _industry,
            city AS _city,
            state AS _state, 
            country AS _country,
            "" AS _persona,
            lead_lifecycle_stage__c AS _lifecycleStage,
            leadsourcedetail,
            mostrecentleadsource,
            mostrecentleadsourcedetail,
            programs.name,
            programs.channel,
            ROW_NUMBER() OVER(
              PARTITION BY email
              ORDER BY marketo.id DESC
            ) AS rownum
        FROM `x-marketing.hyland_marketo.leads` marketo
        LEFT JOIN
          `x-marketing.hyland_marketo.programs` programs
        ON
          marketo.acquisitionprogramid = CAST(programs.id AS STRING)
        WHERE
            email IS NOT NULL
            AND email NOT LIKE '%2x.marketing%'
            AND email NOT LIKE '%hyland.com%'
    )
    WHERE rownum = 1
  ),
  airtable_info AS (
    SELECT
      campaign.id AS id,
      -- CASE
      --   WHEN _senddate = '' THEN CAST(null AS TIMESTAMP)
      --   ELSE CAST(_senddate AS TIMESTAMP)
      -- END AS _campaignSentDate
      CASE
        WHEN airtable._assetid IS NOT NULL
        THEN (
          CASE
            WHEN _senddate = '' THEN CAST(null AS TIMESTAMP)
            ELSE CAST(_senddate AS TIMESTAMP)
          END
        )
        ELSE (
          CASE
            WHEN createdat IS NULL THEN CAST(null AS TIMESTAMP)
            ELSE CAST(createdat AS TIMESTAMP)
          END 
        )
      END AS _campaignSentDate,
      CASE
        WHEN airtable._assetid IS NOT NULL
        THEN 'Yes'
        ELSE 'No'
      END AS EMEAcampaign
      -- '' AS subtype,
      -- '' AS subject,
      -- programid AS contentid,
      -- type,
      -- name,
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
      -- '' AS _2x_campaign
    FROM
      `x-marketing.hyland_marketo.campaigns` campaign
      -- WHERE id IN (247413984,247366742,247239377,245978650,245935666,245977325,245936306,245369808,245369806,245369807,244174255)
    LEFT JOIN 
      `x-marketing.hyland_mysql.db_airtable_email_emea` airtable 
    ON CAST(campaign.id AS STRING) = airtable._assetid
    -- WHERE _assetid IS NOT NULL
    WHERE campaign.id IS NOT NULL
  ),
  email_sent AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        _sdc_sequence,
        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        '' AS _subject,
        '' AS _email,
        activitydate AS _timestamp,
        'sent' AS _engagement,
        '' AS _description,
        CAST(leadid AS STRING) AS _leadid, 
        ROW_NUMBER() OVER(
          PARTITION BY leadid, primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) AS _rownum
      FROM `x-marketing.hyland_marketo.activities_send_email` 
    )
    WHERE
      _rownum = 1 
  ),
  email_delivered AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        _sdc_sequence,
        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        '' AS _subject,
        '' AS _email,
        activitydate AS _timestamp,
        'delivered' AS _engagement,
        '' AS _description,
        CAST(leadid AS STRING) AS _leadid, 
        ROW_NUMBER() OVER(
          PARTITION BY leadid, primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) AS _rownum
      FROM `x-marketing.hyland_marketo.activities_email_delivered` 
    )
    WHERE
      _rownum = 1 
  ),
  email_open AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        _sdc_sequence,
        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        '' AS _subject,
        '' AS _email,
        activitydate AS _timestamp,
        'opened' AS _engagement,
        '' AS _description,
        CAST(leadid AS STRING) AS _leadid, 
        ROW_NUMBER() OVER(
          PARTITION BY leadid, primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) AS _rownum
      FROM `x-marketing.hyland_marketo.activities_open_email`
    )
    WHERE
      _rownum = 1 
  ),
  email_click AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        _sdc_sequence,
        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        '' AS _subject,
        '' AS _email,
        activitydate AS _timestamp,
        'clicked' AS _engagement,
        '' AS _description,
        CAST(leadid AS STRING) AS _leadid, 
        ROW_NUMBER() OVER(
          PARTITION BY leadid, primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) AS _rownum
      FROM `x-marketing.hyland_marketo.activities_click_email`
    )
    WHERE
      _rownum = 1 
  ),
  email_hard_bounce AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        _sdc_sequence,
        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        '' AS _subject,
        '' AS _email,
        activitydate AS _timestamp,
        'hard_bounced' AS _engagement,
        details AS _description,
        CAST(leadid AS STRING) AS _leadid, 
        ROW_NUMBER() OVER(
          PARTITION BY leadid, primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) AS _rownum
      FROM `x-marketing.hyland_marketo.activities_email_bounced`
    )
    WHERE
      _rownum = 1 
  ),
  email_soft_bounce AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        _sdc_sequence,
        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        '' AS _subject,
        '' AS _email,
        activitydate AS _timestamp,
        'soft_bounced' AS _engagement,
        details AS _description,
        CAST(leadid AS STRING) AS _leadid, 
        ROW_NUMBER() OVER(
          PARTITION BY leadid, primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) AS _rownum
      FROM `x-marketing.hyland_marketo.activities_email_bounced_soft`
    )
    WHERE
      _rownum = 1  
  ),
  email_download AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        _sdc_sequence,
        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        '' AS _subject,
        '' AS _email,
        activitydate AS _timestamp,
        'downloaded' AS _engagement,
        '' AS _description,
        CAST(leadid AS STRING) AS _leadid, 
        ROW_NUMBER() OVER(
          PARTITION BY leadid, primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) AS _rownum
      FROM `x-marketing.hyland_marketo.activities_fill_out_form`
      WHERE primary_attribute_value NOT LIKE '%TEST 2X%'
      AND primary_attribute_value NOT LIKE '%Email Unsubscribe Form%'
    )
    WHERE
      _rownum = 1 
  ),
  email_unsubscribed AS (
    SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        _sdc_sequence,
        CAST(primary_attribute_value_id AS STRING) AS _campaignID,
        primary_attribute_value AS _campaign,
        '' AS _subject,
        '' AS _email,
        activitydate AS _timestamp,
        'unsubscribed' AS _engagement,
        '' AS _description,
        CAST(leadid AS STRING) AS _leadid, 
        ROW_NUMBER() OVER(
          PARTITION BY leadid, primary_attribute_value_id 
          ORDER BY activitydate DESC
        ) AS _rownum
      FROM `x-marketing.hyland_marketo.activities_unsubscribe_email`
    )
    WHERE
      _rownum = 1 
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
    SELECT * FROM email_hard_bounce
    UNION ALL
    SELECT * FROM email_soft_bounce
    UNION ALL
    SELECT * FROM email_unsubscribed
  )
SELECT
  engagements.* EXCEPT(_leadid, _email),
  COALESCE(REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)'), "Email") AS _utmsource,
  REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
  REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utmcontent,
  prospect_info.*,
  CAST(airtable_info._campaignSentDate AS TIMESTAMP) AS _campaignSentDate,
  airtable_info.EMEAcampaign
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
  engagements._leadid = prospect_info._id
JOIN
  airtable_info
ON
  engagements._campaignID = CAST(airtable_info.id AS STRING)
;


-- Label Clicks That Are Visits and Set their Page Views
-- UPDATE `hyland.db_email_engagements_log` origin
-- SET 
--     origin._isPageView = true, 
--     origin._totalPageViews = scenario.pageviews,
--     origin._averagePageViews = scenario.pageviews / scenario.visitors
-- FROM (
--     SELECT  
--         CONCAT(_email, _campaignid, _engagement, email._timestamp) AS _key,
--         COUNT(DISTINCT web._visitorid) AS visitors,
--         SUM(web._totalsessionviews) AS pageviews
--     FROM 
--         `x-marketing.hyland.db_email_engagements_log` email 
--     JOIN (
--         SELECT DISTINCT
--             _timestamp,
--             _visitorid,
--             _utmcampaign,
--             _totalsessionviews,
--             _utmmedium,
--             _utmsource,
--         FROM `x-marketing.hyland.db_web_engagements_log`
--     ) web
--     ON DATE(email._timestamp) = DATE(web._timestamp)
--     AND email._utmcampaign = web._utmcampaign
--     WHERE 
--         email._engagement = 'clicked'
--         AND LOWER(web._utmsource) LIKE '%email%'
--     GROUP BY 
--         1
-- ) scenario
-- WHERE CONCAT(_email, _campaignid, _engagement, _timestamp) = scenario._key;


