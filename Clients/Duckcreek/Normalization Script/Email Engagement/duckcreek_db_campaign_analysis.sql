TRUNCATE TABLE `x-marketing.duckcreek.db_campaign_analysis`;

INSERT INTO `x-marketing.duckcreek.db_campaign_analysis` (
  _sdc_sequence,
  _campaignID,
  _contentTitle,
  _subject,
  _email,
  _timestamp,
  _engagement,
  _description,
  _device_type,
  _linkid,
  _duration,
  _response,
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
  _lifecycleStage,
  _utm_campaign
    /*_name, 
    _email, 
    _title, 
    _seniority,
    _function, 
    _tier, 
    _phone, 
    _company, 
    _industry, 
    _revenue, 
    _employees, 
    _city, 
    _state, 
    _country,
    _utm_contentTitle,
    _landingPage,
    _subject,
    _screenshot,
    _campaignSentDate,
    _preview,
    _stage*/
)
-- CREATE OR REPLACE TABLE `x-marketing.duckcreek.db_campaign_analysis` AS
WITH prospect_info AS (
  SELECT
    * EXCEPT( _rownum)
  FROM (
    SELECT
      CAST(vid AS STRING) AS _prospectid,
      property_email.value AS _email,
      CONCAT(property_firstname.value, ' ', property_lastname.value) AS _name,
      COALESCE(associated_company.properties.domain.value, property_hs_email_domain.value) AS _domain,
      properties.jobtitle.value,
      properties.job_function.value AS _function,
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
      property_phone.value AS _phone,
      property_company.value AS _company,
      CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
      property_industry.value AS _industry,
      property_city.value AS _city,
      property_state.value AS _state,
      property_country.value AS _country,
      '' AS _persona,
      property_lifecyclestage.value AS _lifecycleStage,
      ROW_NUMBER() OVER(
          PARTITION BY property_email.value, CONCAT(property_firstname.value, ' ', property_lastname.value)
          ORDER BY vid DESC
      ) AS _rownum
    FROM
      `x-marketing.duckcreek_hubspot.contacts` k
    WHERE
      property_email.value IS NOT NULL
      AND property_email.value NOT LIKE '%2x.marketing%'
      AND property_email.value NOT LIKE '%duckcreek%' 
  )
  WHERE
    _rownum = 1
    AND _domain NOT IN ('duckcreek.com',
      'duckcreek',
      '2x.marketing'
    ) 
),
airtable_info AS (
  SELECT
    id,
    subtype,
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
  _utm_contentTitle, 
  _whatwedo, 
  _subject, _assettitle, _screenshot, _progress, _trimcode, _url_param, 
  _launched, */
    CASE
      WHEN id IN (247413984, 247366742, 247239377, 245978650, 245935666, 245977325, 245936306, 245369808, 245369806, 245369807, 244174255) THEN '2X'
  END
    AS _2x_campaign
  FROM
    `x-marketing.duckcreek_hubspot.campaigns` campaign
    --WHERE id IN (247413984,247366742,247239377,245978650,245935666,245977325,245936306,245369808,245369806,245369807,244174255)
    --LEFT JOIN `x-marketing.duckcreek_mysql.db_airtable_email` airtable ON CAST(campaign.id AS STRING) = airtable._pardotid
    -- WHERE _pardotid IS NOT NULL
),
email_sent AS (
  SELECT
    * EXCEPT(_rownum)
  FROM (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _contentTitle,
      activity.subject AS _subject,
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
      activity.type = 'SENT'  /*AND activity.recipient NOT LIKE '%2x.marketing%'
    AND activity.recipient NOT LIKE '%duckcreek%'*/
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
      campaign.name AS _contentTitle,
      activity.subject AS _subject,
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
      activity.type = 'DELIVERED' /*AND activity.recipient NOT LIKE '%2x.marketing%'
    AND activity.recipient NOT LIKE '%duckcreek%'*/
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
      campaign.name AS _contentTitle,
      activity.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Opened' AS _engagement,
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
      activity.type = 'OPEN'
      AND filteredevent = FALSE
      AND activity.recipient NOT LIKE '%2x.marketing%'
      AND activity.recipient NOT LIKE '%duckcreek%'
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
      activity.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Clicked' AS _engagement,
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
      activity.type = 'CLICK'
      AND filteredevent = FALSE
      AND activity.recipient NOT LIKE '%2x.marketing%'
      AND activity.recipient NOT LIKE '%duckcreek%'
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
      campaign.name AS _contentTitle,
      activity.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Bounced' AS _engagement,
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
      AND activity.recipient NOT LIKE '%2x.marketing%'
      AND activity.recipient NOT LIKE '%duckcreek%'
      AND campaign.name IS NOT NULL )
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
    activity.subject AS _subject,
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
    AND activity.recipient NOT LIKE '%2x.marketing%'
    AND activity.recipient NOT LIKE '%duckcreek%'
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
    activity.subject AS _subject,
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
    AND activity.recipient NOT LIKE '%2x.marketing%'
    AND activity.recipient NOT LIKE '%duckcreek%'
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
    activity.subject AS _subject,
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
    AND activity.recipient NOT LIKE '%2x.marketing%'
    AND activity.recipient NOT LIKE '%duckcreek%'
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
    activity.subject AS _subject,
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
    AND activity.recipient NOT LIKE '%2x.marketing%'
    AND activity.recipient NOT LIKE '%duckcreek%'
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
    activity.subject AS _subject,
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
    AND activity.recipient NOT LIKE '%2x.marketing%'
    AND activity.recipient NOT LIKE '%duckcreek%'
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
    activity.subject AS _subject,
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
    AND activity.recipient NOT LIKE '%2x.marketing%'
    AND activity.recipient NOT LIKE '%duckcreek%'
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
    activity.subject AS _subject,
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
    AND activity.recipient NOT LIKE '%2x.marketing%'
    AND activity.recipient NOT LIKE '%duckcreek%'
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
      activity.subject AS _subject,
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
      COALESCE(form_title, campaign.name) AS _contentTitle,
      campaign.subject,
      activity.email AS _email,
      activity.timestamp AS _timestamp,
      'Downloaded' AS _engagement,
      activity.description AS _description,
      activity.devicetype,
      '' AS linkid,
      '' AS duration,
      "" AS _response,
      ROW_NUMBER() OVER(PARTITION BY email, campaign.name ORDER BY timestamp DESC) AS rownum
    FROM (
      SELECT
        c._sdc_sequence,
        CAST(NULL AS STRING) AS devicetype,
        SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_content=') + 9), '&')[ORDINAL(1)] AS _campaignID,
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
        `x-marketing.duckcreek_hubspot.contacts` c,
        UNNEST(form_submissions) AS form
      JOIN
        `x-marketing.duckcreek_hubspot.forms` forms
      ON
        form.value.form_id = forms.guid
        --WHERE forms.name = '(TGNA) Announcing the Launch Retail-Time Retail'
        ) activity
    LEFT JOIN
      `x-marketing.duckcreek_hubspot.campaigns` campaign
    ON
      activity._campaignID = CAST(campaign.id AS STRING) )
  WHERE
    rownum = 1 
    -- AND _email LIKE '%kikocosmetics.com'
)
SELECT
  engagements.*,
  prospect_info.* EXCEPT (_email),
  name
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
  airtable_info
ON
  engagements._campaignID = CAST(airtable_info.id AS STRING)
UNION ALL
SELECT
  engagements.*,
  prospect_info.* EXCEPT (_email),
  name
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
FROM (
  SELECT * FROM email_download 
  ) AS engagements
LEFT JOIN
  prospect_info
ON
  engagements._email = prospect_info._email
JOIN
  airtable_info
ON
  engagements._campaignID = CAST(airtable_info.id AS STRING); 


------------------------------------------------------------------------
------------------------------- UPDATES --------------------------------
------------------------------------------------------------------------


-- Label Clicks That Are Visits and Set their Page Views
UPDATE `x-marketing.duckcreek.db_campaign_analysis_new` origin  
SET 
    origin.isPageView = true, 
    origin.pageViews = scenario.pageviews,
    origin.reducedPageViews = scenario.pageviews / scenario.visitors
FROM (
    SELECT  
        email._sdc_sequence,
        COUNT(DISTINCT web._visitorid) AS visitors,
        SUM(web._totalsessionviews) AS pageviews
    FROM `x-marketing.duckcreek.db_campaign_analysis` email 
    JOIN (
        SELECT DISTINCT
            _date,
            _visitorid,
            _fullpage,
            _totalsessionviews,
            _utmmedium
        FROM `x-marketing.duckcreek.web_metrics`
    ) web
    ON DATE(email._timestamp) = DATE(web._date)
    AND SPLIT(email._description, '?')[ORDINAL(1)] = web._fullpage
    WHERE email._engagement = 'clicked'
    --AND email._description IS NOT NULL
    --AND email._description NOT LIKE '%mailto%'
    --AND web._utmmedium LIKE '%email%'
    GROUP BY 1
) scenario
WHERE origin._sdc_sequence = scenario._sdc_sequence;



--- Label Bots
UPDATE `x-marketing.duckcreek.db_campaign_analysis` origin  
SET origin._isBot = 'Yes'
FROM (
    SELECT
        CASE WHEN TIMESTAMP_DIFF(click._timestamp, open._timestamp, SECOND) <= 10 THEN click._email 
        ELSE NULL 
        END AS _email, 
        click._utm_campaign 
    FROM `x-marketing.duckcreek.db_campaign_analysis` AS click
    JOIN `x-marketing.duckcreek.db_campaign_analysis` AS open ON LOWER(click._email) = LOWER(open._email)
    AND click._utm_campaign = open._utm_campaign
    WHERE click._engagement = 'Clicked'AND open._engagement = 'Opened'
    EXCEPT DISTINCT
    SELECT 
        conversion._email, 
        conversion._utm_campaign
    FROM `x-marketing.duckcreek.db_campaign_analysis` AS conversion
    WHERE conversion._engagement = 'Downloaded'
) bot
WHERE 
    origin._email = bot._email
AND origin._utm_campaign = bot._utm_campaign
AND origin._engagement IN ('Clicked','Opened');

--- Set Show Export
UPDATE `x-marketing.duckcreek.db_campaign_analysis` origin
SET origin._showExport = 'Yes'
FROM (
    WITH focused_engagement AS (
        SELECT 
            _email, 
            _engagement, 
            _utm_campaign,
            CASE WHEN _engagement = 'Opened' THEN 1
                WHEN _engagement = 'Clicked' THEN 2
                WHEN _engagement = 'Downloaded' THEN 3
            END AS _priority
        FROM `x-marketing.duckcreek.db_campaign_analysis`
        WHERE _engagement IN('Opened', 'Clicked', 'Downloaded')
        ORDER BY 1, 3, 4 DESC 
    ),
    final_engagement AS (
        SELECT * EXCEPT(_priority, _rownum)
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY _email, _utm_campaign ORDER BY _priority DESC) AS _rownum
            FROM focused_engagement
        )
        WHERE _rownum = 1
    )    
    SELECT * FROM final_engagement 
) AS final
WHERE origin._email = final._email
AND origin._engagement = final._engagement
AND origin._utm_campaign = final._utm_campaign;

UPDATE `x-marketing.duckcreek.db_campaign_analysis` origin
SET origin._dropped = 'True'
FROM (
    SELECT 
        _utm_campaign, 
        _email
    FROM (
        SELECT 
            _utm_campaign, 
            _email,
            SUM(CASE WHEN _engagement = 'Sent' THEN 1 END) AS _hasSent,
            SUM(CASE WHEN _engagement = 'Delivered' THEN 1 END) AS _hasDelivered,
            SUM(CASE WHEN _engagement = 'Bounced' THEN 1 END) AS _hasBounced
        FROM 
            `x-marketing.duckcreek.db_campaign_analysis`
        WHERE
            _engagement IN ('Sent', 'Delivered', 'Bounced')
        GROUP BY
            1, 2
    )
    WHERE 
        _hasSent IS NOT NULL
    AND _hasDelivered IS NOT NULL
    AND _hasBounced IS NOT NULL
) scenario
WHERE 
    origin._email = scenario._email
AND origin._utm_campaign = scenario._utm_campaign
AND origin._engagement IN('Delivered', 'Bounced');

UPDATE `x-marketing.duckcreek.db_campaign_analysis` origin
SET origin._notSent = 'True'
FROM (
    SELECT 
        _utm_campaign,
        _email,
    FROM (
        SELECT 
            _utm_campaign, 
            _email,
            SUM(CASE WHEN _engagement = 'Sent' THEN 1 END) AS _hasSent,
            SUM(CASE WHEN _engagement = 'Dropped' THEN 1 END) AS _hasdrop,
            SUM(CASE WHEN _engagement = 'Deffered' THEN 1 END) AS _hasdef
        FROM 
            `x-marketing.duckcreek.db_campaign_analysis`
        WHERE
            _engagement IN ('Sent', 'Deffered','Dropped')
        GROUP BY
            1, 2
    )
    WHERE 
        _hasSent IS NOT NULL
    AND _hasdrop IS NOT NULL
    AND _hasdef IS NOT NULL
) scenario
WHERE
    origin._email = scenario._email
AND origin._utm_campaign = scenario._utm_campaign
AND origin._engagement = 'Sent';

---False delivered

UPDATE `x-marketing.duckcreek.db_campaign_analysis` origin
SET origin._falseDelivered = 'True'
FROM (

      SELECT 
        _utm_campaign, 
        _email,
        _hasSent,_hasdrop
    FROM (
        SELECT 
            _utm_campaign, 
            _email,
            SUM(CASE WHEN _engagement = 'Delivered' THEN 1 END) AS _hasSent,
            SUM(CASE WHEN _engagement = 'Bounced' THEN 1 END) AS _hasdrop,
        FROM 
            `x-marketing.duckcreek.db_campaign_analysis`
        WHERE
            _engagement IN ('Delivered', 'Bounced')
        GROUP BY
            1, 2
    )
    WHERE 
        _hasSent IS NOT NULL
    AND _hasdrop IS NOT NULL

) scenario
WHERE
    origin._email = scenario._email
AND origin._utm_campaign = scenario._utm_campaign
AND origin._engagement IN ( 'Delivered');

-- Label Clicks That Are Visits and Set their Page Views
UPDATE `duckcreek.db_campaign_analysis_new` origin
SET 
    origin.isPageView = true, 
    origin.pageViews = scenario.pageviews,
    origin.reducedPageViews = scenario.pageviews / scenario.visitors
FROM (
    SELECT  
        email._sdc_sequence,
        COUNT(DISTINCT web._visitorid) AS visitors,
        SUM(web._totalsessionviews) AS pageviews
    FROM `x-marketing.duckcreek.db_campaign_analysis` email 
    JOIN (
        SELECT DISTINCT
            -- _date,
            _timestamp,
            _visitorid,
            -- _fullpage,
            _fullurl,
            _totalsessionviews,
            _utmmedium
        FROM `x-marketing.toolsgroup.web_metrics`
    ) web
    ON DATE(email._timestamp) = DATE(web._timestamp)
    AND SPLIT(email._description, '?')[ORDINAL(1)] = web._fullurl
    WHERE email._engagement = 'clicked'
    --AND email._description IS NOT NULL
    --AND email._description NOT LIKE '%mailto%'
    --AND web._utmmedium LIKE '%email%'
    GROUP BY 1
) scenario
WHERE origin._sdc_sequence = scenario._sdc_sequence;

---Forwarded email (has no open event but has click)

/*UPDATE `x-marketing.duckcreek.db_campaign_analysis` origin
SET origin._isForwarded = 'True'
FROM (
    SELECT
        _utm_contentTitle,
        _email
    FROM (
        SELECT
            _utm_contentTitle,
            _email,
            SUM(CASE WHEN _engagement = 'Delivered' THEN 1 END) AS _hasDelivered,
            SUM(CASE WHEN _engagement = 'Opened' THEN 1 END) AS _hasOpened,
            SUM(CASE WHEN _engagement = 'Clicked' THEN 1 END) AS _hasClicked,
        FROM
            `x-marketing.duckcreek.db_campaign_analysis`
        WHERE
            _engagement IN ('Delivered', 'Opened', 'Clicked')
        GROUP BY
            1, 2
    )
    WHERE
        _hasDelivered IS NOT NULL
    AND _hasOpened IS NULL
    AND _hasClicked IS NOT NULL
) scenario
WHERE
    origin._email = scenario._email
AND origin._utm_campaign = scenario._utm_campaign
AND origin._engagement = 'Clicked';

--- Label Forwarded Emails
UPDATE `x-marketing.duckcreek.db_campaign_analysis` origin  
SET origin._isForwarded = 'Forwarded'
FROM (
    SELECT _email, _utm_campaign
    FROM `x-marketing.duckcreek.db_campaign_analysis`
    WHERE _engagement = 'Downloaded' 
    AND _campaignID IS NOT NULL AND _campaignID != ''
    EXCEPT DISTINCT
    SELECT _email, _utm_campaign
    FROM `x-marketing.duckcreek.db_campaign_analysis`
    WHERE _engagement = 'Sent'
    AND _campaignID IS NOT NULL AND _campaignID != ''
) forward
WHERE origin._email = forward._email
AND origin._campaignID = forward._utm_campaign
AND origin._engagement = 'Downloaded';*/

----------------------------------------------------------------------------
---------------------------- Account Engagement ----------------------------
----------------------------------------------------------------------------

CREATE OR REPLACE TABLE `x-marketing.duckcreek.db_account_engagements` AS 
WITH 
#Query to pull all the contacts in the leads table from Marketo
tam_contacts AS (
  SELECT * EXCEPT(_rownum) 
  FROM (
    SELECT DISTINCT
        COALESCE(CAST(vid AS STRING), property_salesforceleadid.value) AS _leadorcontactid,
        CASE 
          WHEN vid IS NOT NULL THEN "Contact"
          WHEN property_salesforceleadid.value IS NULL THEN "Lead"
        END AS _contact_type,
        property_firstname.value AS _firstname, 
        property_lastname.value AS _lastname, 
        property_jobtitle.value AS _title, 
        CAST(NULL AS STRING) AS _2xseniority,
        property_email.value AS _email,
        properties.salesforceaccountid.value AS _accountid,
        property_hs_email_domain.value AS _domain, 
        property_company.value AS _accountname, 
        property_industry.value AS _industry, 
        COALESCE(properties.tier__c.value__st, CAST(NULL AS STRING)) AS _tier, 
        CAST(properties.total_revenue.value__fl AS INTEGER) AS _annualrevenue,
        ROW_NUMBER() OVER(
            PARTITION BY property_email.value 
            ORDER BY prosp._sdc_received_at DESC
        ) _rownum
    FROM 
      `duckcreek_hubspot.contacts` prosp
    -- LEFT JOIN
    --   `fuelcycle_mysql.w_routables` main ON main._email = prosp.email
    WHERE 
      NOT REGEXP_CONTAINS(property_email.value, 'duckcreek|2x.marketing') 
  )
  WHERE _rownum = 1
),
#Query to pull the email engagement 
email_engagement AS (
    SELECT * 
    FROM ( 
      SELECT _email, 
      RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
      EXTRACT(DATETIME FROM _timestamp) AS _date , 
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year,
      _contentTitle, 
      CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
      _description
      FROM 
        (SELECT * FROM `duckcreek.db_campaign_analysis`)
      WHERE 
        /* (EXTRACT(DATE FROM _timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY ) AND CURRENT_DATE() )
        AND */ 
        LOWER(_engagement) NOT IN ('sent','delivered', 'bounced', 'unsubscribed')
    ) a
    WHERE 
      NOT REGEXP_CONTAINS(_domain,'2x.marketing|duckcreek|gmail|yahoo|outlook|hotmail') 
      AND NOT REGEXP_CONTAINS(_contentTitle, 'test')
      AND _domain IS NOT NULL
    --   AND _year = 2022
    ORDER BY 1, 3 DESC, 2 DESC
),
web_views AS (
  SELECT 
    CAST(NULL AS STRING) AS _email, 
    _domain, 
    _date, 
    EXTRACT(WEEK FROM _date) AS _week,  
    EXTRACT(YEAR FROM _date) AS _year, 
    _page AS _pageName, 
    "Web Visit" AS _engagement, 
    CAST(_engagementtime AS STRING) AS _description,
  FROM 
    `duckcreek.web_metrics` web 
  WHERE 
    NOT REGEXP_CONTAINS(_page, 'Unsubscribe')
    AND NOT REGEXP_CONTAINS(LOWER(_utmsource), 'linkedin|google|email') 
    AND (_domain IS NOT NULL AND _domain != '')
  ORDER BY 
    _date DESC
),
ad_clicks AS (
 SELECT 
    CAST(NULL AS STRING) AS _email, 
    _domain, 
    _date, 
    EXTRACT(WEEK FROM _date) AS _week,  
    EXTRACT(YEAR FROM _date) AS _year, 
    _page AS _pageName, 
    "Ad Clicks" AS _engagement, 
    _fullpage AS _description
  FROM 
    `x-marketing.duckcreek.web_metrics` web 
  WHERE 
    NOT REGEXP_CONTAINS(_page, 'Unsubscribe')
    AND REGEXP_CONTAINS(LOWER(_utmsource), 'linkedin|google')
    AND (_domain IS NOT NULL AND _domain != '')
  ORDER BY 
    _date DESC
),
content_engagement AS (
  SELECT 
    CAST(NULL AS STRING) AS _email, 
    _domain, 
    _date, 
    EXTRACT(WEEK FROM _date) AS _week,  
    EXTRACT(YEAR FROM _date) AS _year, 
    _page AS _pageName, 
    "Content Engagement" AS _engagement, 
    _page AS _description
  FROM 
    `duckcreek.web_metrics` web 
  WHERE 
    NOT REGEXP_CONTAINS(_page, 'Unsubscribe')
    AND REGEXP_CONTAINS(LOWER(_page), 'blog|commid=')
    AND (_domain IS NOT NULL AND _domain != '')
  ORDER BY 
    _date DESC
),
form_fills AS (
  SELECT * 
  FROM ( 
      SELECT _email, 
      RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
      EXTRACT(DATETIME FROM _timestamp) AS _date , 
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year,
      _contentTitle, 
      CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
      _description
      FROM 
      (SELECT * FROM `duckcreek.db_campaign_analysis`)
      WHERE 
      /* (EXTRACT(DATE FROM _timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY ) AND CURRENT_DATE() )
      AND */ 
      _engagement IN ('Downloaded')
  ) a
  WHERE 
      NOT REGEXP_CONTAINS(_domain,'2x.marketing|duckcreek|gmail|yahoo|outlook|hotmail') 
      AND _domain IS NOT NULL 
  ORDER BY 1, 3 DESC, 2 DESC
),
dummy_dates AS ( # Each domain needs to be shown regardless if they are part of the bombora report or has 0 engagements
  SELECT
    _date,
    EXTRACT(WEEK FROM _date) AS _week,
    EXTRACT(YEAR FROM _date) AS _year
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS _date 
),
#Combining the engagements - Contact based and account based engagements
contact_engagement AS (
#Contact based engagement query
  SELECT 
    DISTINCT 
    tam_contacts._domain, 
    tam_contacts._email,
    dummy_dates.*EXCEPT(_date), 
    engagements.*EXCEPT(_date, _week, _year, _domain, _email),
    -- CAST(NULL AS INTEGER) AS _avg_bombora_score,
    tam_contacts.*EXCEPT(_domain, _email),
    engagements._date
  FROM 
    dummy_dates
  JOIN (
    SELECT * FROM email_engagement 
    UNION ALL
    SELECT * FROM form_fills
  ) engagements USING(_week, _year)
  JOIN
    tam_contacts USING(_email) 
),
account_engagement AS (
#Account based engagement query
   SELECT 
    DISTINCT 
    tam_accounts._domain, 
    CAST(NULL AS STRING) AS _email,
    dummy_dates.*EXCEPT(_date), 
    engagements.*EXCEPT(_date, _week, _year, _domain, _email),
    CAST(NULL AS STRING) AS _id, 
    CAST(NULL AS STRING) AS _contact_type,
    CAST(NULL AS STRING) AS _firstname, 
    CAST(NULL AS STRING) AS _lastname,
    CAST(NULL AS STRING) AS _title,
    CAST(NULL AS STRING) AS _2xseniority,
    tam_accounts.*EXCEPT(_domain),
    engagements._date
  FROM 
    dummy_dates
  JOIN (
    /* SELECT * FROM intent_score UNION ALL */
    SELECT * FROM web_views UNION ALL
    SELECT * FROM ad_clicks UNION ALL
    SELECT * FROM content_engagement
  ) engagements USING(_week, _year)
  JOIN
    (
      SELECT 
        DISTINCT _domain, 
        _accountid, 
        _accountname, 
        _industry, 
        _tier, 
        _annualrevenue 
      FROM 
        tam_contacts
    ) tam_accounts
    USING(_domain)
),
combined_engagements AS (
  -- SELECT * FROM contact_engagement
  -- UNION DISTINCT
  SELECT * FROM account_engagement
)
SELECT 
  DISTINCT
  _domain,
  _accountid,
  _date,
  SUM(IF(_engagement = 'Email Opened', 1, 0)) AS _emailOpens,
  SUM(IF(_engagement = 'Email Clicked', 1, 0)) AS _emailClicks,
  SUM(IF(_engagement = 'Email Downloaded', 1, 0)) AS _emailDownloads,
  SUM(IF(_engagement = 'Form Filled', 1, 0)) AS _gatedForms,
  SUM(IF(_engagement = 'Web Visit', 1, 0)) AS _webVisits,
  SUM(IF(_engagement = 'Ad Clicks', 1, 0)) AS _adClicks,
FROM 
  combined_engagements
GROUP BY 
  1, 2, 3
ORDER BY _date DESC
;
