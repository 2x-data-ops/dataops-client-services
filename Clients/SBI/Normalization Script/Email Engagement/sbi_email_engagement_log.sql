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

TRUNCATE TABLE `x-marketing.sbi.db_email_engagements_log`;
INSERT INTO `x-marketing.sbi.db_email_engagements_log` (
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
 _campaignSentDate,
 _plan_date,
 _campaignName, 
 _campaign_subject, 
 _what_we_do, 
 _campaign_type,
_email_name,
 _landingPage, 
 _email_sequence, 
 _email_sequence_st, 
 _asset_title, 
 _persona_campaign,
 _screenshot,
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
)
WITH
  prospect_info AS (
      SELECT
        DISTINCT CAST(vid AS STRING) AS _id,
        property_email.value AS _email,
        CONCAT(property_firstname.value, ' ', property_lastname.value) AS _name,
        properties.hs_email_domain.value AS _domain,
        property_jobtitle.value AS _jobTitle,
        properties.job_function.value AS _function,
        CASE 
          WHEN LOWER(properties.jobtitle.value) LIKE '%Senior Counsel%' THEN "VP"
          WHEN LOWER(properties.jobtitle.value) LIKE '%Assistant General Counsel%' THEN "VP" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%General Counsel%' THEN "C-Level" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%Founder%' THEN "C-Level" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%C-Level%' THEN "C-Level" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%CDO%' THEN "C-Level" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%CIO%' THEN "C-Level"
          WHEN LOWER(properties.jobtitle.value) LIKE '%CMO%' THEN "C-Level"
          WHEN LOWER(properties.jobtitle.value) LIKE '%CFO%' THEN "C-Level" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%CEO%' THEN "C-Level"
          WHEN LOWER(properties.jobtitle.value) LIKE '%Chief%' THEN "C-Level" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%coordinator%' THEN "Non-Manager"
          WHEN LOWER(properties.jobtitle.value) LIKE '%COO%' THEN "C-Level" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%Sr. V.P.%' THEN "Senior VP"
          WHEN LOWER(properties.jobtitle.value) LIKE '%Sr.VP%' THEN "Senior VP"  
          WHEN LOWER(properties.jobtitle.value) LIKE '%Senior-Vice Pres%' THEN "Senior VP"  
          WHEN LOWER(properties.jobtitle.value) LIKE '%srvp%' THEN "Senior VP" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%Senior VP%' THEN "Senior VP" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%SR VP%' THEN "Senior VP"  
          WHEN LOWER(properties.jobtitle.value) LIKE '%Sr Vice Pres%' THEN "Senior VP" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%Sr. VP%' THEN "Senior VP" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%Sr. Vice Pres%' THEN "Senior VP"  
          WHEN LOWER(properties.jobtitle.value) LIKE '%S.V.P%' THEN "Senior VP" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%Senior Vice Pres%' THEN "Senior VP"  
          WHEN LOWER(properties.jobtitle.value) LIKE '%Exec Vice Pres%' THEN "Senior VP" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%Exec Vp%' THEN "Senior VP"  
          WHEN LOWER(properties.jobtitle.value) LIKE '%Executive VP%' THEN "Senior VP" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%Exec VP%' THEN "Senior VP"  
          WHEN LOWER(properties.jobtitle.value) LIKE '%Executive Vice President%' THEN "Senior VP" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%EVP%' THEN "Senior VP"  
          WHEN LOWER(properties.jobtitle.value) LIKE '%E.V.P%' THEN "Senior VP" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%SVP%' THEN "Senior VP" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%V.P%' THEN "VP" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%VP%' THEN "VP" 
          WHEN LOWER(properties.jobtitle.value) LIKE '%Vice Pres%' THEN "VP"
          WHEN LOWER(properties.jobtitle.value) LIKE '%V P%' THEN "VP"
          WHEN LOWER(properties.jobtitle.value) LIKE '%President%' THEN "C-Level"
          WHEN LOWER(properties.jobtitle.value) LIKE '%Director%' THEN "Director"
          WHEN LOWER(properties.jobtitle.value) LIKE '%CTO%' THEN "C-Level"
          WHEN LOWER(properties.jobtitle.value) LIKE '%Dir%' THEN "Director"
          WHEN LOWER(properties.jobtitle.value) LIKE '%MDR%' THEN "Non-Manager"
          WHEN LOWER(properties.jobtitle.value) LIKE '%MD%' THEN "Director"
          WHEN LOWER(properties.jobtitle.value) LIKE '%GM%' THEN "Director"
          WHEN LOWER(properties.jobtitle.value) LIKE '%Head%' THEN "VP"
          WHEN LOWER(properties.jobtitle.value) LIKE '%Manager%' THEN "Manager"
          WHEN LOWER(properties.jobtitle.value) LIKE '%escrow%' THEN "Non-Manager"
          WHEN LOWER(properties.jobtitle.value) LIKE '%cross%' THEN "Non-Manager"
          WHEN LOWER(properties.jobtitle.value) LIKE '%crosse%' THEN "Non-Manager"
          WHEN LOWER(properties.jobtitle.value) LIKE '%Assistant%' THEN "Non-Manager"
          WHEN LOWER(properties.jobtitle.value) LIKE '%Partner%' THEN "C-Level"
          WHEN LOWER(properties.jobtitle.value) LIKE '%CRO%' THEN "C-Level"
          WHEN LOWER(properties.jobtitle.value) LIKE '%Chairman%' THEN "C-Level"
          WHEN LOWER(properties.jobtitle.value) LIKE '%Owner%' THEN "C-Level"
        END AS _seniority,
        property_phone.value AS _phone,
        property_company.value AS _company,
        associated_company.properties.annualrevenue.value AS _revenueevenue,
        INITCAP(REPLACE(associated_company.properties.industry.value, '_', ' ')) AS _industry,
        COALESCE(
          property_city.value,
          associated_company.properties.city.value
        ) AS _city,
        COALESCE(
          property_state.value,
          associated_company.properties.state.value
        ) AS _state,
        COALESCE(
          property_country.value, 
          associated_company.properties.country.value
        ) AS _country,
        COALESCE(
          properties.persona.value,
          property_persona.value
        ) AS _persona,
        CASE
          WHEN property_lifecyclestage.value = 'marketingqualifiedlead' THEN 'Marketing Qualified Lead' 
          WHEN property_lifecyclestage.value = 'salesqualifiedlead' THEN 'Sales Qualified Lead' 
          ELSE INITCAP(property_lifecyclestage.value)
        END AS _lifecycleStage,
      FROM
        `x-marketing.sbi_hubspot.contacts`
      WHERE
        property_email.value IS NOT NULL
        AND property_email.value NOT LIKE '%2x.marketing%'
        AND property_email.value NOT LIKE '%sbi.com%'
  ),
  airtable_info AS (
    SELECT
      id,
      /*'' AS subtype,
      campaign.subject,
      contentid,
      campaign.type,
     name,*/
      ------------------------------------------airtable-------------------------
      
      CAST(_sendtime AS TIMESTAMP),
      CAST(_livedate AS TIMESTAMP),
      airtable._campaign, 
      airtable._subject, 
      _whatwedo, 
      airtable._type as campaign_type, 
      _emailname, 
      _landingpage, 
      CAST(	_emailsequence AS STRING), 
      REGEXP_EXTRACT(_emailsequence, r'(\d+)$') AS extracted_number, 
      _assettitle, 
      _persona,
      _screenshot

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
      --'' AS _2x_campaign
    FROM
      `x-marketing.sbi_hubspot.campaigns` campaign
      -- WHERE id IN (247413984,247366742,247239377,245978650,245935666,245977325,245936306,245369808,245369806,245369807,244174255)
      JOIN `x-marketing.sbi_mysql.db_airtable_email` airtable ON CAST(campaign.id AS STRING) = CAST(airtable._campaignid AS STRING)
      -- WHERE _pardotid IS NOT NULL
  ),
  email_sent AS (
    -- SELECT
    --   * EXCEPT(_rownum)
    -- FROM (
    --   SELECT
    --     activity._sdc_sequence,
    --     CAST(activity.emailcampaignid AS STRING) AS _campaignID,
    --     campaign.name AS _campaign,
    --     activity.subject AS _subject,
    --     activity.recipient AS _email,
    --     activity.created AS _timestamp,
    --     'Sent' AS _engagement,
    --     url AS _description, 
    --     devicetype AS _devicetype,
    --     CAST(linkid AS STRING),
    --     --appname,
    --     CAST(duration AS STRING),
    --     response,
    --     ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    --   FROM
    --     `x-marketing.sbi_hubspot.email_events` activity
    --   JOIN
    --     `x-marketing.sbi_hubspot.campaigns` campaign
    --   ON
    --     activity.emailcampaignid = campaign.id
    --   WHERE
    --     activity.type = 'SENT' /*AND activity.recipient NOT LIKE '%2x.marketing%'
    --   AND activity.recipient NOT LIKE '%sbi.com%'*/
    --     AND campaign.name IS NOT NULL )
    -- WHERE
    --   _rownum = 1 
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
      FROM `x-marketing.sbi_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.sbi_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.sbi_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
      WHERE 
      activity.type = 'BOUNCE' 
      ---AND emailcampaignid = 269760036
      AND 
      status.value.change = 'BOUNCED'  
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
      `x-marketing.sbi_hubspot.email_events` activity
    JOIN
      `x-marketing.sbi_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'SENT'  
     
     )
  WHERE
    _rownum = 1 
) SELECT Sent .* FROM Sent
LEFT JOIN bounced ON Sent._email = bounced._email and Sent._campaignID = bounced._campaignID
WHERE bounced._email IS NULL 
--AND Sent._campaignID = "279480425"
  ),
  email_delivered AS (
    -- SELECT
    --   * EXCEPT(_rownum)
    -- FROM (
    --   SELECT
    --     activity._sdc_sequence,
    --     CAST(activity.emailcampaignid AS STRING) AS _campaignID,
    --     campaign.name AS _campaign,
    --     activity.subject AS _subject,
    --     activity.recipient AS _email,
    --     activity.created AS _timestamp,
    --     'Delivered' AS _engagement,
    --     url AS _description,
    --     devicetype AS _devicetype,
    --     CAST(CAST(linkid AS STRING) AS STRING),
    --     --appname,
    --     CAST(duration AS STRING),
    --     response,
    --     ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    --   FROM
    --     `x-marketing.sbi_hubspot.email_events` activity
    --   JOIN
    --     `x-marketing.sbi_hubspot.campaigns` campaign
    --   ON
    --     activity.emailcampaignid = campaign.id
    --   WHERE
    --     activity.type = 'DELIVERED' /*AND activity.recipient NOT LIKE '%2x.marketing%'
    --   AND activity.recipient NOT LIKE '%sbi.com%'*/
    --     AND campaign.name IS NOT NULL )
    -- WHERE
    --   _rownum = 1
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
      FROM `x-marketing.sbi_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.sbi_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.sbi_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
      WHERE 
      activity.type = 'BOUNCE'
      AND 
      status.value.change = 'BOUNCED'  
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
      `x-marketing.sbi_hubspot.email_events` activity
    JOIN
      `x-marketing.sbi_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'DELIVERED'  
   
      AND campaign.name IS NOT NULL  )
  WHERE
    _rownum = 1
) SELECT delivered .* FROM delivered 
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
        `x-marketing.sbi_hubspot.email_events` activity
      JOIN
        `x-marketing.sbi_hubspot.campaigns` campaign
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
        `x-marketing.sbi_hubspot.email_events` activity
      JOIN
        `x-marketing.sbi_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'CLICK'
        AND filteredevent = FALSE
        --AND activity.recipient NOT LIKE '%2x.marketing%'
        --AND activity.recipient NOT LIKE '%sbi.com%'
        AND campaign.name IS NOT NULL )
    WHERE
      _rownum = 1
  ),
  email_softbounce AS (
    -- SELECT
    --   * EXCEPT(_rownum)
    -- FROM (
    --   SELECT
    --     activity._sdc_sequence,
    --     CAST(activity.emailcampaignid AS STRING) AS _campaignID,
    --     campaign.name AS _campaign,
    --     activity.subject AS _subject,
    --     activity.recipient AS _email,
    --     activity.created AS _timestamp,
    --     'Bounced' AS _engagement,
    --     url AS _description,
    --     devicetype AS _devicetype,
    --     CAST(linkid AS STRING),
    --     --appname,
    --     CAST(duration AS STRING),
    --     response,
    --     ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    --   FROM
    --     `x-marketing.sbi_hubspot.email_events` activity
    --   JOIN
    --     `x-marketing.sbi_hubspot.campaigns` campaign
    --   ON
    --     activity.emailcampaignid = campaign.id
    --   WHERE
    --     activity.type = 'BOUNCE'
    --     AND campaign.id = 279480496
    --     --AND activity.recipient NOT LIKE '%2x.marketing%'
    --    -- AND activity.recipient NOT LIKE '%sbi.com%'
    --     AND campaign.name IS NOT NULL )
    -- WHERE
    --   _rownum = 1
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
        FROM `x-marketing.sbi_hubspot.subscription_changes`, UNNEST(changes) AS status 
        JOIN `x-marketing.sbi_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
        JOIN `x-marketing.sbi_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
        WHERE 
        activity.type = 'BOUNCE'
        AND 
        status.value.change = 'BOUNCED'  
        --AND campaign.id = 279480425
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
        `x-marketing.sbi_hubspot.email_events` activity
      JOIN
        `x-marketing.sbi_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'BOUNCE'  
    
      -- AND campaign.name IS NOT NULL AND campaign.id = 279480425  
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
        `x-marketing.sbi_hubspot.email_events` activity
      JOIN
        `x-marketing.sbi_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'DELIVERED'  
    
        AND campaign.name IS NOT NULL  )
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
      FROM `x-marketing.sbi_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.sbi_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.sbi_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
      WHERE 
      activity.type = 'BOUNCE'
      AND 
      status.value.change = 'BOUNCED'  
      --AND campaign.id = 279480496
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
      `x-marketing.sbi_hubspot.email_events` activity
    JOIN
      `x-marketing.sbi_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'BOUNCE'  
   
      AND campaign.name IS NOT NULL  )
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
      `x-marketing.sbi_hubspot.email_events` activity
    JOIN
      `x-marketing.sbi_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'DELIVERED'  
   
      AND campaign.name IS NOT NULL  )
  WHERE
    _rownum = 1
) SELECT email_bounce .* FROM email_bounce
JOIN bounced ON email_bounce._email = bounced._email and email_bounce._campaignID = bounced._campaignID
JOIN delivered ON email_bounce._email = delivered._email and email_bounce._campaignID = delivered._campaignID
--WHERE  email_bounce._campaignID = '279480425'


  ),unsubsribe AS (
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
      FROM `x-marketing.sbi_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.sbi_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.sbi_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
  airtable_info.* EXCEPT (id),
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




