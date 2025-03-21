TRUNCATE TABLE `x-marketing.3x.db_email_engagements_log`;
INSERT INTO `x-marketing.3x.db_email_engagements_log` (
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
  _3x_campaign,
  _quater_segment,
  _category,
  _campaign_subject
)
WITH 
prospect_info AS (
  SELECT * EXCEPT (_rownum)
  FROM (
    SELECT 
    DISTINCT CAST(vid AS STRING) AS _id,
    property_email.value AS _email,
    CONCAT(property_firstname.value, ' ', property_lastname.value) AS _name,
    properties.hs_email_domain.value AS _domain,
    property_jobtitle.value AS _jobTitle,
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
    COALESCE(
        associated_company.properties.name.value,
        property_company.value
    ) AS _company,
    CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenueevenue,
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
        CASE
          WHEN property_lifecyclestage.value = 'marketingqualifiedlead' THEN 'Marketing Qualified Lead' 
          WHEN property_lifecyclestage.value = 'salesqualifiedlead' THEN 'Sales Qualified Lead' 
          ELSE INITCAP(property_lifecyclestage.value)
        END AS _lifecycleStage,
      ROW_NUMBER() OVER(
                PARTITION BY property_email.value 
                ORDER BY vid DESC
            ) 
            AS _rownum
      FROM
        `x-marketing.x3x_hubspot.contacts`
      --WHERE
        --property_email.value IS NOT NULL
        --AND property_email.value NOT LIKE '%2x.marketing%'
  ) WHERE _rownum = 1
),
airtable_info AS (
    SELECT 
        * EXCEPT(_rownum), 
        CASE WHEN _contentTitle IS NULL THEN 'Not 3X campaign' ELSE '3X campaign' END AS _3x_campaign, 
        CASE WHEN EXTRACT(MONTH FROM _liveDate) IN(1,2,3)  THEN "Q1" 
        WHEN EXTRACT(MONTH FROM _liveDate) IN(4,5,6) THEN "Q2"
        WHEN EXTRACT(MONTH FROM _liveDate) IN(7,8,9) THEN "Q3"
        WHEN EXTRACT(MONTH FROM _liveDate) IN(10,11,12) THEN "Q3"
        ELSE NULL 
    END AS _quater_segment
    FROM (

        SELECT 

            CAST(campaign.id AS STRING) AS id,
            SAFE_CAST(airtable._livedate AS TIMESTAMP) AS _liveDate,
            name  AS _contentTitle,
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
        JOIN
             `x-marketing.x_mysql.db_airtable_3x_email` airtable
        ON 
             CAST(campaign.id AS STRING) = airtable._emailid 
            

    ) 
    WHERE _rownum = 1 
   -- AND id IN ( "295023697",'295996323','296375135')
),
email_sent AS (
   WITH dropped AS (
       SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _contentTitle,
      campaign.contentid AS _contentID,
      --activity.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      activity.type AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      '',
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.x3x_hubspot.email_events` activity
    JOIN
      `x-marketing.x3x_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE 
    activity.type = 'DROPPED' 
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
LEFT JOIN dropped  ON Sent._email = dropped._email and Sent._campaignID = dropped._campaignID
WHERE dropped._email IS NULL 
--AND Sent._campaignID = "279480425"
  ),
email_delivered AS (

   WITH dropped AS (
        SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _contentTitle,
      campaign.contentid AS _contentID,
      --activity.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      activity.type AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      '',
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.x3x_hubspot.email_events` activity
    JOIN
      `x-marketing.x3x_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE 
    activity.type = 'DROPPED'
), bounced AS (
     SELECT 
        * EXCEPT(_rownum)
        FROM (
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
      response AS _response,
      ROW_NUMBER() OVER( PARTITION BY c.recipient, activity.emailcampaignid  ORDER BY c.timestamp DESC) AS _rownum
      FROM `x-marketing.x3x_hubspot.subscription_changes` c, UNNEST(changes) AS status 
      JOIN `x-marketing.x3x_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.x3x_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
      WHERE 
      --activity.type = 'BOUNCE'
      --AND 
      status.value.change = 'BOUNCED'  
     
      )
  WHERE
    _rownum = 1
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
    
      -- AND campaign.name IS NOT NULL AND campaign.id = 279480425  
        )
    WHERE
      _rownum = 1
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
  )
  WHERE
    _rownum = 1
) SELECT delivered .* FROM delivered 
LEFT JOIN dropped ON delivered._email = dropped._email and delivered._campaignID = dropped._campaignID
LEFT JOIN bounced ON delivered._email = bounced._email and delivered._campaignID = bounced._campaignID
LEFT JOIN email_bounce ON delivered._email = email_bounce._email and delivered._campaignID = email_bounce._campaignID
WHERE bounced._email IS NULL AND dropped._email IS NULL 
AND email_bounce._email IS NULL 

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
       -- AND campaign.name IS NOT NULL 
        )
    WHERE
      _rownum = 1
  ),
  email_softbounce AS (

       WITH bounced AS (
         SELECT 
        * EXCEPT(_rownum)
        FROM (
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
      response AS _response,
      ROW_NUMBER() OVER( PARTITION BY c.recipient, activity.emailcampaignid  ORDER BY c.timestamp DESC) AS _rownum
      FROM `x-marketing.x3x_hubspot.subscription_changes` c, UNNEST(changes) AS status 
      JOIN `x-marketing.x3x_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.x3x_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
      WHERE 
      --activity.type = 'BOUNCE'
      --AND 
      status.value.change = 'BOUNCED'  
     
      )
  WHERE
    _rownum = 1
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
  WHERE  bounced._email IS NULL 
  ),email_hardBounce AS (
    WITH bounced AS (
      SELECT 
        * EXCEPT(_rownum)
        FROM (
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
      response AS _response,
      ROW_NUMBER() OVER( PARTITION BY c.recipient, activity.emailcampaignid  ORDER BY c.timestamp DESC) AS _rownum
      FROM `x-marketing.x3x_hubspot.subscription_changes` c, UNNEST(changes) AS status 
      JOIN `x-marketing.x3x_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.x3x_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
      WHERE 
      --activity.type = 'BOUNCE'
      --AND 
      status.value.change = 'BOUNCED'  
     
      )
  WHERE
    _rownum = 1 
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
        LEFT JOIN delivered ON email_bounce._email = delivered._email and email_bounce._campaignID = delivered._campaignID

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
      ---AND activity.recipient NOT LIKE '%2x.marketing%'
    
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
     SELECT *,
  CASE WHEN static_list_id = 1976 THEN "295023697" 
    WHEN static_list_id = 2233 THEN "310225715"
    WHEN static_list_id = 2544 THEN "321034018"
    ELSE NULL 
    END AS _campaignid
     FROM (
    SELECT  
      CAST(vid AS STRING) AS _id,
      property_email.value AS _email,
      CONCAT(property_firstname.value, ' ', property_lastname.value) AS _name,
      properties.hs_email_domain.value AS _domain,
      property_jobtitle.value AS _jobTitle,
      properties.job_function.value AS _function,
      list_memberships.value.timestamp AS _timestamp,
      activity._sdc_sequence,
    CASE WHEN vid = 50308113847 AND list_memberships.value.static_list_id = 2524 THEN 2544
    WHEN vid = 50315245887 AND list_memberships.value.static_list_id = 2524 THEN 2544
    ELSE 
list_memberships.value.static_list_id END static_list_id,
    FROM `x-marketing.x3x_hubspot.contacts` activity, 
    Unnest (list_memberships) list_memberships
    LEFT JOIN `x-marketing.x3x_hubspot.contact_lists` list ON list_memberships.value.static_list_id = list.
    listid
  )

    WHERE /*properties.email.value = '1chimneymanplus@gmail.com'*/ static_list_id IN
    (1976,2233,2544)
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
), customer_external AS (
  SELECT
    --customer_townhall_reg. *,
    customer_townhall_reg. _sdc_sequence,
    customer_townhall_reg._campaignid ,
    customer_townhall_reg._campaign,
    customer_townhall_reg._subject,
    customer_townhall_reg._email,
    customer_townhall_reg._timestamp,
   "External Register" AS _engagement,
    customer_townhall_reg._description,
     customer_townhall_reg. _devicetype,
    _linkid,
    _duration,
    _response,
    FROM customer_reg customer_townhall_reg
    LEFT JOIN airtable_info campaign  ON 
           customer_townhall_reg._campaignID
 = campaign.id
 LEFT JOIN email_sent ON customer_townhall_reg._email = email_sent._email
    WHERE 
    email_sent._email  IS NULL 
), false_delivered AS ( 
  WITH dropped AS (
        SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _contentTitle,
      campaign.contentid AS _contentID,
      --activity.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      activity.type AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      '',
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.x3x_hubspot.email_events` activity
    JOIN
      `x-marketing.x3x_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE 
    activity.type = 'DROPPED'
), bounced AS (
   SELECT
      * EXCEPT(_rownum)
    FROM (
    SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID, 
      campaign.name  AS _campaign,
      activity.subject AS _subject,
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
     )
    WHERE
      _rownum = 1
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
    
      -- AND campaign.name IS NOT NULL AND campaign.id = 279480425  
        )
    WHERE
      _rownum = 1
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
        'False Delivered' AS _engagement,
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
  )
  WHERE
    _rownum = 1
), combine_bounce AS (
  SELECT
    * EXCEPT(_rownum)
  FROM (
  SELECT *, 
  ROW_NUMBER() OVER(PARTITION BY _email, _campaignID ORDER BY _timestamp DESC) AS _rownum 
  FROM (
    SELECT * FROM email_bounce 
    UNION ALL 
    SELECT * FROM bounced
    )
  ) WHERE
    _rownum = 1

) SELECT delivered .* FROM delivered 
LEFT JOIN dropped ON delivered._email = dropped._email and delivered._campaignID = dropped._campaignID
--LEFT JOIN bounced ON delivered._email = bounced._email and delivered._campaignID = bounced._campaignID
JOIN  combine_bounce email_bounce
 ON delivered._email = email_bounce._email and delivered._campaignID = email_bounce._campaignID
WHERE 
--bounced._email IS NULL AND 
dropped._email IS NULL 
--AND email_bounce._email IS NULL
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
   UNION ALL
   SELECT * 
   FROM customer_external
   UNION ALL 
   SELECT * 
   FROM false_delivered 
  ), customer_list AS (
    SELECT company, 
    category 
    FROM `x-marketing.x_google_sheets_new.Customer_List`
  )
SELECT
  engagements.*,
  COALESCE(REGEXP_EXTRACT(_description, r'[?&]utm_source=([^&]+)'), "Email") AS _utmsource,
  REGEXP_EXTRACT(_description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
  REGEXP_EXTRACT(_description, r'[?&]utm_content=([^&]+)') AS _utmcontent,
  prospect_info.* EXCEPT (_email),
  airtable_info.* EXCEPT (id),
  COALESCE(category, "Prospect") AS _category,
CONCAT(_campaign, "\n\n", engagements._subject)
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
LEFT JOIN customer_list 
ON prospect_info._company = customer_list.company
;


--- Label Bots ---
UPDATE `x-marketing.3x.db_email_engagements_log`origin  
SET origin._isBot = 'true'
FROM (
    SELECT DISTINCT

        _email,
        _contentTitle

    FROM 
        `x-marketing.3x.db_email_engagements_log`
    WHERE 
        _description LIKE '%https://3x.wise-portal.com/iclick/iclick.php%'
) bot
WHERE 
    origin._email = bot._email
AND origin._contentTitle = bot._contentTitle
AND origin._engagement IN ('Clicked');


-- Label Clicks That Are Visits and Set their Page Views
UPDATE`x-marketing.3x.db_email_engagements_log`origin
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
        `x-marketing.3x.db_email_engagements_log`email 
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


CREATE OR REPLACE TABLE `x-marketing.3x.db_email_engagements_consolidate` AS 
SELECT 
  _email,
  _company,_category,
  _name,  _title,_quater_segment,
  SUM(CASE WHEN _engagement = 'Sent' THEN 1 END)AS Sent,
  SUM(CASE WHEN _engagement='Delivered' THEN 1 ELSE 0 END ) AS Delivered,
  SUM(CASE WHEN _engagement = 'Soft Bounced' THEN 1 
  WHEN _engagement = 'Hard Bounced' THEN 1 ELSE 0 END ) AS Bounced,
  SUM(CASE WHEN _engagement='Opened' THEN 1 ELSE 0 END ) AS Opened,
  SUM(CASE WHEN _engagement='Clicked' THEN 1 ELSE 0 END ) AS Clicked,
  SUM(CASE WHEN _engagement = 'Register' THEN 1 ELSE 0 END ) AS Register,
  SUM(CASE WHEN _engagement = 'Do Not Register' THEN 1 ELSE 0 END ) AS Do_Not_register,
  SUM (CASE WHEN _engagement = 'Sent + Register' THEN 1 ELSE 0 END ) AS sent_register,
  SUM (CASE WHEN _engagement = 'External Register' THEN 1 ELSE 0 END ) AS external_register,
   SUM (CASE WHEN _engagement = 'Unsubscribed' THEN 1 ELSE 0 END ) AS  unsubsribe
  FROM `x-marketing.3x.db_email_engagements_log`
  WHERE _segment_campaign = 'Customer Marketing - CTH'
  GROUP BY 1,2,3,4,5,6;

  CREATE OR REPLACE TABLE `x-marketing.3x.db_email_engagements_consolidate_campaign` AS 
SELECT 
  _email,
  _company,_category,
  _name,  _title,  _campaignID,
  _utmcampaign,_subject,_campaignSentDate,_campaign_subject,
  SUM(CASE WHEN _engagement = 'Sent' THEN 1 END)AS Sent,
  SUM(CASE WHEN _engagement='Delivered' THEN 1 ELSE 0 END ) AS Delivered,
  SUM(CASE WHEN _engagement = 'Soft Bounced' THEN 1 
  WHEN _engagement = 'Hard Bounced' THEN 1 ELSE 0 END ) AS Bounced,
  SUM(CASE WHEN _engagement='Opened' THEN 1 ELSE 0 END ) AS Opened,
  SUM(CASE WHEN _engagement='Clicked' THEN 1 ELSE 0 END ) AS Clicked,
  SUM(CASE WHEN _engagement = 'Register' THEN 1 ELSE 0 END ) AS Register,
  SUM(CASE WHEN _engagement = 'Do Not Register' THEN 1 ELSE 0 END ) AS Do_Not_register,
  SUM (CASE WHEN _engagement = 'Sent + Register' THEN 1 ELSE 0 END ) AS sent_register,
  SUM (CASE WHEN _engagement = 'External Register' THEN 1 ELSE 0 END ) AS external_register,
   SUM (CASE WHEN _engagement = 'Unsubscribed' THEN 1 ELSE 0 END ) AS  unsubsribe,
  SUM(CASE WHEN _engagement = 'False Delivered' THEN 1 ELSE 0 END ) AS false_delivered
  FROM `x-marketing.3x.db_email_engagements_log`
  --WHERE _segment_campaign = 'Customer Marketing'
  GROUP BY 1,2,3,4,5,6,7,8,9,10
