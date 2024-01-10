TRUNCATE TABLE `x-marketing.ems.db_email_engagements_log`;
INSERT INTO `x-marketing.ems.db_email_engagements_log` (
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
  _download_source,
  _prospectID,
  _name,
  _domain,
  _title,
  _function,
  _job_role,
  _mql_date,
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
  _persona,
  _lifecycleStage,
  _leadscore,
  _leadstatus,
  _ipqc_check,
  _hubspotscore,
  _company_id,
  _company_segment,
  _lead_segment,
  _segment, 
  _property_leadstatus,
  _companylinkedinbio, 
  _company_linkedin, 
  _employee_range, 
  _employee_range_c, 
  _numberofemployees, 
  _annualrevenue, 
  _annual_revenue_range, 
  _annual_revenue_range_c,
  _salesforceaccountid,
  _salesforceleadid, 
  _salesforcecontactid,
  _subject,
  _assettitle,
  _screenshot,
  _assettype,
  _requestername,
  _emailsegment,
  _campaignsegment,
  _emailid,
  _livedate,
  _senddate,
  _cihomeurl,
  _campaignCode,
  _emailname
)
WITH prospect_info AS (
  SELECT * EXCEPT (_rownum)
  FROM (
  SELECT  
     
    CAST(vid AS STRING) AS _id,
    properties.email.value AS _email,
    CONCAT(properties.firstname.value,' ',properties.lastname.value) AS _name,
    associated_company.properties.domain.value AS _domain,
    properties.jobtitle.value AS _jobtitle,
    '' AS _function,
    '' AS _jobrole,
    properties.hs_lifecyclestage_marketingqualifiedlead_date.value AS _mqldate,
    properties.hs_analytics_source.value AS _source,
    properties.hs_latest_source.value AS _latest_source,
    '' AS _seniority,
    properties.phone.value AS _phone,
    properties.company.value AS _company,
    CAST(associated_company.properties.revenue__c.value AS STRING) AS _revenue,
    associated_company.properties.industry__c.value AS _industry,
    properties.city.value AS _city,
    properties.state.value AS _state,
    properties.country.value AS _country,
    '' AS _persona,
    properties.lifecyclestage.value AS _lifecycleStage,
    CAST(NULL AS INT64) AS _leadscore,
    properties.leadstatus.value AS _leadstatus,
    '' AS _ipqc_check,
    properties.hubspotscore.value AS _hubspotScore,
    associated_company.company_id AS _companyID,
    '' AS _companySegment,
    '' AS _leadSegment,
    '' AS _segment,
    properties.leadstatus.value AS _propertyLeadstatus,
    associated_company.properties.linkedinbio.value AS _companylinkedinbio,
    associated_company.properties.linkedin_company_page.value AS _company_linkedin,
    '' AS _employee_range,
    '' AS _employee_range_c,
    CAST(associated_company.properties.numberofemployees.value AS NUMERIC) AS _numberofemployees,
    CAST(associated_company.properties.annualrevenue.value AS NUMERIC) AS _annualrevenue,
    '' AS _annual_revenue_range,
    '' AS _annual_revenue_range_c,
    properties.salesforceaccountid.value AS _sfdcaccountid,
    properties.salesforceleadid.value AS _sfdcleadid,
    properties.salesforcecontactid.value AS _sfdccontactid,
    ROW_NUMBER() OVER(PARTITION BY properties.email.value ORDER BY property_createdate.value DESC) AS _rownum
  FROM
    `x-marketing.ems_hubspot.contacts`
  -- WHERE
  --   properties.email.value IS NOT NULL
  --   AND properties.email.value NOT LIKE '%2x.marketing%'
  --   AND properties.email.value NOT LIKE '%ems.com%'
  --   AND properties.email.value NOT LIKE '%test%'
  ) WHERE _rownum = 1
),
airtable_info AS (
 SELECT  CAST(id AS STRING) AS _pardotid,
    name AS _code , 
    subject AS _subject, 
    _assettitle, 
    _screenshot, 
    _assetType,
    _requesterName,
    _email,
    _campaign,
    _campaignid,
    CASE WHEN _emailid = "" THEN NULL ELSE CAST(_emailid AS INT64) END AS _emailid,
    CASE WHEN _livedate = "" THEN NULL ELSE CAST(_livedate AS DATE) END AS _livedate,
    CASE WHEN  _senddate = "" THEN NULL ELSE CAST(_senddate AS DATE) END AS _senddate,
    email._cihomeurl,
    email._code AS _campaignCode,
    _emailname
FROM `x-marketing.ems_hubspot.campaigns` campaign
 JOIN `x-marketing.ems_mysql.db_airtable_email` email ON CAST(campaign.id AS STRING) = _emailid
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
      `x-marketing.ems_hubspot.email_events` activity
    JOIN
      `x-marketing.ems_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE 
    activity.type = 'DROPPED'
), Sent AS (
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
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      '',
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.ems_hubspot.email_events` activity
    JOIN
      `x-marketing.ems_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'SENT'  
      --AND activity.recipient NOT LIKE '%2x.marketing' AND activity.recipient NOT LIKE '%ems%'
      --AND campaign.name IS NOT NULL
      )
  WHERE
    _rownum = 1 
) SELECT Sent .* FROM Sent
LEFT JOIN bounced ON Sent._email = bounced._email and Sent._campaignID = bounced._campaignID
WHERE bounced._email IS NULL
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
      `x-marketing.ems_hubspot.email_events` activity
    JOIN
      `x-marketing.ems_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE 
    activity.type = 'DROPPED'
), bounced AS (
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
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      '',
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.ems_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.ems_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.ems_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
      WHERE 
      activity.type = 'BOUNCE'
      AND 
      status.value.change = 'BOUNCED'
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
        `x-marketing.ems_hubspot.email_events` activity
      JOIN
        `x-marketing.ems_hubspot.campaigns` campaign
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
      campaign.name AS _contentTitle,
      campaign.contentid AS _contentID,
      --activity.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Delivered' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      '',
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.ems_hubspot.email_events` activity
    JOIN
      `x-marketing.ems_hubspot.campaigns` campaign
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
--LEFT JOIN email_bounce ON delivered._email = email_bounce._email and delivered._campaignID = email_bounce._campaignID
WHERE bounced._email IS NULL AND dropped._email IS NULL 
--AND email_bounce._email IS NULL 


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
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      '',
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.ems_hubspot.email_events` activity
    JOIN
      `x-marketing.ems_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
       activity.type = 'OPEN'
      AND filteredevent = FALSE
      -- AND activity.recipient NOT LIKE '%2x.marketing' AND activity.recipient NOT LIKE '%ems%'
      -- AND campaign.name IS NOT NULL 
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
      campaign.name AS _contentTitle,
      campaign.contentid AS _contentID,
      --activity.subject AS _subject,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Clicked' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      '',
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.ems_hubspot.email_events` activity
    JOIN
      `x-marketing.ems_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'CLICK'
      AND filteredevent = FALSE
      -- AND activity.recipient NOT LIKE '%2x.marketing' AND activity.recipient NOT LIKE '%ems%'
      -- AND campaign.name IS NOT NULL
      )
  WHERE
    _rownum = 1
),
email_hardbounce AS (
    WITH bounced AS (
      SELECT
      activity._sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID, 
           campaign.name AS _contentTitle,
      campaign.contentid AS _contentID,
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
      FROM `x-marketing.ems_hubspot..subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.ems_hubspot..email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.ems_hubspot..campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
         campaign.name AS _contentTitle,
      campaign.contentid AS _contentID,
      activity.recipient AS _email,
      activity.created AS _timestamp,
      'Hard Bounced' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      '',
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.ems_hubspot..email_events` activity
    JOIN
      `x-marketing.ems_hubspot..campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'BOUNCE'  
   
      AND campaign.name IS NOT NULL  )
  WHERE
    _rownum = 1
) SELECT email_bounce .* FROM email_bounce
JOIN bounced ON email_bounce._email = bounced._email and email_bounce._campaignID = bounced._campaignID

),email_softbounce AS (
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
      `x-marketing.ems_hubspot.email_events` activity
    JOIN
      `x-marketing.ems_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE 
    activity.type = 'DROPPED'
),bounced AS (
        SELECT
        activity._sdc_sequence,
         CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _contentTitle,
      campaign.contentid AS _contentID,
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
        FROM `x-marketing.ems_hubspot.subscription_changes`, UNNEST(changes) AS status 
        JOIN `x-marketing.ems_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
        JOIN `x-marketing.ems_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
        WHERE 
        activity.type = 'BOUNCE'
        AND 
        status.value.change = 'BOUNCED'  
  ), email_bounce AS (
  SELECT
      * EXCEPT(_rownum)
    FROM (
      SELECT
        activity._sdc_sequence,
         CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _contentTitle,
      campaign.contentid AS _contentID,
        activity.recipient AS _email,
        activity.created AS _timestamp,
        'Soft Bounced' AS _engagement,
        url AS _description,
        devicetype AS _device_type,
        CAST(linkid AS STRING) _linkid,
        --appname,
        CAST(duration AS STRING) _duration,
        response AS _response,
        '',
        ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM
        `x-marketing.ems_hubspot.email_events` activity
      JOIN
        `x-marketing.ems_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'BOUNCE'  
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
      campaign.name AS _contentTitle,
      campaign.contentid AS _contentID,
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
        `x-marketing.ems_hubspot.email_events` activity
      JOIN
        `x-marketing.ems_hubspot.campaigns` campaign
      ON
        activity.emailcampaignid = campaign.id
      WHERE
        activity.type = 'DELIVERED'  
    
        AND campaign.name IS NOT NULL  )
    WHERE
      _rownum = 1
  ) SELECT email_bounce .* 
  FROM email_bounce
  LEFT JOIN bounced ON email_bounce._email = bounced._email and email_bounce._campaignID = bounced._campaignID
  LEFT JOIN delivered ON email_bounce._email = delivered._email and email_bounce._campaignID = delivered._campaignID
  WHERE  bounced._email IS NULL AND delivered._email IS NULL 
)
,email_defferred AS (
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
      --appname,
      CAST(duration AS STRING) _duration,
    response AS _response,
    '',
    ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM `x-marketing.ems_hubspot.email_events` activity
    JOIN `x-marketing.ems_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
    campaign.name AS _contentTitle,
    campaign.contentid AS _contentID,
    --activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Dropped' AS _engagement,
    url AS _description,
    devicetype AS _device_type,
   CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
    response AS _response,
    '',
    ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM `x-marketing.ems_hubspot.email_events` activity
    JOIN `x-marketing.ems_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
    campaign.name AS _contentTitle,
    campaign.contentid AS _contentID,
    --activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Suppressed' AS _engagement,
    url AS _description,
    devicetype AS _device_type,
    CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
    response AS _response,
    '',
    ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM `x-marketing.ems_hubspot.email_events` activity
    JOIN `x-marketing.ems_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
    campaign.name AS _contentTitle,
    campaign.contentid AS _contentID,
    --activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Processed' AS _engagement,
    url AS _description,
    devicetype AS _device_type,
    CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
    response AS _response,
    '',
    ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM `x-marketing.ems_hubspot.email_events` activity
    JOIN `x-marketing.ems_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
    campaign.name AS _contentTitle,
    campaign.contentid AS _contentID,
    --activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Forward' AS _engagement,
    url AS _description,
    devicetype AS _device_type,
    CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
    response AS _response,
    '',
    ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM `x-marketing.ems_hubspot.email_events` activity
    JOIN `x-marketing.ems_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
    campaign.name AS _contentTitle,
    campaign.contentid AS _contentID,
    --activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Spam' AS _engagement,
    url AS _description,
    devicetype AS _device_type,
    CAST(linkid AS STRING) _linkid,
      --appname,
      CAST(duration AS STRING) _duration,
    response AS _response,
    '',
    ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM `x-marketing.ems_hubspot.email_events` activity
    JOIN `x-marketing.ems_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
    campaign.name AS _contentTitle,
    campaign.contentid AS _contentID,
    --activity.subject AS _subject,
    activity.recipient AS _email,
    activity.created AS _timestamp,
    'Print' AS _engagement,
    url AS _description,
    devicetype AS _device_type,
    CAST(linkid AS STRING) _linkid,
    --appname,
    CAST(duration AS STRING) _duration,
    response AS _response,
    '',
    ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM `x-marketing.ems_hubspot.email_events` activity
    JOIN `x-marketing.ems_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
    WHERE 
    activity.type = 'PRINT'
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
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      '',
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.ems_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.ems_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.ems_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
        campaignguid
      FROM
        `x-marketing.ems_hubspot.contacts` c,
        UNNEST(form_submissions) AS form
      JOIN
        `x-marketing.ems_hubspot.forms` forms
      ON
        form.value.form_id = forms.guid
        ) activity
    JOIN
      `x-marketing.ems_hubspot.campaigns` campaign
    ON
      activity._campaignID = CAST(campaign.id AS STRING) )
  WHERE
    rownum = 1 

)
SELECT
  engagements.* EXCEPT (_contentid),
  prospect_info.* EXCEPT (_email),
  airtable_info.* EXCEPT(_pardotid,_code,_campaignid)
FROM (
  SELECT * FROM email_sent
  UNION ALL
  SELECT * FROM email_delivered
  UNION ALL
  SELECT * FROM email_open
  UNION ALL
  SELECT * FROM email_click
  UNION ALL
  SELECT * FROM email_hardbounce
  UNION ALL 
  SELECT * FROM  email_softbounce
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
  engagements._campaignID = airtable_info._pardotid;