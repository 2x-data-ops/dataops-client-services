TRUNCATE TABLE `x-marketing.sandler_network.db_email_engagement_combined`;
INSERT INTO `x-marketing.sandler_network.db_email_engagement_combined` (
  _sdc_sequence,
  _campaignID,
  _contentID,
  _email,
  _timestamp,
  _engagement,
  _description,
  _device_type,
  _linkid,
  _duration,
  _response,
  contact_id,
  _company_name,
  _company_domain,
  _painpoint,
  _assettitle,
  _assettype,
  _screenshot,
  _subject,
  _pillars,
  _emailtype,
  _emailname,
  _webinarTheme,
  _instance
)
WITH prospect_info AS (
WITH network_prospect_info AS (
  SELECT DISTINCT
    CAST(vid AS STRING) AS contact_id,
    a.properties.email.value AS _email,
    a.properties.company.value AS _company_name,
    c.property_domain.value AS _company_domain
  FROM `x-marketing.sandler_network_hubspot.contacts` a
  LEFT JOIN `x-marketing.sandler_network_hubspot.companies` c ON a.properties.company.value = c.properties.name.value
  WHERE
    a.properties.email.value IS NOT NULL
    -- AND LOWER(a.properties.email.value) NOT LIKE '%2x.marketing%'
    -- AND LOWER(a.properties.email.value) NOT LIKE '%sandler.com%'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.properties.email.value, vid, a.properties.company.value ORDER BY c.property_domain.value DESC) = 1
),
sandler_prospect_info AS (
  SELECT DISTINCT
    CAST(vid AS STRING) AS contact_id,
    a.properties.email.value,
    a.properties.company.value,
    c.property_domain.value
  FROM `x-marketing.sandler_hubspot.contacts` a
  LEFT JOIN `x-marketing.sandler_hubspot.companies` c ON a.properties.company.value = c.properties.name.value
  WHERE
    a.properties.email.value IS NOT NULL
    -- AND LOWER(a.properties.email.value) NOT LIKE '%2x.marketing%'
    -- AND LOWER(a.properties.email.value) NOT LIKE '%sandler.com%'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.properties.email.value, vid, a.properties.company.value ORDER BY c.property_domain.value DESC) = 1
)
SELECT *
FROM (
  SELECT * FROM network_prospect_info
  UNION ALL
  SELECT * FROM sandler_prospect_info
)
),
airtable_info AS (
WITH network_airtable AS (
SELECT _painpoint,
       _assettitle, 
       _assettype, 	
       _screenshot,
       _subject,
       _emailid,
       _pillars,
       _emailtype,
       _emailname,
       _webinartheme,
       'Sandler Network' AS _instance
FROM `x-marketing.sandlernetwork_mysql_2.sandlernetwork_db_airtable_email`
),
sandler_airtable AS (
SELECT _painpoint,
       _assettitle, 
       _assettype, 	
       _screenshot,
       _subject,
       _emailid,
       _pillars,
       _emailtype,
       _emailname,
       _webinartheme,
       'Sandler' AS _instance
FROM `x-marketing.sandler_mysql.db_airtable_email`
)
SELECT *
FROM (
  SELECT * FROM network_airtable
  UNION ALL
  SELECT * FROM sandler_airtable
)
),

overall_sent AS (

WITH network_sent AS (
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
      CAST(duration AS STRING) _duration,
      response AS _response,
      '',
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sandler_network_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.sandler_network_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
      WHERE 
      activity.type = 'BOUNCE' 
      AND 
      status.value.change = 'BOUNCED'  
), Sent AS (
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
      'Sent' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.sandler_network_hubspot.email_events` activity
    JOIN
      `x-marketing.sandler_network_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'SENT'
      AND campaign.name IS NOT NULL)
  WHERE
    _rownum = 1 
) SELECT Sent .* FROM Sent
LEFT JOIN bounced ON Sent._email = bounced._email and Sent._campaignID = bounced._campaignID
WHERE bounced._email IS NULL 
),

sandler_sent AS (
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
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sandler_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.sandler_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.sandler_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
      WHERE 
      activity.type = 'BOUNCE' 
      AND 
      status.value.change = 'BOUNCED'  
), Sent AS (
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
      'Sent' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.sandler_hubspot.email_events` activity
    JOIN
      `x-marketing.sandler_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'SENT'
      AND campaign.name IS NOT NULL)
  WHERE
    _rownum = 1 
) SELECT Sent .* FROM Sent
LEFT JOIN bounced ON Sent._email = bounced._email and Sent._campaignID = bounced._campaignID
WHERE bounced._email IS NULL 
)
SELECT *
FROM (
  SELECT *, 'Sandler Network' AS _instance FROM network_sent
  UNION ALL
  SELECT *, 'Sandler' AS _instance FROM sandler_sent
)
),

overall_delivered AS (
WITH network_delivered AS (
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
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sandler_network_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.sandler_network_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.sandler_network_hubspot.email_events` activity
    JOIN
      `x-marketing.sandler_network_hubspot.campaigns` campaign
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
),

sandler_delivered AS (
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
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sandler_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.sandler_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.sandler_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
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
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.sandler_hubspot.email_events` activity
    JOIN
      `x-marketing.sandler_hubspot.campaigns` campaign
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
)
SELECT *
FROM (
  SELECT *, 'Sandler Network' AS _instance FROM network_delivered
  UNION ALL
  SELECT *, 'Sandler' AS _instance FROM sandler_delivered
)
),

overall_open AS (
WITH network_open AS (
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
      'Opened' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.sandler_network_hubspot.email_events` activity
    JOIN
      `x-marketing.sandler_network_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
       activity.type = 'OPEN'
      AND filteredevent = FALSE
      AND campaign.name IS NOT NULL)
  WHERE
    _rownum = 1 
),

sandler_open AS (
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
      'Opened' AS _engagement,
      url AS _description,
      devicetype AS _device_type,
      CAST(linkid AS STRING) _linkid,
      CAST(duration AS STRING) _duration,
      response AS _response,
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
      AND campaign.name IS NOT NULL)
  WHERE
    _rownum = 1
)
SELECT *
FROM (
  SELECT *, 'Sandler Network' AS _instance FROM network_open
  UNION ALL
  SELECT *, 'Sandler' AS _instance FROM sandler_open
)

),

overall_clicks AS (
WITH network_clicks AS (
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
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.sandler_network_hubspot.email_events` activity
    JOIN
      `x-marketing.sandler_network_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'CLICK'
      AND filteredevent = FALSE
      AND campaign.name IS NOT NULL  )
  WHERE
    _rownum = 1
),

sandler_clicks AS (
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
      AND campaign.name IS NOT NULL  )
  WHERE
    _rownum = 1
)
SELECT *
FROM (
  SELECT *, 'Sandler Network' AS _instance FROM network_clicks
  UNION ALL
  SELECT *, 'Sandler' AS _instance FROM sandler_clicks
)

),
overall_bounced AS (
  WITH network_bounce AS (
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
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.sandler_network_hubspot.email_events` activity
    JOIN
      `x-marketing.sandler_network_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'BOUNCE' 
      ---AND emailcampaignid = 269760036
      AND campaign.name IS NOT NULL)
  WHERE
    _rownum = 1
  ),

  sandler_bounce AS (
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
      --appname,
      CAST(duration AS STRING) _duration,
      response AS _response,
      ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
    FROM
      `x-marketing.sandler_hubspot.email_events` activity
    JOIN
      `x-marketing.sandler_hubspot.campaigns` campaign
    ON
      activity.emailcampaignid = campaign.id
    WHERE
      activity.type = 'BOUNCE' 
      ---AND emailcampaignid = 269760036
      AND campaign.name IS NOT NULL  )
  WHERE
    _rownum = 1
  )
  SELECT *
  FROM (
    SELECT *, 'Sandler Network' AS _instance FROM network_bounce
    UNION ALL
    SELECT *, 'Sandler' AS _instance FROM sandler_bounce
  )

),

overall_unsubscribed AS (
  WITH network_unsubscribed AS (
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
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sandler_network_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.sandler_network_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
      WHERE 
      activity.type = 'STATUSCHANGE'
      AND status.value.change = 'UNSUBSCRIBED' 
      AND campaign.name IS NOT NULL 
    )
  WHERE _rownum = 1
  ),
  sandler_unsubscribed AS (
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
      ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sandler_hubspot.subscription_changes`, UNNEST(changes) AS status 
      JOIN `x-marketing.sandler_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.sandler_hubspot.campaigns` campaign ON  activity.emailcampaignid = campaign.id
      WHERE 
      activity.type = 'STATUSCHANGE'
      AND status.value.change = 'UNSUBSCRIBED' 
      AND campaign.name IS NOT NULL 
    )
  WHERE _rownum = 1
  )
  SELECT *
  FROM (
    SELECT *, 'Sandler Network' AS _instance FROM network_unsubscribed
    UNION ALL
    SELECT *, 'Sandler' AS _instance FROM sandler_unsubscribed
  )
),

overall_hard_bounce AS (
WITH sandler_hard_bounced AS (
  WITH HardBounced AS (
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT 
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      campaign.contentid AS _contentID,
      activity.recipient AS _email,
      CAST(subs.timestamp AS TIMESTAMP) AS _timestamp,
      'Hard Bounced' AS _engagement,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER(PARTITION BY subs.recipient, activity.emailcampaigngroupid ORDER BY subs.timestamp DESC) AS _rownum
      FROM `x-marketing.sandler_hubspot.subscription_changes` subs, UNNEST(changes) AS status
      JOIN `x-marketing.sandler_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.sandler_hubspot.campaigns` campaign ON activity.emailcampaignid = campaign.id
      WHERE status.value.change = 'BOUNCED'
    )
    WHERE _rownum = 1
  ),
  SoftBounced AS (
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Soft Bounced' AS _engagement,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sandler_hubspot.email_events` activity
      JOIN `x-marketing.sandler_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'BOUNCE'
      AND campaign.name IS NOT NULL
    )
    WHERE _rownum = 1
  ),
  Delivered AS(
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Delivered' AS _engagement,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sandler_hubspot.email_events` activity
      JOIN `x-marketing.sandler_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'DELIVERED'
      AND campaign.name IS NOT NULL
    )WHERE _rownum = 1
  )
   SELECT HardBounced.* 
   FROM HardBounced
  JOIN SoftBounced ON HardBounced._email = Softbounced._email AND HardBounced._campaignID = SoftBounced._campaignID
),

network_hard_bounced AS (
  WITH HardBounced AS (
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT 
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      campaign.contentid AS _contentID,
      activity.recipient AS _email,
      CAST(subs.timestamp AS TIMESTAMP) AS _timestamp,
      'Hard Bounced' AS _engagement,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER(PARTITION BY subs.recipient, activity.emailcampaigngroupid ORDER BY subs.timestamp DESC) AS _rownum
      FROM `x-marketing.sandler_network_hubspot.subscription_changes` subs, UNNEST(changes) AS status
      JOIN `x-marketing.sandler_network_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign ON activity.emailcampaignid = campaign.id
      WHERE status.value.change = 'BOUNCED'
    )
    WHERE _rownum = 1
  ),
  SoftBounced AS (
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Soft Bounced' AS _engagement,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sandler_network_hubspot.email_events` activity
      JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'BOUNCE'
      AND campaign.name IS NOT NULL
    )
    WHERE _rownum = 1
  ),
  Delivered AS(
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Delivered' AS _engagement,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sandler_network_hubspot.email_events` activity
      JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'DELIVERED'
      AND campaign.name IS NOT NULL
    )WHERE _rownum = 1
  )
   SELECT HardBounced.* 
   FROM HardBounced
  JOIN SoftBounced ON HardBounced._email = Softbounced._email AND HardBounced._campaignID = SoftBounced._campaignID  
)
SELECT *, 'Sandler' AS _instance FROM sandler_hard_bounced
UNION ALL
SELECT *, 'Sandler Network' AS _instance FROM network_hard_bounced
),

overall_soft_bounce AS (
 WITH sandler_soft_bounced AS (
  WITH HardBounced AS (
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT 
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Hard Bounced' AS _engagement,
      activity.recipient AS _email,
      CAST(subs.timestamp AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER(PARTITION BY subs.recipient, activity.emailcampaigngroupid ORDER BY subs.timestamp DESC) AS _rownum
      FROM `x-marketing.sandler_hubspot.subscription_changes` subs, UNNEST(changes) AS status
      JOIN `x-marketing.sandler_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.sandler_hubspot.campaigns` campaign ON activity.emailcampaignid = campaign.id
      WHERE status.value.change = 'BOUNCED'
    )
    WHERE _rownum = 1
  ),
  SoftBounced AS (
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      campaign.contentid AS _contentID,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      'Soft Bounced' AS _engagement,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sandler_hubspot.email_events` activity
      JOIN `x-marketing.sandler_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'BOUNCE'
      AND campaign.name IS NOT NULL
    )
    WHERE _rownum = 1
  ),
  Delivered AS(
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Delivered' AS _engagement,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sandler_hubspot.email_events` activity
      JOIN `x-marketing.sandler_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'DELIVERED'
      AND campaign.name IS NOT NULL
    )
    WHERE _rownum = 1
  )
  SELECT SoftBounced.* FROM SoftBounced
  LEFT JOIN HardBounced ON SoftBounced._email = HardBounced._email AND SoftBounced._campaignID = HardBounced._campaignID
  LEFT JOIN Delivered ON SoftBounced._email = Delivered._email AND SoftBounced._campaignID = Delivered._campaignID
  WHERE HardBounced._email IS NULL AND Delivered._email IS NULL 
  ),

  network_soft_bounced AS (
  WITH HardBounced AS (
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT 
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Hard Bounced' AS _engagement,
      activity.recipient AS _email,
      CAST(subs.timestamp AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER(PARTITION BY subs.recipient, activity.emailcampaigngroupid ORDER BY subs.timestamp DESC) AS _rownum
      FROM `x-marketing.sandler_network_hubspot.subscription_changes` subs, UNNEST(changes) AS status
      JOIN `x-marketing.sandler_network_hubspot.email_events` activity ON status.value.causedbyevent.id = activity.id
      JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign ON activity.emailcampaignid = campaign.id
      WHERE status.value.change = 'BOUNCED'
    )
    WHERE _rownum = 1
  ),
  SoftBounced AS (
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      campaign.contentid AS _contentID,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      'Soft Bounced' AS _engagement,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sandler_network_hubspot.email_events` activity
      JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'BOUNCE'
      AND campaign.name IS NOT NULL
    )
    WHERE _rownum = 1
  ),
  Delivered AS(
    SELECT * EXCEPT (_rownum)
    FROM (
      SELECT
      activity._sdc_sequence AS _sdc_sequence,
      CAST(activity.emailcampaignid AS STRING) AS _campaignID,
      campaign.name AS _campaign,
      'Delivered' AS _engagement,
      activity.recipient AS _email,
      CAST(activity.created AS TIMESTAMP) AS _timestamp,
      activity.url AS _description,
      activity.devicetype AS _device_type,
      CAST(activity.linkid AS STRING) AS _linkid,
      CAST(activity.duration AS STRING) AS _duration,
      activity.response AS _response,
      ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) AS _rownum
      FROM `x-marketing.sandler_network_hubspot.email_events` activity
      JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
      WHERE activity.type = 'DELIVERED'
      AND campaign.name IS NOT NULL
    )
    WHERE _rownum = 1
  )
  SELECT SoftBounced.* FROM SoftBounced
  LEFT JOIN HardBounced ON SoftBounced._email = HardBounced._email AND SoftBounced._campaignID = HardBounced._campaignID
  LEFT JOIN Delivered ON SoftBounced._email = Delivered._email AND SoftBounced._campaignID = Delivered._campaignID
  WHERE HardBounced._email IS NULL AND Delivered._email IS NULL    
  )
  SELECT *, 'Sandler' AS _instance FROM sandler_soft_bounced
  UNION ALL
  SELECT *, 'Sandler Network' AS _instance FROM network_soft_bounced
)

SELECT 
  engagements.* EXCEPT (_contentTitle, _instance),
  prospect_info.* EXCEPT (_email),
  airtable_info.* EXCEPT (_emailid)
FROM (
  SELECT * FROM overall_sent
  UNION ALL
  SELECT * FROM overall_delivered
  UNION ALL
  SELECT * FROM overall_open
  UNION ALL
  SELECT * FROM overall_clicks
  UNION ALL
  SELECT * FROM overall_unsubscribed
  UNION ALL
  SELECT * FROM overall_hard_bounce
  UNION ALL
  SELECT * FROM overall_soft_bounce
) engagements
LEFT JOIN
  prospect_info ON prospect_info._email = engagements._email
JOIN
  airtable_info ON _emailid = _campaignID AND engagements._instance = airtable_info._instance
/*WHERE _campaignID IN ('289594398', 
'289593362',
'289593058',
'289592909',
'289591279',
'289593361',
'289588290',
'289588342',
'289591278')*/
QUALIFY ROW_NUMBER() OVER (PARTITION BY _sdc_sequence, _campaignID, _email  ORDER BY _timestamp DESC) = 1