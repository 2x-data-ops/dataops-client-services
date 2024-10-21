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
  WITH network_prospect_info AS (
    SELECT DISTINCT
      CAST(vid AS STRING) AS contact_id,
      a.properties.email.value AS _email,
      a.properties.company.value AS _company_name,
      c.property_domain.value AS _company_domain
    FROM `x-marketing.sandler_network_hubspot.contacts` a
    LEFT JOIN `x-marketing.sandler_network_hubspot.companies` c
      ON a.properties.company.value = c.properties.name.value
    WHERE a.properties.email.value IS NOT NULL -- AND LOWER(a.properties.email.value) NOT LIKE '%2x.marketing%'
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
    LEFT JOIN `x-marketing.sandler_hubspot.companies` c
      ON a.properties.company.value = c.properties.name.value
    WHERE a.properties.email.value IS NOT NULL -- AND LOWER(a.properties.email.value) NOT LIKE '%2x.marketing%'
      -- AND LOWER(a.properties.email.value) NOT LIKE '%sandler.com%'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.properties.email.value, vid, a.properties.company.value ORDER BY c.property_domain.value DESC) = 1
  ),
  prospect_info AS (
    SELECT
      *
    FROM network_prospect_info
    UNION ALL
    SELECT
      *
    FROM sandler_prospect_info
  ),
  network_airtable AS (
    SELECT
      _painpoint,
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
    SELECT
      _painpoint,
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
  ),
  airtable_info AS (
    SELECT
      *
    FROM network_airtable
    UNION ALL
    SELECT
      *
    FROM sandler_airtable
  ),
  network_hard_bounced_source AS (
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
      activity.response AS _response
    FROM `x-marketing.sandler_network_hubspot.subscription_changes` subs,
      UNNEST (changes) AS status
    JOIN `x-marketing.sandler_network_hubspot.email_events` activity
      ON status.value.causedbyevent.id = activity.id
    JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE status.value.change = 'BOUNCED'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY subs.recipient, activity.emailcampaigngroupid ORDER BY subs.timestamp DESC) = 1
  ),
  network_sent_source AS (
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
      response AS _response
    FROM `x-marketing.sandler_network_hubspot.email_events` activity
    JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'SENT'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  network_sent AS (
    SELECT
      network_sent_source.*
    FROM network_sent_source
    LEFT JOIN network_hard_bounced_source
      ON network_sent_source._email = network_hard_bounced_source._email
      AND network_sent_source._campaignID = network_hard_bounced_source._campaignID
    WHERE network_hard_bounced_source._email IS NULL
  ),
  sandler_hard_bounced_source AS (
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
      activity.response AS _response
    FROM `x-marketing.sandler_hubspot.subscription_changes` subs,
      UNNEST (changes) AS status
    JOIN `x-marketing.sandler_hubspot.email_events` activity
      ON status.value.causedbyevent.id = activity.id
    JOIN `x-marketing.sandler_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE status.value.change = 'BOUNCED'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY subs.recipient, activity.emailcampaigngroupid ORDER BY subs.timestamp DESC) = 1

  ),
  sandler_sent_source AS (
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
      response AS _response
    FROM `x-marketing.sandler_hubspot.email_events` activity
    JOIN `x-marketing.sandler_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'SENT'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1

  ),
  sandler_sent AS (
    SELECT
      sandler_sent_source.*
    FROM sandler_sent_source
    LEFT JOIN sandler_hard_bounced_source
      ON sandler_sent_source._email = sandler_hard_bounced_source._email
      AND sandler_sent_source._campaignID = sandler_hard_bounced_source._campaignID
    WHERE sandler_hard_bounced_source._email IS NULL
  ),
  overall_sent AS (
    SELECT
      *,
      'Sandler Network' AS _instance
    FROM network_sent
    UNION ALL
    SELECT
      *,
      'Sandler' AS _instance
    FROM sandler_sent
  ),
  network_delivered_source AS (
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
      response AS _response
    FROM `x-marketing.sandler_network_hubspot.email_events` activity
    JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'DELIVERED'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  network_delivered AS (
    SELECT
      network_delivered_source.*
    FROM network_delivered_source
    LEFT JOIN network_hard_bounced_source
      ON network_delivered_source._email = network_hard_bounced_source._email
      AND network_delivered_source._campaignID = network_hard_bounced_source._campaignID
    WHERE network_hard_bounced_source._email IS NULL
  ),
  sandler_delivered_source AS (
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
    FROM `x-marketing.sandler_hubspot.email_events` activity
    JOIN `x-marketing.sandler_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'DELIVERED'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  sandler_delivered AS (
    SELECT
      sandler_delivered_source.*
    FROM sandler_delivered_source
    LEFT JOIN sandler_hard_bounced_source
      ON sandler_delivered_source._email = sandler_hard_bounced_source._email
      AND sandler_delivered_source._campaignID = sandler_hard_bounced_source._campaignID
    WHERE sandler_hard_bounced_source._email IS NULL
  ),
  overall_delivered AS (
    SELECT
      *,
      'Sandler Network' AS _instance
    FROM network_delivered
    UNION ALL
    SELECT
      *,
      'Sandler' AS _instance
    FROM sandler_delivered
  ),
  network_open AS (
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
    FROM `x-marketing.sandler_network_hubspot.email_events` activity
    JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'OPEN'
      AND filteredevent = FALSE
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  sandler_open AS (
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
      response AS _response
    FROM `x-marketing.sandler_hubspot.email_events` activity
    JOIN `x-marketing.sandler_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'OPEN'
      AND filteredevent = FALSE
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  overall_open AS (
    SELECT
      *,
      'Sandler Network' AS _instance
    FROM network_open
    UNION ALL
    SELECT
      *,
      'Sandler' AS _instance
    FROM sandler_open
  ),
  network_clicks AS (
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
      response AS _response
    FROM `x-marketing.sandler_network_hubspot.email_events` activity
    JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign
     ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'CLICK'
      AND filteredevent = FALSE
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  sandler_clicks AS (
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
      response AS _response
    FROM `x-marketing.sandler_hubspot.email_events` activity
    JOIN `x-marketing.sandler_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'CLICK'
      AND filteredevent = FALSE
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  overall_clicks AS (
    SELECT
      *,
      'Sandler Network' AS _instance
    FROM network_clicks
    UNION ALL
    SELECT
      *,
      'Sandler' AS _instance
    FROM sandler_clicks
  ),
  network_bounce AS (
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
      response AS _response
    FROM `x-marketing.sandler_network_hubspot.email_events` activity
    JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'BOUNCE' ---AND emailcampaignid = 269760036
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  sandler_bounce AS (
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
      response AS _response
    FROM `x-marketing.sandler_hubspot.email_events` activity
    JOIN `x-marketing.sandler_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'BOUNCE' ---AND emailcampaignid = 269760036
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  overall_bounced AS (
    SELECT
      *,
      'Sandler Network' AS _instance
    FROM network_bounce
    UNION ALL
    SELECT
      *,
      'Sandler' AS _instance
    FROM sandler_bounce
  ),
  network_unsubscribed AS (
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
      response AS _response
    FROM `x-marketing.sandler_network_hubspot.subscription_changes`,
      UNNEST (changes) AS status
    JOIN `x-marketing.sandler_network_hubspot.email_events` activity
      ON status.value.causedbyevent.id = activity.id
    JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'STATUSCHANGE'
      AND status.value.change = 'UNSUBSCRIBED'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  sandler_unsubscribed AS (
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
      response AS _response
    FROM `x-marketing.sandler_hubspot.subscription_changes`,
      UNNEST (changes) AS status
    JOIN `x-marketing.sandler_hubspot.email_events` activity
      ON status.value.causedbyevent.id = activity.id
    JOIN `x-marketing.sandler_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'STATUSCHANGE'
      AND status.value.change = 'UNSUBSCRIBED'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER( PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  overall_unsubscribed AS (
    SELECT
      *,
      'Sandler Network' AS _instance
    FROM network_unsubscribed
    UNION ALL
    SELECT
      *,
      'Sandler' AS _instance
    FROM sandler_unsubscribed
  ),
  sandler_soft_bounced_source AS (
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
    FROM `x-marketing.sandler_hubspot.email_events` activity
    JOIN `x-marketing.sandler_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'BOUNCE'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  sandler_hard_bounced AS (
    SELECT
      sandler_hard_bounced_source.*
    FROM sandler_hard_bounced_source
    JOIN sandler_soft_bounced_source
      ON sandler_hard_bounced_source._email = sandler_soft_bounced_source._email
      AND sandler_hard_bounced_source._campaignID = sandler_soft_bounced_source._campaignID
  ),
  network_soft_bounced_source AS (
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
      activity.response AS _response
    FROM `x-marketing.sandler_network_hubspot.email_events` activity
    JOIN `x-marketing.sandler_network_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'BOUNCE'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  network_hard_bounced AS (
    SELECT
      network_hard_bounced_source.*
    FROM network_hard_bounced_source
    JOIN network_soft_bounced_source
      ON network_hard_bounced_source._email = network_soft_bounced_source._email
      AND network_hard_bounced_source._campaignID = network_soft_bounced_source._campaignID
  ),
  overall_hard_bounce AS (
    SELECT
      *,
      'Sandler' AS _instance
    FROM sandler_hard_bounced
    UNION ALL
    SELECT
      *,
      'Sandler Network' AS _instance
    FROM network_hard_bounced
  ),
  sandler_soft_bounced AS (
    SELECT
      sandler_soft_bounced_source.*
    FROM sandler_soft_bounced_source
    LEFT JOIN sandler_hard_bounced
      ON sandler_soft_bounced_source._email = sandler_hard_bounced._email
      AND sandler_soft_bounced_source._campaignID = sandler_hard_bounced._campaignID
    LEFT JOIN sandler_delivered_source
      ON sandler_soft_bounced_source._email = sandler_delivered_source._email
      AND sandler_soft_bounced_source._campaignID = sandler_delivered_source._campaignID
    WHERE sandler_hard_bounced._email IS NULL
      AND sandler_delivered_source._email IS NULL
  ),
  network_soft_bounced AS (
    SELECT
      network_soft_bounced_source.*
    FROM network_soft_bounced_source
    LEFT JOIN network_hard_bounced_source
      ON network_soft_bounced_source._email = network_hard_bounced_source._email
      AND network_soft_bounced_source._campaignID = network_hard_bounced_source._campaignID
    LEFT JOIN network_delivered_source
      ON network_soft_bounced_source._email = network_delivered_source._email
      AND network_soft_bounced_source._campaignID = network_delivered_source._campaignID
    WHERE network_hard_bounced_source._email IS NULL
      AND network_delivered_source._email IS NULL
  ),
  overall_soft_bounce AS (
    SELECT
      *,
      'Sandler' AS _instance
    FROM sandler_soft_bounced
    UNION ALL
    SELECT
      *,
      'Sandler Network' AS _instance
    FROM network_soft_bounced
  ),
  engagements AS (
    SELECT
      *
    FROM overall_sent
    UNION ALL
    SELECT
      *
    FROM overall_delivered
    UNION ALL
    SELECT
      *
    FROM overall_open
    UNION ALL
    SELECT
      *
    FROM overall_clicks
    UNION ALL
    SELECT
      *
    FROM overall_unsubscribed
    UNION ALL
    SELECT
      *
    FROM overall_hard_bounce
    UNION ALL
    SELECT
      *
    FROM overall_soft_bounce
  )
SELECT
  engagements.* EXCEPT (_contentTitle, _instance),
  prospect_info.* EXCEPT (_email),
  airtable_info.* EXCEPT (_emailid)
FROM
  engagements
LEFT JOIN prospect_info
  ON prospect_info._email = engagements._email
JOIN airtable_info
  ON _emailid = _campaignID
  AND engagements._instance = airtable_info._instance
  /*WHERE _campaignID IN ('289594398', 
  '289593362',
  '289593058',
  '289592909',
  '289591279',
  '289593361',
  '289588290',
  '289588342',
  '289591278')*/
QUALIFY ROW_NUMBER() OVER (PARTITION BY _sdc_sequence, _campaignID, _email  ORDER BY _timestamp DESC) = 1;