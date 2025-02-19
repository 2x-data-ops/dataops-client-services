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
TRUNCATE TABLE `x-marketing.logicsource.db_email_engagements_log`;

INSERT INTO `x-marketing.logicsource.db_email_engagements_log` (
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
  _sales_follow_up_progress,
  _leadsource,
  _createdate,
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
  _campaignCode
  )
  WITH prospect_info AS (
    SELECT DISTINCT
      CAST(_id AS STRING) AS _id,
      _email,
      _name,
      _domain,
      _jobtitle,
      _function,
      _jobrole,
      _mqldate,
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
      _hubspotScore,
      _companyID,
      _companySegment,
      _leadSegment,
      _segment,
      _propertyLeadstatus,
      _companylinkedinbio,
      _company_linkedin,
      _employee_range,
      _employee_range_c,
      _numberofemployees,
      _annualrevenue,
      _annual_revenue_range,
      _annual_revenue_range_c,
      _sfdcaccountid,
      _sfdcleadid,
      _sfdccontactid,
      _sales_follow_up_progress,
      _leadsource,
      _createddate
    FROM `logicsource.db_icp_database_log`
    WHERE _email IS NOT NULL
      AND _email NOT LIKE '%2x.marketing%'
      AND _email NOT LIKE '%logicsource.com%'
  ),
  airtable_info AS (
    SELECT
      CAST(id AS STRING) AS _pardotid,
      name AS _code,
      subject AS _subject,
      _assettitle,
      _screenshot,
      _assetType,
      _requesterName,
      CASE
        WHEN id = 262574330 THEN "LSI MI TOFU01 #1"
        ELSE _email
      END AS _email,
      _campaign,
      _campaignid,
      CASE
        WHEN _emailid = "" THEN NULL
        ELSE CAST(_emailid AS INT64)
      END AS _emailid,
      CASE
        WHEN _livedate = "" THEN NULL
        ELSE CAST(_livedate AS DATE)
      END AS _livedate,
      CASE
        WHEN _senddate = "" THEN NULL
        ELSE CAST(_senddate AS DATE)
      END AS _senddate,
      email._cihomeurl,
      email._code AS _campaignCode
    FROM `x-marketing.logicsource_hubspot.campaigns` campaign
    JOIN `x-marketing.logicsource_mysql.db_airtable_email` email
      ON CAST(campaign.id AS STRING) = _campaignid --END
  ),
  bounced AS (
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
      ''
    FROM `x-marketing.logicsource_hubspot.subscription_changes`,
      UNNEST (changes) AS status
    JOIN `x-marketing.logicsource_hubspot.email_events` activity
      ON status.value.causedbyevent.id = activity.id
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'BOUNCE' ---AND emailcampaignid = 269760036
      AND status.value.change = 'BOUNCED'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  Sent AS (
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
      ''
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'SENT' ---AND emailcampaignid = 269760036
      AND activity.recipient NOT IN ('colingilmore2@gmail.com', 'x@gmail.com')
      AND activity.recipient NOT LIKE '%2x.marketing'
      AND activity.recipient NOT LIKE '%logicsource%'
      AND activity.recipient NOT LIKE '%medifastinc.com'
      AND activity.recipient NOT LIKE '%@ckr.com%'
      AND activity.recipient NOT LIKE '%@ircinc.com%'
      AND activity.recipient NOT LIKE '%finnpartners.com%'
      AND activity.recipient NOT LIKE '%oceanstatejoblot.com%'
      AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_sent AS (
    SELECT
      Sent.*
    FROM Sent
    LEFT JOIN bounced
      ON Sent._email = bounced._email
      AND Sent._campaignID = bounced._campaignID
    WHERE bounced._email IS NULL
  ),
  delivered AS (
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
      ''
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'DELIVERED'
      AND activity.recipient NOT IN ('colingilmore2@gmail.com', 'x@gmail.com')
      AND activity.recipient NOT LIKE '%2x.marketing'
      AND activity.recipient NOT LIKE '%logicsource%'
      AND activity.recipient NOT LIKE '%medifastinc.com'
      AND activity.recipient NOT LIKE '%@ckr.com%'
      AND activity.recipient NOT LIKE '%@ircinc.com%'
      AND activity.recipient NOT LIKE '%finnpartners.com%'
      AND activity.recipient NOT LIKE '%oceanstatejoblot.com%'
      AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_delivered AS (
    SELECT
      delivered.*
    FROM delivered
    LEFT JOIN bounced
      ON delivered._email = bounced._email
      AND delivered._campaignID = bounced._campaignID
    WHERE bounced._email IS NULL
  ),
  email_open AS (
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
      ''
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'OPEN'
      AND filteredevent = FALSE
      AND activity.recipient NOT IN ('colingilmore2@gmail.com', 'x@gmail.com')
      AND activity.recipient NOT LIKE '%2x.marketing'
      AND activity.recipient NOT LIKE '%logicsource%'
      AND activity.recipient NOT LIKE '%medifastinc.com'
      AND activity.recipient NOT LIKE '%@ckr.com%'
      AND activity.recipient NOT LIKE '%@ircinc.com%'
      AND activity.recipient NOT LIKE '%finnpartners.com%'
      AND activity.recipient NOT LIKE '%oceanstatejoblot.com%'
      AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_click AS (
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
      ''
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'CLICK'
      AND filteredevent = FALSE
      AND activity.recipient NOT IN ('colingilmore2@gmail.com', 'x@gmail.com')
      AND activity.recipient NOT LIKE '%2x.marketing'
      AND activity.recipient NOT LIKE '%logicsource%'
      AND activity.recipient NOT LIKE '%medifastinc.com'
      AND activity.recipient NOT LIKE '%@ckr.com%'
      AND activity.recipient NOT LIKE '%@ircinc.com%'
      AND activity.recipient NOT LIKE '%finnpartners.com%'
      AND activity.recipient NOT LIKE '%oceanstatejoblot.com%'
      AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_bounce AS (
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
      ''
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'BOUNCE' ---AND emailcampaignid = 269760036
      AND activity.recipient NOT IN ('colingilmore2@gmail.com', 'x@gmail.com')
      AND activity.recipient NOT LIKE '%2x.marketing'
      AND activity.recipient NOT LIKE '%logicsource%'
      AND activity.recipient NOT LIKE '%medifastinc.com'
      AND activity.recipient NOT LIKE '%@ckr.com%'
      AND activity.recipient NOT LIKE '%@ircinc.com%'
      AND activity.recipient NOT LIKE '%finnpartners.com%'
      AND activity.recipient NOT LIKE '%oceanstatejoblot.com%'
      AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_deferred AS (
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
      ''
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'DEFERRED'
      AND activity.recipient NOT IN ('colingilmore2@gmail.com', 'x@gmail.com')
      AND activity.recipient NOT LIKE '%2x.marketing'
      AND activity.recipient NOT LIKE '%logicsource%'
      AND activity.recipient NOT LIKE '%medifastinc.com'
      AND activity.recipient NOT LIKE '%@ckr.com%'
      AND activity.recipient NOT LIKE '%@ircinc.com%'
      AND activity.recipient NOT LIKE '%finnpartners.com%'
      AND activity.recipient NOT LIKE '%oceanstatejoblot.com%'
      AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_dropped AS (
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
      ''
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'DROPPED'
      AND activity.recipient NOT IN ('colingilmore2@gmail.com', 'x@gmail.com')
      AND activity.recipient NOT LIKE '%2x.marketing'
      AND activity.recipient NOT LIKE '%logicsource%'
      AND activity.recipient NOT LIKE '%medifastinc.com'
      AND activity.recipient NOT LIKE '%@ckr.com%'
      AND activity.recipient NOT LIKE '%@ircinc.com%'
      AND activity.recipient NOT LIKE '%finnpartners.com%'
      AND activity.recipient NOT LIKE '%oceanstatejoblot.com%'
      AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_suppressed AS (
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
      ''
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'SUPPRESSED'
      AND activity.recipient NOT IN ('colingilmore2@gmail.com', 'x@gmail.com')
      AND activity.recipient NOT LIKE '%2x.marketing'
      AND activity.recipient NOT LIKE '%logicsource%'
      AND activity.recipient NOT LIKE '%medifastinc.com'
      AND activity.recipient NOT LIKE '%@ckr.com%'
      AND activity.recipient NOT LIKE '%@ircinc.com%'
      AND activity.recipient NOT LIKE '%finnpartners.com%'
      AND activity.recipient NOT LIKE '%oceanstatejoblot.com%'
      AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_processed AS (
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
      ''
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'PROCESSED'
      AND activity.recipient NOT IN ('colingilmore2@gmail.com', 'x@gmail.com')
      AND activity.recipient NOT LIKE '%2x.marketing'
      AND activity.recipient NOT LIKE '%logicsource%'
      AND activity.recipient NOT LIKE '%medifastinc.com'
      AND activity.recipient NOT LIKE '%@ckr.com%'
      AND activity.recipient NOT LIKE '%@ircinc.com%'
      AND activity.recipient NOT LIKE '%finnpartners.com%'
      AND activity.recipient NOT LIKE '%oceanstatejoblot.com%'
      AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_forward AS (
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
      ''
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'FORWARD'
      AND activity.recipient NOT IN ('colingilmore2@gmail.com', 'x@gmail.com')
      AND activity.recipient NOT LIKE '%2x.marketing'
      AND activity.recipient NOT LIKE '%logicsource%'
      AND activity.recipient NOT LIKE '%medifastinc.com'
      AND activity.recipient NOT LIKE '%@ckr.com%'
      AND activity.recipient NOT LIKE '%@ircinc.com%'
      AND activity.recipient NOT LIKE '%finnpartners.com%'
      AND activity.recipient NOT LIKE '%oceanstatejoblot.com%'
      AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_spam AS (
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
      ''
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'SPAMREPORT'
      AND activity.recipient NOT IN ('colingilmore2@gmail.com', 'x@gmail.com')
      AND activity.recipient NOT LIKE '%2x.marketing'
      AND activity.recipient NOT LIKE '%logicsource%'
      AND activity.recipient NOT LIKE '%medifastinc.com'
      AND activity.recipient NOT LIKE '%@ckr.com%'
      AND activity.recipient NOT LIKE '%@ircinc.com%'
      AND activity.recipient NOT LIKE '%finnpartners.com%'
      AND activity.recipient NOT LIKE '%oceanstatejoblot.com%'
      AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_print AS (
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
      ''
    FROM `x-marketing.logicsource_hubspot.email_events` activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'PRINT'
      AND activity.recipient NOT IN ('colingilmore2@gmail.com', 'x@gmail.com')
      AND activity.recipient NOT LIKE '%2x.marketing'
      AND activity.recipient NOT LIKE '%logicsource%'
      AND activity.recipient NOT LIKE '%medifastinc.com'
      AND activity.recipient NOT LIKE '%@ckr.com%'
      AND activity.recipient NOT LIKE '%@ircinc.com%'
      AND activity.recipient NOT LIKE '%finnpartners.com%'
      AND activity.recipient NOT LIKE '%oceanstatejoblot.com%'
      AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  email_unsubcribed AS (
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
      ''
    FROM `x-marketing.logicsource_hubspot.subscription_changes`,
      UNNEST (changes) AS status
    JOIN `x-marketing.logicsource_hubspot.email_events` activity
      ON status.value.causedbyevent.id = activity.id
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign
      ON activity.emailcampaignid = campaign.id
    WHERE activity.type = 'STATUSCHANGE'
      AND status.value.change = 'UNSUBSCRIBED'
      AND activity.recipient NOT IN ('colingilmore2@gmail.com', 'x@gmail.com')
      AND activity.recipient NOT LIKE '%2x.marketing'
      AND activity.recipient NOT LIKE '%logicsource%'
      AND activity.recipient NOT LIKE '%medifastinc.com'
      AND activity.recipient NOT LIKE '%@ckr.com%'
      AND activity.recipient NOT LIKE '%@ircinc.com%'
      AND activity.recipient NOT LIKE '%finnpartners.com%'
      AND activity.recipient NOT LIKE '%oceanstatejoblot.com%'
      AND activity.recipient NOT LIKE '%@osjl.com%'
      AND campaign.name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.recipient, campaign.name ORDER BY activity.created DESC) = 1
  ),
  form_filled AS (
    SELECT
      c._sdc_sequence,
      CAST(NULL AS STRING) AS devicetype,
      SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_hsmi=') + 6), '&')[ORDINAL(1)] AS _campaignID,
      --utm_content
      REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, 'utm_campaign') + 13), '&')[ORDINAL(1)], '%20', ' '), '%3A',':') AS _contentTitle,
      SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_source=') + 8), '&')[ORDINAL(1)] AS _utm_source,
      form.value.title AS form_title,
      properties.email.value AS email,
      form.value.timestamp AS timestamp,
      'Downloaded' AS engagement,
      form.value.page_url AS description,
      campaignguid,
    FROM `x-marketing.logicsource_hubspot.contacts` c,
      UNNEST (form_submissions) AS form
    JOIN `x-marketing.logicsource_hubspot.forms` forms
      ON form.value.form_id = forms.guid
  ),
  email_download AS (
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
    FROM form_filled AS activity
    JOIN `x-marketing.logicsource_hubspot.campaigns` campaign
      ON activity._campaignID = CAST(campaign.id AS STRING)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY email, description ORDER BY timestamp DESC) = 1
  ),
  contact_list AS (
    SELECT
      vid,
      properties.email.value AS email,
      property_recent_conversion_event_name.value,
      property_createdate.value AS _created_date
    FROM `x-marketing.logicsource_hubspot.contacts` c,
      UNNEST (list_memberships) list_memberships
    LEFT JOIN `x-marketing.logicsource_hubspot.contact_lists` list
      ON list_memberships.value.static_list_id = list.listid
    WHERE list_memberships.value.static_list_id = 547
  ),
  contacts AS (
    SELECT
      c._sdc_sequence,
      vid,
      CAST(NULL AS STRING) AS devicetype,
      SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url,  '_hsmi=') + 9), '&')[ORDINAL(1)] AS _campaignID,
      properties.utm_campaign.value AS _contentTitle,
      SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_source=') + 8), '&')[ORDINAL(1)] AS _utm_source,
      form.value.title AS form_title,
      properties.email.value AS email,
      form.value.timestamp AS timestamp,
      'MQL' AS engagement,
      form.value.page_url AS description,
      campaignguid,
    FROM `x-marketing.logicsource_hubspot.contacts` c,
      UNNEST (form_submissions) AS form
    JOIN `x-marketing.logicsource_hubspot.forms` forms
      ON form.value.form_id = forms.guid
  ),
  mql AS (
    SELECT
      c._sdc_sequence,
      _campaignID,
      _contentTitle,
      NULL AS _contentID,
      list.email,
      _created_date,
      'MQL' AS engagement,
      description,
      devicetype,
      CAST(list.vid AS STRING) AS linkid,
      '' AS duration,
      "" AS _response,
      _utm_source,
    FROM contact_list AS list
    LEFT JOIN contacts AS c
      ON list.vid = c.vid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY list.vid ORDER BY list.vid DESC) = 1
  ),
  engagements AS (
    SELECT
      *
    FROM email_sent
    UNION ALL
    SELECT
      *
    FROM email_delivered
    UNION ALL
    SELECT
      *
    FROM email_open
    UNION ALL
    SELECT
      *
    FROM email_click
    UNION ALL
    SELECT
      *
    FROM email_bounce
    UNION ALL
    SELECT
      *
    FROM email_unsubcribed
    UNION ALL
    SELECT
      *
    FROM email_deferred
    UNION ALL
    SELECT
      *
    FROM email_dropped
    UNION ALL
    SELECT
      *
    FROM email_suppressed
    UNION ALL
    SELECT
      *
    FROM email_processed
    UNION ALL
    SELECT
      *
    FROM email_forward
    UNION ALL
    SELECT
      *
    FROM email_spam
    UNION ALL
    SELECT
      *
    FROM email_print
  )
SELECT
  engagements.* EXCEPT (_contentid),
  prospect_info.* EXCEPT (_email),
  airtable_info.* EXCEPT (_pardotid, _code, _campaignid)
FROM engagements
LEFT JOIN prospect_info
  ON engagements._email = prospect_info._email
JOIN airtable_info
  ON engagements._campaignID = airtable_info._pardotid
UNION ALL
SELECT
  engagements.* EXCEPT (_contentid),
  prospect_info.* EXCEPT (_email),
  airtable_info.* EXCEPT (_pardotid, _code, _campaignid)
FROM email_download AS engagements
LEFT JOIN prospect_info
  ON engagements._email = prospect_info._email
LEFT JOIN airtable_info
  ON engagements._campaignID = airtable_info._pardotid
UNION ALL
SELECT
  engagements.* EXCEPT (_contentid),
  prospect_info.* EXCEPT (_email),
  airtable_info.* EXCEPT (_pardotid, _code, _campaignid)
FROM mql AS engagements
LEFT JOIN airtable_info
  ON engagements._campaignID = airtable_info._pardotid
LEFT JOIN prospect_info
  ON CAST(engagements.linkid AS STRING) = prospect_info._id
WHERE linkid NOT IN ('22925883771', '26823143566')
QUALIFY ROW_NUMBER() OVER (PARTITION BY linkid ORDER BY linkid DESC) = 1; 

--- Label Bots
UPDATE `x-marketing.logicsource.db_email_engagements_log` origin
SET origin._isBot = 'Yes'
FROM
  (
    SELECT
      CASE
        WHEN TIMESTAMP_DIFF(click._timestamp, open._timestamp, SECOND) <= 10 THEN click._email
        ELSE NULL
      END AS _email,
      click._contentTitle
    FROM `x-marketing.logicsource.db_email_engagements_log` AS click
    JOIN `x-marketing.logicsource.db_email_engagements_log` AS open
      ON LOWER(click._email) = LOWER(open._email)
      AND click._contentTitle = open._contentTitle
    WHERE click._engagement = 'Clicked'
      AND open._engagement = 'Opened'
    EXCEPT DISTINCT
    SELECT
      conversion._email,
      conversion._contentTitle
    FROM `x-marketing.logicsource.db_email_engagements_log` AS conversion
    WHERE conversion._engagement = 'Downloaded'
  ) bot
WHERE origin._email = bot._email
  AND origin._contentTitle = bot._contentTitle
  AND origin._engagement IN ('Clicked', 'Opened');

--- Set Show Export
UPDATE `x-marketing.logicsource.db_email_engagements_log` origin
SET origin._showExport = 'Yes'
FROM
  (
    WITH focused_engagement AS (
      SELECT
        _email,
        _engagement,
        _contentTitle,
        CASE
          WHEN _engagement = 'Opened' THEN 1
          WHEN _engagement = 'Clicked' THEN 2
          WHEN _engagement = 'Downloaded' THEN 3
        END AS _priority
      FROM `x-marketing.logicsource.db_email_engagements_log`
      WHERE _engagement IN ('Opened', 'Clicked', 'Downloaded')
      ORDER BY 1, 3, 4 DESC
      ),
      final_engagement AS (
        SELECT
          focused_engagement.* EXCEPT (_priority),
        FROM focused_engagement
        QUALIFY ROW_NUMBER() OVER(PARTITION BY _email, _contentTitle ORDER BY _priority DESC) = 1
      )
    SELECT
      *
    FROM final_engagement
  ) AS final
WHERE origin._email = final._email
  AND origin._engagement = final._engagement
  AND origin._contentTitle = final._contentTitle;

UPDATE `x-marketing.logicsource.db_email_engagements_log` origin
SET origin._dropped = 'True'
FROM
  (
    WITH has_engagements AS (
      SELECT
        _contentTitle,
        _email,
        SUM(IF(_engagement = 'Sent', 1, NULL)) AS _hasSent,
        SUM(IF(_engagement = 'Delivered', 1, NULL)) AS _hasDelivered,
        SUM(IF(_engagement = 'Bounced', 1, NULL)) AS _hasBounced,
      FROM `x-marketing.logicsource.db_email_engagements_log`
      WHERE _engagement IN ('Sent', 'Delivered', 'Bounced')
      GROUP BY 1, 2
      )
    SELECT
      _contentTitle,
      _email
    FROM has_engagements
    WHERE _hasSent IS NOT NULL
      AND _hasDelivered IS NOT NULL
      AND _hasBounced IS NOT NULL
  ) scenario
WHERE origin._email = scenario._email
  AND origin._contentTitle = scenario._contentTitle
  AND origin._engagement IN ('Delivered', 'Bounced');

UPDATE `x-marketing.logicsource.db_email_engagements_log` origin
SET origin._notSent = 'True'
FROM
  (
    WITH has_engagements AS (
      SELECT
        _contentTitle,
        _email,
        _emailid,
        SUM(IF(_engagement = 'Sent', 1, NULL)) AS _hasSent,
        SUM(IF(_engagement = 'Delivered', 1, NULL)) AS _hasdelivered,
        SUM(IF(_engagement = 'Deffered', 1, NULL)) AS _hasdef,
        SUM(IF(_engagement = 'Bounced', 1, NULL)) AS _hasBounced
      FROM `x-marketing.logicsource.db_email_engagements_log`
      WHERE _engagement IN ('Sent', 'Deffered', 'Delivered', 'Bounced')
      GROUP BY 1, 2, 3
      )
    SELECT
      _contentTitle,
      _email
    FROM has_engagements
    WHERE _hasSent IS NOT NULL
      AND _hasdelivered IS NULL
      AND _hasdef IS NOT NULL
      AND _hasBounced IS NULL
  ) scenario
WHERE origin._email = scenario._email
  AND origin._contentTitle = scenario._contentTitle
  AND origin._engagement = 'Sent';

---False delivered
UPDATE `x-marketing.logicsource.db_email_engagements_log` origin
SET origin._falseDelivered = 'True'
FROM
  (
    WITH has_engagements AS (
      SELECT
        _contentTitle,
        _email,
        _emailid,
        SUM(IF(_engagement = 'Delivered', 1, NULL)) AS _hasDelivered,
        SUM(IF(_engagement = 'Bounced', 1, NULL)) AS _hasBounced
      FROM `x-marketing.logicsource.db_email_engagements_log`
      WHERE _engagement IN ('Delivered', 'Bounced')
      GROUP BY 1, 2, 3
    )
    SELECT
      _contentTitle,
      _email,
      _hasDelivered,
      _hasBounced
    FROM has_engagements
    WHERE _hasDelivered IS NOT NULL
      AND _hasBounced IS NOT NULL
  ) scenario
WHERE origin._email = scenario._email
  AND origin._contentTitle = scenario._contentTitle
  AND origin._engagement IN ('Delivered');

-- Label Clicks That Are Visits and Set their Page Views
UPDATE `x-marketing.logicsource.db_email_engagements_log` origin
SET origin.isPageView = TRUE,
    origin._totalPageViews = scenario.pageviews,
    origin._averagePageViews = scenario.pageviews / scenario.visitors
FROM
  (
    WITH web_engagements AS (
      SELECT DISTINCT
        _timestamp,
        _visitorid,
        _utmcampaign,
        _totalsessionviews,
        _utmmedium,
        _utmsource,
      FROM `x-marketing.logicsource.db_web_engagements_log`
    )
    SELECT
      CONCAT(_email, _campaignid, _engagement, email._timestamp) AS _key,
      COUNT(DISTINCT web._visitorid) AS visitors,
      SUM(web._totalsessionviews) AS pageviews
    FROM `x-marketing.logicsource.db_email_engagements_log` email
    JOIN web_engagements AS web
      ON DATE(email._timestamp) = DATE(web._timestamp)
      AND email._contentTitle = web._utmcampaign
    WHERE email._engagement = 'Clicked'
      AND LOWER(web._utmsource) LIKE '%email%'
    GROUP BY 1
  ) scenario
WHERE CONCAT(_email, _campaignid, _engagement, _timestamp) = scenario._key;

--CREATE OR REPLACE TABLE `x-marketing.logicsource.report_icp_database` AS 
TRUNCATE TABLE `x-marketing.logicsource.report_icp_database`;

INSERT INTO `x-marketing.logicsource.report_icp_database` (
  _prospectid,
  _email,
  _name,
  _domain,
  value,
  _function,
  _jobrole,
  _mqldate,
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
  leadscore,
  _leadstatus,
  _ipqc_check,
  property_hubspotscore,
  company_id,
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
  _salesforcecontactid
)
SELECT
  CAST(vid AS STRING) AS _prospectid,
  property_email.value AS _email,
  COALESCE(
    CONCAT(
      property_firstname.value,
      ' ',
      property_lastname.value
    ),
    property_firstname.value
  ) AS _name,
  associated_company.properties.domain.value AS _domain,
  properties.jobtitle.value,
  properties.job_function.value AS _function,
  CASE
    WHEN property_job_role__organic_.value IS NOT NULL THEN property_job_role__organic_.value
    ELSE property_job_role.value
  END AS _jobrole,
  properties.hs_lifecyclestage_marketingqualifiedlead_date.value AS _mqldate,
  properties.hs_analytics_source.value AS _source,
  properties.hs_latest_source.value AS _latest_source,
  CASE
    WHEN property_management_level__organic_.value IS NOT NULL THEN property_management_level__organic_.value
    ELSE property_management_level.value
  END AS _seniority,
  property_phone.value AS _phone,
  associated_company.properties.name.value AS _company,
  CAST(associated_company.properties.annualrevenue.value AS STRING) AS _revenue,
  associated_company.properties.industry.value AS _industry,
  property_city.value AS _city,
  property_state.value AS _state,
  property_country.value AS _country,
  '' AS _persona,
  property_lifecyclestage.value AS _lifecycleStage,
  CAST(l.lead_score__c AS INT64) AS leadscore,
  properties.hs_lead_status.value AS _leadstatus,
  properties.ipqc_check.value AS _ipqc_check,
  property_hubspotscore.value AS property_hubspotscore,
  associated_company.company_id,
  associated_company.properties.segment__c.value AS _company_segment,
  property_lead_segment.value AS _lead_segment,
  property_segment__c.value AS _segment,
  property_leadstatus.value AS _property_leadstatus,
  associated_company.properties.linkedinbio.value AS _companylinkedinbio,
  associated_company.properties.linkedin_company_page.value AS _company_linkedin,
  associated_company.properties.employee_range.value AS _employee_range,
  associated_company.properties.employee_range_c.value AS _employee_range_c,
  CAST(associated_company.properties.numberofemployees.value AS NUMERIC) AS _numberofemployees,
  CAST(associated_company.properties.annualrevenue.value AS NUMERIC) AS _annualrevenue,
  associated_company.properties.annual_revenue_range.value AS _annual_revenue_range,
  associated_company.properties.annual_revenue_range_c.value AS _annual_revenue_range_c,
  associated_company.properties.salesforceaccountid.value AS _salesforceaccountid,
  properties.salesforceleadid.value AS _salesforceleadid,
  properties.salesforcecontactid.value AS _salesforcecontactid
FROM `x-marketing.logicsource_hubspot.contacts` k
LEFT JOIN `x-marketing.logicsource_salesforce.Lead` l
  ON LOWER(l.email) = LOWER(property_email.value)
WHERE property_email.value IS NOT NULL
  AND property_email.value NOT LIKE '%2x.marketing%'
  AND property_email.value NOT LIKE '%logicsource%';

------------------------------------------------
-------------- Content Analytics ---------------
------------------------------------------------
-- CREATE OR REPLACE TABLE `x-marketing.logicsource.db_email_content_analytics` AS
TRUNCATE TABLE `x-marketing.logicsource.db_email_content_analytics`;
INSERT INTO `x-marketing.logicsource.db_email_content_analytics`
SELECT
  email.* EXCEPT(_cihomeurl, _persona, _sales_follow_up_progress, _leadsource, _createdate),
  content._contentitem,
  content._contenttype,
  content._gatingstrategy,
  content._homeurl,
  content._summary,
  content._status,
  content._buyerstage,
  content._vertical,
  content._persona
FROM `x-marketing.logicsource.db_email_engagements_log` email
JOIN `x-marketing.logicsource_mysql.db_airtable_content_inventory` content
  ON email._cihomeurl = content._homeurl;