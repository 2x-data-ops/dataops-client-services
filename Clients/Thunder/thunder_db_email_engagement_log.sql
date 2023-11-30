TRUNCATE TABLE `x-marketing.thunder.db_email_engagements_log`;

INSERT INTO `x-marketing.thunder.db_email_engagements_log` (
  _sdc_sequence,
  _prospectID,
  _email,  
  _campaignID,
  _timestamp,
  _engagement,
  _description,
  _name,
  _title,
  _phone,
  _company,
  _revenue,
  _industry,
  _city,
  _state, 
  _country
  -- _contentTitle, 
  -- _utmcampaign,
  -- _utm_source,
  -- _subject, 
  -- _campaignSentDate, 
  -- _screenshot, 
  -- _landingPage,
  -- _classification,
  -- _emailName,
  -- _seniority,
  -- _website,
  -- _crmleadfid,
  -- _crmcontactfid,
  -- _score,
  -- _batch
)
--Getting prospect info details from prospect table--
--No airtable at the moment--
WITH prospect_info AS (
  SELECT
    CAST(id AS STRING) AS _prospectID,
    prospect.email AS _email,
    CONCAT(first_name, ' ', last_name) AS _name,
    job_title AS _title,
    -- function AS _function,
    -- website AS _website,
    phone AS _phone,
    INITCAP(company) AS _company,
    annual_revenue AS _revenue,
    INITCAP(industry) AS _industry,
    city AS _city,
    state AS _state,
    country AS _country
  FROM `x-marketing.thunder_pardot.prospects` prospect

  --should join with salesforce contact--
  --later--
),
sent_email AS (
  SELECT * EXCEPT(_rownum)
  FROM(
    SELECT
      activity._sdc_sequence,
      CAST(prospect.id AS STRING) AS _prospectID,
      prospect.email AS _email,
      CAST(activity.campaign_id AS STRING) AS _campaignID,     
      activity.created_at AS _timestamp,
      'Sent' AS _engagement,
      '' AS _description,
      ROW_NUMBER() OVER(PARTITION BY prospect.email, prospect.id
       ORDER BY activity.created_at DESC ) AS _rownum
    FROM `x-marketing.thunder_pardot.visitor_activities` activity
    LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
      ON activity.prospect_id = prospect.id
    WHERE type_name = 'Email'
      AND type = 6 /* This specified the email event to Sent */
  )
  WHERE _rownum = 1
),
hardbounced_email AS (
  SELECT * EXCEPT(_rownum)
  FROM(
      SELECT
      activity._sdc_sequence,
      CAST(prospect.id AS STRING) AS _prospectID,
      prospect.email AS _email,
      CAST(activity.campaign_id AS STRING) AS _campaignID, 
      activity.created_at AS _timestamp,
      'Hard Bounced' AS _engagement,
      '' AS _description,
      ROW_NUMBER() OVER(PARTITION BY prospect.email, prospect.id
       ORDER BY activity.created_at DESC ) AS _rownum
    FROM `x-marketing.thunder_pardot.visitor_activities` activity
    LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
      ON activity.prospect_id = prospect.id
    WHERE activity.type in (13) /* Unsubscribe Page / Indirect Unsubscribe Open */
  )
  WHERE _rownum = 1
),
softbounced_email AS (
  SELECT * EXCEPT(_rownum)
  FROM(
      SELECT
      activity._sdc_sequence,
      CAST(prospect.id AS STRING) AS _prospectID,
      prospect.email AS _email,
      CAST(activity.campaign_id AS STRING) AS _campaignID, 
      activity.created_at AS _timestamp,
      'Soft Bounced' AS _engagement,
      '' AS _description,
      ROW_NUMBER() OVER(PARTITION BY prospect.email, prospect.id
       ORDER BY activity.created_at DESC ) AS _rownum
    FROM `x-marketing.thunder_pardot.visitor_activities` activity
    LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
      ON activity.prospect_id = prospect.id
    WHERE activity.type in (36) /* Unsubscribe Page / Indirect Unsubscribe Open */
  )
  WHERE _rownum = 1
),
allbounced_email AS (
  SELECT * FROM hardbounced_email
  UNION ALL
  SELECT * FROM softbounced_email
),
opened_email AS (
    SELECT * EXCEPT(_rownum)
  FROM(
      SELECT
      activity._sdc_sequence,
      CAST(prospect.id AS STRING) AS _prospectID,
      prospect.email AS _email,
      CAST(activity.campaign_id AS STRING) AS _campaignID, 
      activity.created_at AS _timestamp,
      'Opened' AS _engagement,
      '' AS _description,
      ROW_NUMBER() OVER(PARTITION BY prospect.email, prospect.id
       ORDER BY activity.created_at DESC ) AS _rownum
    FROM `x-marketing.thunder_pardot.visitor_activities` activity
    LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
      ON activity.prospect_id = prospect.id
    WHERE activity.type = 11 /* This specified the email event to Open */
  )
  WHERE _rownum = 1
),
clicked_email AS (
  SELECT * EXCEPT(_rownum)
  FROM(
      SELECT
      activity._sdc_sequence,
      CAST(prospect.id AS STRING) AS _prospectID,
      prospect.email AS _email,
      CAST(NULL AS STRING) AS _campaignID, 
      activity.created_at AS _timestamp,
      'Clicked' AS _engagement,
      url AS _description,
      ROW_NUMBER() OVER(PARTITION BY prospect.email, prospect.id
       ORDER BY activity.created_at DESC ) AS _rownum
    FROM `x-marketing.thunder_pardot.email_clicks` activity
    LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
      ON activity.prospect_id = prospect.id
  )
  WHERE _rownum = 1
),
unsubscribed_email AS(
  SELECT * EXCEPT(_rownum)
  FROM(
      SELECT
      activity._sdc_sequence,
      CAST(prospect.id AS STRING) AS _prospectID,
      prospect.email AS _email,
      CAST(activity.campaign_id AS STRING) AS _campaignID, 
      activity.created_at AS _timestamp,
      'Unsubscribed' AS _engagement,
      '' AS _description,
      ROW_NUMBER() OVER(PARTITION BY prospect.email, prospect.id
       ORDER BY activity.created_at DESC ) AS _rownum
    FROM `x-marketing.thunder_pardot.visitor_activities` activity
    LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
      ON activity.prospect_id = prospect.id
    WHERE  activity.type in (12, 35)    /* Unsubscribe Page / Indirect Unsubscribe Open */
  )
  WHERE _rownum = 1  
),
delivered_email AS (
  SELECT
      origin._sdc_sequence,
      origin._prospectID,
      origin._email,
      origin._campaignID,     
      origin._timestamp,
      'Delivered' AS _engagement,
      origin._description
  FROM sent_email origin
  JOIN (
    SELECT
      sent_email._prospectID,
      sent_email._campaignID
    FROM
      sent_email
    
    EXCEPT DISTINCT 

    SELECT
      allbounced_email._prospectID,
      allbounced_email._campaignID
    FROM
      allbounced_email
  ) scenario
  ON origin._prospectID = scenario._prospectID
  AND origin._campaignID = scenario._campaignID
),
engagements AS (
  SELECT * FROM sent_email
  UNION ALL
  SELECT * FROM hardbounced_email
  UNION ALL
  SELECT * FROM softbounced_email
  UNION ALL
  SELECT * FROM delivered_email
  UNION ALL
  SELECT * FROM opened_email
  UNION ALL
  SELECT * FROM clicked_email
  UNION ALL
  SELECT * FROM unsubscribed_email
)
SELECT
  engagements.*,
  prospect_info.* EXCEPT(_email, _prospectID)
FROM engagements
LEFT JOIN prospect_info
ON prospect_info._prospectID = engagements._prospectID















