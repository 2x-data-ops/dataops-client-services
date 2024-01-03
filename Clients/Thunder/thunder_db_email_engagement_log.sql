TRUNCATE TABLE `x-marketing.thunder.db_email_engagements_log`;

INSERT INTO `x-marketing.thunder.db_email_engagements_log` (
  _sdc_sequence,
  _prospectID,
  _email,  
  _campaignID,
  _timestamp,
  _engagement,
  _description,
  _list_email_id,
  _name,
  _jobtitle,
  _website,
  _phone,
  _company,
  _annualrevenue,
  _employees,
  _industry,
  _city,
  _state, 
  _country,
  _createddate,
  _updateddate,
  _crm_contact_fid,
  _crm_lead_fid,
  _utmcampaign,
  _screenshot,
  _assettitle,
  -- _mql,
  _subject,
  -- _emailproof,
  -- _whatwedo,
  _lists,
  _assettype,
  -- _senddate,
  -- _livedate,
  _asseturl,
  _emailname,
  _contenttype,
  _landingpage

  -- _contentTitle, 
  -- _utm_source,
  -- _classification,
  -- _seniority,
  -- _score,
  -- _batch
)
--Getting prospect info details from prospect table--
WITH prospect_info AS (
  SELECT
    CAST(id AS STRING) AS _prospectID,
    prospect.email AS _email,
    CONCAT(first_name, ' ', last_name) AS _name,
    job_title AS _jobtitle,
    website AS _website,
    phone AS _phone,
    INITCAP(company) AS _company,
    annual_revenue AS _annualrevenue,
    employees AS _employees,
    INITCAP(industry) AS _industry,
    city AS _city,
    state AS _state,
    country AS _country,
    created_at AS _createddate,
    updated_at AS _updateddate,
    crm_contact_fid AS _crm_contact_fid,
    crm_lead_fid AS _crm_lead_fid,

  FROM `x-marketing.thunder_pardot.prospects` prospect

  --should join with salesforce contact--
  --later--
),
airtable_info AS (
  SELECT
     _screenshot,
     _assettitle,
    --  _mql,
     _subject,
    --  _airtableid,
    --  _emailproof,
    --  _whatwedo,
    --  _campaignid,
     _code AS _lists,
     _emailid AS _list_email_id,
     _assettype,
    --  _senddate,
     _livedate,
     _asseturl,
    --  _emailname,
     _subscriptiontype AS _contenttype,
     _landingpage
  FROM thunder_mysql.db_airtable_email
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
      CAST(list_email_id AS STRING) _list_email_id,
      ROW_NUMBER() OVER(PARTITION BY activity.prospect_id, activity.campaign_id
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
      CAST(list_email_id AS STRING) _list_email_id,
      ROW_NUMBER() OVER(PARTITION BY activity.prospect_id, activity.campaign_id
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
      CAST(list_email_id AS STRING) _list_email_id,
      ROW_NUMBER() OVER(PARTITION BY activity.prospect_id, activity.campaign_id
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
      CAST(list_email_id AS STRING) _list_email_id,
      ROW_NUMBER() OVER(PARTITION BY activity.prospect_id, activity.campaign_id
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
      CAST(list_email_id AS STRING) _list_email_id,
      ROW_NUMBER() OVER(PARTITION BY activity.prospect_id, activity.list_email_id
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
      CAST(list_email_id AS STRING) _list_email_id,
      ROW_NUMBER() OVER(PARTITION BY activity.prospect_id, activity.campaign_id
       ORDER BY activity.created_at DESC ) AS _rownum
    FROM `x-marketing.thunder_pardot.visitor_activities` activity
    LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
      ON activity.prospect_id = prospect.id
    WHERE  activity.type in (12, 35)    /* Unsubscribe Page / Indirect Unsubscribe Open */
  )
  WHERE _rownum = 1  
),
delivered_email AS (
  -- SELECT
  --     origin._sdc_sequence,
  --     origin._prospectID,
  --     origin._email,
  --     origin._campaignID,     
  --     origin._timestamp,
  --     'Delivered' AS _engagement,
  --     origin._description,
  --     origin._list_email_id
  -- FROM sent_email origin
  -- JOIN (
  --   SELECT
  --     sent_email._prospectID,
  --     sent_email._campaignID,
  --     sent_email._list_email_id
  --   FROM
  --     sent_email
    
  --   EXCEPT DISTINCT 

  --   SELECT
  --     allbounced_email._prospectID,
  --     allbounced_email._campaignID,
  --     allbounced_email._list_email_id
  --   FROM
  --     allbounced_email
  -- ) scenario
  -- ON origin._prospectID = scenario._prospectID
  -- AND origin._campaignID = scenario._campaignID
  -- AND origin._list_email_id = scenario._list_email_id

  SELECT
  sent._sdc_sequence,
  sent._prospectID,
  sent._email,
  sent._campaignID,
  sent._timestamp,
  'Delivered' AS _engagement,
  sent._description,
  sent._list_email_id
FROM sent_email AS sent
LEFT JOIN allbounced_email
  ON sent._prospectID = allbounced_email._prospectID
  AND sent._campaignID = allbounced_email._campaignID
WHERE allbounced_email._prospectID IS NULL 
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
),
campaign_info AS(
  SELECT
    id AS _campaignID,
    name AS _utmcampaign
  FROM `x-marketing.thunder_pardot.campaigns`
)
-- ,
-- list AS (
--   SELECT
--     id AS _listid,
--     name AS _listname,
--   FROM `x-marketing.thunder_pardot.lists` 
-- )
--Combine prospect info left join with engagement together with campaign info---
SELECT
  engagements.*,
  prospect_info.* EXCEPT(_email, _prospectID),
  campaign_info.* EXCEPT(_campaignID),
  airtable_info.* EXCEPT(_list_email_id)
FROM engagements
LEFT JOIN prospect_info
  ON engagements._prospectID = prospect_info._prospectID 
LEFT JOIN campaign_info
  ON engagements._campaignID = CAST(campaign_info._campaignID AS STRING)
LEFT JOIN airtable_info
  ON engagements._list_email_id = airtable_info._list_email_id;


---OPPS Combined With Email Engagement---

CREATE OR REPLACE TABLE `x-marketing.thunder.db_email_opps_combined` AS 
SELECT
  email.* EXCEPT(_website),
  REGEXP_REPLACE(_website, r'^(http:\/\/www\.|http:\/\/)', '') AS _website,
  opps.*
FROM `thunder.db_email_engagements_log` email
JOIN `thunder.db_sf_opportunities` opps
ON REGEXP_REPLACE(email._website, r'^(http:\/\/www\.|http:\/\/)', '') = opps.domain




-- SELECT DISTINCT domain
-- FROM `thunder.db_sf_opportunities` opps

-- SELECT DISTINCT
-- REGEXP_REPLACE(_website, r'^(http:\/\/www\.|http:\/\/)', '') AS _website
-- FROM `thunder.db_email_engagements_log` email







