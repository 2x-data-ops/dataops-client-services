TRUNCATE TABLE `x-marketing.thunder.db_email_engagements_log`;

INSERT INTO `x-marketing.thunder.db_email_engagements_log` (
  _sdc_sequence,
  _prospectID, 
  _campaignID,
  _timestamp,
  _engagement,
  _description,
  _form_handler_id,
  _form_id,
  _list_email_id,
  _email_template_id,
  _email,
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
  _sfdc_leadid,
  _account_owner,
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
  _landingpage,
  _utmcampaign,
  _campaign_name_gs,
  _email_type_gs,
  _email_template_name

  -- _contentTitle, 
  -- _utm_source,
  -- _classification,
  -- _seniority,
  -- _score,
  -- _batch
)
--Getting prospect info details from prospect table--
WITH prospect_info_consolidate AS (
WITH lead_sfdc AS (
SELECT lead.id,
       lead.email,
       lead.ownerid,
       user.name AS owner_name
FROM `x-marketing.thunder_salesforce.Lead` lead
LEFT JOIN (SELECT name, id FROM `x-marketing.thunder_salesforce.User`) user ON user.id = lead.ownerid
),
prospect_info AS (
  WITH prospect1 AS (
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
        crm_lead_fid AS _crm_lead_fid
    FROM `x-marketing.thunder_pardot.prospects` prospect
),
prospect2 AS (
    SELECT
        _prospectid,
        _email,
        CONCAT(_firstname, ' ', _lastname) AS _name,
        _jobtitle,
        _website,
        _phone,
        INITCAP(_company) AS _company,
        _annualrevenue,
        _employees,
        INITCAP(_industry) AS _industry,
        _city,
        _state,
        _country,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', _createddate) AS _createddate,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', _updateddate) AS _updateddate,
        _crmcontactfid AS _crm_contact_fid,
        _crmleadfid AS _crm_lead_fid
    FROM `thunder_mysql.db_thunder_pardot_prospect_list`
),
prospect_union AS (
SELECT * FROM prospect1
UNION ALL
SELECT * FROM prospect2
),
distinct_prospect AS (
SELECT *,
    ROW_NUMBER() OVER(PARTITION BY _prospectID, _email) AS rownum
FROM prospect_union
)
SELECT * EXCEPT (rownum)
FROM distinct_prospect
WHERE rownum = 1
)
SELECT prospect_info.*, lead_sfdc.id AS sfdc_leadid, lead_sfdc.owner_name AS account_owner
FROM prospect_info
LEFT JOIN lead_sfdc ON lead_sfdc.email = prospect_info._email

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
  FROM `thunder_mysql.db_airtable_email`
),
sent_email AS (
    SELECT
      activity._sdc_sequence,
      CAST(activity.prospect_id AS STRING) AS _prospectID,
      -- prospect.email AS _email,
      CAST(activity.campaign_id AS STRING) AS _campaignID,     
      activity.created_at AS _timestamp,
      'Sent' AS _engagement,
      '' AS _description,
      CAST (activity.form_handler_id AS STRING) AS _form_handler_id,
      CAST (activity.form_id AS STRING) AS _form_id,
      CAST(list_email_id AS STRING) _list_email_id,
      CAST(email_template_id AS STRING) AS _email_template_id,
    FROM `x-marketing.thunder_pardot.visitor_activities` activity
    LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
      ON activity.prospect_id = prospect.id
    WHERE type_name = 'Email'
      AND type = 6 /* This specified the email event to Sent */
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.prospect_id, activity.campaign_id, list_email_id
      ORDER BY activity.created_at DESC ) = 1
),
hardbounced_email AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    -- prospect.email AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID, 
    activity.created_at AS _timestamp,
    'Hard Bounced' AS _engagement,
    '' AS _description,
    CAST (activity.form_handler_id AS STRING) AS _form_handler_id,
    CAST (activity.form_id AS STRING) AS _form_id,
    CAST(list_email_id AS STRING) _list_email_id,
    CAST(email_template_id AS STRING) AS _email_template_id,
  FROM `x-marketing.thunder_pardot.visitor_activities` activity
  LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
    ON activity.prospect_id = prospect.id
  WHERE activity.type in (13) /* Unsubscribe Page / Indirect Unsubscribe Open */
  QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.prospect_id, activity.campaign_id, list_email_id
    ORDER BY activity.created_at DESC ) = 1
),
softbounced_email AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    -- prospect.email AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID, 
    activity.created_at AS _timestamp,
    'Soft Bounced' AS _engagement,
    '' AS _description,
    CAST (activity.form_handler_id AS STRING) AS _form_handler_id,
    CAST (activity.form_id AS STRING) AS _form_id,
    CAST(list_email_id AS STRING) _list_email_id,
    CAST(email_template_id AS STRING) AS _email_template_id,
  FROM `x-marketing.thunder_pardot.visitor_activities` activity
  LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
    ON activity.prospect_id = prospect.id
  WHERE activity.type in (36) /* Unsubscribe Page / Indirect Unsubscribe Open */
  QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.prospect_id, activity.campaign_id, list_email_id
    ORDER BY activity.created_at DESC ) = 1
),
delivered_email AS (
  WITH sent AS (
    SELECT
      activity._sdc_sequence,
      CAST(activity.prospect_id AS STRING) AS _prospectID,
      -- prospect.email AS _email,
      CAST(activity.campaign_id AS STRING) AS _campaignID,     
      activity.created_at AS _timestamp,
      'Delivered' AS _engagement,
      '' AS _description,
      CAST (activity.form_handler_id AS STRING) AS _form_handler_id,
      CAST (activity.form_id AS STRING) AS _form_id,
      CAST(list_email_id AS STRING) _list_email_id,
      CAST(email_template_id AS STRING) AS _email_template_id,
    FROM `x-marketing.thunder_pardot.visitor_activities` activity
    LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
      ON activity.prospect_id = prospect.id
    WHERE type_name = 'Email'
      AND type = 6 /* This specified the email event to Sent */
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.prospect_id, activity.campaign_id, list_email_id
      ORDER BY activity.created_at DESC ) = 1
  ),
  allbounce AS (
    WITH hardbounced AS (
    SELECT
      activity._sdc_sequence,
      CAST(activity.prospect_id AS STRING) AS _prospectID,
      -- prospect.email AS _email,
      CAST(activity.campaign_id AS STRING) AS _campaignID, 
      activity.created_at AS _timestamp,
      'Hard Bounced' AS _engagement,
      '' AS _description,
      CAST (activity.form_handler_id AS STRING) AS _form_handler_id,
      CAST (activity.form_id AS STRING) AS _form_id,
      CAST(list_email_id AS STRING) _list_email_id,
      CAST(email_template_id AS STRING) AS _email_template_id,
    FROM `x-marketing.thunder_pardot.visitor_activities` activity
    LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
      ON activity.prospect_id = prospect.id
    WHERE activity.type in (13) /* Unsubscribe Page / Indirect Unsubscribe Open */
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.prospect_id, activity.campaign_id, list_email_id
      ORDER BY activity.created_at DESC ) = 1
    ),
    softbounced AS (
      SELECT
        activity._sdc_sequence,
        CAST(activity.prospect_id AS STRING) AS _prospectID,
        -- prospect.email AS _email,
        CAST(activity.campaign_id AS STRING) AS _campaignID, 
        activity.created_at AS _timestamp,
        'Soft Bounced' AS _engagement,
        '' AS _description,
        CAST (activity.form_handler_id AS STRING) AS _form_handler_id,
        CAST (activity.form_id AS STRING) AS _form_id,
        CAST(list_email_id AS STRING) _list_email_id,
        CAST(email_template_id AS STRING) AS _email_template_id,
      FROM `x-marketing.thunder_pardot.visitor_activities` activity
      LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
        ON activity.prospect_id = prospect.id
      WHERE activity.type in (36) /* Unsubscribe Page / Indirect Unsubscribe Open */
      QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.prospect_id, activity.campaign_id, list_email_id
        ORDER BY activity.created_at DESC ) = 1
    )
    SELECT * FROM hardbounced
    UNION ALL
    SELECT * FROM softbounced
  )
  SELECT sent.*
  FROM sent
  LEFT JOIN allbounce
  ON sent._prospectID = allbounce._prospectID
    AND sent._campaignID = allbounce._campaignID
  WHERE allbounce._prospectID IS NULL
),
opened_email AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    -- prospect.email AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID, 
    activity.created_at AS _timestamp,
    'Opened' AS _engagement,
    '' AS _description,
    CAST (activity.form_handler_id AS STRING) AS _form_handler_id,
    CAST (activity.form_id AS STRING) AS _form_id,
    CAST(list_email_id AS STRING) _list_email_id,
    CAST(email_template_id AS STRING) AS _email_template_id,
  FROM `x-marketing.thunder_pardot.visitor_activities` activity
  LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
    ON activity.prospect_id = prospect.id
  WHERE activity.type = 11 /* This specified the email event to Open */
  QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.prospect_id, activity.campaign_id, list_email_id
    ORDER BY activity.created_at DESC ) = 1
),
clicked_email AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    CAST(gsheet.campaign_id AS STRING) AS _campaignID, 
    activity.created_at AS _timestamp,
    'Clicked' AS _engagement,
    url AS _description,
    CAST(NULL AS STRING) AS form_handler_id,
    CAST(NULL AS STRING) AS form_id,
    CAST(activity.list_email_id AS STRING) _list_email_id,
    CAST(email_template_id AS STRING) AS _email_template_id,
  FROM `x-marketing.thunder_pardot.email_clicks` activity
  LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
    ON activity.prospect_id = prospect.id
  LEFT JOIN `x-marketing.thunder_googlesheets.Sheet1` gsheet
    ON activity.email_template_id = gsheet._email_template_id
  QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.prospect_id, activity.email_template_id, activity.list_email_id
    ORDER BY activity.created_at DESC ) = 1
),
unsubscribed_email AS(
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    -- prospect.email AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID, 
    activity.created_at AS _timestamp,
    'Unsubscribed' AS _engagement,
    '' AS _description,
    CAST(list_email_id AS STRING) _list_email_id,
    CAST(email_template_id AS STRING) AS _email_template_id,
    CAST (activity.form_handler_id AS STRING) AS _form_handler_id,
    CAST (activity.form_id AS STRING) AS _form_id,
  FROM `x-marketing.thunder_pardot.visitor_activities` activity
  LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
    ON activity.prospect_id = prospect.id
  WHERE  activity.type in (12, 35)    /* Unsubscribe Page / Indirect Unsubscribe Open */
  QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.prospect_id, activity.campaign_id, list_email_id
    ORDER BY activity.created_at DESC ) = 1
),
form_filled AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    CAST(activity.campaign_id AS STRING) AS _campaignID,
    activity.created_at AS _timestamp,
    'Form Filled' AS _engagement,
    '' AS _description,
    CAST(NULL AS STRING) _list_email_id,
    CAST(email_template_id AS STRING) AS _email_template_id,
    CAST (activity.form_handler_id AS STRING) AS _form_handler_id,
    CAST (activity.form_id AS STRING) AS _form_id,
  FROM
    `x-marketing.thunder_pardot.visitor_activities` activity
  LEFT JOIN
    `x-marketing.thunder_pardot.prospects` prospect
  ON
    activity.prospect_id = prospect.id
  WHERE
    activity.type_name IN ('Form', 'Form Handler')
  AND 
    activity.type = 4   /* Download */
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
  UNION ALL
  SELECT * FROM form_filled
),
get_template_name AS (
    SELECT
      campaign_name AS _campaign_name_gs,
      _email_template_id,
      email_type AS _email_type_gs,
      email_template_name AS _email_template_name
    FROM `x-marketing.thunder_googlesheets.Sheet1`
),
campaign_info AS (
  WITH gsheet_campaign AS (
    SELECT
      campaign_id AS _campaignID,
      campaign_name AS _utmcampaign,
    FROM `x-marketing.thunder_googlesheets.Sheet1`
  ),
  pardot_campaign AS (
    SELECT
      id AS _campaignID,
      name AS _utmcampaign,
    FROM `x-marketing.thunder_pardot.campaigns`
    WHERE id NOT IN (567823, 567838, 576482, 568105, 576479, 567811, 579707, 579698, 577913, 577925, 577919, 577922, 590557, 603314, 598405, 611389)


  ),
  combined_campaign AS (
    SELECT * FROM gsheet_campaign
    UNION ALL
    SELECT * FROM pardot_campaign
  )
  SELECT * FROM combined_campaign
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _campaignID, _utmcampaign) = 1 
),
--Combine prospect info left join with engagement together with campaign info---
alldata AS (
SELECT
  engagements.*,
  prospect_info_consolidate.* EXCEPT(_prospectID),
  -- get_template_name.* EXCEPT(_email_template_id),
  airtable_info.* EXCEPT(_list_email_id),
  campaign_info._utmcampaign
FROM engagements
LEFT JOIN prospect_info_consolidate
  ON engagements._prospectID = prospect_info_consolidate._prospectID 
LEFT JOIN campaign_info
  ON engagements._campaignID = CAST(campaign_info._campaignID AS STRING)
LEFT JOIN airtable_info
  ON engagements._list_email_id = airtable_info._list_email_id
)
SELECT alldata.*,
get_template_name.* EXCEPT(_email_template_id)
FROM alldata
LEFT JOIN get_template_name
  ON alldata._email_template_id = CAST(get_template_name._email_template_id AS STRING);
-- WHERE 
-- -- _campaignID = '567823'
-- _engagement = 'Sent'
-- AND _name like 'Zachary Wagner'


---OPPS Combined With Email Engagement---

CREATE OR REPLACE TABLE `x-marketing.thunder.db_email_opps_combined` AS 
SELECT
  email.* EXCEPT(_website),
  REGEXP_REPLACE(REGEXP_REPLACE(_website, '^http://', ''), '^.*?\\.([^\\.]+\\.[^\\.]+)$', '\\1') AS _website,
  opps.*
FROM `thunder.db_email_engagements_log` email
JOIN `thunder.db_sf_opportunities` opps
ON REGEXP_REPLACE(REGEXP_REPLACE(_website, '^http://', ''), '^.*?\\.([^\\.]+\\.[^\\.]+)$', '\\1') = opps.domain



