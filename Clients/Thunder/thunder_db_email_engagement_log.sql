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
  FROM thunder_mysql.db_airtable_email
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
    CAST(NULL AS STRING) AS _campaignID, 
    activity.created_at AS _timestamp,
    'Clicked' AS _engagement,
    url AS _description,
    CAST(NULL AS STRING) AS form_handler_id,
    CAST(NULL AS STRING) AS form_id,
    CAST(list_email_id AS STRING) _list_email_id,
    CAST(email_template_id AS STRING) AS _email_template_id,
  FROM `x-marketing.thunder_pardot.email_clicks` activity
  LEFT JOIN `x-marketing.thunder_pardot.prospects` prospect
    ON activity.prospect_id = prospect.id
  QUALIFY ROW_NUMBER() OVER(PARTITION BY activity.prospect_id, activity.email_template_id, list_email_id
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
  SELECT * FROM opened_email
  UNION ALL
  SELECT * FROM clicked_email
  UNION ALL
  SELECT * FROM unsubscribed_email
  UNION ALL
  SELECT * FROM form_filled
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
  prospect_info_consolidate.* EXCEPT(_prospectID),
  campaign_info.* EXCEPT(_campaignID),
  airtable_info.* EXCEPT(_list_email_id)
FROM engagements
LEFT JOIN prospect_info_consolidate
  ON engagements._prospectID = prospect_info_consolidate._prospectID 
LEFT JOIN campaign_info
  ON engagements._campaignID = CAST(campaign_info._campaignID AS STRING)
LEFT JOIN airtable_info
  ON engagements._list_email_id = airtable_info._list_email_id;


INSERT INTO `x-marketing.thunder.db_email_engagements_log`(
  _sdc_sequence,_campaignID,_engagement,_email,_prospectID,_timestamp,_description,_list_email_id,_email_template_id, _name,_phone,_jobtitle,_seniority,_segment,_persona,_tier,_company,_domain,_industry,_subIndustry,_country,_city,_annualrevenue,_employees,_subject,_screenshot,_landingPage,_utm_source,_utmcampaign,_utm_medium,_contentID,_contentTitle,_storyBrandStage,_abstract,_salesforceLeadStage,_salesforceLastActivity,_salesforceCreated,_salesforceOpportunityStage,_salesforceOpportunityValue,_salesforceOpportunityName,_salesforceOpportunityCreated,_sfdcAccountID,_sfdcLeadID,_sfdcContactID,_sfdcOpportunityID,_meetingScheduledDate,_salesforceOpportunityCloseDate,_state,_function,_lb_email,_utm_content,_campaignSentDate,_subCampaign,_preview,_isPageView,_stage,_totalPageViews,_averagePageViews,_device_type,_duration,_response,_linkid,_lifecycleStage,_isBot,_notSent,_showExport,_dropped,_falseDelivered,_createddate,_updateddate,_crm_contact_fid,_crm_lead_fid,_lists,_contenttype,_createdby,_assets,_website,_asseturl,_assettitle,_assettype,_emailname,_form_id,_form_handler_id
)
WITH sent AS (
SELECT * FROM `x-marketing.thunder.db_email_engagements_log`
WHERE _engagement = 'Sent'
),
allbounce AS (
  SELECT * FROM `x-marketing.thunder.db_email_engagements_log`
  WHERE _engagement IN ('Hard Bounced', 'Soft Bounced')
)
SELECT sent._sdc_sequence,sent._campaignID,'Delivered' AS _engagement,sent._email,sent._prospectID,sent._timestamp,sent._description,sent._list_email_id,sent._email_template_id,sent._name,sent._phone,sent._jobtitle,sent._seniority,sent._segment,sent._persona,sent._tier,sent._company,sent._domain,sent._industry,sent._subIndustry,sent._country,sent._city,sent._annualrevenue,sent._employees,sent._subject,sent._screenshot,sent._landingPage,sent._utm_source,sent._utmcampaign,sent._utm_medium,sent._contentID,sent._contentTitle,sent._storyBrandStage,sent._abstract,sent._salesforceLeadStage,sent._salesforceLastActivity,sent._salesforceCreated,sent._salesforceOpportunityStage,sent._salesforceOpportunityValue,sent._salesforceOpportunityName,sent._salesforceOpportunityCreated,sent._sfdcAccountID,sent._sfdcLeadID,sent._sfdcContactID,sent._sfdcOpportunityID,sent._meetingScheduledDate,sent._salesforceOpportunityCloseDate,sent._state,sent._function,sent._lb_email,sent._utm_content,sent._campaignSentDate,sent._subCampaign,sent._preview,sent._isPageView,sent._stage,sent._totalPageViews,sent._averagePageViews,sent._device_type,sent._duration,sent._response,sent._linkid,sent._lifecycleStage,sent._isBot,sent._notSent,sent._showExport,sent._dropped,sent._falseDelivered,sent._createddate,sent._updateddate,sent._crm_contact_fid,sent._crm_lead_fid,sent._lists,sent._contenttype,sent._createdby,sent._assets,sent._website,sent._asseturl,sent._assettitle,sent._assettype,sent._emailname,sent._form_id,sent._form_handler_id
  FROM sent
LEFT JOIN allbounce
ON sent._prospectID = allbounce._prospectID
    AND sent._campaignID = allbounce._campaignID
 WHERE allbounce._prospectID IS NULL;


---OPPS Combined With Email Engagement---

CREATE OR REPLACE TABLE `x-marketing.thunder.db_email_opps_combined` AS 
SELECT
  email.* EXCEPT(_website),
  REGEXP_REPLACE(REGEXP_REPLACE(_website, '^http://', ''), '^.*?\\.([^\\.]+\\.[^\\.]+)$', '\\1') AS _website,
  opps.*
FROM `thunder.db_email_engagements_log` email
JOIN `thunder.db_sf_opportunities` opps
ON REGEXP_REPLACE(REGEXP_REPLACE(_website, '^http://', ''), '^.*?\\.([^\\.]+\\.[^\\.]+)$', '\\1') = opps.domain




-- SELECT DISTINCT domain
-- FROM `thunder.db_sf_opportunities` opps

-- SELECT DISTINCT
-- REGEXP_REPLACE(_website, r'^(http:\/\/www\.|http:\/\/)', '') AS _website
-- FROM `thunder.db_email_engagements_log` email