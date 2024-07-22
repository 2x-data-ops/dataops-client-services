-- INSERT INTO `x-marketing.thunder.db_email_engagements_log` (
--     _sdc_sequence,
--     _campaignID,
--     _engagement,
--     _email,
--     _prospectID,
--     _timestamp,
--     _description,
--     _list_email_id,
--     _name,
--     _phone,
--     _jobtitle,
--     _company,
--     _industry,
--     _country,
--     _city,
--     _annualrevenue,
--     _employees,
--     _subject,
--     _screenshot,
--     _landingPage,
--     _utmcampaign,
--     _state,
--     _createddate,
--     _updateddate,
--     _crm_contact_fid,
--     _crm_lead_fid,
--     _lists,
--     _contenttype,
--     _website,
--     _asseturl,
--     _assettitle,
--     _assettype,
--     _form_id,
--     _form_handler_id
-- )


-- CREATE OR REPLACE TABLE `dummy_table.dummy_thunder_email`
-- PARTITION BY DATE(_timestamp)
-- CLUSTER BY _engagement
-- AS
WITH prospect_info AS (
SELECT
  CAST(id AS STRING) AS _prospectID,
  prospect.campaign_id,
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
base_email AS (
SELECT
  activity._sdc_sequence,
  CAST(activity.prospect_id AS STRING) AS _prospectID,
  CAST(activity.campaign_id AS STRING) AS _campaignID,     
  activity.created_at AS _timestamp,
  '' AS _description,
  CAST (activity.form_handler_id AS STRING) AS _form_handler_id,
  CAST (activity.form_id AS STRING) AS _form_id,
  CAST(list_email_id AS STRING) _list_email_id,
  type_name,
  type,
FROM `x-marketing.thunder_pardot.visitor_activities`  activity
),
sent_email AS (
  SELECT *,
    'Sent' AS _engagement
  FROM base_email
  WHERE type_name = 'Email'
    AND type = 6
  QUALIFY ROW_NUMBER() OVER(PARTITION BY _prospectID, _campaignID, _list_email_id
  ORDER BY _timestamp DESC) = 1
),
hardbounced_email AS (
  SELECT *,
    'Hard Bounced' AS _engagement
  FROM base_email
  WHERE type = 13
  QUALIFY ROW_NUMBER() OVER(PARTITION BY _prospectID, _campaignID, _list_email_id
  ORDER BY _timestamp DESC) = 1
),
softbounced_email AS (
  SELECT *,
    'Soft Bounced' AS _engagement
  FROM base_email
  WHERE type = 36
  QUALIFY ROW_NUMBER() OVER(PARTITION BY _prospectID, _campaignID, _list_email_id
  ORDER BY _timestamp DESC) = 1
),
opened_email AS (
  SELECT *,
    'Opened' AS _engagement
  FROM base_email
  WHERE type = 11
  QUALIFY ROW_NUMBER() OVER(PARTITION BY _prospectID, _campaignID, _list_email_id
  ORDER BY _timestamp DESC) = 1
),
unsubs_email AS (
  SELECT *,
    'Unsubscribed' AS _engagement
  FROM base_email
    WHERE type IN (12,35)
  QUALIFY ROW_NUMBER() OVER(PARTITION BY _prospectID, _campaignID, _list_email_id
  ORDER BY _timestamp DESC) = 1
),
delivered_email AS (
  WITH allbounced AS (
    SELECT * FROM hardbounced_email
    UNION ALL
    SELECT * FROM softbounced_email
  )
  SELECT sent_email.* EXCEPT (_engagement),
  'Delivered' AS _engagement
  FROM sent_email
  LEFT JOIN allbounced
  ON sent_email._prospectID = allbounced._prospectID
  AND sent_email._campaignID = allbounced._campaignID
  WHERE allbounced._prospectID IS NULL
),
clicked_email AS (
  WITH main AS (
    SELECT
      click._sdc_sequence,
      CAST(click.prospect_id AS STRING) AS _prospectID,
      CAST(TRIM(airtable._code) AS STRING) AS _lists,     
      click.created_at AS _timestamp,
      click.url AS _description,
      CAST (NULL AS STRING) AS _form_handler_id,
      CAST (NULL AS STRING) AS _form_id,
      CAST(list_email_id AS STRING) AS _list_email_id,
      '' AS type_name,
      '' AS type,
      'Clicked' AS _engagement,
      airtable._screenshot,
      airtable._assettitle,
      airtable._subject,
      airtable._assettype,
      airtable._livedate,
      airtable._asseturl,
      airtable._subscriptiontype AS _contenttype,
      airtable._landingpage
    FROM `x-marketing.thunder_pardot.email_clicks`  click
    LEFT JOIN `thunder_mysql.db_airtable_email` airtable
      ON CAST(click.list_email_id AS STRING) = airtable._emailid
    QUALIFY ROW_NUMBER() OVER(PARTITION BY click.prospect_id, click.list_email_id
    ORDER BY click.created_at DESC ) = 1
  ),
  campaign AS (
    SELECT id AS campaign_id,
    name AS _utmcampaign
    FROM `x-marketing.thunder_pardot.campaigns`
  )
  SELECT   main._sdc_sequence,
 CAST(prospect_info.campaign_id AS STRING) AS _campaignID,
 main._engagement,
 prospect_info._email,
 main._prospectID,
 main._timestamp,
 main._description,
 main._list_email_id,
prospect_info._name,
 prospect_info._phone,
prospect_info._jobtitle,
 prospect_info._company,
 prospect_info._industry,
 prospect_info._country,
 prospect_info._city,
  prospect_info._annualrevenue,
 prospect_info._employees,
 main._subject,
 main._screenshot,
 main._landingpage,
 campaign._utmcampaign,
 prospect_info._state,
 prospect_info._createddate,
 prospect_info._updateddate,
 prospect_info._crm_contact_fid,
 prospect_info._crm_lead_fid,
 main._lists,
 main._contenttype,
 prospect_info._website,
 main._asseturl,
 main._assettitle,
 main._assettype,
 main._form_id,
 main._form_handler_id
 FROM main
  LEFT JOIN prospect_info
  ON main._prospectID = prospect_info._prospectID
  LEFT JOIN campaign
  ON prospect_info.campaign_id = campaign.campaign_id

),
engagements_combined AS (
  SELECT * FROM sent_email
  UNION ALL
  SELECT * FROM hardbounced_email
  UNION ALL
  SELECT * FROM softbounced_email
  UNION ALL
  SELECT * FROM opened_email
  UNION ALL
  SELECT * FROM unsubs_email
  -- UNION ALL
  -- SELECT * FROM delivered_email
  --formfilled

),
--get campaign details
campaign_info AS (
  SELECT
    engagements_combined.*,
    campaign.name AS _utmcampaign
  FROM engagements_combined
  LEFT JOIN `x-marketing.thunder_pardot.campaigns` campaign
    ON engagements_combined._campaignID = CAST(campaign.id AS STRING)
),
-- join with prospect
engagement_with_prospect AS (
  SELECT campaign_info.*,
  prospect_info.* EXCEPT (_prospectID, campaign_id)
  FROM campaign_info
  LEFT JOIN prospect_info
    ON campaign_info._prospectID = prospect_info._prospectID 
),
engagement_with_airtable AS (
  SELECT engagement_with_prospect.*,
     airtable._screenshot,
     airtable._assettitle,
     airtable._subject,
     CAST(TRIM(airtable._code) AS STRING) AS _lists,
     airtable._assettype,
     airtable._asseturl,
     airtable._subscriptiontype AS _contenttype,
     airtable._landingpage
  FROM engagement_with_prospect
  LEFT JOIN `thunder_mysql.db_airtable_email` airtable
  ON engagement_with_prospect._list_email_id = airtable._emailid
  -- WHERE airtable._code != '0'
)
-- SELECT _lists, ARRAY_AGG(STRUCT (_utmcampaign, _email, _engagement)) AS details 
-- FROM engagement_with_airtable
-- GROUP BY _lists

-- SELECT
--     _sdc_sequence,
--     _campaignID,
--     _engagement,
--     _email,
--     _prospectID,
--     _timestamp,
--     _description,
--     _list_email_id,
--     _name,
--     _phone,
--     _jobtitle,
--     _company,
--     _industry,
--     _country,
--     _city,
--     _annualrevenue,
--     _employees,
--     _subject,
--     _screenshot,
--     _landingPage,
--     _utmcampaign,
--     _state,
--     _createddate,
--     _updateddate,
--     _crm_contact_fid,
--     _crm_lead_fid,
--     _lists,
--     _contenttype,
--     _website,
--     _asseturl,
--     _assettitle,
--     _assettype,
--     _form_id,
--     _form_handler_id,



--  FROM engagement_with_airtable
--   UNION ALL
  SELECT * FROM clicked_email
  -- WHERE
  -- type_name = 'Email'
  --   AND type = 6
    -- AND
    -- _list_email_id = '770565889'
    -- AND
    -- _campaignID = '597553'
    -- AND
    -- _prospectID ='52176647'


-- WHERE CAST(_list_email_id AS STRING) = '743650063'
-- AND _engagement = 'Delivered'
-- AND _lists != '0'
-- WHERE _utmcampaign IS NULL


-- all good except click
--

