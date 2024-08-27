TRUNCATE TABLE `x-marketing.bestpass.db_email_engagements_log`;

INSERT INTO `x-marketing.bestpass.db_email_engagements_log` (
  _sdc_sequence,
  _contentTitle,
  _timestamp,
  _prospectID,
  _description,
  _campaignID,
  _engagement,
  -- _leadid_1,
  _email,
  _name,
  _title,
  _persona,
  _function,
  _phone,
  _company,
  _industry,
  _annualrevenue,
  _city,
  _state,
  _campaign_name,
  _programID,
  _program_name
  -- _statecode,
  -- _personSource

)

WITH prospect_info AS (
  SELECT
    CAST(leads.id AS STRING) AS _prospectID,
    leads.email AS _email,
    CONCAT(leads.firstname,' ', leads.lastname) AS _name,
    leads.title AS _title,
    CAST(NULL AS STRING) AS _persona,
    CAST(NULL AS STRING) AS _function,
    -- tier_level__c AS _tier,
    leads.phone AS _phone,
    leads.company AS _company,
    -- CAST(marketo.numberofemployees AS STRING) AS _numemployees,
    leads.industry AS _industry,
    CAST (leads.annualrevenue AS STRING) AS _annualrevenue,
    leads.city AS _city,
    leads.state AS _state,
    -- leads.statecode AS _statecode,
    -- leadSource AS _personSource,
    -- createdat,
    -- email_address_domain__c AS _domain
  FROM
    `x-marketing.bestpass_marketo.leads` leads
),
sent_email AS (
  SELECT 
    _sdc_sequence,  
    primary_attribute_value AS _contentTitle, 
    activitydate AS _timestamp, 
    CAST(leadid AS STRING) AS _prospectID, 
    '' AS _description,
    CAST(campaignid AS STRING) AS _campaignID,
    'Sent' AS _engagement,
  FROM `x-marketing.bestpass_marketo.activities_send_email`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value ORDER BY activitydate DESC
    ) = 1
),
delivered_email AS (
  SELECT 
    _sdc_sequence,  
    primary_attribute_value AS _contentTitle, 
    activitydate AS _timestamp, 
    CAST(leadid AS STRING) AS _leadid, 
    '' AS _description,
    CAST(campaignid AS STRING) AS _campaignID,
    'Delivered' AS _engagement,
  FROM `x-marketing.bestpass_marketo.activities_email_delivered`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value ORDER BY activitydate DESC
    ) = 1
),
opened_email AS (
  SELECT 
    _sdc_sequence,  
    primary_attribute_value AS _contentTitle, 
    activitydate AS _timestamp, 
    CAST(leadid AS STRING) AS _leadid, 
    '' AS _description,
    CAST(campaignid AS STRING) AS _campaignID,
    'Opened' AS _engagement,
  FROM `x-marketing.bestpass_marketo.activities_open_email`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value ORDER BY activitydate DESC
    ) = 1
),
clicked_email AS (
  SELECT 
    _sdc_sequence,  
    primary_attribute_value AS _contentTitle, 
    activitydate AS _timestamp, 
    CAST(leadid AS STRING) AS _leadid, 
    '' AS _description,
    CAST(campaignid AS STRING) AS _campaignID,
    'Clicked' AS _engagement,
  FROM `x-marketing.bestpass_marketo.activities_click_email`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value ORDER BY activitydate DESC
    ) = 1
),
bounced_email AS (
  SELECT 
    _sdc_sequence,  
    primary_attribute_value AS _contentTitle, 
    activitydate AS _timestamp, 
    CAST(leadid AS STRING) AS _leadid, 
    '' AS _description,
    CAST(campaignid AS STRING) AS _campaignID,
    'Bounced' AS _engagement,
  FROM `x-marketing.bestpass_marketo.activities_email_bounced`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value ORDER BY activitydate DESC
    ) = 1
),
unsubscribed_email AS (
  SELECT 
    _sdc_sequence,  
    primary_attribute_value AS _contentTitle, 
    activitydate AS _timestamp, 
    CAST(leadid AS STRING) AS _leadid, 
    '' AS _description,
    CAST(campaignid AS STRING) AS _campaignID,
    'Unsubscribed' AS _engagement,
  FROM `x-marketing.bestpass_marketo.activities_unsubscribe_email`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value ORDER BY activitydate DESC
    ) = 1
),
campaign AS (
  SELECT
    CAST(id AS STRING) AS id,
    name AS _campaign_name,
    CAST(programid AS STRING) AS _programID,
    programname AS _program_name
  FROM `x-marketing.bestpass_marketo.campaigns`
),
engagements_combined AS (
  SELECT * FROM sent_email
  UNION ALL
  SELECT * FROM delivered_email
  UNION ALL
  SELECT * FROM opened_email
  UNION ALL
  SELECT * FROM clicked_email
  UNION ALL
  SELECT * FROM bounced_email
  UNION ALL
  SELECT * FROM unsubscribed_email
)
SELECT
  engagements_combined.*,
  prospect_info.* EXCEPT (_prospectID),
  campaign.* EXCEPT (id)
 FROM engagements_combined
LEFT JOIN prospect_info
ON engagements_combined._prospectID = prospect_info._prospectID
LEFT JOIN campaign
ON engagements_combined._campaignID = campaign.id
-- SELECT *
-- FROM prospect_info