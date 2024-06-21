WITH prospect_info AS (
  SELECT
    CAST(leads.id AS STRING) AS _leadid,
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
    leads.statecode AS _statecode,
    leadSource AS _personSource,
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
    CAST(leadid AS STRING) AS _leadid, 
    '' AS _description, 
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
    'Bounced' AS _engagement,
  FROM `x-marketing.bestpass_marketo.activities_email_bounced`
  QUALIFY ROW_NUMBER() OVER(PARTITION BY leadid, primary_attribute_value ORDER BY activitydate DESC
    ) = 1
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
)
SELECT * FROM engagements_combined
LEFT JOIN prospect_info
ON engagements_combined._leadid = prospect_info._leadid
-- SELECT *
-- FROM prospect_info